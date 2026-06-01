const std = @import("std");
const fs = std.fs;
const time = std.time;
const Uri = std.Uri;

const PORT = 8080;
const MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024;
const DAILY_LIMIT = 32 * 1024 * 1024 * 1024;
const CHUNK_SIZE = 1024 * 1024;
const DEFAULT_RETENTION_SEC = 48 * 60 * 60;
const DATA_DIR = "/data";

fn parseTtl(ttl: []const u8) i64 {
    if (ttl.len < 2) return DEFAULT_RETENTION_SEC;
    const num = std.fmt.parseInt(i64, ttl[0 .. ttl.len - 1], 10) catch return DEFAULT_RETENTION_SEC;
    const unit = ttl[ttl.len - 1];
    return switch (unit) {
        'm' => num * 60,
        'h' => num * 60 * 60,
        'd' => num * 24 * 60 * 60,
        'w' => num * 7 * 24 * 60 * 60,
        'M' => num * 30 * 24 * 60 * 60,
        'y' => num * 365 * 24 * 60 * 60,
        else => DEFAULT_RETENTION_SEC,
    };
}

const FileMeta = struct {
    path: []const u8,
    name: []const u8,
    size: u64,
    expires: i64,
};

const IpDay = struct { ip: u32, day: i64 };

const IpDayContext = struct {
    pub fn hash(_: @This(), k: IpDay) u64 {
        return @as(u64, k.ip) << 32 | @as(u64, @intCast(k.day));
    }
    pub fn eql(_: @This(), a: IpDay, b: IpDay) bool {
        return a.ip == b.ip and a.day == b.day;
    }
};

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = gpa.allocator();
var files = std.StringHashMap(FileMeta).init(allocator);
var ip_usage = std.HashMap(IpDay, u64, IpDayContext, 80).init(allocator);
var mutex = std.Thread.Mutex{};

pub fn main() !void {
    try fs.cwd().makePath(DATA_DIR);

    const cleanup_thread = try std.Thread.spawn(.{}, cleanupTask, .{});
    _ = cleanup_thread;

    var server = std.net.StreamServer.init(.{ .reuse_address = true });
    try server.listen(try std.net.Address.parseIp4("0.0.0.0", PORT));

    std.log.info("Server listening on :{d}", .{PORT});

    while (true) {
        const conn = try server.accept();
        const thread = try std.Thread.spawn(.{}, handleConnection, .{conn});
        thread.detach();
    }
}

fn getIp(addr: std.net.Address) u32 {
    return @as(u32, @bitCast(addr.in.sa.addr));
}

fn readRequest(conn: std.net.StreamServer.Connection, buf: []u8) !?usize {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try conn.stream.read(buf[total..]);
        if (n == 0) break;
        total += n;
        if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n")) |_| return total;
    }
    return if (total > 0) total else null;
}

fn getHeader(headers: []const u8, name: []const u8) ?[]const u8 {
    var iter = std.mem.split(u8, headers, "\r\n");
    while (iter.next()) |line| {
        if (std.mem.startsWith(u8, line, name)) {
            return line[name.len..];
        }
    }
    return null;
}

fn handleConnection(conn: std.net.StreamServer.Connection) !void {
    defer conn.stream.close();
    const client_ip = getIp(conn.address);

    var buf: [65536]u8 = undefined;
    const n = (try readRequest(conn, &buf)) orelse return;
    if (n == 0) return;

    const request = buf[0..n];
    const end_of_header = std.mem.indexOf(u8, request, "\r\n\r\n") orelse return;
    const headers = request[0..end_of_header];
    const body_start = end_of_header + 4;

    var req_iter = std.mem.split(u8, headers, " ");
    const method = req_iter.next() orelse return;
    const path = req_iter.next() orelse return;

    if (std.mem.eql(u8, method, "GET")) {
        if (std.mem.eql(u8, path, "/")) {
            try serveIndex(conn);
        } else if (std.mem.eql(u8, path, "/a.png")) {
            try serveFavicon(conn);
        } else if (std.mem.startsWith(u8, path, "/s/")) {
            try serveFile(conn, path[3..]);
        } else {
            try send404(conn);
        }
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/chunk")) {
        try handleChunk(conn, headers, request, body_start, n, client_ip);
    } else {
        try send404(conn);
    }
}

fn serveIndex(conn: std.net.StreamServer.Connection) !void {
    const html = @embedFile("index.html");
    const response = try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{html.len});
    defer allocator.free(response);
    try conn.stream.writeAll(response);
    try conn.stream.writeAll(html);
}

fn serveFavicon(conn: std.net.StreamServer.Connection) !void {
    const png = @embedFile("a.png");
    const response = try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{png.len});
    defer allocator.free(response);
    try conn.stream.writeAll(response);
    try conn.stream.writeAll(png);
}

fn serveFile(conn: std.net.StreamServer.Connection, id: []const u8) !void {
    mutex.lock();
    const meta = files.get(id);
    mutex.unlock();

    if (meta == null) return send404(conn);
    const m = meta.?;

    if (time.timestamp() > m.expires) {
        mutex.lock();
        _ = files.remove(id);
        mutex.unlock();
        fs.cwd().deleteFile(m.path) catch {};
        const folder_path = fs.path.dirname(m.path);
        if (folder_path) |p| {
            fs.cwd().deleteDir(p) catch {};
        }
        return send404(conn);
    }

    const file = fs.cwd().openFile(m.path, .{}) catch return send404(conn);
    defer file.close();
    const stat = try file.stat();

    const header = try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"{s}\"\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{ m.name, stat.size });
    defer allocator.free(header);
    try conn.stream.writeAll(header);

    var buf: [8192]u8 = undefined;
    while (true) {
        const bytes = try file.read(&buf);
        if (bytes == 0) break;
        try conn.stream.writeAll(buf[0..bytes]);
    }
}

fn checkLimit(ip: u32, size: u64) !bool {
    const day = @divFloor(time.timestamp(), 86400);
    const key = IpDay{ .ip = ip, .day = day };
    mutex.lock();
    const current = ip_usage.get(key) orelse 0;
    if (current + size > DAILY_LIMIT) {
        mutex.unlock();
        return false;
    }
    const gop = ip_usage.getOrPut(key) catch {
        mutex.unlock();
        return false;
    };
    if (gop.found_existing) {
        gop.value_ptr.* += size;
    } else {
        gop.key_ptr.* = key;
        gop.value_ptr.* = size;
    }
    mutex.unlock();
    return true;
}

fn handleChunk(conn: std.net.StreamServer.Connection, headers: []const u8, request: []const u8, body_start: usize, request_len: usize, ip: u32) !void {
    const content_len = blk: {
        const val = getHeader(headers, "Content-Length: ") orelse {
            std.log.warn("Missing Content-Length", .{});
            return sendError(conn, "Missing Content-Length");
        };
        break :blk std.fmt.parseInt(u64, val, 10) catch {
            std.log.warn("Invalid Content-Length: {s}", .{val});
            return sendError(conn, "Invalid Content-Length");
        };
    };

    const upload_id = blk: {
        const val = getHeader(headers, "X-Upload-Id: ") orelse {
            std.log.warn("Missing X-Upload-Id", .{});
            return sendError(conn, "Missing X-Upload-Id");
        };
        break :blk try allocator.dupe(u8, val);
    };
    defer allocator.free(upload_id);

    const chunk_idx = blk: {
        const val = getHeader(headers, "X-Chunk-Index: ") orelse "0";
        break :blk std.fmt.parseInt(u32, val, 10) catch 0;
    };

    const total_chunks = blk: {
        const val = getHeader(headers, "X-Total-Chunks: ") orelse "1";
        break :blk std.fmt.parseInt(u32, val, 10) catch 1;
    };

    const filename = blk: {
        const val = getHeader(headers, "X-Filename: ") orelse "unnamed";
        const decoded = Uri.unescapeString(allocator, val) catch {
            break :blk try allocator.dupe(u8, val);
        };
        break :blk decoded;
    };
    defer allocator.free(filename);

    const ttl = blk: {
        const val = getHeader(headers, "X-Ttl: ") orelse "48h";
        break :blk try allocator.dupe(u8, val);
    };
    defer allocator.free(ttl);

    const total_size = blk: {
        const val = getHeader(headers, "X-Total-Size: ") orelse {
            std.log.warn("Missing X-Total-Size", .{});
            return sendError(conn, "Missing X-Total-Size");
        };
        break :blk std.fmt.parseInt(u64, val, 10) catch {
            std.log.warn("Invalid X-Total-Size", .{});
            return sendError(conn, "Invalid X-Total-Size");
        };
    };

    if (total_size > MAX_FILE_SIZE) return sendError(conn, "File too large");
    if (chunk_idx == 0 and !try checkLimit(ip, total_size)) return sendError(conn, "Daily limit exceeded");

    const folder_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ DATA_DIR, upload_id });
    defer allocator.free(folder_path);
    if (chunk_idx == 0) {
        fs.cwd().makePath(folder_path) catch |err| {
            std.log.warn("Failed to create path: {any}", .{err});
            return sendError(conn, "Server error");
        };
    }

    const chunk_path = try std.fmt.allocPrint(allocator, "{s}/{s}.{d}.tmp", .{ folder_path, upload_id, chunk_idx });
    defer allocator.free(chunk_path);

    const file = fs.cwd().createFile(chunk_path, .{}) catch |err| {
        std.log.warn("Failed to create chunk file: {any}", .{err});
        return sendError(conn, "Server error");
    };
    defer file.close();

    var body_read: usize = 0;
    if (body_start < request_len) {
        const first_chunk = request[body_start..request_len];
        const to_write = @min(first_chunk.len, content_len);
        file.writeAll(first_chunk[0..to_write]) catch |err| {
            std.log.warn("Failed to write chunk: {any}", .{err});
            return sendError(conn, "Server error");
        };
        body_read = to_write;
    }

    var buf: [CHUNK_SIZE]u8 = undefined;
    while (body_read < content_len) {
        const need = @min(buf.len, content_len - body_read);
        const bytes = conn.stream.read(buf[0..need]) catch |err| {
            std.log.warn("Failed to read from connection: {any}", .{err});
            return;
        };
        if (bytes == 0) break;
        file.writeAll(buf[0..bytes]) catch |err| {
            std.log.warn("Failed to write chunk: {any}", .{err});
            return sendError(conn, "Server error");
        };
        body_read += bytes;
    }

    if (chunk_idx == total_chunks - 1) {
        const final_path = try std.fmt.allocPrint(allocator, "{s}/{s}.bin", .{ folder_path, upload_id });
        defer allocator.free(final_path);
        const final_file = fs.cwd().createFile(final_path, .{}) catch |err| {
            std.log.warn("Failed to create final file: {any}", .{err});
            return sendError(conn, "Server error");
        };
        defer final_file.close();

        var i: u32 = 0;
        while (i < total_chunks) : (i += 1) {
            const part_path = try std.fmt.allocPrint(allocator, "{s}/{s}.{d}.tmp", .{ folder_path, upload_id, i });
            defer allocator.free(part_path);
            const part = fs.cwd().openFile(part_path, .{}) catch |err| {
                std.log.warn("Missing chunk {d}: {any}", .{ i, err });
                continue;
            };
            defer part.close();
            var pbuf: [CHUNK_SIZE]u8 = undefined;
            while (true) {
                const bytes = part.read(&pbuf) catch |err| {
                    std.log.warn("Failed to read chunk {d}: {any}", .{ i, err });
                    break;
                };
                if (bytes == 0) break;
                final_file.writeAll(pbuf[0..bytes]) catch |err| {
                    std.log.warn("Failed to write to final: {any}", .{err});
                    return sendError(conn, "Server error");
                };
            }
            fs.cwd().deleteFile(part_path) catch {};
        }

        const id_copy = try allocator.dupe(u8, upload_id);
        const path_copy = try allocator.dupe(u8, final_path);

        mutex.lock();
        try files.put(id_copy, .{
            .path = path_copy,
            .name = try allocator.dupe(u8, filename),
            .size = total_size,
            .expires = time.timestamp() + parseTtl(ttl),
        });
        mutex.unlock();

        std.log.info("File uploaded: {s} ({d} bytes)", .{ filename, total_size });
    }

    const resp = if (chunk_idx == total_chunks - 1)
        try std.fmt.allocPrint(allocator, "{{\"id\":\"{s}\",\"url\":\"/s/{s}\",\"name\":\"{s}\"}}", .{ upload_id, upload_id, filename })
    else
        try std.fmt.allocPrint(allocator, "{{\"ok\":true,\"chunk\":{d}}}", .{chunk_idx});
    defer allocator.free(resp);

    const response = try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ resp.len, resp });
    defer allocator.free(response);
    try conn.stream.writeAll(response);
}

fn send404(conn: std.net.StreamServer.Connection) !void {
    const body = "Not Found";
    const response = try std.fmt.allocPrint(allocator, "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ body.len, body });
    defer allocator.free(response);
    try conn.stream.writeAll(response);
}

fn sendError(conn: std.net.StreamServer.Connection, msg: []const u8) !void {
    const response = try std.fmt.allocPrint(allocator, "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ msg.len, msg });
    defer allocator.free(response);
    try conn.stream.writeAll(response);
}

fn generateId() ![]const u8 {
    const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    var id: [8]u8 = undefined;
    var rand = std.rand.DefaultPrng.init(@intCast(time.milliTimestamp()));
    for (&id) |*c| {
        c.* = chars[rand.random().int(u8) % chars.len];
    }
    return try allocator.dupe(u8, &id);
}

fn cleanupTask() !void {
    while (true) {
        time.sleep(10 * 60 * time.ns_per_s);
        const now = time.timestamp();
        const day = @divFloor(now, 86400);

        mutex.lock();
        var to_delete = std.ArrayList([]const u8).init(allocator);
        defer to_delete.deinit();

        var iter = files.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.expires < now) try to_delete.append(entry.key_ptr.*);
        }

        for (to_delete.items) |id| {
            if (files.getEntry(id)) |entry| {
                const path_copy = try allocator.dupe(u8, entry.value_ptr.path);
                const folder = fs.path.dirname(path_copy);
                fs.cwd().deleteFile(entry.value_ptr.path) catch {};
                if (folder) |p| {
                    fs.cwd().deleteDir(p) catch {};
                }
                allocator.free(path_copy);
                allocator.free(entry.value_ptr.path);
                allocator.free(entry.value_ptr.name);
                _ = files.remove(id);
                allocator.free(id);
            }
        }

        var ip_iter = ip_usage.iterator();
        while (ip_iter.next()) |entry| {
            if (entry.key_ptr.day < day - 1) {
                _ = ip_usage.remove(entry.key_ptr.*);
            }
        }
        mutex.unlock();
    }
}

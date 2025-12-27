const std = @import("std");
const net = std.net;
const kadmin = @import("admin.zig");
const message_util = @import("message.zig");
const producer = @import("producer.zig");

fn startProducer(admin: *kadmin.KAdmin, pos: usize) !void {
    try admin.readFromProducer(pos);
}

pub fn initKAdmin() !void {
    var admin = try kadmin.KAdmin.init();
    while (true) {
        try admin.startAdminServer();
        for (admin.producer_streams_state.items, 0..) |state, i| {
            if (state != 0) {
                continue;
            }
            // Spawn a thread to read from it.
            const thread_1 = try std.Thread.spawn(.{}, startProducer, .{ @as(*kadmin.KAdmin, &admin), @as(usize, i) });
            // TODO: Join it somewhere, but it's still auto clean up upon end of stream.
            _ = thread_1;
        }
    }
}

pub fn initProducer() !void {
    const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
    const port_int = try std.fmt.parseInt(u16, port_str, 10);
    const topic_str = std.mem.span(std.os.argv[3]); // 3rd argument is the topic
    const topic_int = try std.fmt.parseInt(u32, topic_str, 10);
    var p = try producer.Producer.init(port_int, topic_int);
    try p.startProducerServer();
    // Read input from stdin and write to the producer.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    while (try readLineFromStdin(&rd)) |line| {
        try p.writeTestMessage(line);
    }
    p.close();
}

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        try initKAdmin();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "producer")) {
        try initProducer();
    } else {
        // TODO: Init other type of process
    }
}

/// Test echo function
pub fn clientConnectTCPAndEcho(port: u16) !void {
    const address = try net.Address.parseIp4("127.0.0.1", port);
    var stream = try net.tcpConnectToAddress(address);
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(&stream_read_buff);
    var stream_wr = stream.writer(&stream_write_buff);
    while (try readLineFromStdin(&rd)) |line| {
        std.debug.print("Sent to server and echo command: {s}\n", .{line});
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .ECHO = line,
        });
        // Try to read back from the stream
        if (try message_util.readMessageFromStream(&stream_rd)) |data| {
            std.debug.print("Received from server: {s}\n", .{data.R_ECHO});
        }
    }
}

pub fn readLineFromStdin(stdin_rd: *std.fs.File.Reader) !?[]u8 {
    const line = stdin_rd.interface.takeSentinel('\n') catch |err| {
        switch (err) {
            error.EndOfStream => {
                // EOF done, nothing to do.
                return null;
            },
            else => {
                return err;
            },
        }
    };
    return line;
}

pub fn readFromStream(stream_rd: *net.Stream.Reader) !?[]u8 {
    const header = try stream_rd.file_reader.interface.takeByte();
    if (header != 0) {
        const data = try stream_rd.file_reader.interface.take(header);
        return data;
    } else {
        return null;
    }
}

pub fn writeToStream(stream_wr: *net.Stream.Writer, data: []u8) !void {
    try stream_wr.interface.writeByte(@intCast(data.len)); // Send how many byte written
    try stream_wr.interface.writeAll(data);
    try stream_wr.interface.flush();
}

pub fn spawnServer() !void {
    const thread_1 = try std.Thread.spawn(.{}, startServer, .{@as(u16, 10001)});
    const thread_2 = try std.Thread.spawn(.{}, startServer, .{@as(u16, 10002)});
    std.Thread.join(thread_1);
    std.Thread.join(thread_2);
}

pub fn startServer(port: u16) !void {
    std.debug.print("Start server on port {}\n", .{port});
    const address = try net.Address.parseIp4("127.0.0.1", port);
    var server = try address.listen(.{ .reuse_address = true }); // TCP server
    const connection = try server.accept(); // Accept a connection
    // Read data from the client and output
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    // Bidirectional read/write
    var stream_rd = connection.stream.reader(&stream_read_buff);
    var stream_wr = connection.stream.writer(&stream_write_buff);
    while (true) {
        if (try readFromStream(&stream_rd)) |data| {
            std.debug.print("Receive message from client port = {}: {s}\n", .{ port, data });
            if (std.mem.eql(u8, data, "bye")) {
                // Allow exit on bye
                break;
            }
            // Send back to the client: I have seen it
            const sent_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{data});
            try writeToStream(&stream_wr, sent_data);
        }
    }
    defer server.stream.close();
}

pub fn clientConnectTCP(port: u16) !void {
    const address = try net.Address.parseIp4("127.0.0.1", port);
    var stream = try net.tcpConnectToAddress(address);
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(&stream_read_buff);
    var stream_wr = stream.writer(&stream_write_buff);
    while (try readLineFromStdin(&rd)) |line| {
        std.debug.print("Sent to server: {s}\n", .{line});
        try writeToStream(&stream_wr, line);
        if (std.mem.eql(u8, line, "bye")) {
            return;
        }
        // Try to read back from the stream
        if (try readFromStream(&stream_rd)) |data| {
            std.debug.print("Received from server: {s}\n", .{data});
        }
    }
}

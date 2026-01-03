const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const kadmin = @import("admin.zig");
const message_util = @import("message.zig");
const producer = @import("producer.zig");
const consumer = @import("consumer.zig");

pub fn startAdminServerPanic(self: *kadmin.KAdmin, io: Io, gpa: Allocator, group: *Io.Group) void {
    self.startAdminServer(io, gpa, group) catch {
        @panic("Cannot start admin server");
    };
}

pub fn initKAdmin() !void {
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    // var group = Io.Group.init;
    var admin = try kadmin.KAdmin.init();

    const gpa = std.heap.page_allocator;

    var group = Io.Group.init;
    try group.concurrent(io, startAdminServerPanic, .{ &admin, io, gpa, &group });

    group.wait(io); // Clean up everything.
}

pub fn initProducer() !void {
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
    const port_int = try std.fmt.parseInt(u16, port_str, 10);
    const topic_str = std.mem.span(std.os.argv[3]); // 3rd argument is the topic
    const topic_int = try std.fmt.parseInt(u32, topic_str, 10);
    var p = try producer.ProducerProcess.init(port_int, topic_int);
    try p.startProducerServer(io);
    // Don't read from stdin anymore! Just run forever!
    while (true) {
        try p.writeMessage(io, try std.fmt.allocPrint(std.heap.page_allocator, "Ping from {}", .{port_int}));
        try std.Io.sleep(io, .fromMilliseconds(100), .awake);
    }
    p.close();
}

pub fn initConsumer() !void {
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const port = try std.fmt.parseInt(u16, std.mem.span(std.os.argv[2]), 10); // 2nd argument is the port
    const topic = try std.fmt.parseInt(u32, std.mem.span(std.os.argv[3]), 10); // 3rd argument is the topic
    const group = try std.fmt.parseInt(u32, std.mem.span(std.os.argv[4]), 10); // 4th argument is the topic
    const sleep_mili = try std.fmt.parseInt(i64, std.mem.span(std.os.argv[5]), 10); // 5th argument is the sleep time in milli
    var c = try consumer.ConsumerProcess.init(port, topic, group);
    try c.startConsumerServer(io);
    // Always try to receive message
    while (true) {
        try std.Io.sleep(io, .fromMilliseconds(sleep_mili), .awake);
        try c.sendReadyMessage(io);
        try c.receiveMessage(io);
    }
    c.close();
}

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        try initKAdmin();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "producer")) {
        try initProducer();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "consumer")) {
        try initConsumer();
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

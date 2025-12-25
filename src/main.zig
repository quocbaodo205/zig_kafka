const std = @import("std");
const zig_kafka = @import("zig_kafka");
const net = std.net;

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        try startServer();
    } else {
        try clientConnectTCP();
    }
}

// TODO: Study how to make unix socket works. Currently only IPAddress with tcp protocol works.
pub fn startServer() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var server = try address.listen(.{ .reuse_address = true }); // TCP server
    const connection = try server.accept(); // Accept a connection
    // Read data from the client and output
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    // Bidirectional read/write
    var stream_rd = connection.stream.reader(&stream_read_buff);
    var stream_wr = connection.stream.writer(&stream_write_buff);
    while (true) {
        const header = try stream_rd.file_reader.interface.takeByte(); // TODO: End of stream error handling
        if (header != 0) {
            const data = try stream_rd.file_reader.interface.take(header);
            std.debug.print("Receive message from client: {s}\n", .{data});
            if (std.mem.eql(u8, data, "bye")) {
                // Allow exit on bye
                break;
            }
            const sent_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{data});
            // Send back to the client: I have seen it
            try stream_wr.interface.writeByte(@intCast(sent_data.len)); // Send how many byte written
            try stream_wr.interface.writeAll(sent_data);
            try stream_wr.interface.flush();
        }
    }
    defer server.stream.close();
}

pub fn clientConnectTCP() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var stream = try net.tcpConnectToAddress(address);
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(&stream_read_buff);
    var stream_wr = stream.writer(&stream_write_buff);
    // Using take delim: https://github.com/ziglang/zig/issues/25597#issuecomment-3410445340
    while (try rd.interface.takeDelimiter('\n')) |line| {
        std.debug.print("Sent to server: {s}\n", .{line});
        try stream_wr.interface.writeByte(@intCast(line.len));
        try stream_wr.interface.writeAll(line);
        try stream_wr.interface.flush();
        if (std.mem.eql(u8, line, "bye")) {
            return;
        }
        // Try to read back from the stream
        const header = try stream_rd.file_reader.interface.takeByte(); // TODO: End of stream error handling
        if (header != 0) {
            const data = try stream_rd.file_reader.interface.take(header);
            std.debug.print("Received from server: {s}\n", .{data});
        }
    }
}

test "simple test" {
    const gpa = std.testing.allocator;
    var list: std.ArrayList(i32) = .empty;
    defer list.deinit(gpa); // Try commenting this out and see if zig detects the memory leak!
    try list.append(gpa, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "fuzz example" {
    const Context = struct {
        fn testOne(context: @This(), input: []const u8) anyerror!void {
            _ = context;
            // Try passing `--fuzz` to `zig build test` and see if it manages to fail this test case!
            try std.testing.expect(!std.mem.eql(u8, "canyoufindme", input));
        }
    };
    try std.testing.fuzz(Context{}, Context.testOne, .{});
}

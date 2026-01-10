const std = @import("std");
const net = std.Io.net;
const message_util = @import("message.zig");

const ADMIN_PORT: u16 = 10000;

pub const ProducerProcess = struct {
    const Self = @This();

    topic: u32,
    port: u16,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // Local var after creating a TCP server
    server: net.Server,
    stream: net.Stream,

    pub fn init(port: u16, topic: u32) !Self {
        return Self{
            .topic = topic,
            .port = port,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .server = undefined,
            .stream = undefined,
        };
    }

    fn sendPortDataToKAdmin(self: *Self, io: std.Io) !void {
        // Connect to kadmin process
        const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
        var stream = try address.connect(io, .{ .mode = .stream });

        // Send register message to kadmin
        var stream_rd = stream.reader(io, &self.read_buffer);
        var stream_wr = stream.writer(io, &self.write_buffer);
        std.debug.print("Sent to server the port: {}, topic: {}\n", .{ self.port, self.topic });
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .P_REG = message_util.ProducerRegisterMessage{
                .topic = self.topic,
                .port = self.port,
            },
        });
        // Try to read back the response from kadmin
        if (try message_util.readMessageFromStream(&stream_rd)) |res| {
            std.debug.print("Received ACK from server: {}\n", .{res.R_P_REG});
        }
        // Stream should be closed by the kadmin, no need to close ourselve.
    }

    pub fn startProducerServer(self: *Self, io: std.Io) !void {
        // Open the server
        const address = try net.IpAddress.parseIp4("127.0.0.1", self.port);
        self.server = try address.listen(io, .{ .reuse_address = true }); // TCP server

        // If no error, then send the port to admin
        try self.sendPortDataToKAdmin(io);

        // After that accept a connection.
        self.stream = try self.server.accept(io); // Block until got a connection

        // Later, we can use the self.connection to read / write message.
    }

    /// Write the input message to the stream in the correct PCM format
    pub fn writeMessage(self: *Self, io: std.Io, message: []u8) !void {
        // Create timestamp
        const now = try std.Io.Clock.now(.real, io);
        const ts: u64 = @intCast(std.Io.Timestamp.toMilliseconds(now));
        // Init the read/write stream.
        var stream_rd = self.stream.reader(io, &self.read_buffer);
        var stream_wr = self.stream.writer(io, &self.write_buffer);
        // Write echo message
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .PCM = message_util.ProduceConsumeMessage{
                .message = message,
                .producer_port = self.port,
                .timestamp = ts,
            },
        });
        // std.debug.print("Written {s} at {}\n", .{ message, ts });
        // Read back response echo message
        if (try message_util.readMessageFromStream(&stream_rd)) |_| {
            // std.debug.print("Got back from the admin: {}\n", .{m.R_PCM});
        }
    }

    pub fn close(self: *Self) void {
        self.stream.close();
    }
};

const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");

const ADMIN_PORT: u16 = 10000;

pub const Consumer = struct {
    const Self = @This();

    topic: u32,
    port: u16,
    group_id: u32,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // Local var after creating a TCP server
    server: net.Server,
    connection: net.Server.Connection,

    pub fn init(port: u16, topic: u32, group_id: u32) !Self {
        return Self{
            .topic = topic,
            .port = port,
            .group_id = group_id,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .server = undefined,
            .connection = undefined,
        };
    }

    fn sendInitDataToKAdmin(self: *Self) !void {
        // Connect to kadmin process
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        var stream = try net.tcpConnectToAddress(address);

        // Send register message to kadmin
        var stream_rd = stream.reader(&self.read_buffer);
        var stream_wr = stream.writer(&self.write_buffer);
        std.debug.print("Sent to server the port: {}, topic: {}, group_id: {}\n", .{ self.port, self.topic, self.group_id });
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .C_REG = message_util.ConsumerRegisterMessage{
                .topic = self.topic,
                .port = self.port,
                .group_id = self.group_id,
            },
        });
        // Try to read back the response from kadmin
        if (try message_util.readMessageFromStream(&stream_rd)) |res| {
            std.debug.print("Received ACK from server: {}\n", .{res.R_C_REG});
        }
        // Stream should be closed by the kadmin, no need to close ourselve.
    }

    pub fn startConsumerServer(self: *Self) !void {
        // Open the server
        const address = try net.Address.parseIp4("127.0.0.1", self.port);
        self.server = try address.listen(.{ .reuse_address = true }); // TCP server

        // If no error, then send the port to admin
        try self.sendInitDataToKAdmin();

        // After that accept a connection.
        self.connection = try self.server.accept(); // Block until got a connection

        // Later, we can use the self.connection to read / write message.
    }

    pub fn close(self: *Self) void {
        self.server.stream.close();
    }
};

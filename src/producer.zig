const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");

const ADMIN_PORT: u16 = 10000;

pub const Producer = struct {
    const Self = @This();

    port: u16,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // Local var after creating a TCP server
    server: net.Server,
    connection: net.Server.Connection,

    pub fn init(port: u16) !Self {
        return Self{
            .port = port,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .server = undefined,
            .connection = undefined,
        };
    }

    fn sendPortDataToKAdmin(self: *Self) !void {
        // Connect to kadmin process
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        var stream = try net.tcpConnectToAddress(address);

        // Send register message to kadmin
        var stream_rd = stream.reader(&self.read_buffer);
        var stream_wr = stream.writer(&self.write_buffer);
        const port_str = try std.fmt.allocPrint(std.heap.page_allocator, "{}", .{self.port});
        std.debug.print("Sent to server the port: {s}\n", .{port_str});
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .P_REG = port_str,
        });
        // Try to read back the response from kadmin
        if (try message_util.readMessageFromStream(&stream_rd)) |res| {
            std.debug.print("Received ACK from server: {}\n", .{res.R_P_REG});
        }
        // Stream should be closed by the kadmin, no need to close ourselve.
    }

    pub fn startProducerServer(self: *Self) !void {
        // Open the server
        const address = try net.Address.parseIp4("127.0.0.1", self.port);
        self.server = try address.listen(.{ .reuse_address = true }); // TCP server

        // If no error, then send the port to admin
        try self.sendPortDataToKAdmin();

        // After that accept a connection.
        self.connection = try self.server.accept(); // Block until got a connection

        // Later, we can use the self.connection to read / write message.
    }

    pub fn writeTestMessage(self: *Self, message: []u8) !void {
        // Init the read/write stream.
        var stream_rd = self.connection.stream.reader(&self.read_buffer);
        var stream_wr = self.connection.stream.writer(&self.write_buffer);
        // Write echo message
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .ECHO = try std.fmt.allocPrint(std.heap.page_allocator, "Producer port {}, message = {s}", .{ self.port, message }),
        });
        // Read back response echo message
        if (try message_util.readMessageFromStream(&stream_rd)) |m| {
            std.debug.print("Got back from the admin: {s}\n", .{m.R_ECHO});
        }
    }

    pub fn close(self: *Self) void {
        self.server.stream.close();
    }
};

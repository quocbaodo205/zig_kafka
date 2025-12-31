const std = @import("std");
const net = std.Io.net;
const message_util = @import("message.zig");

const ADMIN_PORT: u16 = 10000;

pub const ConsumerProcess = struct {
    const Self = @This();

    topic: u32,
    port: u16,
    group_id: u32,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // Local var after creating a TCP server
    server: net.Server,
    stream: net.Stream,

    pub fn init(port: u16, topic: u32, group_id: u32) !Self {
        return Self{
            .topic = topic,
            .port = port,
            .group_id = group_id,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .server = undefined,
            .stream = undefined,
        };
    }

    fn sendInitDataToKAdmin(self: *Self, io: std.Io) !void {
        // Connect to kadmin process
        const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
        var stream = try address.connect(io, .{ .mode = .stream });

        // Send register message to kadmin
        var stream_rd = stream.reader(io, &self.read_buffer);
        var stream_wr = stream.writer(io, &self.write_buffer);
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

    pub fn startConsumerServer(self: *Self, io: std.Io) !void {
        // Open the server
        const address = try net.IpAddress.parseIp4("127.0.0.1", self.port);
        self.server = try address.listen(io, .{ .reuse_address = true }); // TCP server

        // If no error, then send the port to admin
        try self.sendInitDataToKAdmin(io);

        // After that accept a connection.
        self.stream = try self.server.accept(io); // Block until got a connection

        // Later, we can use the self.connection to read / write message.
    }

    /// Block until the kadmin accept our ready message.
    pub fn sendReadyMessage(self: *Self, io: std.Io) !void {
        // Init the read/write stream.
        var stream_rd = self.stream.reader(io, &self.read_buffer);
        var stream_wr = self.stream.writer(io, &self.write_buffer);
        // Send the ready message
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .C_RD = 0,
        });
        std.debug.print("Sent ready to kadmin for consumer on port {}\n", .{self.port});
        // Read ACK message
        if (try message_util.readMessageFromStream(&stream_rd)) |_| {
            // Debug print
            std.debug.print("Admin ack the ready\n", .{});
        }
    }

    /// Block until we received a PCM message.
    pub fn receiveMessage(self: *Self, io: std.Io) !void {
        // Init the read/write stream.
        var stream_rd = self.stream.reader(io, &self.read_buffer);
        var stream_wr = self.stream.writer(io, &self.write_buffer);
        // Read PCM message
        if (try message_util.readMessageFromStream(&stream_rd)) |message| {
            // Debug print
            std.debug.print("Receive message {s} from producer {} at ts = {}\n", .{ message.PCM.message, message.PCM.producer_port, message.PCM.timestamp });
            // Write response message
            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                .R_PCM = 0,
            });
        }
    }

    pub fn close(self: *Self) void {
        self.server.stream.close();
    }
};

pub const ConsumerData = struct {
    const Self = @This();

    port: u16,
    stream: net.Stream,
    stream_state: u8,

    pub fn new(port: u16, stream: net.Stream, stream_state: u8) Self {
        return Self{
            .port = port,
            .stream = stream,
            .stream_state = stream_state,
        };
    }
};

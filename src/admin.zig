const std = @import("std");
const net = std.net;

const ADMIN_PORT: u16 = 10000;
const MessageType = enum(u8) {
    ECHO = 1,
    // Other message type here
};

const Message = union(MessageType) {
    ECHO: []u8,
};

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

pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.Address,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
        };
    }

    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self) !void {
        // Create a server on the address and wait for a connection
        var server = try self.admin_address.listen(.{ .reuse_address = true }); // TCP server
        const connection = try server.accept(); // Block until got a connection
        defer server.stream.close(); // Close the stream after

        // Init the read/write stream.
        var stream_rd = connection.stream.reader(&self.read_buffer);
        var stream_wr = connection.stream.writer(&self.write_buffer);

        // Read and process message
        if (try readFromStream(&stream_rd)) |message| {
            const parsed_message = self.parseAdminMessage(message);
            if (parsed_message) |m| {
                if (try self.processAdminMessage(m)) |response_message| {
                    try writeToStream(&stream_wr, response_message);
                }
            } else {}
        }
    }

    /// Parse message into formatted Message
    fn parseAdminMessage(_: *Self, message: []u8) ?Message {
        switch (message[0]) {
            @intFromEnum(MessageType.ECHO) => {
                // Remove the first byte (it's type anyway)
                return Message{ .ECHO = message[1..] };
            },
            else => {
                // Do nothing here
                return null;
            },
        }
    }

    /// Parse a message sent to the admin process and call the correct processing function
    fn processAdminMessage(self: *Self, message: Message) !?[]u8 {
        switch (message) {
            MessageType.ECHO => |echo_message| {
                return try self.processEchoMessage(echo_message);
            },
        }
    }

    fn processEchoMessage(_: *Self, message: []u8) ![]u8 {
        const return_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{message});
        return return_data;
    }
};

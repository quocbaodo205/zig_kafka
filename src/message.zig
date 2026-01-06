/// Utility to read/write to message from a stream
const std = @import("std");
const Io = std.Io;
const net = Io.net;

pub const MessageType = enum(u8) {
    ECHO = 1,
    P_REG = 2, // Register a producer
    C_REG = 3, // Register a consumer
    PCM = 4, // A message from producer that needs to be sent to consumer.
    C_RD = 5, // A message say that this consumer is ready for receiving message.
    // Return message start at 100
    R_ECHO = 101,
    R_P_REG = 102,
    R_C_REG = 103,
    R_PCM = 104,
    R_C_RD = 105,
};

pub const ProducerRegisterMessage = struct {
    topic: u32,
    port: u16,

    const Self = @This();

    pub fn new(data: []u8) Self {
        // First 4 bytes is the topic
        const topic: u32 = std.mem.readInt(u32, data[0..4], .big);
        // Next 2 bytes is the port
        const port: u16 = std.mem.readInt(u16, data[4..6], .big);
        return ProducerRegisterMessage{
            .topic = topic,
            .port = port,
        };
    }

    pub fn convertToBytes(self: *const Self, buffer: []u8) ![]u8 {
        std.mem.writeInt(u32, buffer[0..4], self.topic, .big);
        std.mem.writeInt(u16, buffer[4..6], self.port, .big);
        return buffer[0..6];
    }
};

pub const ConsumerRegisterMessage = struct {
    port: u16,
    topic: u32,
    group_id: u32,

    const Self = @This();

    /// Convert from a bytes message
    pub fn new(data: []u8) Self {
        // First 4 bytes is the topic
        const topic: u32 = std.mem.readInt(u32, data[0..4], .big);
        // Next 2 bytes is the port
        const port: u16 = std.mem.readInt(u16, data[4..6], .big);
        // Next 4 bytes is the group_id
        const group_id: u32 = std.mem.readInt(u32, data[6..10], .big);
        return ConsumerRegisterMessage{
            .topic = topic,
            .port = port,
            .group_id = group_id,
        };
    }

    pub fn convertToBytes(self: *const Self, buffer: []u8) ![]u8 {
        std.mem.writeInt(u32, buffer[0..4], self.topic, .big);
        std.mem.writeInt(u16, buffer[4..6], self.port, .big);
        std.mem.writeInt(u32, buffer[6..10], self.group_id, .big);
        return buffer[0..10];
    }
};

pub const ProduceConsumeMessage = struct {
    producer_port: u16,
    timestamp: u64,
    message: []u8,

    const Self = @This();

    /// Convert from a bytes message
    pub fn new(data: []u8) Self {
        // First 2 bytes is the producer port
        const producer_port: u16 = std.mem.readInt(u16, data[0..2], .big);
        // Next 8 bytes is the timestamp in u64
        const ts: u64 = std.mem.readInt(u64, data[2..10], .big);
        // The rest is the message in []u8
        return ProduceConsumeMessage{
            .producer_port = producer_port,
            .timestamp = ts,
            .message = data[10..],
        };
    }

    pub fn convertToBytes(self: *const Self, buffer: []u8) ![]u8 {
        std.mem.writeInt(u16, buffer[0..2], self.producer_port, .big);
        std.mem.writeInt(u64, buffer[2..10], self.timestamp, .big);
        const message_len = self.message.len;
        @memcpy(buffer[10 .. 10 + message_len], self.message);
        return buffer[0 .. 10 + message_len];
    }
};

pub const Message = union(MessageType) {
    ECHO: []u8,
    P_REG: ProducerRegisterMessage,
    C_REG: ConsumerRegisterMessage,
    PCM: ProduceConsumeMessage,
    C_RD: u8, // Just a byte
    R_ECHO: []u8, // Echo back the message
    R_P_REG: u8, // Just return a number as ack
    R_C_REG: u8, // Just return a number as ack
    R_PCM: u8, // Just return a number as ack upon sent / receive
    R_C_RD: u8, // Just ack
};

pub const message_future =
    Io.Future(@typeInfo(@typeInfo(@TypeOf(readMessageFromStream)).@"fn".return_type.?).error_union.error_set!?Message);

pub fn parseMessage(message: []u8) ?Message {
    switch (message[0]) {
        @intFromEnum(MessageType.ECHO) => {
            return Message{ .ECHO = message[1..] };
        },
        @intFromEnum(MessageType.R_ECHO) => {
            return Message{ .R_ECHO = message[1..] };
        },
        @intFromEnum(MessageType.P_REG) => {
            return Message{ .P_REG = ProducerRegisterMessage.new(message[1..]) };
        },
        @intFromEnum(MessageType.R_P_REG) => {
            return Message{ .R_P_REG = message[1] };
        },
        @intFromEnum(MessageType.C_REG) => {
            return Message{ .C_REG = ConsumerRegisterMessage.new(message[1..]) };
        },
        @intFromEnum(MessageType.R_C_REG) => {
            return Message{ .R_C_REG = message[1] };
        },
        @intFromEnum(MessageType.PCM) => {
            return Message{ .PCM = ProduceConsumeMessage.new(message[1..]) };
        },
        @intFromEnum(MessageType.R_PCM) => {
            return Message{ .R_PCM = message[1] };
        },
        @intFromEnum(MessageType.C_RD) => {
            return Message{ .C_RD = message[1] };
        },
        @intFromEnum(MessageType.R_C_RD) => {
            return Message{ .R_PCM = message[1] };
        },
        else => {
            // Do nothing here
            return null;
        },
    }
}

fn readFromStream(stream_rd: *net.Stream.Reader) !?[]u8 {
    const header = try stream_rd.interface.takeByte();
    if (header != 0) {
        const data = try stream_rd.interface.take(header);
        return data;
    } else {
        return null;
    }
}

/// Read a message from the stream
pub fn readMessageFromStream(stream_rd: *net.Stream.Reader) !?Message {
    const data = try readFromStream(stream_rd);
    if (data) |m| {
        return parseMessage(m);
    } else {
        return null;
    }
}

/// Put into a queue on done. This should be use in an async or concurrent.
pub fn readMessageFromStreamPutQueue(io: Io, stream_rd: *net.Stream.Reader, queue: Io.Queue(Message)) !void {
    const data = try readFromStream(stream_rd);
    if (data) |m| {
        if (parseMessage(m)) |ms| {
            try queue.putOne(io, ms);
        }
    }
}

pub fn readMessageFromStreamAsync(io: Io, stream_rd: *net.Stream.Reader) message_future {
    return io.async(readMessageFromStream, .{stream_rd});
}

fn writeDataToStreamWithType(stream_wr: *net.Stream.Writer, mtype: u8, data: []u8) !void {
    try stream_wr.interface.writeByte(@intCast(data.len + 1)); // Send how many byte written
    try stream_wr.interface.writeByte(mtype); // Send the type
    try stream_wr.interface.writeAll(data);
    try stream_wr.interface.flush();
}

/// Write a message to the stream
pub fn writeMessageToStream(stream_wr: *net.Stream.Writer, message: Message) !void {
    switch (message) {
        MessageType.ECHO => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.ECHO), data);
        },
        MessageType.R_ECHO => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_ECHO), data);
        },
        MessageType.P_REG => |rm| {
            var buf: [1024]u8 = undefined;
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.P_REG), try rm.convertToBytes(&buf));
        },
        MessageType.R_P_REG => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_P_REG), &data);
        },
        MessageType.C_REG => |rm| {
            var buf: [1024]u8 = undefined;
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.C_REG), try rm.convertToBytes(&buf));
        },
        MessageType.R_C_REG => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_C_REG), &data);
        },
        MessageType.PCM => |pcm| {
            var buf: [1024]u8 = undefined;
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.PCM), try pcm.convertToBytes(&buf));
        },
        MessageType.R_PCM => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_PCM), &data);
        },
        MessageType.C_RD => |s| {
            var buf: [1]u8 = [1]u8{s};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.C_RD), &buf);
        },
        MessageType.R_C_RD => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_C_RD), &data);
        },
    }
}

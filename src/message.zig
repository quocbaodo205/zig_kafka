/// Utility to read/write to message from a stream
const std = @import("std");
const net = std.net;

pub const MessageType = enum(u8) {
    ECHO = 1,
    P_REG = 2, // Register a producer
    // Return message start at 100
    R_ECHO = 101,
    R_P_REG = 102,
};

pub const Message = union(MessageType) {
    ECHO: []u8,
    P_REG: []u8, // A string contain the port number
    R_ECHO: []u8, // Echo back the message
    R_P_REG: u8, // Just return a number as ack
};

fn parseMessage(message: []u8) ?Message {
    switch (message[0]) {
        @intFromEnum(MessageType.ECHO) => {
            return Message{ .ECHO = message[1..] };
        },
        @intFromEnum(MessageType.R_ECHO) => {
            return Message{ .R_ECHO = message[1..] };
        },
        @intFromEnum(MessageType.P_REG) => {
            return Message{ .P_REG = message[1..] };
        },
        @intFromEnum(MessageType.R_P_REG) => {
            return Message{ .R_P_REG = message[1] };
        },
        else => {
            // Do nothing here
            return null;
        },
    }
}

fn readFromStream(stream_rd: *net.Stream.Reader) !?[]u8 {
    const header = try stream_rd.file_reader.interface.takeByte();
    if (header != 0) {
        const data = try stream_rd.file_reader.interface.take(header);
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
        MessageType.P_REG => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.P_REG), data);
        },
        MessageType.R_ECHO => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_ECHO), data);
        },
        MessageType.R_P_REG => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_P_REG), &data);
        },
    }
}

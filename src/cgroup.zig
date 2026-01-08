const std = @import("std");
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;
const consumer = @import("consumer.zig");

pub const CGroup = struct {
    const Self = @This();

    group_id: u32,
    topic_id: u32,
    offset: usize,
    // Consumers
    consumers: std.ArrayList(consumer.ConsumerData),

    pub fn new(gpa: Allocator, group_id: u32, topic_id: u32, offset: usize) !Self {
        return Self{
            .group_id = group_id,
            .topic_id = topic_id,
            .offset = offset,
            // Consumers
            .consumers = try std.ArrayList(consumer.ConsumerData).initCapacity(gpa, 10),
        };
    }

    /// Add a new consumer to a consumer group with the given group ID and return the consumer group position.
    /// Assume exist (check outside not in this function)
    pub fn addConsumer(self: *Self, _: std.Io, gpa: Allocator, port: u16, stream: net.Stream) !u8 {
        std.debug.print("Added a consumer with port: {}, topic: {}, group: {}\n", .{ port, self.topic_id, self.group_id });
        try self.consumers.append(gpa, consumer.ConsumerData.new(port, self.group_id, self.topic_id, stream, 0));
        const pos = self.consumers.items.len - 1;
        // const c: *consumer.ConsumerData = &self.consumers.items[pos];
        return @intCast(pos);
    }
};

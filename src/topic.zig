const std = @import("std");
const queue = @import("queue.zig");
const CGroup = @import("cgroup.zig").CGroup;
const net = std.net;
const message_util = @import("message.zig");

pub const Topic = struct {
    const Self = @This();
    const QueueType = queue.Queue(message_util.ProduceConsumeMessage, 1000);

    topic_id: u32,
    mq: QueueType,
    cgroups: std.ArrayList(CGroup),
    cgroups_offset: std.ArrayList(usize),
    is_advancing: bool,

    pub fn new(topic_id: u32) !Self {
        return Self{
            .topic_id = topic_id,
            .mq = QueueType.new(),
            .cgroups = try std.ArrayList(CGroup).initCapacity(std.heap.page_allocator, 10),
            .cgroups_offset = try std.ArrayList(usize).initCapacity(std.heap.page_allocator, 10),
            .is_advancing = false,
        };
    }

    /// Add a new consumer group that consume messages from this topic
    pub fn addCGroup(self: *Self, cgroup: *const CGroup) !void {
        try self.cgroups.append(std.heap.page_allocator, cgroup.*);
        try self.cgroups_offset.append(std.heap.page_allocator, 0); // First offset is 0
        std.debug.print("Added a consumer group: port = {}, topic = {} with offset 0\n", .{ cgroup.consumer_ports, self.topic_id });
    }

    /// Add a new consumer to a consumer group with the given group ID.
    /// Assume exist (check outside not in this function)
    pub fn addConsumer(self: *Self, port: u16, stream: net.Stream, group_id: u32) !void {
        for (self.cgroups.items) |*cg| {
            if (cg.group_id == group_id) {
                std.debug.print("Added a consumer with port: {}, topic: {}, group: {}\n", .{ port, self.topic_id, group_id });
                try cg.consumer_ports.append(std.heap.page_allocator, port);
                try cg.consumer_streams.append(std.heap.page_allocator, stream);
                try cg.consumer_streams_state.append(std.heap.page_allocator, 0);
                return;
            }
        }
    }

    /// Push a new message to be consumed
    pub fn addMessage(self: *Self, message: *message_util.ProduceConsumeMessage) void {
        self.mq.push_back(message);
    }

    fn advanceConsumerGroup(self: *Self, offset: usize, pos: usize) !void {
        const message = self.mq.peek(offset);
        if (message == null) {
            return;
        }
        // Write message at that offset
        self.cgroups.items[pos].writeMessageToAnyConsumer(message_util.Message{ .PCM = message.? }) catch |err| {
            return err;
        };
        // Advance the offset if good.
        self.cgroups_offset.items[pos] += 1;
    }

    pub fn advanceAllConsumerGroupBlocking(self: *Self) !void {
        if (self.cgroups_offset.items.len == 0) {
            return;
        }
        for (self.cgroups_offset.items, 0..) |offset, i| {
            try self.advanceConsumerGroup(offset, i);
        }
    }
};

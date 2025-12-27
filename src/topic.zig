const std = @import("std");
const queue = @import("queue.zig");
const CGroup = @import("cgroup.zig").CGroup;
const net = std.net;

pub const Topic = struct {
    const Self = @This();
    const QueueType = queue.Queue([]u8, 10000);

    topic_id: u32,
    mq: QueueType,
    cgroups: std.ArrayList(CGroup),
    cgroups_offset: std.ArrayList(usize),

    pub fn new(topic_id: u32) !Self {
        return Self{
            .topic_id = topic_id,
            .mq = QueueType.new(),
            .cgroups = try std.ArrayList(CGroup).initCapacity(std.heap.page_allocator, 10),
            .cgroups_offset = try std.ArrayList(usize).initCapacity(std.heap.page_allocator, 10),
        };
    }

    /// Add a new consumer group that consume messages from this topic
    pub fn addCGroup(self: *Self, cgroup: *const CGroup) !void {
        try self.cgroups.append(std.heap.page_allocator, cgroup.*);
        try self.cgroups_offset.append(std.heap.page_allocator, 0); // First offset is 0
    }

    /// Add a new consumer to a consumer group with the given group ID.
    /// Assume exist (check outside not in this function)
    pub fn addConsumer(self: *Self, port: u16, stream: net.Stream, group_id: u32) !void {
        for (self.cgroups.items) |*cg| {
            if (cg.group_id == group_id) {
                try cg.consumer_ports.append(std.heap.page_allocator, port);
                try cg.consumer_streams.append(std.heap.page_allocator, stream);
                try cg.consumer_streams_state.append(std.heap.page_allocator, 0);
            }
        }
    }

    /// Push a new message to be consumed
    pub fn addMessage(self: *Self, message: []const u8) void {
        self.mq.push_back(&message);
    }
};

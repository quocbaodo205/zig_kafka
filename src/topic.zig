const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const queue = @import("queue.zig");
const CGroup = @import("cgroup.zig").CGroup;
const net = std.net;
const message_util = @import("message.zig");

pub const Topic = struct {
    const Self = @This();

    topic_id: u32,
    // Message queue
    cgroups: std.ArrayList(CGroup),
    // Sync lock
    topic_lock: Io.Mutex,
    is_terminated: bool,

    pub fn new(gpa: Allocator, topic_id: u32) !Self {
        return Self{
            .topic_id = topic_id,
            .cgroups = try std.ArrayList(CGroup).initCapacity(gpa, 10),
            .topic_lock = .init,
            .is_terminated = false,
        };
    }

    /// Add a new consumer group that consume messages from this topic.
    pub fn addNewCGroup(self: *Self, io: Io, gpa: Allocator, cgroup_id: u32) !void {
        try self.topic_lock.lock(io); // Block cgroup for adding
        defer self.topic_lock.unlock(io); // Release on exit
        try self.cgroups.append(gpa, try CGroup.new(gpa, cgroup_id, self.topic_id));
        std.debug.print("Added a consumer group: group = {}, topic = {}\n", .{ cgroup_id, self.topic_id });
    }

    /// Push a new PCM to this topic to be consumed.
    pub fn addMessage(self: *Self, io: Io, gpa: Allocator, message: *const message_util.ProduceConsumeMessage) !void {
        // Clone message and put to each consumer group.
        for (self.cgroups.items) |*cg| {
            const m_copy = try gpa.create(message_util.ProduceConsumeMessage);
            m_copy.message = try std.mem.concat(gpa, u8, &.{ "", message.message });
            m_copy.producer_port = message.producer_port;
            m_copy.timestamp = message.timestamp;
            try cg.add_message(io, m_copy);
        }
    }
};

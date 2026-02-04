const std = @import("std");
const Io = std.Io;
const ProduceConsumeMessage = @import("message.zig").ProduceConsumeMessage;
const Partition = @import("partition.zig").Partition;
const Allocator = std.mem.Allocator;
const iou = std.os.linux.IoUring;

pub const CGroup = struct {
    const Self = @This();

    group_id: u32,
    topic_id: u32,
    ring: *iou,
    bg: *iou.BufferGroup,
    // Partition management
    partitions: std.ArrayList(Partition),
    cgroup_lock: Io.Mutex,

    // Statistic
    message_added: u32,

    pub fn new(gpa: Allocator, group_id: u32, topic_id: u32) !Self {
        const ring = try gpa.create(iou);
        ring.* = try iou.init(8, 0);
        const bg = try gpa.create(iou.BufferGroup);
        bg.* = try iou.BufferGroup.init(ring, gpa, @as(u16, @intCast(group_id)), 1024, 8);
        return Self{
            .group_id = group_id,
            .topic_id = topic_id,
            .ring = ring,
            .bg = bg,
            .partitions = try std.ArrayList(Partition).initCapacity(gpa, 3),
            .cgroup_lock = Io.Mutex.init,
            .message_added = 0,
        };
    }

    /// Add a PCM to the consumer group's partitions.
    /// Follow most free partition style.
    pub fn add_message(self: *Self, io: Io, message: *ProduceConsumeMessage) !void {
        try self.cgroup_lock.lock(io);
        defer self.cgroup_lock.unlock(io);
        // First, get partition with the minimal size
        var min_i: usize = 0;
        var min_num: usize = 100000;
        for (self.partitions.items, 0..) |*p, i| {
            if (p.len() < min_num) {
                min_num = p.len();
                min_i = i;
            }
        }
        // std.debug.print("Adding message to partition at pos = {}\n", .{min_i}); // DEBUG
        // Add to the min_i
        // Lock this partition, try to add it.
        const pt = &self.partitions.items[min_i];
        try pt.partition_lock.lock(io);
        while (!pt.add(message)) {
            pt.partition_lock.unlock(io);
            // Sleep 1s to allow others to work on it.
            try io.sleep(.fromSeconds(1), .awake);
            // Lock again, repeat until can add message.
            try pt.partition_lock.lock(io);
        }
        pt.total_added += 1;
        pt.partition_lock.unlock(io);
        pt.cond.broadcast(io); // Broadcast a new message has come.
        self.message_added += 1;
        // std.debug.print("Done adding message to partition at pos = {}\n", .{min_i}); // DEBUG
    }

    pub fn deinit(self: *Self, gpa: Allocator) void {
        self.partitions.deinit(gpa);
        self.bg.deinit(gpa);
        self.ring.deinit();
        gpa.destroy(self.bg);
        gpa.destroy(self.ring);
    }
};

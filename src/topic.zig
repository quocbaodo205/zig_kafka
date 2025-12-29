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
    mq_lock: std.Thread.RwLock.Impl,
    topic_lock: std.Thread.RwLock.Impl,

    pub fn new(topic_id: u32) !Self {
        return Self{
            .topic_id = topic_id,
            .mq = QueueType.new(),
            .cgroups = try std.ArrayList(CGroup).initCapacity(std.heap.page_allocator, 10),
            .cgroups_offset = try std.ArrayList(usize).initCapacity(std.heap.page_allocator, 10),
            .is_advancing = false,
            .mq_lock = std.Thread.RwLock.Impl{},
            .topic_lock = std.Thread.RwLock.Impl{},
        };
    }

    /// Add a new consumer group that consume messages from this topic
    pub fn addCGroup(self: *Self, cgroup: *const CGroup) !void {
        self.mq_lock.lockShared(); // Block until acquire
        defer self.mq_lock.unlockShared(); // Release on exit
        self.topic_lock.lock(); // Block cgroup for adding
        defer self.topic_lock.unlockShared(); // Release on exit
        try self.cgroups.append(std.heap.page_allocator, cgroup.*);
        try self.cgroups_offset.append(std.heap.page_allocator, self.mq.pop_num); // First offset is the number of popped element.
        std.debug.print("Added a consumer group: port = {}, topic = {} with offset 0\n", .{ cgroup.consumer_ports, self.topic_id });
    }

    /// Add a new consumer to a consumer group with the given group ID and return the cgroup position.
    /// Assume exist (check outside not in this function)
    pub fn addConsumer(self: *Self, port: u16, stream: net.Stream, group_id: u32) !usize {
        for (self.cgroups.items, 0..) |*cg, i| {
            if (cg.group_id == group_id) {
                std.debug.print("Added a consumer with port: {}, topic: {}, group: {}\n", .{ port, self.topic_id, group_id });
                try cg.consumer_ports.append(std.heap.page_allocator, port);
                try cg.consumer_streams.append(std.heap.page_allocator, stream);
                try cg.consumer_streams_state.append(std.heap.page_allocator, 0);
                return i;
            }
        }
        return 0;
    }

    /// Push a new message to be consumed
    pub fn addMessage(self: *Self, message: *message_util.ProduceConsumeMessage) void {
        self.mq_lock.lock(); // Block until acquire
        self.mq.push_back(message);
        self.mq_lock.unlock(); // Release
    }

    pub fn advanceConsumerGroup(self: *Self, pos: usize) !void {
        while (true) {
            const offset = self.cgroups_offset.items[pos]; // Assume safe since only 1 thread can change the offset (this thread)
            const message = self.mq.peek(offset);
            if (message == null) {
                continue;
            }
            // Write message at that offset
            self.cgroups.items[pos].writeMessageToAnyConsumer(message_util.Message{ .PCM = message.? }) catch |err| {
                return err; // Return here since the error in unrecoverable
            };
            // Advance the offset if good.
            self.cgroups_offset.items[pos] += 1;
        }
    }

    pub fn tryPopMessage(self: *Self) !void {
        self.topic_lock.lock();
        if (self.is_advancing) {
            self.topic_lock.unlock();
            return;
        }
        self.is_advancing = true;
        self.topic_lock.unlock();
        while (true) {
            std.Thread.sleep(10 * 1000000000); // Every 10s
            self.topic_lock.lockShared(); // Need the number of consumer group to be stable
            var min_offset: usize = 1000000000;
            for (self.cgroups_offset.items) |offset| {
                min_offset = @min(min_offset, offset);
            }
            std.debug.print("Get to popping in topic {}, min_offset = {}, pop_num = {}\n", .{ self.topic_id, min_offset, self.mq.pop_num });
            self.mq_lock.lock(); // Lock to pop
            while (min_offset > self.mq.pop_num) {
                _ = self.mq.pop_front();
            }
            defer self.mq_lock.unlock();
            self.topic_lock.unlockShared(); // Unlock on done loop
        }
    }
};

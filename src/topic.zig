const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const queue = @import("queue.zig");
const CGroup = @import("cgroup.zig").CGroup;
const net = std.net;
const message_util = @import("message.zig");

pub const Topic = struct {
    const Self = @This();
    const QueueType = queue.Queue(*message_util.ProduceConsumeMessage, 1000);

    topic_id: u32,
    // Message queue
    mq: QueueType,
    cgroups: std.ArrayList(CGroup),
    // Sync lock
    mq_lock: std.Thread.RwLock.Impl,
    topic_lock: std.Thread.RwLock.Impl,

    pub fn new(gpa: Allocator, topic_id: u32) !Self {
        return Self{
            .topic_id = topic_id,
            .mq = QueueType.new(),
            .cgroups = try std.ArrayList(CGroup).initCapacity(gpa, 10),
            .mq_lock = std.Thread.RwLock.Impl{},
            .topic_lock = std.Thread.RwLock.Impl{},
        };
    }

    /// Add a new consumer group that consume messages from this topic.
    pub fn addNewCGroup(self: *Self, gpa: Allocator, cgroup_id: u32) !void {
        self.mq_lock.lockShared(); // Block until acquire
        defer self.mq_lock.unlockShared(); // Release on exit
        self.topic_lock.lock(); // Block cgroup for adding
        defer self.topic_lock.unlock(); // Release on exit
        try self.cgroups.append(gpa, try CGroup.new(cgroup_id, self.topic_id, self.mq.pop_num));
        std.debug.print("Added a consumer group: group = {}, topic = {} with offset 0\n", .{ cgroup_id, self.topic_id });
    }

    /// Push a new PCM to this topic to be consumed.
    pub fn addMessage(self: *Self, io: Io, gpa: Allocator, message: *const message_util.ProduceConsumeMessage) !void {
        self.mq_lock.lock(); // Block until acquire
        // Put all messages there.
        // You need to copy the whole pcm...
        const m_copy = try gpa.create(message_util.ProduceConsumeMessage);
        m_copy.message = try std.mem.concat(gpa, u8, &.{ "", message.message });
        m_copy.producer_port = message.producer_port;
        m_copy.timestamp = message.timestamp;
        while (!self.mq.push_back(m_copy)) {
            // Queue will fail to add if full.
            // Sleep 1s + unlock to allow somethings to work on it.
            self.mq_lock.unlock(); // Release
            std.debug.print("Unlock and sleep since cannot add...\n", .{});
            try Io.sleep(io, .fromSeconds(1), .awake);
            std.debug.print("Lock it\n", .{});
            self.mq_lock.lock(); // Block until acquire
        }
        self.mq_lock.unlock(); // Release
    }

    /// Pop mesages for this topic only.
    pub fn tryPopMessage(self: *Self, io: Io, gpa: Allocator) void {
        while (true) {
            std.Io.sleep(io, .fromSeconds(10), .awake) catch {
                return;
            }; // Every 10s
            std.debug.print("Done sleep, try to get lock and pop...\n", .{});
            self.topic_lock.lockShared(); // Need the number of consumer group to be stable
            std.debug.print("Got the topic lock\n", .{});
            var min_offset: usize = 1000000000;
            for (self.cgroups.items) |*cg| {
                min_offset = @min(min_offset, cg.offset);
            }
            std.debug.print("Get to popping in topic {}, min_offset = {}, pop_num = {}\n", .{ self.topic_id, min_offset, self.mq.pop_num });
            self.mq_lock.lock(); // Lock to pop
            if (min_offset == 1000000000) {
                self.mq_lock.unlock();
                self.topic_lock.unlockShared(); // Unlock on done loop
                continue;
            }
            while (min_offset > self.mq.pop_num) {
                if (self.mq.pop_front()) |old_data| {
                    // Clean up
                    gpa.free(old_data.message);
                    gpa.destroy(old_data);
                }
            }
            defer self.mq_lock.unlock();
            self.topic_lock.unlockShared(); // Unlock on done loop
        }
    }
};

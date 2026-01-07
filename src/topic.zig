const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const queue = @import("queue.zig");
const CGroup = @import("cgroup.zig").CGroup;
const net = std.net;
const message_util = @import("message.zig");

pub const Topic = struct {
    const Self = @This();
    const QueueType = queue.Queue(message_util.ProduceConsumeMessage, 1000);

    topic_id: u32,
    // Message queue
    mq: QueueType,
    cgroups: std.ArrayList(CGroup),
    // Sync data
    is_advancing: bool, // To check if the topic has already started advancing
    mq_lock: std.Thread.RwLock.Impl,
    topic_lock: std.Thread.RwLock.Impl,

    pub fn new(gpa: Allocator, topic_id: u32) !Self {
        return Self{
            .topic_id = topic_id,
            .mq = QueueType.new(),
            .cgroups = try std.ArrayList(CGroup).initCapacity(gpa, 10),
            .is_advancing = false,
            .mq_lock = std.Thread.RwLock.Impl{},
            .topic_lock = std.Thread.RwLock.Impl{},
        };
    }

    /// Add a new consumer group that consume messages from this topic
    pub fn addNewCGroup(self: *Self, gpa: Allocator, cgroup_id: u32, topic: u32) !void {
        self.mq_lock.lockShared(); // Block until acquire
        defer self.mq_lock.unlockShared(); // Release on exit
        self.topic_lock.lock(); // Block cgroup for adding
        defer self.topic_lock.unlockShared(); // Release on exit
        try self.cgroups.append(gpa, try CGroup.new(gpa, cgroup_id, topic, self.mq.pop_num));
        std.debug.print("Added a consumer group: port = {}, topic = {} with offset 0\n", .{ cgroup_id, self.topic_id });
    }

    /// Push a new message to be consumed
    pub fn addAsyncMessage(self: *Self, io: std.Io, message_task: *message_util.message_future) void {
        // await and parse
        const message = message_task.await(io) catch {
            self.mq_lock.unlock(); // Release
            @panic("Read error");
        };
        self.mq_lock.lock(); // Block until acquire
        switch (message.?) {
            message_util.MessageType.PCM => |*pcm| {
                while (!self.mq.push_back(pcm)) {
                    self.mq_lock.unlock(); // Release
                    std.Io.sleep(io, .fromSeconds(1), .awake) catch {
                        @panic("Cannot sleep!");
                    };
                    self.mq_lock.lock(); // Block until acquire
                }
                self.mq_lock.unlock(); // Release
            },
            else => {
                self.mq_lock.unlock(); // Release
                return;
            },
        }
    }

    /// Push new messages to be consumed
    pub fn addMessageMulti(self: *Self, io: std.Io, messages: []*message_util.ProduceConsumeMessage) void {
        self.mq_lock.lock(); // Block until acquire
        // Put all messages there.
        for (messages) |message| {
            while (!self.mq.push_back(message)) {
                self.mq_lock.unlock(); // Release
                std.Io.sleep(io, .fromSeconds(1), .awake) catch {
                    @panic("Cannot sleep!");
                };
                self.mq_lock.lock(); // Block until acquire
            }
        }
        self.mq_lock.unlock(); // Release
    }

    pub fn advanceConsumerGroup(self: *Self, io: std.Io, cgroup: *CGroup) void {
        while (true) {
            const message = self.mq.peek(cgroup.offset);
            if (message == null) {
                continue;
            }
            // Write message at that offset
            cgroup.writeMessageToAnyConsumer(io, message_util.Message{ .PCM = message.? }) catch {
                // @panic("Stream cannot be written"); // TODO: Log what?
                return;
            };
            // Advance the offset if good.
            cgroup.offset += 1;
        }
    }

    pub fn tryPopMessage(self: *Self, io: std.Io) void {
        self.topic_lock.lock();
        if (self.is_advancing) {
            self.topic_lock.unlock();
            return;
        }
        self.is_advancing = true;
        self.topic_lock.unlock();
        while (true) {
            std.Io.sleep(io, .fromSeconds(10), .awake) catch {
                return;
            }; // Every 10s
            self.topic_lock.lockShared(); // Need the number of consumer group to be stable
            var min_offset: usize = 1000000000;
            for (self.cgroups.items) |*cg| {
                min_offset = @min(min_offset, cg.offset);
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

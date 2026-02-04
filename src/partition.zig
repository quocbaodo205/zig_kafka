const std = @import("std");
const Io = std.Io;
const queue = @import("queue.zig");
const ProduceConsumeMessage = @import("message.zig").ProduceConsumeMessage;

pub const Partition = struct {
    const Self = @This();

    cond: Io.Condition,
    partition_lock: Io.Mutex,
    mq: queue.Queue(*ProduceConsumeMessage, 10000),
    total_consumed: u32,
    total_added: u32,
    is_ready: bool,

    pub fn new() Self {
        return Self{
            .cond = .init,
            .partition_lock = .init,
            .mq = queue.Queue(*ProduceConsumeMessage, 10000).new(),
            .total_consumed = 0,
            .total_added = 0,
            .is_ready = false,
        };
    }

    /// Assumption: Only 1 thread doing the add, and only 1 thread doing the get / pop
    pub fn add(self: *Self, pcm: *ProduceConsumeMessage) bool {
        return self.mq.push_back(pcm);
    }

    pub fn get(self: *Self) ?*ProduceConsumeMessage {
        return self.mq.front();
    }

    pub fn pop(self: *Self) ?*ProduceConsumeMessage {
        return self.mq.pop_front();
    }

    pub fn len(self: *Self) usize {
        return self.mq.len;
    }
};

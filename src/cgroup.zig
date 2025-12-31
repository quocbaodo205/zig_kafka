const std = @import("std");
const net = std.net;
const queue = @import("queue.zig");
const message_util = @import("message.zig");
const consumer = @import("consumer.zig");

pub const CGroup = struct {
    const Self = @This();

    group_id: u32,
    group_topic: u32,
    offset: usize,
    // Consumers
    consumers: std.ArrayList(consumer.ConsumerData),
    // Ready queue (round-robin partition)
    ready_consumer_mq: queue.Queue(*consumer.ConsumerData, 1000),
    ready_lock: std.Thread.RwLock.Impl,

    pub fn new(id: u32, topic: u32, offset: usize) !Self {
        return Self{
            .group_id = id,
            .group_topic = topic,
            .offset = offset,
            // Consumers
            .consumers = try std.ArrayList(consumer.ConsumerData).initCapacity(std.heap.page_allocator, 10),
            .ready_lock = std.Thread.RwLock.Impl{},
            .ready_consumer_mq = queue.Queue(*consumer.ConsumerData, 1000).new(),
        };
    }

    /// Add a new consumer to a consumer group with the given group ID and return the consumer group position.
    /// Assume exist (check outside not in this function)
    pub fn addConsumer(self: *Self, port: u16, stream: net.Stream) !void {
        std.debug.print("Added a consumer with port: {}, topic: {}, group: {}\n", .{ port, self.group_topic, self.group_id });
        try self.consumers.append(std.heap.page_allocator, consumer.ConsumerData.new(port, stream, 0));
        const c: *consumer.ConsumerData = &self.consumers.items[self.consumers.items.len - 1];
        // Spawn a thread to process ready message right after add.
        const th = try std.Thread.spawn(.{}, CGroup.processReadyMessageFromConsumer, .{ self, c });
        _ = th; // No need to join
    }

    pub fn processReadyMessageFromConsumer(self: *CGroup, c: *consumer.ConsumerData) !void {
        // Internal for read and write
        var read_buffer: [1024]u8 = undefined;
        var write_buffer: [1024]u8 = undefined;
        var stream_rd = c.stream.reader(&read_buffer);
        var stream_wr = c.stream.writer(&write_buffer);
        // This will block until read
        if (try message_util.readMessageFromStream(&stream_rd)) |_| {
            std.debug.print("Got a ready from consumer {}\n", .{c.port});
            self.ready_lock.lock();
            defer self.ready_lock.unlock();
            std.debug.print("Locked the ready queue\n", .{});
            self.ready_consumer_mq.push_back(&c);
            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                .R_C_RD = 0,
            });
            std.debug.print("Admin ACK the ready for consumer {}\n", .{c.port});
        }
    }

    /// Try to write a message to any consumer (block until consumed or error)
    pub fn writeMessageToAnyConsumer(self: *Self, message: message_util.Message) !void {
        var global_err: ?anyerror = null;
        // Loop forever and get the first ready in the queue
        while (true) {
            self.ready_lock.lock();
            const maybe_c = self.ready_consumer_mq.pop_front();
            if (maybe_c) |c| {
                std.debug.print("Start writing to consumer port {}\n", .{c.port});
                // Internal for write
                var write_buffer: [1024]u8 = undefined;
                var stream_wr = c.stream.writer(&write_buffer);
                message_util.writeMessageToStream(&stream_wr, message) catch |err| {
                    // Cannot write somehow
                    global_err = err;
                    c.stream_state = 1;
                    continue;
                };
                // Internal for read: Have to read back the R_PCM
                var read_buffer: [1024]u8 = undefined;
                var stream_rd = c.stream.reader(&read_buffer);
                const response = message_util.readMessageFromStream(&stream_rd) catch |err| {
                    // Cannot read back
                    global_err = err;
                    c.stream_state = 1;
                    continue;
                };
                if (response != null) {
                    if (response.?.R_PCM == 0) {
                        std.debug.print("Got a R_PCM ack back from {}\n", .{c.port});
                        self.ready_lock.unlock();
                        // Spawn a thread to process ready message again.
                        const th = try std.Thread.spawn(.{}, CGroup.processReadyMessageFromConsumer, .{ self, c });
                        _ = th; // No need to join
                        return; // Written to one of them, done.
                    }
                } else {
                    // TODO: Process error here
                    self.ready_lock.unlock();
                    return;
                }
            } else {
                self.ready_lock.unlock();
            }
        }
        return global_err.?;
    }
};

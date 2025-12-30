const std = @import("std");
const net = std.net;
const queue = @import("queue.zig");
const message_util = @import("message.zig");

pub const CGroup = struct {
    const Self = @This();

    group_id: u32,
    group_topic: u32,
    // Consumers
    consumer_ports: std.ArrayList(u16),
    consumer_streams: std.ArrayList(net.Stream),
    consumer_streams_state: std.ArrayList(u8),
    ready_consumer_mq: queue.Queue(usize, 1000),
    ready_lock: std.Thread.RwLock.Impl,

    pub fn new(id: u32, topic: u32) !Self {
        return Self{
            .group_id = id,
            .group_topic = topic,
            // Consumers
            .consumer_ports = try std.ArrayList(u16).initCapacity(std.heap.page_allocator, 10),
            .consumer_streams = try std.ArrayList(net.Stream).initCapacity(std.heap.page_allocator, 10),
            .consumer_streams_state = try std.ArrayList(u8).initCapacity(std.heap.page_allocator, 10),
            .ready_lock = std.Thread.RwLock.Impl{},
            .ready_consumer_mq = queue.Queue(usize, 1000).new(),
        };
    }

    pub fn processReadyMessageFromConsumer(self: *CGroup, c_pos: usize) !void {
        // Internal for read and write
        var read_buffer: [1024]u8 = undefined;
        var write_buffer: [1024]u8 = undefined;
        var stream_rd = self.consumer_streams.items[c_pos].reader(&read_buffer);
        var stream_wr = self.consumer_streams.items[c_pos].writer(&write_buffer);
        // This will block until read
        if (try message_util.readMessageFromStream(&stream_rd)) |_| {
            std.debug.print("Got a ready from consumer {}\n", .{self.consumer_ports.items[c_pos]});
            self.ready_lock.lock();
            defer self.ready_lock.unlock();
            std.debug.print("Locked the ready queue\n", .{});
            self.ready_consumer_mq.push_back(&c_pos);
            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                .R_C_RD = 0,
            });
            std.debug.print("Admin ACK the ready for consumer {}\n", .{self.consumer_ports.items[c_pos]});
        }
    }

    /// Try to write a message to any consumer (block until consumed or error)
    pub fn writeMessageToAnyConsumer(self: *Self, message: message_util.Message) !void {
        var global_err: ?anyerror = null;
        // Loop forever and get the first ready in the queue
        while (true) {
            self.ready_lock.lock();
            const maybe_p = self.ready_consumer_mq.pop_front();
            if (maybe_p) |i| {
                std.debug.print("Start writing to consumer at pos {}\n", .{i});
                // Internal for write
                var write_buffer: [1024]u8 = undefined;
                var stream_wr = self.consumer_streams.items[i].writer(&write_buffer);
                message_util.writeMessageToStream(&stream_wr, message) catch |err| {
                    // Cannot write somehow
                    global_err = err;
                    self.consumer_streams_state.items[i] = 1;
                    continue;
                };
                // Internal for read: Have to read back the R_PCM
                var read_buffer: [1024]u8 = undefined;
                var stream_rd = self.consumer_streams.items[i].reader(&read_buffer);
                const response = message_util.readMessageFromStream(&stream_rd) catch |err| {
                    // Cannot read back
                    global_err = err;
                    self.consumer_streams_state.items[i] = 1;
                    continue;
                };
                if (response != null) {
                    if (response.?.R_PCM == 0) {
                        std.debug.print("Got a R_PCM ack back from {}\n", .{i});
                        self.ready_lock.unlock();
                        // Spawn a thread to process ready message again.
                        const th = try std.Thread.spawn(.{}, CGroup.processReadyMessageFromConsumer, .{ self, i });
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

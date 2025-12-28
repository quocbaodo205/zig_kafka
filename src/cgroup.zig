const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");

pub const CGroup = struct {
    const Self = @This();

    group_id: u32,
    group_topic: u32,
    // Consumers
    consumer_ports: std.ArrayList(u16),
    consumer_streams: std.ArrayList(net.Stream),
    consumer_streams_state: std.ArrayList(u8),

    pub fn new(id: u32, topic: u32) !Self {
        return Self{
            .group_id = id,
            .group_topic = topic,
            // Consumers
            .consumer_ports = try std.ArrayList(u16).initCapacity(std.heap.page_allocator, 10),
            .consumer_streams = try std.ArrayList(net.Stream).initCapacity(std.heap.page_allocator, 10),
            .consumer_streams_state = try std.ArrayList(u8).initCapacity(std.heap.page_allocator, 10),
        };
    }

    /// Try to write a message to any consumer (block until consumed or error)
    pub fn writeMessageToAnyConsumer(self: *Self, message: message_util.Message) !void {
        var global_err: ?anyerror = null;
        for (self.consumer_streams_state.items, 0..) |state, i| {
            if (state != 0) {
                continue; // Dead consumer
            }
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
                    return; // Written to one of them, done.
                }
            }
        }
        return global_err.?;
    }
};

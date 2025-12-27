const std = @import("std");
const net = std.net;

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
};

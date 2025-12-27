const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");
const cgroup = @import("cgroup.zig");
const topic = @import("topic.zig");

const ADMIN_PORT: u16 = 10000;

pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.Address,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // A list of topic that the admin keeps track
    topics: std.ArrayList(topic.Topic),

    // A list of producer address (port).
    producer_ports: std.ArrayList(u16),
    producer_streams: std.ArrayList(net.Stream),
    producer_streams_state: std.ArrayList(u8),
    producer_topics: std.ArrayList(u32),

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
            // Topics
            .topics = try std.ArrayList(topic.Topic).initCapacity(std.heap.page_allocator, 10),
            // Producer storage init
            .producer_ports = try std.ArrayList(u16).initCapacity(std.heap.page_allocator, 10),
            .producer_streams = try std.ArrayList(net.Stream).initCapacity(std.heap.page_allocator, 10),
            .producer_streams_state = try std.ArrayList(u8).initCapacity(std.heap.page_allocator, 10),
            .producer_topics = try std.ArrayList(u32).initCapacity(std.heap.page_allocator, 10),
        };
    }

    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self) !void {
        // Create a server on the address and wait for a connection
        var server = try self.admin_address.listen(.{ .reuse_address = true }); // TCP server
        const connection = try server.accept(); // Block until got a connection

        // Init the read/write stream.
        var stream_rd = connection.stream.reader(&self.read_buffer);
        var stream_wr = connection.stream.writer(&self.write_buffer);

        // Read and process message
        if (try message_util.readMessageFromStream(&stream_rd)) |message| {
            if (try self.processMessage(message)) |response_message| {
                try message_util.writeMessageToStream(&stream_wr, response_message);
            }
        }

        server.stream.close(); // Close the stream after
    }

    /// Read from a connected producer at index.
    pub fn readFromProducer(self: *Self, index: usize) !void {
        // Don't have to read if it's already reading or closed previously.
        if (self.producer_streams_state.items[index] != 0) {
            return;
        }
        self.producer_streams_state.items[index] = 1;
        // Use the registered stream.
        var stream_read_buff: [1024]u8 = undefined;
        var stream_write_buff: [1024]u8 = undefined;
        var stream_rd = self.producer_streams.items[index].reader(&stream_read_buff);
        var stream_wr = self.producer_streams.items[index].writer(&stream_write_buff);
        // Read from the stream: Blocking until the stream is closed.
        while (true) {
            const read_result = message_util.readMessageFromStream(&stream_rd) catch |err| {
                switch (err) {
                    error.EndOfStream => {
                        // Producer closed the stream, no need to read again.
                        break;
                    },
                    else => {
                        return err;
                    },
                }
            };
            if (read_result) |message| {
                if (try self.processMessage(message)) |response_message| {
                    try message_util.writeMessageToStream(&stream_wr, response_message);
                }
            }
        }
        std.debug.print("Producer on port {} is gone\n", .{self.producer_ports.items[index]});
    }

    /// Parse a message and call the correct processing function
    fn processMessage(self: *Self, message: message_util.Message) !?message_util.Message {
        switch (message) {
            message_util.MessageType.ECHO => |echo_message| {
                const response_data = try self.processEchoMessage(echo_message);
                return message_util.Message{
                    .R_ECHO = response_data,
                };
            },
            message_util.MessageType.P_REG => |producer_register_message| {
                const response = try self.processProducerRegisterMessage(&producer_register_message);
                return message_util.Message{
                    .R_P_REG = response,
                };
            },
            message_util.MessageType.C_REG => |consumer_register_message| {
                const response = try self.processConsumerRegisterMessage(&consumer_register_message);
                return message_util.Message{
                    .R_C_REG = response,
                };
            },
            else => {
                // TODO: Process another message.
                return null;
            },
        }
    }

    fn processEchoMessage(_: *Self, message: []u8) ![]u8 {
        const return_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{message});
        return return_data;
    }

    fn processProducerRegisterMessage(self: *Self, rm: *const message_util.ProducerRegisterMessage) !u8 {
        // Connect to the server and add a stream to the list:
        const address = try net.Address.parseIp4("127.0.0.1", rm.port);
        const stream = try net.tcpConnectToAddress(address);
        // Put into a list of producer
        try self.producer_ports.append(std.heap.page_allocator, rm.port);
        try self.producer_streams.append(std.heap.page_allocator, stream);
        try self.producer_streams_state.append(std.heap.page_allocator, 0);
        try self.producer_topics.append(std.heap.page_allocator, rm.topic);
        // Add the topic if not exist
        var topic_exist = false;
        for (self.topics.items) |tp| {
            if (tp.topic_id == rm.topic) {
                topic_exist = true;
                break;
            }
        }
        if (!topic_exist) {
            const new_topic = try topic.Topic.new(rm.topic);
            try self.topics.append(
                std.heap.page_allocator,
                new_topic,
            );
        }
        // Debug print the list of registered producer:
        std.debug.print("Registered a producer, list of producer: {any}\n", .{self.producer_ports.items});
        return 0;
    }

    fn processConsumerRegisterMessage(self: *Self, rm: *const message_util.ConsumerRegisterMessage) !u8 {
        // Check if topic exist
        var exist = false;
        var topic_pos: usize = 0;
        for (self.topics.items, 0..) |tp, i| {
            if (tp.topic_id == rm.topic) {
                topic_pos = i;
                exist = true;
                break;
            }
        }
        if (!exist) {
            return 1; // We only accept known topic.
        }
        // Connect to the server
        const address = try net.Address.parseIp4("127.0.0.1", rm.port);
        const stream = try net.tcpConnectToAddress(address);
        // Add this data to the correct consumer group.
        // Check if consumer group with this ID exist, add if not
        exist = false;
        for (self.topics.items[topic_pos].cgroups.items) |cg| {
            if (cg.group_id == rm.group_id) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            const new_group = try cgroup.CGroup.new(rm.group_id, rm.topic);
            try self.topics.items[topic_pos].addCGroup(&new_group);
        }
        // Add the port, stream and stream_state
        try self.topics.items[topic_pos].addConsumer(rm.port, stream, rm.group_id);
        std.debug.print("Added a consumer with port: {}, topic: {}, group: {}\n", .{ rm.port, rm.topic, rm.group_id });
        return 0;
    }
};

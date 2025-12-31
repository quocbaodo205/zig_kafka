const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");
const cgroup = @import("cgroup.zig");
const topic = @import("topic.zig");
const producer = @import("producer.zig");

const ADMIN_PORT: u16 = 10000;

pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.Address,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // A list of topic that the admin keeps track
    topics: std.ArrayList(topic.Topic),
    topic_threads: std.ArrayList(std.Thread),

    // A list of producer.
    producers: std.ArrayList(producer.ProducerData),
    producer_threads: std.ArrayList(std.Thread),

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
            // Topics
            .topics = try std.ArrayList(topic.Topic).initCapacity(std.heap.page_allocator, 10),
            .topic_threads = try std.ArrayList(std.Thread).initCapacity(std.heap.page_allocator, 10),
            // Producer storage init
            .producers = try std.ArrayList(producer.ProducerData).initCapacity(std.heap.page_allocator, 10),
            .producer_threads = try std.ArrayList(std.Thread).initCapacity(std.heap.page_allocator, 10),
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
            if (try self.processAdminMessage(message)) |response_message| {
                try message_util.writeMessageToStream(&stream_wr, response_message);
            }
        }

        server.stream.close(); // Close the stream after
    }

    pub fn closeAdminServer(self: *Self) void {
        self.closeAllProducer();
        self.closeAllTopic();
    }

    fn closeAllProducer(self: *Self) void {
        // Close all open producer stream
        for (self.producer_streams_state.items, 0..) |st, i| {
            if (st == 0) {
                self.producer_streams.items[i].close();
            }
        }
        // Join all current open thread.
        for (self.producer_threads.items) |th| {
            th.join();
        }
    }

    fn closeAllTopic(self: *Self) void {
        // Close all topic stream
        for (self.topics.items) |*tp| {
            for (tp.cgroups.items) |*cg| {
                for (cg.consumer_streams_state.items, 0..) |st, i| {
                    if (st == 0) {
                        cg.consumer_streams.items[i].close();
                    }
                }
            }
        }
        // Join all current open thread.
        for (self.topic_threads.items) |th| {
            th.join();
        }
    }

    /// Read from a connected producer at index.
    fn readFromProducer(self: *Self, pd: *producer.ProducerData) !void {
        // Don't have to read if it's already reading or closed previously.
        if (pd.stream_state != 0) {
            return;
        }
        std.debug.print("Start reading from producer with port {}\n", .{pd.port});
        pd.stream_state = 1;
        // Use the registered stream.
        var stream_read_buff: [1024]u8 = undefined;
        var stream_write_buff: [1024]u8 = undefined;
        var stream_rd = pd.stream.reader(&stream_read_buff);
        var stream_wr = pd.stream.writer(&stream_write_buff);
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
                if (try self.processProducerMessage(message, pd)) |response_message| {
                    try message_util.writeMessageToStream(&stream_wr, response_message);
                }
            }
        }
        std.debug.print("Producer on port {} is gone\n", .{pd.port});
    }

    fn processProducerMessage(self: *Self, message: message_util.Message, pd: *producer.ProducerData) !?message_util.Message {
        switch (message) {
            message_util.MessageType.PCM => |pcm| {
                const response = try self.processPCM(&pcm, pd);
                return message_util.Message{
                    .R_PCM = response,
                };
            },
            else => {
                // TODO: Process another message.
                return null;
            },
        }
    }

    /// Parse a message and call the correct processing function
    fn processAdminMessage(self: *Self, message: message_util.Message) !?message_util.Message {
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
        try self.producers.append(std.heap.page_allocator, producer.ProducerData.new(rm.topic, rm.port, stream, 0));
        const pd: *producer.ProducerData = &self.producers.items[self.producers.items.len - 1];
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
        std.debug.print("Registered a producer on port {}, topic {}\n", .{ rm.port, rm.topic });
        // Upon register, just start consuming in another thread.
        const thread = try std.Thread.spawn(.{}, KAdmin.readFromProducer, .{ @as(*Self, self), pd });
        try self.producer_threads.append(std.heap.page_allocator, thread);
        return 0;
    }

    fn processConsumerRegisterMessage(self: *Self, rm: *const message_util.ConsumerRegisterMessage) !u8 {
        // Check if topic exist
        var exist = false;
        var tp: *topic.Topic = undefined;
        for (self.topics.items) |*t| {
            if (t.topic_id == rm.topic) {
                tp = t;
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
        var cg: *cgroup.CGroup = undefined;
        for (tp.cgroups.items) |*c| {
            if (c.group_id == rm.group_id) {
                cg = c;
                exist = true;
                break;
            }
        }
        if (!exist) {
            try tp.addNewCGroup(rm.group_id, rm.topic);
            cg = &tp.cgroups.items[tp.cgroups.items.len - 1];
        }
        // Add the port, stream and stream_state
        try cg.addConsumer(rm.port, stream);
        // After start, can start advancing right away:
        const thread = try std.Thread.spawn(.{}, topic.Topic.advanceConsumerGroup, .{ tp, cg });
        try self.topic_threads.append(std.heap.page_allocator, thread);
        // Thread to start popping message from the topic queue
        const pop_thread = try std.Thread.spawn(.{}, topic.Topic.tryPopMessage, .{tp});
        _ = pop_thread; // TODO: join with a way to cancel somehow on exit.
        return 0;
    }

    fn processPCM(self: *Self, pcm: *const message_util.ProduceConsumeMessage, pd: *producer.ProducerData) !u8 {
        // Send this message to the correct topic
        for (self.topics.items) |*tp| {
            if (tp.topic_id == pd.topic) {
                var copyData = try std.heap.page_allocator.create(message_util.ProduceConsumeMessage);
                copyData.producer_port = pcm.producer_port;
                copyData.timestamp = pcm.timestamp;
                copyData.message = try std.heap.page_allocator.alloc(u8, pcm.message.len);
                @memcpy(copyData.message, pcm.message);
                tp.addMessage(copyData);
                return 0;
            }
        }
        return 1; // Cannot find the topic!
    }
};

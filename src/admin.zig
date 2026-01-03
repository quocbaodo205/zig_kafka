const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const message_util = @import("message.zig");
const cgroup = @import("cgroup.zig");
const topic = @import("topic.zig");
const producer = @import("producer.zig");

const ADMIN_PORT: u16 = 10000;

pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.IpAddress,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // A list of topic that the admin keeps track
    topics: std.ArrayList(topic.Topic),
    topic_threads: std.ArrayList(std.Thread),

    // A list of producer.
    producers: std.ArrayList(producer.ProducerData),

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
            // Topics
            .topics = try std.ArrayList(topic.Topic).initCapacity(std.heap.page_allocator, 10),
            .topic_threads = try std.ArrayList(std.Thread).initCapacity(std.heap.page_allocator, 10),
            // Producer storage init
            .producers = try std.ArrayList(producer.ProducerData).initCapacity(std.heap.page_allocator, 10),
        };
    }

    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self, io: Io, gpa: Allocator, group: *Io.Group) !void {
        // Create a server on the address and wait for a connection
        var server = try self.admin_address.listen(io, .{ .reuse_address = true }); // TCP server

        while (true) {
            const stream = try server.accept(io); // Block until got a connection

            // Init the read/write stream.
            var stream_rd = stream.reader(io, &self.read_buffer);
            var stream_wr = stream.writer(io, &self.write_buffer);
            // Read and process message
            if (try message_util.readMessageFromStream(&stream_rd)) |message| {
                if (try self.processAdminMessage(io, gpa, group, message)) |response_message| {
                    try message_util.writeMessageToStream(&stream_wr, response_message);
                }
            }
            stream.close(io); // Close the stream after
        }
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

    /// Parse a message and call the correct processing function
    fn processAdminMessage(self: *Self, io: Io, gpa: Allocator, group: *Io.Group, message: message_util.Message) !?message_util.Message {
        switch (message) {
            message_util.MessageType.ECHO => |echo_message| {
                const response_data = try self.processEchoMessage(echo_message);
                return message_util.Message{
                    .R_ECHO = response_data,
                };
            },
            message_util.MessageType.P_REG => |producer_register_message| {
                const response = try self.processProducerRegisterMessage(io, gpa, group, &producer_register_message);
                return message_util.Message{
                    .R_P_REG = response,
                };
            },
            message_util.MessageType.C_REG => |consumer_register_message| {
                const response = try self.processConsumerRegisterMessage(io, gpa, group, &consumer_register_message);
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

    // =========================== Producer ======================
    fn processProducerRegisterMessage(self: *Self, io: std.Io, gpa: Allocator, group: *Io.Group, rm: *const message_util.ProducerRegisterMessage) !u8 {
        // Connect to the server and add a stream to the list:
        const address = try net.IpAddress.parseIp4("127.0.0.1", rm.port);
        const stream = try address.connect(io, .{ .mode = .stream });
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
                gpa,
                new_topic,
            );
            const tp = &self.topics.items[self.topics.items.len - 1];
            // Thread to start popping message from the topic queue
            try group.concurrent(io, topic.Topic.tryPopMessage, .{ tp, io });
        }
        // Debug print the list of registered producer:
        std.debug.print("Registered a producer on port {}, topic {}\n", .{ rm.port, rm.topic });
        // Upon register, put it to the queue.
        group.concurrent(io, KAdmin.readFromProducer, .{ self, io, pd }) catch {
            @panic("Cannot start reading from producer");
        };
        // try self.producers_queue.putOne(io, pd.*);
        return 0;
    }

    /// Read from a connected producer at index.
    fn readFromProducer(self: *Self, io: std.Io, pd: *producer.ProducerData) void {
        std.debug.print("Start reading from producer with port {}\n", .{pd.port});
        // Use the registered stream.
        var stream_read_buff: [1024]u8 = undefined;
        var stream_write_buff: [1024]u8 = undefined;
        var stream_rd = pd.stream.reader(io, &stream_read_buff);
        var stream_wr = pd.stream.writer(io, &stream_write_buff);
        // Read from the stream: Blocking until the stream is closed.
        while (true) {
            var read_task = message_util.readMessageFromStreamAsync(io, &stream_rd);
            const put_res = self.processAsyncPCM(io, &read_task, pd) catch {
                @panic("Cannot process message correctly...");
            };
            message_util.writeMessageToStream(&stream_wr, message_util.Message{
                .R_PCM = put_res,
            }) catch {
                @panic("Cannot write to stream");
            };
        }
        std.debug.print("Producer on port {} is gone\n", .{pd.port});
    }

    /// Pretty sure it's gonna be PCM...
    fn processAsyncPCM(self: *Self, io: std.Io, message_task: *message_util.message_future, pd: *producer.ProducerData) !u8 {
        // Send this message to the correct topic
        for (self.topics.items) |*tp| {
            if (tp.topic_id == pd.topic) {
                tp.addAsyncMessage(io, message_task);
                return 0;
            }
        }
        return 1; // Cannot find the topic!
    }

    fn processProducerMessage(self: *Self, io: std.Io, message: message_util.Message, pd: *producer.ProducerData) !?message_util.Message {
        switch (message) {
            message_util.MessageType.PCM => |pcm| {
                const response = try self.processPCM(io, &pcm, pd);
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

    fn processPCM(self: *Self, io: std.Io, pcm: *const message_util.ProduceConsumeMessage, pd: *producer.ProducerData) !u8 {
        // Send this message to the correct topic
        for (self.topics.items) |*tp| {
            if (tp.topic_id == pd.topic) {
                var copyData = try std.heap.page_allocator.create(message_util.ProduceConsumeMessage);
                copyData.producer_port = pcm.producer_port;
                copyData.timestamp = pcm.timestamp;
                copyData.message = try std.heap.page_allocator.alloc(u8, pcm.message.len);
                @memcpy(copyData.message, pcm.message);
                tp.addMessage(io, copyData);
                return 0;
            }
        }
        return 1; // Cannot find the topic!
    }

    // ============================= Consumer ============================

    /// Return the position of the consumer in the list
    fn processConsumerRegisterMessage(self: *Self, io: Io, gpa: Allocator, group: *Io.Group, rm: *const message_util.ConsumerRegisterMessage) !u8 {
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
            return 255; // We only accept known topic.
        }
        // Connect to the server
        const address = try net.IpAddress.parseIp4("127.0.0.1", rm.port);
        const stream = try address.connect(io, .{ .mode = .stream });
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
            try tp.addNewCGroup(gpa, rm.group_id, rm.topic);
            cg = &tp.cgroups.items[tp.cgroups.items.len - 1];
            // After start, can start advancing right away:
            try group.concurrent(io, topic.Topic.advanceConsumerGroup, .{ tp, io, cg });
        }
        // Add the port, stream and stream_state
        const pos = try cg.addConsumer(io, rm.port, stream);
        return pos;
    }
};

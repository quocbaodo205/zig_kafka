const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const iou = std.os.linux.IoUring;

const message_util = @import("message.zig");
const cgroup = @import("cgroup.zig");
const topic = @import("topic.zig");
const producer = @import("producer.zig");
const consumer = @import("consumer.zig");

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

    aring: *iou,
    pring: *iou,
    cring: *iou,
    pbg: *iou.BufferGroup,
    cbg: *iou.BufferGroup,

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init(gpa: Allocator, aring: *iou, pring: *iou, cring: *iou, pbg: *iou.BufferGroup, cbg: *iou.BufferGroup) !Self {
        const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
            // Topics
            .topics = try std.ArrayList(topic.Topic).initCapacity(gpa, 10),
            .topic_threads = try std.ArrayList(std.Thread).initCapacity(gpa, 10),
            // Producer storage init
            .producers = try std.ArrayList(producer.ProducerData).initCapacity(gpa, 10),
            // Ring init
            .aring = aring,
            .pring = pring,
            .cring = cring,
            .pbg = pbg,
            .cbg = cbg,
        };
    }

    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self, io: Io, gpa: Allocator) !void {
        // Create a server on the address and wait for a connection
        var server = try self.admin_address.listen(io, .{ .reuse_address = true }); // TCP server

        // io_uring accept
        _ = try self.aring.accept_multishot(0, server.socket.handle, &null, null, 0);
        _ = try self.aring.submit();

        // Event loop to process admin accept
        // Use inline completion since these messages are supposed to be quick.
        // Also don't have to batch, these are very short and fast.
        while (true) {
            const comp_entry = try self.aring.copy_cqe();

            const err = comp_entry.err();
            if (err == .SUCCESS) {
                if (comp_entry.user_data == 10) {
                    // The correct user data. Can be anything
                    // You can use @intFromPtr and @ptrFromInt to pass in the pointer and check type.
                    const fd = comp_entry.res; // The fd for the accepted socket (read / write using it)
                    // New stream, with the accepted socket.
                    var stream = net.Stream{ .socket = net.Socket{ .handle = fd, .address = self.admin_address } };
                    // Init the read/write stream.
                    var stream_rd = stream.reader(io, &self.read_buffer);
                    var stream_wr = stream.writer(io, &self.write_buffer);
                    // Read and process message
                    if (try message_util.readMessageFromStream(&stream_rd)) |message| {
                        if (try self.processAdminMessage(io, gpa, message)) |response_message| {
                            try message_util.writeMessageToStream(&stream_wr, response_message);
                        }
                    }
                    stream.close(io); // Close the stream after

                }
            } else {
                std.debug.print("Err = {any}\n", .{err});
            }
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
    fn processAdminMessage(self: *Self, io: Io, gpa: Allocator, message: message_util.Message) !?message_util.Message {
        switch (message) {
            message_util.MessageType.ECHO => |echo_message| {
                const response_data = try self.processEchoMessage(gpa, echo_message);
                return message_util.Message{
                    .R_ECHO = response_data,
                };
            },
            message_util.MessageType.P_REG => |producer_register_message| {
                const response = try self.processProducerRegisterMessage(io, gpa, &producer_register_message);
                return message_util.Message{
                    .R_P_REG = response,
                };
            },
            message_util.MessageType.C_REG => |consumer_register_message| {
                const response = try self.processConsumerRegisterMessage(io, gpa, &consumer_register_message);
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

    fn processEchoMessage(_: *Self, gpa: Allocator, message: []u8) ![]u8 {
        const return_data = try std.fmt.allocPrint(gpa, "I have received: {s}", .{message});
        return return_data;
    }

    // =========================== Producer ======================
    fn processProducerRegisterMessage(self: *Self, io: std.Io, gpa: Allocator, rm: *const message_util.ProducerRegisterMessage) !u8 {
        // Connect to the server and add a stream to the list:
        const address = try net.IpAddress.parseIp4("127.0.0.1", rm.port);
        const stream = try address.connect(io, .{ .mode = .stream });
        // Put into a list of producer
        try self.producers.append(gpa, producer.ProducerData.new(rm.topic, rm.port, stream, 0));
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
            const new_topic = try topic.Topic.new(gpa, rm.topic);
            try self.topics.append(
                gpa,
                new_topic,
            );
            const tp = &self.topics.items[self.topics.items.len - 1];
            // Thread to start popping message from the topic queue. No need to manage.
            try std.Thread.spawn(.{}, topic.Topic.tryPopMessage, .{ tp, io });
        }
        // Debug print the list of registered producer:
        std.debug.print("Registered a producer on port {}, topic {}\n", .{ rm.port, rm.topic });
        // Upon register, put it to the queue.
        const pd_data = @intFromPtr(pd);
        _ = try self.pbg.recv_multishot(pd_data, stream.socket.handle, 0);
        _ = try self.pring.submit();
        return 0;
    }

    /// Handle all producer messages in a loop of recv_multishot.
    /// These messages are supposed to be PCM.
    pub fn handleProducersLoop(self: *Self, io: Io, gpa: Allocator) !void {
        var write_buf: [1024]u8 = undefined;
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        var port_data: [65536][]u8 = [_][]u8{undefined} * 65536;

        // Event loop (wait until completion queue has something)
        while (true) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.pring.copy_cqes(cqes, 1);
            // Recreate the full data buffer
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS) {
                    std.debug.print("Err = {any}\n", .{err});
                    continue;
                }
                // User data covert back to a ProducerData pointer, to know which producer it received from.
                var pd: *producer.ProducerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                const num_read: usize = @intCast(cqe.res);
                if (num_read == 0) {
                    pd.stream.close(io);
                    continue;
                }
                const data_full = try self.pbg.get(cqe); // Get result for this cqe
                // User the port as a way to of storage: each producer port will have a growable buffer that we can just copy data in.
                const port: usize = @intCast(pd.port);
                // Copy outside, put it back to kernel after...
                if (port_data[port] == undefined) {
                    port_data[port] = std.mem.concat(gpa, u8, .{ "", data_full });
                } else {
                    port_data[port] = std.mem.concat(gpa, u8, .{ port_data[port], data_full });
                }
                try self.pbg.put(cqe); // Give it back cuz not needed anymore.
                // Check if the message is good to be processed.
                const need_len: usize = @as(usize, @intCast(port_data[port][0])) + 1;
                if (port_data[port].len != need_len) {
                    continue;
                }
                // Good to be parsed. Ignore the length first byte.
                if (message_util.parseMessage(port_data[port][1..])) |m| {
                    switch (m) {
                        message_util.MessageType.PCM => |pcm| {
                            const res = try self.processPCM(io, &pcm, pd);
                            var wr = pd.stream.writer(io, &write_buf);
                            try message_util.writeMessageToStream(&wr, message_util.Message{
                                .R_PCM = res,
                            });
                        },
                        else => {
                            // Not supported
                            std.debug.print("Not supported message of other type\n", .{});
                        },
                    }
                }
                // Make undefined again.
                port_data[port] = undefined;
            }
        }
    }

    fn processPCM(self: *Self, io: std.Io, message: *message_util.ProduceConsumeMessage, pd: *producer.ProducerData) !u8 {
        // Send this message to the correct topic
        for (self.topics.items) |*tp| {
            if (tp.topic_id == pd.topic) {
                tp.addMessage(io, message);
                return 0;
            }
        }
        return 1; // Cannot find the topic!
    }

    // ============================= Consumer ============================

    /// Return the position of the consumer in the list
    fn processConsumerRegisterMessage(self: *Self, io: Io, gpa: Allocator, rm: *const message_util.ConsumerRegisterMessage) !u8 {
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
        }
        // Add the port, stream and stream_state
        const pos = try cg.addConsumer(io, gpa, rm.port, stream);
        const cd = &cg.consumers.items[pos];
        // Each consumer will send a ready to start receiving messages. This is just 1 byte.
        const cd_data = @intFromPtr(cd);
        _ = try self.cbg.recv_multishot(cd_data, consumer.stream.socket.handle, 0);
        _ = try self.cring.submit();
        return pos;
    }

    /// Handle all consumer messages in a loop of recv_multishot.
    /// These are expected to be C_RD only.
    pub fn handleConsumersLoop(self: *KAdmin, io: Io) !void {
        var write_buf: [1024]u8 = undefined;
        var read_buf: [1024]u8 = undefined;
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        // Event loop
        while (true) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.pring.copy_cqes(cqes, 1);
            // Recreate the full data buffer, these are just 1 bytes so no concat need.
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS) {
                    std.debug.print("Err = {any}\n", .{err});
                    continue;
                }
                var cd: *consumer.ConsumerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                // Upon receive, it's a ready for sure.
                const num_read: usize = @intCast(cqe.res);
                if (num_read == 0) {
                    cd.stream.close(io);
                    continue;
                }
                _ = try self.pbg.get(cqe); // Get result for this cqe
                try self.pbg.put(cqe); // Give it back cuz not needed anymore.
                var stream_wr = cd.stream.writer(io, &write_buf);
                // consumer is ready, first write the ack (blocking)
                try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                    .R_C_RD = 0,
                });
                // Then write one of the PCM in the topic:
                for (self.topics.items) |*tp| {
                    if (tp.topic_id != cd.topic_id) {
                        continue;
                    }
                    for (tp.cgroups.items) |*cg| {
                        if (cg.group_id != cd.group_id) {
                            continue;
                        }
                        // Topic and group is here.
                        // - Lock and peek the topic to get PCM
                        tp.mq_lock.lock();
                        if (tp.mq.peek(cg.offset)) |m| {
                            // Write message to the stream
                            try message_util.writeMessageToStream(&stream_wr, m);
                            // Read back the ack.
                            var stream_rd = cd.stream.reader(io, &read_buf);
                            if (try message_util.readMessageFromStream(&stream_rd)) |response| {
                                if (response.R_PCM == 0) {
                                    std.debug.print("Got a R_PCM ack back from {}\n", .{cd.port});
                                }
                            }
                            cg.offset += 1;
                        }
                        tp.mq_lock.unlock();
                    }
                }
            }
        }
    }
};

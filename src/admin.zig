const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const iou = std.os.linux.IoUring;

const message_util = @import("message.zig");
const cgroup = @import("cgroup.zig");
const topic = @import("topic.zig");

const ADMIN_PORT: u16 = 10000;

pub const ProducerData = struct {
    const Self = @This();

    topic: *topic.Topic,
    port: u16,
    stream: net.Stream,

    pub fn new(tp: *topic.Topic, port: u16, stream: net.Stream) Self {
        return Self{ .topic = tp, .port = port, .stream = stream };
    }
};

pub const ConsumerData = struct {
    const Self = @This();

    port: u16,
    group: *cgroup.CGroup,
    topic: *topic.Topic,
    stream: net.Stream,

    pub fn new(port: u16, group: *cgroup.CGroup, tp: *topic.Topic, stream: net.Stream) Self {
        return Self{
            .port = port,
            .stream = stream,
            .group = group,
            .topic = tp,
        };
    }
};

pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.IpAddress,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // A list of topic that the admin keeps track
    topics: std.ArrayList(topic.Topic),

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
        _ = try self.aring.accept_multishot(0, server.socket.handle, null, null, 0);
        _ = try self.aring.submit();

        // Event loop to process admin accept
        // Use inline completion since these messages are supposed to be quick.
        // Also don't have to batch, these are very short and fast.
        std.debug.print("Starting admin server...\n", .{});
        while (true) {
            const comp_entry = try self.aring.copy_cqe();

            const err = comp_entry.err();
            if (err == .SUCCESS) {
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
            } else {
                std.debug.print("Err = {any}\n", .{err});
            }
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
                std.debug.print("Consumer register message = {any}\n", .{consumer_register_message});
                try self.processConsumerRegisterMessage(io, gpa, &consumer_register_message);
                return message_util.Message{
                    .R_C_REG = 0,
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
    fn processProducerRegisterMessage(self: *Self, io: Io, gpa: Allocator, rm: *const message_util.ProducerRegisterMessage) !u8 {
        // Connect to the server and add a stream to the list:
        const address = try net.IpAddress.parseIp4("127.0.0.1", rm.port);
        const stream = try address.connect(io, .{ .mode = .stream });
        // Add the topic if not exist
        var topic_o: ?*topic.Topic = null;
        for (self.topics.items) |*t| {
            if (t.topic_id == rm.topic) {
                topic_o = t;
                break;
            }
        }
        if (topic_o == null) {
            const new_topic = try topic.Topic.new(gpa, rm.topic);
            try self.topics.append(
                gpa,
                new_topic,
            );
            topic_o = &self.topics.items[self.topics.items.len - 1];
            // Thread to start popping message from the topic queue. No need to manage.
            _ = try std.Thread.spawn(.{}, topic.Topic.tryPopMessage, .{ topic_o.?, io, gpa });
        }
        if (topic_o) |tp| {
            const pd: *ProducerData = try gpa.create(ProducerData);
            pd.* = ProducerData.new(tp, rm.port, stream);
            std.debug.print("Registered a producer on port {}, topic {}\n", .{ rm.port, rm.topic });
            // Upon register, put it to the queue.
            const pd_data = @intFromPtr(pd);
            _ = try self.pbg.recv_multishot(pd_data, stream.socket.handle, 0);
            _ = try self.pring.submit();
        }
        return 0;
    }

    /// Handle all producer messages in a loop of recv_multishot.
    /// These messages are supposed to be PCM.
    pub fn handleProducersLoop(self: *Self, io: Io, gpa: Allocator) !void {
        var write_buf: [1024]u8 = undefined;
        var cqes: [4]std.os.linux.io_uring_cqe = undefined;
        // In case messages are split, use this to collect + send.
        var port_data = try gpa.alloc(?[]u8, 65536);
        @memset(port_data, null);
        // Event loop (wait until completion queue has something)
        while (true) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.pring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS) {
                    std.debug.print("Err = {any}\n", .{err});
                    continue;
                }
                // User data covert back to a ProducerData pointer, to know which producer it received from.
                var pd: *ProducerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                const port: usize = @intCast(pd.port);
                const num_read: usize = @intCast(cqe.res);
                if (num_read == 0) {
                    // Clean up when stream is broken.
                    // TODO: Cancel the recv_multishot.
                    pd.stream.close(io);
                    gpa.destroy(pd);
                    if (port_data[port]) |sl| {
                        gpa.free(sl);
                        port_data[port] = null;
                    }
                    continue;
                }
                const data_full = try self.pbg.get(cqe); // Get result for this cqe
                // User the port as a way to of storage: each producer port will have a growable buffer that we can just copy data in.
                // Copy outside, put it back to kernel after...
                if (port_data[port]) |current_data| {
                    port_data[port] = try std.mem.concat(gpa, u8, &.{ current_data, data_full });
                } else {
                    port_data[port] = try std.mem.concat(gpa, u8, &.{ "", data_full });
                }
                try self.pbg.put(cqe); // Give it back cuz not needed anymore.
                if (port_data[port]) |cur_data| {
                    // Check if the message is good to be processed.
                    const need_len: usize = @as(usize, @intCast(cur_data[0])) + 1;
                    if (cur_data.len != need_len) {
                        continue;
                    }
                    // Good to be parsed. Ignore the length first byte.
                    if (message_util.parseMessage(cur_data[1..])) |m| {
                        switch (m) {
                            message_util.MessageType.PCM => |pcm| {
                                try pd.topic.addMessage(io, gpa, &pcm); // Copy and Block until can add.
                                var wr = pd.stream.writer(io, &write_buf);
                                try message_util.writeMessageToStream(&wr, message_util.Message{
                                    .R_PCM = 0,
                                });
                            },
                            else => {
                                // Not supported
                                std.debug.print("Not supported message of other type that PCM\n", .{});
                            },
                        }
                    }
                    // Cleanup and make undefined;
                    gpa.free(cur_data);
                    port_data[port] = null;
                }
            }
        }
    }

    // ============================= Consumer ============================

    /// Return the position of the consumer in the list
    fn processConsumerRegisterMessage(self: *Self, io: Io, gpa: Allocator, rm: *const message_util.ConsumerRegisterMessage) !void {
        // Check if topic exist
        var exist = false;
        var tp: *topic.Topic = undefined;
        for (self.topics.items) |*t| {
            if (t.topic_id == rm.topic_id) {
                tp = t;
                exist = true;
                break;
            }
        }
        if (!exist) {
            return; // We only accept known topic.
        }
        // Connect to the server
        const address = try net.IpAddress.parseIp4("127.0.0.1", rm.port);
        const stream = try address.connect(io, .{ .mode = .stream });
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
            try tp.addNewCGroup(gpa, rm.group_id);
            cg = &tp.cgroups.items[tp.cgroups.items.len - 1];
        }
        // Add the port, stream and stream_state
        const cd = try gpa.create(ConsumerData);
        cd.* = ConsumerData.new(rm.port, cg, tp, stream);
        // Each consumer will send a ready to start receiving messages. This is just 1 byte.
        const cd_data = @intFromPtr(cd);
        _ = try self.cbg.recv_multishot(cd_data, cd.stream.socket.handle, 0);
        const n = try self.cring.submit();
        std.debug.print("Send {} recv_multishot here to {}\n", .{ n, cd.port });
    }

    /// Handle all consumer messages in a loop of recv_multishot.
    /// These are expected to be C_RD and R_C_PCM (1 byte only)
    pub fn handleConsumersLoop(self: *KAdmin, io: Io, gpa: Allocator) !void {
        var write_buf: [1024]u8 = undefined;
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        // Event loop
        while (true) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.cring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer, these are just 1 bytes so no concat need.
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS) {
                    std.debug.print("Err = {any}\n", .{err});
                    continue;
                }
                var cd: *ConsumerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                // Upon receive, it's a ready for sure.
                const num_read: usize = @intCast(cqe.res);
                if (num_read == 0) {
                    cd.stream.close(io);
                    gpa.destroy(cd);
                    continue;
                }
                // Get and put back seems unneeded, but this is for clearing the buffer.
                const data = try self.cbg.get(cqe); // Get result for this cqe
                // Good to be parsed. Ignore the length first byte.
                if (message_util.parseMessage(data[0..])) |m| {
                    switch (m) {
                        message_util.MessageType.C_RD => |_| {
                            // std.debug.print("C_RD got from {}\n", .{cd.port});
                            var stream_wr = cd.stream.writer(io, &write_buf);
                            // consumer is ready, first write the ack (blocking)
                            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                                .R_C_RD = 0,
                            });
                            // Then write one of the PCM in the topic
                            cd.topic.mq_lock.lockShared();
                            // std.debug.print("Got the lock for {}\n", .{cd.port});
                            if (cd.topic.mq.peek(cd.group.offset)) |pcm| {
                                // Write message to the stream
                                try message_util.writeMessageToStream(&stream_wr, message_util.Message{ .PCM = pcm.* });
                                // std.debug.print("Written to stream for port {}\n", .{cd.port});
                                cd.group.offset += 1;
                            }
                            cd.topic.mq_lock.unlockShared();
                        },
                        message_util.MessageType.R_C_PCM => |_| {
                            // std.debug.print("R_PCM got from {}\n", .{cd.port});
                        },
                        else => {
                            // Not supported
                            std.debug.print("Not supported message of other type that PCM\n", .{});
                        },
                    }
                }

                try self.cbg.put(cqe); // Give it back cuz not needed anymore.
            }
        }
    }
};

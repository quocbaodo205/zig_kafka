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
    data_buf: ?[]u8,

    pub fn new(tp: *topic.Topic, port: u16, stream: net.Stream) Self {
        return Self{
            .topic = tp,
            .port = port,
            .stream = stream,
            .data_buf = null,
        };
    }
};

pub const ConsumerData = struct {
    const Self = @This();

    port: u16,
    group: *cgroup.CGroup,
    topic: *topic.Topic,
    stream: net.Stream,
    current_timeout: *std.os.linux.kernel_timespec,

    pub fn new(gpa: Allocator, port: u16, group: *cgroup.CGroup, tp: *topic.Topic, stream: net.Stream) !Self {
        // Create a timespec of 10ms
        const ts = try gpa.create(std.os.linux.kernel_timespec);
        ts.sec = 0;
        ts.nsec = 10000000;
        return Self{
            .port = port,
            .stream = stream,
            .group = group,
            .topic = tp,
            .current_timeout = ts,
        };
    }
};

pub const WriteData = struct {
    full_slice: []u8, // Slice, allocated inside
    fd: net.Socket.Handle,
    cur_written: usize,
    need_written: usize,
};

pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.IpAddress,
    is_terminated: bool,

    // A list of topic that the admin keeps track
    topics: std.ArrayList(topic.Topic),

    aring: *iou,
    pring: *iou,
    cring: *iou,
    wring: *iou,
    rring: *iou,
    pbg: *iou.BufferGroup,
    cbg: *iou.BufferGroup,

    // A list of all stream to clean up.
    stream_list: std.ArrayList(net.Stream),

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init(gpa: Allocator, aring: *iou, pring: *iou, cring: *iou, wring: *iou, rring: *iou, pbg: *iou.BufferGroup, cbg: *iou.BufferGroup) !Self {
        const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .is_terminated = false,
            .admin_address = address,
            // Topics
            .topics = try std.ArrayList(topic.Topic).initCapacity(gpa, 10),
            // Ring init
            .aring = aring,
            .pring = pring,
            .cring = cring,
            .wring = wring,
            .rring = rring,
            .pbg = pbg,
            .cbg = cbg,
            .stream_list = try std.ArrayList(net.Stream).initCapacity(gpa, 10),
        };
    }

    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self, io: Io, gpa: Allocator, timeout_s: i64) !void {
        var cqes: [2]std.os.linux.io_uring_cqe = undefined;
        // Create a server on the address and wait for a connection
        var server = try self.admin_address.listen(io, .{ .reuse_address = true }); // TCP server

        // io_uring accept
        _ = try self.aring.accept_multishot(0, server.socket.handle, null, null, 0);
        if (timeout_s != 0) {
            _ = try self.aring.timeout(0, &.{
                .sec = timeout_s,
                .nsec = 0,
            }, 0, 0);
        }
        _ = try self.aring.submit();

        // Read / write buffer for admin only.
        var read_buf: [1024]u8 = undefined;
        var write_buf: [1024]u8 = undefined;

        var terminated = false;

        // Event loop to process admin accept
        // Use inline completion since these messages are supposed to be quick.
        // Also don't have to batch, these are very short and fast.
        std.debug.print("Starting admin server...\n", .{});
        while (!self.is_terminated) {
            const num_ready = try self.aring.copy_cqes(&cqes, 1);
            for (cqes[0..num_ready]) |cqe| {
                const err = cqe.err();
                if (err == .TIME) {
                    if (!terminated) {
                        terminated = true;
                        _ = try self.aring.timeout(0, &.{
                            .sec = 5,
                            .nsec = 0,
                        }, 0, 0);
                        _ = try self.aring.submit();
                        continue;
                    } else {
                        std.debug.print("Shutdown all stream: {} streams\n", .{self.stream_list.items.len});
                        for (self.stream_list.items) |stream| {
                            try stream.shutdown(io, .both);
                        }
                        return;
                    }
                }
                if (err == .SUCCESS) {
                    const fd = cqe.res; // The fd for the accepted socket (read / write using it)
                    // New stream, with the accepted socket.
                    var stream = net.Stream{ .socket = net.Socket{ .handle = fd, .address = self.admin_address } };
                    // Init the read/write stream.
                    var stream_rd = stream.reader(io, &read_buf);
                    var stream_wr = stream.writer(io, &write_buf);
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
        std.debug.print("Terminated admin\n", .{});
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
            try self.stream_list.append(gpa, stream);
            // std.debug.print("Registered a producer on port {}, topic {}\n", .{ rm.port, rm.topic });
            // Upon register, put it to the queue.
            const pd_data = @intFromPtr(pd);
            _ = try self.pbg.recv_multishot(pd_data, stream.socket.handle, 0);
            _ = try self.pring.submit();
        }
        return 0;
    }

    /// Handle all producer messages in a loop of recv_multishot.
    /// These messages are supposed to be PCM.
    pub fn handleProducersLoop(self: *Self, io: Io, gpa: Allocator, timeout_s: i64) !void {
        var cqes: [4]std.os.linux.io_uring_cqe = undefined;
        if (timeout_s != 0) {
            _ = try self.pring.timeout(0, &.{
                .sec = timeout_s,
                .nsec = 0,
            }, 0, 0);
            _ = try self.pring.submit();
        }
        var terminated = false;
        // Event loop (wait until completion queue has something)
        while (!self.is_terminated) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.pring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err == .TIME) {
                    // Timeout here, done...
                    if (!terminated) {
                        terminated = true;
                        _ = try self.pring.timeout(0, &.{
                            .sec = 10,
                            .nsec = 0,
                        }, 0, 0);
                        _ = try self.pring.submit();
                        continue;
                    } else {
                        std.debug.print("Done producer loop\n", .{});
                        return;
                    }
                }
                if (err != .SUCCESS) {
                    std.debug.print("Err in producer = {any}\n", .{err});
                    continue;
                }
                // User data covert back to a ProducerData pointer, to know which producer it received from.
                var pd: *ProducerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                if (terminated) {
                    // try pd.stream.shutdown(io, .both);
                    continue;
                }
                if (cqe.flags & std.os.linux.IORING_CQE_F_MORE == 0) {
                    // On EOF, recv_multishot is automatically cancel, so we just need to free
                    std.debug.print("EOF from the producer port = {}\n", .{pd.port});
                    // pd.stream.close(io);
                    if (pd.data_buf) |sl| {
                        gpa.free(sl);
                    }
                    gpa.destroy(pd);
                    continue;
                }
                const data_full = try self.pbg.get(cqe); // Get result for this cqe
                // User the port as a way to of storage: each producer port will have a growable buffer that we can just copy data in.
                // Copy outside, put it back to kernel after...
                if (pd.data_buf) |current_data| {
                    pd.data_buf = try std.mem.concat(gpa, u8, &.{ current_data, data_full });
                } else {
                    pd.data_buf = try std.mem.concat(gpa, u8, &.{ "", data_full });
                }
                try self.pbg.put(cqe); // Give it back cuz not needed anymore.
                if (pd.data_buf) |cur_data| {
                    // Check if we have recv all message from the socket
                    if (cqe.flags & std.os.linux.IORING_CQE_F_SOCK_NONEMPTY > 0) {
                        continue;
                    }
                    // Good to be parsed. Ignore the length first byte.
                    if (message_util.parseMessage(cur_data[1..])) |m| {
                        switch (m) {
                            message_util.MessageType.PCM => |pcm| {
                                try pd.topic.addMessage(io, gpa, &pcm); // Copy and Block until can add.
                                try self.writeMessageToFD(gpa, pd.stream.socket.handle, message_util.Message{
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
                    // gpa.free(cur_data);
                    pd.data_buf = null;
                }
            }
        }
        std.debug.print("Terminated producer\n", .{});
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
        cd.* = try ConsumerData.new(gpa, rm.port, cg, tp, stream);
        try self.stream_list.append(gpa, stream);
        // Each consumer will send a ready to start receiving messages. This is just 1 byte.
        const cd_data = @intFromPtr(cd);
        _ = try self.cbg.recv_multishot(cd_data, cd.stream.socket.handle, 0);
        _ = try self.cring.submit();
        // std.debug.print("Send {} recv_multishot here to {}\n", .{ n, cd.port });
    }

    /// Retry for the consumer write PCM
    pub fn handleRetryLoop(self: *KAdmin, gpa: Allocator, timeout_s: i64) !void {
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        if (timeout_s != 0) {
            _ = try self.rring.timeout(0, &.{
                .sec = timeout_s,
                .nsec = 0,
            }, 0, 0);
            _ = try self.rring.submit();
        }
        // Event loop
        while (!self.is_terminated) {
            const num_recv = try self.rring.copy_cqes(&cqes, 1);
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                // A timeout will return .TIME
                if (err != .TIME) {
                    std.debug.print("Err in retry = {any}\n", .{err});
                    continue;
                }
                if (cqe.user_data == 0) {
                    std.debug.print("Done in retry\n", .{});
                    return;
                }
                var cd: *ConsumerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                if (cd.topic.mq.peek(cd.group.offset)) |pcm| {
                    cd.current_timeout.nsec = 10000000;
                    cd.current_timeout.sec = 0;
                    cd.topic.mq_lock.lockShared();
                    // Write message to the stream, no wait and just +1
                    try self.writeMessageToFD(gpa, cd.stream.socket.handle, message_util.Message{ .PCM = pcm.* });
                    cd.group.offset += 1;
                    cd.topic.mq_lock.unlockShared();
                } else {
                    // Double the timeout and send to the retry queue
                    if (cd.current_timeout.sec != 0) {
                        cd.current_timeout.sec *= 2;
                    } else {
                        cd.current_timeout.nsec *= 2;
                        if (cd.current_timeout.nsec > 1000000000) {
                            cd.current_timeout.sec += @divTrunc(cd.current_timeout.nsec, 1000000000);
                            cd.current_timeout.nsec = @rem(cd.current_timeout.nsec, 1000000000);
                        }
                    }
                    _ = try self.rring.timeout(cqe.user_data, cd.current_timeout, 0, 0);
                    _ = try self.rring.submit();
                }
            }
        }
        std.debug.print("Terminated retry\n", .{});
    }

    /// Handle all consumer messages in a loop of recv_multishot.
    /// These are expected to be C_RD and R_C_PCM (1 byte only)
    pub fn handleConsumersLoop(self: *KAdmin, _: Io, gpa: Allocator, timeout_s: i64) !void {
        // var write_buf: [1024]u8 = undefined;
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        if (timeout_s != 0) {
            _ = try self.cring.timeout(0, &.{
                .sec = timeout_s,
                .nsec = 0,
            }, 0, 0);
            _ = try self.cring.submit();
        }
        var terminated = false;
        // Event loop
        while (!self.is_terminated) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.cring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer, these are just 1 bytes so no concat need.
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err == .TIME) {
                    if (!terminated) {
                        terminated = true;
                        _ = try self.cring.timeout(0, &.{
                            .sec = 10,
                            .nsec = 0,
                        }, 0, 0);
                        _ = try self.cring.submit();
                        continue;
                    } else {
                        std.debug.print("Done consumer loop\n", .{});
                        return;
                    }
                }
                if (err != .SUCCESS) {
                    std.debug.print("Err handling consumer = {any}\n", .{err});
                    continue;
                }
                var cd: *ConsumerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                if (terminated) {
                    continue;
                }
                // Upon receive, it's a ready for sure.
                // const num_read: usize = @intCast(cqe.res);
                if (cqe.flags & std.os.linux.IORING_CQE_F_MORE == 0) {
                    std.debug.print("EOF from the consumer port = {}\n", .{cd.port});
                    // cd.stream.close(io);
                    gpa.destroy(cd.current_timeout);
                    gpa.destroy(cd);
                    _ = try self.rring.cancel(0, cqe.user_data, 0);
                    _ = try self.rring.submit();
                    continue;
                }
                // Get and put back seems unneeded, but this is for clearing the buffer.
                const data = try self.cbg.get(cqe); // Get result for this cqe
                // Good to be parsed. Ignore the length first byte.
                if (message_util.parseMessage(data[0..])) |m| {
                    switch (m) {
                        message_util.MessageType.R_C_PCM => |_| {
                            // Write the PCM to the consumer
                            // std.debug.print("Got the lock for {}\n", .{cd.port});
                            if (cd.topic.mq.peek(cd.group.offset)) |pcm| {
                                cd.current_timeout.nsec = 10000000;
                                cd.current_timeout.sec = 0;
                                cd.topic.mq_lock.lockShared();
                                // Write message to the stream, no wait and just +1
                                try self.writeMessageToFD(gpa, cd.stream.socket.handle, message_util.Message{ .PCM = pcm.* });
                                cd.group.offset += 1;
                                cd.topic.mq_lock.unlockShared();
                            } else {
                                // Send to the retry queue
                                _ = try self.rring.timeout(cqe.user_data, cd.current_timeout, 0, 0);
                                _ = try self.rring.submit();
                            }
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
        std.debug.print("Terminated consumer\n", .{});
    }

    // ============================ Write / Send ============================

    /// Write a message using `send` to a socket.
    /// Alloc and put the ptr to the user_data
    fn writeMessageToFD(self: *KAdmin, gpa: Allocator, fd: net.Socket.Handle, message: message_util.Message) !void {
        const buf = try gpa.alloc(u8, 1024);
        const wd = try gpa.create(WriteData);
        wd.full_slice = buf;
        switch (message) {
            message_util.MessageType.PCM => |pcm| {
                const data = try pcm.convertToBytesWithLengthAndType(buf);
                wd.fd = fd;
                wd.cur_written = 0;
                wd.need_written = data.len;
                const user_data = @intFromPtr(wd);
                _ = try self.wring.send(user_data, fd, data, 0);
                _ = try self.wring.submit();
            },
            message_util.MessageType.R_PCM => |ack| {
                buf[0] = ack;
                wd.fd = fd;
                wd.cur_written = 0;
                wd.need_written = 1;
                const user_data = @intFromPtr(wd);
                _ = try self.wring.send(user_data, fd, buf[0..1], 0);
                _ = try self.wring.submit();
            },
            else => {
                // Not supported.
            },
        }
    }

    /// Event loop to handle all write. These just return a buffer to us for now.
    /// If you need to handle different type of write, you can create a struct for it.
    pub fn handleWriteLoop(self: *KAdmin, gpa: Allocator, timeout_s: i64) !void {
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        if (timeout_s != 0) {
            _ = try self.wring.timeout(0, &.{
                .sec = timeout_s,
                .nsec = 0,
            }, 0, 0);
            _ = try self.wring.submit();
        }
        // Event loop
        while (!self.is_terminated) {
            const num_write = try self.wring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer, these are just 1 bytes so no concat need.
            for (cqes[0..num_write]) |cqe| {
                const err = cqe.err();
                if (err == .TIME) {
                    std.debug.print("Done write loop\n", .{});
                    return;
                }
                if (err != .SUCCESS) {
                    std.debug.print("Err sending data = {any}\n", .{err});
                    continue;
                }
                // Free data
                const wd: *WriteData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                wd.cur_written += @as(usize, @intCast(cqe.res));
                if (wd.cur_written < wd.need_written) {
                    std.debug.print("Written data to fd = {any}, total = {}, need = {}\n", .{ wd.fd, wd.cur_written, wd.need_written });
                    // Need to keep writing.
                    _ = try self.wring.send(cqe.user_data, wd.fd, wd.full_slice[wd.cur_written..], 0);
                    _ = try self.wring.submit();
                } else {
                    gpa.free(wd.full_slice);
                    gpa.destroy(wd);
                }
            }
        }
        std.debug.print("Terminated write\n", .{});
    }
};

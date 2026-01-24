const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const linux = std.os.linux;
const iou = linux.IoUring;

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

    // A list of topic that the admin keeps track
    topics: std.ArrayList(topic.Topic),

    aring: *iou,
    pring: *iou,
    cring: *iou,
    wring: *iou,
    pbg: *iou.BufferGroup,
    cbg: *iou.BufferGroup,

    // A list of all stream to clean up.
    stream_list: std.ArrayList(net.Stream),

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init(gpa: Allocator, aring: *iou, pring: *iou, cring: *iou, wring: *iou, pbg: *iou.BufferGroup, cbg: *iou.BufferGroup) !Self {
        const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            // Topics
            .topics = try std.ArrayList(topic.Topic).initCapacity(gpa, 10),
            // Ring init
            .aring = aring,
            .pring = pring,
            .cring = cring,
            .wring = wring,
            .pbg = pbg,
            .cbg = cbg,
            .stream_list = try std.ArrayList(net.Stream).initCapacity(gpa, 10),
        };
    }

    /// Write the current state to file as a way of persistent.
    /// Call when the state changed, like adding a new topic or adding a consumer group.
    pub fn writeOverallStateToFile(self: *Self, io: Io) !void {
        const f = try Io.Dir.createFile(Io.Dir.cwd(), io, "overall_state", .{ .read = false, .truncate = true });
        defer f.close(io); // Persist by close
        var buf: [10240]u8 = undefined; // Big buffer
        var wr = f.writer(io, &buf);
        try wr.seekTo(0); // Write at 0
        // Format: First number is the number of topic
        try wr.interface.writeInt(u16, @as(u16, @intCast(self.topics.items.len)), .big);
        for (self.topics.items) |tp| {
            // Next 2 numbers are topic id and #cgroup
            try wr.interface.writeInt(u32, tp.topic_id, .big);
            try wr.interface.writeInt(u16, @as(u16, @intCast(tp.cgroups.items.len)), .big);
            // Following is all the cgroup_id
            for (tp.cgroups.items) |cg| {
                try wr.interface.writeInt(u32, cg.group_id, .big);
            }
        }
        try wr.flush(); // Flush at once in the end
    }

    /// Read back the current overall state from disk into the `KAdmin` structure.
    /// Can be use after an init.
    pub fn readOverallStateFromFile(self: *Self, io: Io, gpa: Allocator) !void {
        const f = try Io.Dir.openFile(Io.Dir.cwd(), io, "overall_state", .{ .mode = .read_only });
        defer f.close(io);
        var buf: [10240]u8 = undefined;
        var rd = f.reader(io, &buf);
        const num_topic: u16 = try rd.interface.takeInt(u16, .big);
        // Clean up current topics
        self.topics.clearRetainingCapacity();
        for (0..num_topic) |_| {
            // Create a new topic and push it
            const topic_id: u32 = try rd.interface.takeInt(u32, .big);
            var tp = try topic.Topic.new(gpa, topic_id);
            const num_cg: u16 = try rd.interface.takeInt(u16, .big);
            for (0..num_cg) |_| {
                const group_id: u32 = try rd.interface.takeInt(u32, .big);
                // Offset = 0, will read the offset from another file
                tp.addNewCGroup(gpa, group_id);
            }
            self.topics.append(gpa, tp);
        }
    }

    /// Cancel by sending nop, upon which we will cancel the loop
    pub fn sendCancelSignal(self: *Self) !void {
        _ = try self.aring.nop(0);
        _ = try self.aring.submit();
        _ = try self.pring.nop(0);
        _ = try self.pring.submit();
        _ = try self.cring.nop(0);
        _ = try self.cring.submit();
        _ = try self.wring.nop(0);
        _ = try self.wring.submit();
        // TODO: Send cancel to each consumer groups ring
    }

    pub fn deinit(self: *Self, gpa: Allocator) void {
        self.aring.deinit();
        self.pring.deinit();
        self.cring.deinit();
        self.wring.deinit();
        self.pbg.deinit(gpa);
        self.cbg.deinit(gpa);
        self.stream_list.deinit(gpa);
        self.topics.deinit(gpa);
    }

    // ==================================== Admin handling ==============================

    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self, io: Io, group: *Io.Group, gpa: Allocator) void {
        // Anon function
        const temp = struct {
            fn isHandledTerminate(t_self: *Self, t_io: Io, cqe: linux.io_uring_cqe) !bool {
                if (cqe.user_data == 0) {
                    std.debug.print("Shutdown all stream: {} streams\n", .{t_self.stream_list.items.len});
                    for (t_self.stream_list.items) |stream| {
                        try stream.shutdown(t_io, .both);
                    }
                    // On done, also set topic states
                    for (t_self.topics.items) |*tp| {
                        tp.is_terminated = true;
                        for (tp.cgroups.items) |*cg| {
                            // Send a nop
                            _ = try cg.ring.nop(0);
                            _ = try cg.ring.submit();
                        }
                    }
                    std.debug.print("Terminated admin\n", .{});
                    return true;
                }
                return false;
            }

            pub fn startAdminServer(t_self: *Self, t_io: Io, t_group: *Io.Group, t_gpa: Allocator) !void {
                var cqes: [2]std.os.linux.io_uring_cqe = undefined;
                // Create a server on the address and wait for a connection
                var server = t_self.admin_address.listen(t_io, .{ .reuse_address = true }) catch |err| {
                    std.debug.print("Cannot listen to port {}, err = {any}\n", .{ ADMIN_PORT, err });
                    return;
                };

                // io_uring accept
                _ = try t_self.aring.accept_multishot(1, server.socket.handle, null, null, 0);
                _ = try t_self.aring.submit();

                // Read / write buffer for admin only.
                var read_buf: [1024]u8 = undefined;
                var write_buf: [1024]u8 = undefined;

                // Event loop to process admin accept
                std.debug.print("Starting admin server...\n", .{});
                while (true) {
                    const num_ready = try t_self.aring.copy_cqes(&cqes, 1);
                    for (cqes[0..num_ready]) |cqe| {
                        if (try isHandledTerminate(t_self, t_io, cqe)) {
                            return;
                        }
                        const err = cqe.err();
                        if (err == .SUCCESS) {
                            const fd = cqe.res; // The fd for the accepted socket (read / write using it)
                            // New stream, with the accepted socket.
                            var stream = net.Stream{ .socket = net.Socket{ .handle = fd, .address = t_self.admin_address } };
                            // Init the read/write stream.
                            var stream_rd = stream.reader(t_io, &read_buf);
                            var stream_wr = stream.writer(t_io, &write_buf);
                            // Read and process message
                            if (try message_util.readMessageFromStream(&stream_rd)) |message| {
                                if (try t_self.processAdminMessage(t_io, t_group, t_gpa, message)) |response_message| {
                                    try message_util.writeMessageToStream(&stream_wr, response_message);
                                }
                            }
                            stream.close(t_io); // Close the stream after
                        } else {
                            std.debug.print("Err = {any}\n", .{err});
                        }
                    }
                }
            }
        };
        temp.startAdminServer(self, io, group, gpa) catch |err| {
            std.debug.print("Error starting admin: {any}\n", .{err});
        };
    }

    /// Parse a message and call the correct processing function
    fn processAdminMessage(self: *Self, io: Io, group: *Io.Group, gpa: Allocator, message: message_util.Message) !?message_util.Message {
        switch (message) {
            message_util.MessageType.ECHO => |echo_message| {
                const response_data = try self.processEchoMessage(gpa, echo_message);
                return message_util.Message{
                    .R_ECHO = response_data,
                };
            },
            message_util.MessageType.P_REG => |producer_register_message| {
                const response = try self.processProducerRegisterMessage(io, group, gpa, &producer_register_message);
                return message_util.Message{
                    .R_P_REG = response,
                };
            },
            message_util.MessageType.C_REG => |consumer_register_message| {
                try self.processConsumerRegisterMessage(io, group, gpa, &consumer_register_message);
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
    fn processProducerRegisterMessage(self: *Self, io: Io, group: *Io.Group, gpa: Allocator, rm: *const message_util.ProducerRegisterMessage) !u8 {
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
            // Persist to disk
            try self.writeOverallStateToFile(io);
            topic_o = &self.topics.items[self.topics.items.len - 1];
            try group.concurrent(io, topic.Topic.tryPopMessage, .{ topic_o.?, io, gpa });
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

    // ============================= Consumer ============================

    /// Return the position of the consumer in the list
    fn processConsumerRegisterMessage(self: *Self, io: Io, group: *Io.Group, gpa: Allocator, rm: *const message_util.ConsumerRegisterMessage) !void {
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
            try tp.addNewCGroup(io, gpa, rm.group_id);
            try self.writeOverallStateToFile(io); //Persist to disk
            cg = &tp.cgroups.items[tp.cgroups.items.len - 1];
            // Thread for handling consumer loop for this CGroup.
            try group.concurrent(io, handleConsumersLoop, .{ self, io, gpa, cg });
        }
        // Add the port, stream and stream_state
        const cd = try gpa.create(ConsumerData);
        cd.* = try ConsumerData.new(gpa, rm.port, cg, tp, stream);
        try self.stream_list.append(gpa, stream);
        // Each consumer will send a ready to start receiving messages. This is just 1 byte.
        const cd_data = @intFromPtr(cd);
        _ = try cg.bg.recv_multishot(cd_data, cd.stream.socket.handle, 0);
        _ = try cg.ring.submit();
    }

    // ============================ Write / Send ============================

    /// Write a message using `send` to a socket.
    /// Alloc and put the ptr to the user_data
    fn writeMessageToFD(self: *KAdmin, gpa: Allocator, fd: net.Socket.Handle, message: message_util.Message) !void {
        const buf = try gpa.alloc(u8, 1024);
        const wd = try gpa.create(WriteData);
        wd.full_slice = buf; // dealloc on write done
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
};

// =================================== Event loops ========================================

/// Handle all producer messages in a loop of recv_multishot.
/// These messages are supposed to be PCM.
pub fn handleProducersLoop(self: *KAdmin, io: Io, gpa: Allocator) void {
    const temp = struct {
        /// Handle full message in the ProducerData
        fn handleFullPCMMessage(t_self: *KAdmin, t_io: Io, t_gpa: Allocator, pd: *ProducerData) !void {
            if (message_util.parseMessage(pd.data_buf.?[1..])) |m| {
                switch (m) {
                    message_util.MessageType.PCM => |pcm| {
                        try pd.topic.addMessage(t_io, t_gpa, &pcm); // Copy and Block until can add.
                        try t_self.writeMessageToFD(t_gpa, pd.stream.socket.handle, message_util.Message{
                            .R_PCM = 0,
                        });
                    },
                    else => {
                        // Not supported
                        std.debug.print("Not supported message of other type that PCM\n", .{});
                    },
                }
            }
            // Clean up;
            t_gpa.free(pd.data_buf.?);
            pd.data_buf = null;
        }

        /// Handle EOF signal from recv, meaning the producer has already left.
        fn isHandledEOF(t_gpa: Allocator, cqe: linux.io_uring_cqe, pd: *ProducerData) bool {
            if (cqe.flags & std.os.linux.IORING_CQE_F_MORE == 0) {
                // On EOF, recv_multishot is automatically cancel, so we just need to free
                std.debug.print("EOF from the producer port = {}\n", .{pd.port});
                if (pd.data_buf) |sl| {
                    t_gpa.free(sl);
                }
                t_gpa.destroy(pd);
                return true;
            }
            return false;
        }

        pub fn handleProducersLoop(t_self: *KAdmin, t_io: Io, t_gpa: Allocator) !void {
            var cqes: [1 << 3]linux.io_uring_cqe = undefined;
            // Event loop (wait until completion queue has something)
            while (true) {
                // Multi recv, this can comes from multiple stream.
                const num_recv = try t_self.pring.copy_cqes(&cqes, 1);
                // Recreate the full data buffer
                for (cqes[0..num_recv]) |cqe| {
                    if (cqe.user_data == 0) {
                        // End
                        std.debug.print("Terminated producers\n", .{});
                        return;
                    }
                    const err = cqe.err();
                    if (err != .SUCCESS) {
                        std.debug.print("Err in producer = {any}\n", .{err});
                        continue;
                    }
                    // User data covert back to a ProducerData pointer, to know which producer it received from.
                    var pd: *ProducerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                    // Handle recv EOF
                    if (isHandledEOF(t_gpa, cqe, pd)) {
                        continue;
                    }
                    const data_full = try t_self.pbg.get(cqe); // Get result for this cqe
                    // User the port as a way to of storage: each producer port will have a growable buffer that we can just copy data in.
                    // Copy outside, put it back to kernel after...
                    if (pd.data_buf) |current_data| {
                        // Memory is lost here...
                        const old_buf = current_data;
                        pd.data_buf = try std.mem.concat(t_gpa, u8, &.{ current_data, data_full });
                        t_gpa.free(old_buf);
                    } else {
                        pd.data_buf = try std.mem.concat(t_gpa, u8, &.{ "", data_full });
                    }
                    try t_self.pbg.put(cqe); // Give it back cuz not needed anymore.
                    // Check if we have recv all message from the socket
                    if (cqe.flags & std.os.linux.IORING_CQE_F_SOCK_NONEMPTY > 0) {
                        continue;
                    }
                    // Good to be parsed. Ignore the length first byte.
                    try handleFullPCMMessage(t_self, t_io, t_gpa, pd);
                }
            }
        }
    };
    temp.handleProducersLoop(self, io, gpa) catch |err| {
        std.debug.print("Error handling producer loop = {any}\n", .{err});
    };
}

/// Handle all consumer messages in a loop of recv_multishot.
/// These are expected to be C_RD and R_C_PCM (1 byte only)
pub fn handleConsumersLoop(self: *KAdmin, io: Io, gpa: Allocator, cg: *cgroup.CGroup) void {
    const temp = struct {
        fn isHandledEOF(t_gpa: Allocator, cqe: linux.io_uring_cqe, cd: *ConsumerData) !bool {
            if (cqe.flags & std.os.linux.IORING_CQE_F_MORE == 0) {
                std.debug.print("EOF from the consumer port = {}\n", .{cd.port});
                // std.process.exit(1); // DEBUG
                t_gpa.destroy(cd.current_timeout);
                t_gpa.destroy(cd);
                return true;
            }
            return false;
        }

        pub fn handleConsumersLoop(t_self: *KAdmin, t_io: Io, t_gpa: Allocator, t_cg: *cgroup.CGroup) !void {
            var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
            while (true) {
                // Multi recv, this can comes from multiple stream.
                // recv is an ack from the consumer which contains only 1 byte.
                const num_recv = try t_cg.ring.copy_cqes(&cqes, 1);
                for (cqes[0..num_recv]) |cqe| {
                    if (cqe.user_data == 0) {
                        t_cg.deinit(t_gpa);
                        std.debug.print("Terminated consumers\n", .{});
                        return;
                    }
                    const err = cqe.err();
                    if (err != .SUCCESS) {
                        std.debug.print("Err handling consumer = {any}\n", .{err});
                        continue;
                    }
                    var cd: *ConsumerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                    // Upon receive, it's a ready for sure.
                    if (try isHandledEOF(t_gpa, cqe, cd)) {
                        continue;
                    }
                    // Get and put back seems unneeded, but this is for clearing the buffer.
                    const data = try t_cg.bg.get(cqe); // Get result for this cqe
                    // Good to be parsed. Ignore the length first byte.
                    if (message_util.parseMessage(data[0..])) |m| {
                        switch (m) {
                            message_util.MessageType.R_C_PCM => |_| {
                                // std.debug.print("Ready from consumer {}, fd = {}\n", .{ cd.port, cd.stream.socket.handle }); // DEBUG
                                // Write the PCM to the consumer
                                try cd.topic.mq_lock.lock(t_io);
                                if (!cd.topic.mq.can_peek(cd.group.offset)) {
                                    // std.debug.print("Consumer group {any} is too fast, wait now...\n", .{cd.group.group_id});
                                    cd.topic.cond.waitUncancelable(t_io, &cd.topic.mq_lock);
                                    // After wake up, mq_lock is locked, and there's data so we can peek now.
                                    // std.debug.print("Consumer group {any} is woken up!\n", .{cd.group.group_id});
                                }
                                if (!cd.topic.mq.can_peek(cd.group.offset)) {
                                    @panic("Still cannot peek!\n");
                                }
                                const pcm = cd.topic.mq.peek(cd.group.offset).?;
                                // Write message to the stream, no wait and just +1
                                try t_self.writeMessageToFD(t_gpa, cd.stream.socket.handle, message_util.Message{ .PCM = pcm.* });
                                cd.group.offset += 1;
                                cd.topic.mq_lock.unlock(t_io);
                            },
                            else => {
                                // Not supported
                                std.debug.print("Not supported message of other type that PCM\n", .{});
                            },
                        }
                    }

                    try t_cg.bg.put(cqe); // Give it back cuz not needed anymore.
                }
            }
        }
    };
    temp.handleConsumersLoop(self, io, gpa, cg) catch |err| {
        std.debug.print("Error handling consumer loop = {any}\n", .{err});
    };
}

/// Event loop to handle all write. These just return a buffer to us for now.
/// If you need to handle different type of write, you can create a struct for it.
pub fn handleWriteLoop(self: *KAdmin, gpa: Allocator) void {
    const temp = struct {
        pub fn handleWriteLoop(t_self: *KAdmin, t_gpa: Allocator) !void {
            var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
            // Event loop
            while (true) {
                const num_write = try t_self.wring.copy_cqes(&cqes, 1);
                for (cqes[0..num_write]) |cqe| {
                    if (cqe.user_data == 0) {
                        std.debug.print("Terminated write\n", .{});
                        return;
                    }
                    const err = cqe.err();
                    if (err != .SUCCESS) {
                        std.debug.print("Err sending data = {any}\n", .{err});
                        continue;
                    }
                    const wd: *WriteData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                    wd.cur_written += @as(usize, @intCast(cqe.res));
                    if (wd.cur_written < wd.need_written) {
                        std.debug.print("Written data to fd = {any}, total = {}, need = {}\n", .{ wd.fd, wd.cur_written, wd.need_written });
                        // Need to keep writing.
                        _ = try t_self.wring.send(cqe.user_data, wd.fd, wd.full_slice[wd.cur_written..], 0);
                        _ = try t_self.wring.submit();
                    } else {
                        // std.debug.print("Written full data to fd = {any}\n", .{wd.fd}); // DEBUG
                        // Free data on done
                        t_gpa.free(wd.full_slice);
                        t_gpa.destroy(wd);
                    }
                }
            }
        }
    };
    temp.handleWriteLoop(self, gpa) catch |err| {
        std.debug.print("Error handling loop = {any}\n", .{err});
    };
}

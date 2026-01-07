const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const kadmin = @import("admin.zig");
const message_util = @import("message.zig");
const producer = @import("producer.zig");
const consumer = @import("consumer.zig");
const iou = std.os.linux.IoUring;
const posix = std.posix;

const ADMIN_PORT: u16 = 10000;

// ================================= IO Uring stuff =========================
fn uringStartEchoServer() !void {
    // Some init stuff
    const gpa = std.heap.smp_allocator;
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    var read_buf: [1024]u8 = undefined;
    var write_buf: [1024]u8 = undefined;

    // IO Uring stuff
    var ring = try iou.init(1 << 7, 0);

    // Start server
    const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
    var server = try address.listen(io, .{ .reuse_address = true }); // TCP server
    std.debug.print("Server bind socket at {any}\n", .{server.socket});

    // accept: https://man7.org/linux/man-pages/man2/accept.2.html
    // fd: server.socket.handle: to accept from
    // addr and addrlen is to be filled with data. If provide, data will be fill when peer connect.
    // return error and a new file descriptor for the accepted socket
    // Multishot allows 1 submission -> multiple completion entry, so we don't have to resubmit.

    var addr: std.posix.sockaddr = undefined;
    var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    _ = try ring.accept_multishot(10, server.socket.handle, &addr, &addr_len, 0);
    const count = try ring.submit();
    std.debug.print("Tasks submitted: {}\n", .{count});

    // Event loop (wait until completion queue has something)
    while (true) {
        // Will wait until we have an entry.
        const comp_entry = try ring.copy_cqe();
        const err = comp_entry.err();
        if (err == .SUCCESS) {
            if (comp_entry.user_data == 10) {
                // The correct user data. Can be anything
                // You can use @intFromPtr and @ptrFromInt to pass in the pointer and check type.
                const fd = comp_entry.res; // The fd for the accepted socket (read / write using it)
                std.debug.print("Something connected: addr = {any}, the return FD = {}\n", .{ addr, fd });
                // New stream, with the accepted socket.
                var stream = net.Stream{ .socket = net.Socket{ .handle = fd, .address = address } };
                var rd = stream.reader(io, &read_buf);
                var wr = stream.writer(io, &write_buf);
                if (try message_util.readMessageFromStream(&rd)) |res| {
                    const data = res.ECHO;
                    std.debug.print("Got client message: {s}\n", .{data});
                    try message_util.writeMessageToStream(&wr, message_util.Message{
                        .R_ECHO = data,
                    });
                }
                stream.close(io); // Close and wait for another accept. Avoid create a new fd every time a new client connect.

            }
        } else {
            std.debug.print("Err = {any}\n", .{err});
        }
    }
}

fn uringStartReadServer() !void {
    // Some init stuff
    const gpa = std.heap.smp_allocator;
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    // var read_buf: [1024]u8 = undefined;
    var write_buf: [1024]u8 = undefined;

    // IO Uring stuff
    var ring = try iou.init(1 << 7, 0);

    // Start server
    const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
    var server = try address.listen(io, .{ .reuse_address = true }); // TCP server
    std.debug.print("Server bind socket at {any}\n", .{server.socket});
    const stream = try server.accept(io); // We just accept here, blocking till can.

    // recv(2): https://man7.org/linux/man-pages/man2/recv.2.html
    // user data, fd, buffer and flag.
    // return the number of byte received. 0 if EOF

    // Multishot version need a BufferGroup
    var bg = try iou.BufferGroup.init(&ring, gpa, 10, 1024, 1 << 3);
    _ = try bg.recv_multishot(15, stream.socket.handle, 0);

    // var recv_buf: [1024]u8 = undefined;
    // const rb = iou.RecvBuffer{ .buffer = &recv_buf };
    // _ = try ring.recv(15, stream.socket.handle, rb, 0);
    const count = try ring.submit();
    std.debug.print("Tasks submitted: {}\n", .{count});

    // Event loop (wait until completion queue has something)
    while (true) {
        // Will wait until we have an entry.
        const comp_entry = try ring.copy_cqe();
        const err = comp_entry.err();
        if (err == .SUCCESS) {
            if (comp_entry.user_data == 15) {
                // The correct user data. Can be anything
                // You can use @intFromPtr and @ptrFromInt to pass in the pointer and check type.
                const num_read: usize = @intCast(comp_entry.res); // The fd for the accepted socket (read / write using it)
                if (num_read == 0) {
                    stream.close(io);
                    break;
                }
                const data_full = try bg.get(comp_entry); // Get result for this cqe
                // First is #bytes
                const data = data_full[1..];
                // const data = recv_buf[1..num_read];
                std.debug.print("Read the return #bytes = {}\n", .{num_read});
                std.debug.print("Data = {any}\n", .{data});
                var wr = stream.writer(io, &write_buf);
                if (message_util.parseMessage(data)) |m| {
                    switch (m) {
                        message_util.MessageType.ECHO => |e| {
                            std.debug.print("Received echo = {s}\n", .{e});
                            try message_util.writeMessageToStream(&wr, message_util.Message{
                                .R_ECHO = e,
                            });
                        },
                        // Per testing, we will not read back the message we just sent, so all is good.
                        message_util.MessageType.R_ECHO => |e| {
                            std.debug.print("Received my own r_echo = {s}\n", .{e});
                        },
                        else => {
                            // Ignore
                        },
                    }
                } else {
                    std.debug.print("No message can be parsed\n", .{});
                }
            }
        } else {
            std.debug.print("Err = {any}\n", .{err});
        }
    }
}

fn acceptFromClient() !void {
    // Some init stuff
    const gpa = std.heap.smp_allocator;
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
    var stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io); // Close on done

    var read_buf: [1024]u8 = undefined;
    var write_buf: [1024]u8 = undefined;

    // Send register message to kadmin
    var stream_rd = stream.reader(io, &read_buf);
    var stream_wr = stream.writer(io, &write_buf);
    try message_util.writeMessageToStream(&stream_wr, message_util.Message{
        .ECHO = try std.fmt.allocPrint(gpa, "Hello!", .{}),
    });
    std.debug.print("Written to the stream\n", .{});
    try io.sleep(.fromSeconds(10), .awake);
    // Try to read back the response from kadmin
    if (try message_util.readMessageFromStream(&stream_rd)) |res| {
        std.debug.print("Received R_ECHO from server: {s}\n", .{res.R_ECHO});
    }
    try io.sleep(.fromSeconds(10), .awake);
}

pub fn startAdminServerPanic(self: *kadmin.KAdmin, io: Io, gpa: Allocator, group: *Io.Group) void {
    self.startAdminServer(io, gpa, group) catch {
        @panic("Cannot start admin server");
    };
}

pub fn initKAdmin() !void {
    const gpa = std.heap.smp_allocator;

    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var admin = try kadmin.KAdmin.init(gpa);
    var group = Io.Group.init;
    // std.Thread.spawn(.{}, kadmin.KAdmin.startAdminServer, .{&admin, io, gpa});
    try group.concurrent(io, startAdminServerPanic, .{ &admin, io, gpa, &group });
    try group.await(io); // Clean up everything.
}

pub fn initProducer() !void {
    const gpa = std.heap.smp_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
    const port_int = try std.fmt.parseInt(u16, port_str, 10);
    const topic_str = std.mem.span(std.os.argv[3]); // 3rd argument is the topic
    const topic_int = try std.fmt.parseInt(u32, topic_str, 10);
    var p = try producer.ProducerProcess.init(port_int, topic_int);
    try p.startProducerServer(io);
    // Don't read from stdin anymore! Just run forever!
    while (true) {
        try p.writeMessage(io, try std.fmt.allocPrint(gpa, "Ping from {}", .{port_int}));
        try std.Io.sleep(io, .fromMilliseconds(100), .awake);
    }
    p.close();
}

pub fn initConsumer() !void {
    const gpa = std.heap.smp_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const port = try std.fmt.parseInt(u16, std.mem.span(std.os.argv[2]), 10); // 2nd argument is the port
    const topic = try std.fmt.parseInt(u32, std.mem.span(std.os.argv[3]), 10); // 3rd argument is the topic
    const group = try std.fmt.parseInt(u32, std.mem.span(std.os.argv[4]), 10); // 4th argument is the topic
    const sleep_mili = try std.fmt.parseInt(i64, std.mem.span(std.os.argv[5]), 10); // 5th argument is the sleep time in milli
    var c = try consumer.ConsumerProcess.init(port, topic, group);
    try c.startConsumerServer(io);
    // Always try to receive message
    while (true) {
        try std.Io.sleep(io, .fromMilliseconds(sleep_mili), .awake);
        try c.sendReadyMessage(io);
        try c.receiveMessage(io);
    }
    c.close();
}

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        // try uringStartEchoServer(); // Test accept_multishot.
        // try uringStartReadServer(); // Test recv_multishot.
        try initKAdmin();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "producer")) {
        try initProducer();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "consumer")) {
        try initConsumer();
    } else {
        // TODO: Init other type of process
        // try acceptFromClient(); // Use for testing
    }
}

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

pub fn initKAdmin() !void {
    // TODO: Process terminal signal to clean up.
    const gpa = std.heap.smp_allocator;

    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    // Init all rings and bg
    var aring = try iou.init(8, 0);
    var pring = try iou.init(8, 0);
    var cring = try iou.init(8, 0);
    var pbg = try iou.BufferGroup.init(&pring, gpa, 10, 1024, 8);
    var cbg = try iou.BufferGroup.init(&cring, gpa, 11, 1024, 8);
    // Start needed threads for event loops
    var admin = try kadmin.KAdmin.init(gpa, &aring, &pring, &cring, &pbg, &cbg);
    // try admin.startAdminServer(io, gpa);
    var th = try std.Thread.spawn(.{}, kadmin.KAdmin.startAdminServer, .{ &admin, io, gpa });
    var th2 = try std.Thread.spawn(.{}, kadmin.KAdmin.handleProducersLoop, .{ &admin, io, gpa });
    var th3 = try std.Thread.spawn(.{}, kadmin.KAdmin.handleConsumersLoop, .{ &admin, io, gpa });
    th.join();
    th2.join();
    th3.join();
}

pub fn initProducer(args: []const [:0]const u8) !void {
    const gpa = std.heap.smp_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const port_str = args[2]; // 2nd argument is the port
    const port_int = try std.fmt.parseInt(u16, port_str, 10);
    const topic_str = args[3]; // 3rd argument is the topic
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

pub fn initConsumer(args: []const [:0]const u8) !void {
    const gpa = std.heap.smp_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const port = try std.fmt.parseInt(u16, args[2], 10); // 2nd argument is the port
    const topic = try std.fmt.parseInt(u32, args[3], 10); // 3rd argument is the topic
    const group = try std.fmt.parseInt(u32, args[4], 10); // 4th argument is the topic
    const sleep_mili = try std.fmt.parseInt(i64, args[5], 10); // 5th argument is the sleep time in milli
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

pub fn main(init: std.process.Init) !void {
    // args, no more argv cuz footgun.
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    for (args) |arg| {
        std.log.info("arg: {s}", .{arg});
    }
    if (std.mem.eql(u8, args[1], "server")) {
        try initKAdmin();
    } else if (std.mem.eql(u8, args[1], "producer")) {
        try initProducer(args);
    } else if (std.mem.eql(u8, args[1], "consumer")) {
        try initConsumer(args);
    } else {
        // TODO: Init other type of process
    }
}

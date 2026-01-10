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
    var wring = try iou.init(8, 0);
    var pbg = try iou.BufferGroup.init(&pring, gpa, 10, 1024, 8);
    var cbg = try iou.BufferGroup.init(&cring, gpa, 11, 1024, 8);
    // Start needed threads for event loops
    var admin = try kadmin.KAdmin.init(gpa, &aring, &pring, &cring, &wring, &pbg, &cbg);
    // try admin.startAdminServer(io, gpa);
    var th = try std.Thread.spawn(.{}, kadmin.KAdmin.startAdminServer, .{ &admin, io, gpa });
    var th2 = try std.Thread.spawn(.{}, kadmin.KAdmin.handleProducersLoop, .{ &admin, io, gpa });
    var th3 = try std.Thread.spawn(.{}, kadmin.KAdmin.handleConsumersLoop, .{ &admin, io, gpa });
    var th4 = try std.Thread.spawn(.{}, kadmin.KAdmin.handleWriteLoop, .{ &admin, gpa });
    th.join();
    th2.join();
    th3.join();
    th4.join();
}

pub fn initProducer(args: []const [:0]const u8) !void {
    const port_str = args[2]; // 2nd argument is the port
    const port_int = try std.fmt.parseInt(u16, port_str, 10);
    const topic_str = args[3]; // 3rd argument is the topic
    const topic_int = try std.fmt.parseInt(u32, topic_str, 10);
    try initProducerWithParams(port_int, topic_int, 100);
}

pub fn initProducerWithParams(port: u16, topic: u32, delay_ms: i64) !void {
    const gpa = std.heap.smp_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    var p = try producer.ProducerProcess.init(port, topic);
    try p.startProducerServer(io);
    // Don't read from stdin anymore! Just run forever!
    while (true) {
        try p.writeMessage(io, try std.fmt.allocPrint(gpa, "Ping from {}", .{port}));
        try std.Io.sleep(io, .fromMilliseconds(delay_ms), .awake);
    }
    p.close();
}

pub fn initConsumer(args: []const [:0]const u8) !void {
    const port = try std.fmt.parseInt(u16, args[2], 10); // 2nd argument is the port
    const topic = try std.fmt.parseInt(u32, args[3], 10); // 3rd argument is the topic
    const group = try std.fmt.parseInt(u32, args[4], 10); // 4th argument is the topic
    const delay_ms = try std.fmt.parseInt(i64, args[5], 10); // 5th argument is the sleep time in milli
    try initConsumerWithParams(port, topic, group, delay_ms);
}

pub fn initConsumerWithParams(port: u16, topic: u32, group: u32, delay_ms: i64) !void {
    const gpa = std.heap.smp_allocator;
    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    var c = try consumer.ConsumerProcess.init(port, topic, group);
    try c.startConsumerServer(io);
    // Always try to receive message
    while (true) {
        try std.Io.sleep(io, .fromMilliseconds(delay_ms), .awake);
        try c.sendReadyMessage(io);
        // Super small pause after for no reason???
        // try std.Io.sleep(io, .fromMilliseconds(delay_ms), .awake);
        try c.receiveMessage(io);
    }
    c.close();
}

// Bench setup with thread spawn
pub fn initBench() !void {
    const gpa = std.heap.smp_allocator;
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    var admin_th = try std.Thread.spawn(.{}, initKAdmin, .{});
    try io.sleep(.fromSeconds(2), .awake);
    // var producer1_th = try std.Thread.spawn(.{}, initProducerWithParams, .{ 50000, 1, 100 });
    // try io.sleep(.fromSeconds(1), .awake);
    // var producer2_th = try std.Thread.spawn(.{}, initProducerWithParams, .{ 50001, 1, 100 });
    // try io.sleep(.fromSeconds(1), .awake);
    var producer3_th = try std.Thread.spawn(.{}, initProducerWithParams, .{ 50002, 2, 100 });
    try io.sleep(.fromSeconds(1), .awake);
    // var consumer1_th = try std.Thread.spawn(.{}, initConsumerWithParams, .{ 30000, 1, 1, 200 });
    // try io.sleep(.fromSeconds(1), .awake);
    // var consumer2_th = try std.Thread.spawn(.{}, initConsumerWithParams, .{ 30001, 1, 1, 200 });
    // try io.sleep(.fromSeconds(1), .awake);
    // var consumer3_th = try std.Thread.spawn(.{}, initConsumerWithParams, .{ 31000, 1, 2, 50 });
    // try io.sleep(.fromSeconds(1), .awake);
    var consumer4_th = try std.Thread.spawn(.{}, initConsumerWithParams, .{ 40000, 2, 1, 50 });
    try io.sleep(.fromSeconds(1), .awake);

    defer admin_th.join();
    // defer producer1_th.join();
    // defer producer2_th.join();
    defer producer3_th.join();
    // defer consumer1_th.join();
    // defer consumer2_th.join();
    // defer consumer3_th.join();
    defer consumer4_th.join();
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
    } else if (std.mem.eql(u8, args[1], "bench")) {
        try initBench();
    } else {
        // TODO: Init other type of process
    }
}

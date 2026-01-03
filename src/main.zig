const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = Io.net;
const kadmin = @import("admin.zig");
const message_util = @import("message.zig");
const producer = @import("producer.zig");
const consumer = @import("consumer.zig");

pub fn startAdminServerPanic(self: *kadmin.KAdmin, io: Io, gpa: Allocator, group: *Io.Group) void {
    self.startAdminServer(io, gpa, group) catch {
        @panic("Cannot start admin server");
    };
}

pub fn initKAdmin() !void {
    const gpa = std.heap.smp_allocator;

    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{
        // .async_limit = .limited(8),
        // .concurrent_limit = .limited(8),
    });
    defer threaded.deinit();
    const io = threaded.io();
    // const evented = try gpa.create(std.Io.Evented);
    // try std.Io.Evented.init(evented, gpa, .{});
    // defer evented.deinit();
    // const io = evented.io();

    var admin = try kadmin.KAdmin.init(gpa);
    var group = Io.Group.init;
    try group.concurrent(io, startAdminServerPanic, .{ &admin, io, gpa, &group });

    group.wait(io); // Clean up everything.
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
        try initKAdmin();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "producer")) {
        try initProducer();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "consumer")) {
        try initConsumer();
    } else {
        // TODO: Init other type of process
    }
}

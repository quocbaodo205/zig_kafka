/// Utility to manage stream
const std = @import("std");
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;

pub const StreamIO = struct {
    const Self = @This();

    stream: *net.Stream,
    // Reader and writer
    read_buf: []u8,
    write_buf: []u8,
    reader: *net.Stream.Reader,
    writer: *net.Stream.Writer,

    pub fn init(io: Io, gpa: Allocator, stream: *net.Stream) !Self {
        const rb = try gpa.alloc(u8, 1024);
        const wb = try gpa.alloc(u8, 1024);
        return Self{
            .stream = stream,
            .read_buf = rb,
            .write_buf = wb,
            .reader = stream.reader(io, &rb),
            .writer = stream.writer(io, &wb),
        };
    }

    pub fn deinit(self: *Self, gpa: Allocator) void {
        gpa.free(self.read_buf);
        gpa.free(self.write_buf);
        // Stream free is done else where.
    }

    pub fn readAndWrite(self: *Self) void {}
};

const std = @import("std");
const Allocator = std.mem.Allocator;
const iou = std.os.linux.IoUring;

pub const CGroup = struct {
    const Self = @This();

    group_id: u32,
    topic_id: u32,
    offset: usize,
    ring: *iou,
    bg: *iou.BufferGroup,

    pub fn new(gpa: Allocator, group_id: u32, topic_id: u32, offset: usize) !Self {
        const ring = try gpa.create(iou);
        ring.* = try iou.init(8, 0);
        const bg = try gpa.create(iou.BufferGroup);
        bg.* = try iou.BufferGroup.init(ring, gpa, @as(u16, @intCast(group_id)), 1024, 8);
        return Self{
            .group_id = group_id,
            .topic_id = topic_id,
            .offset = offset,
            .ring = ring,
            .bg = bg,
        };
    }

    pub fn deinit(self: Self, gpa: Allocator) void {
        self.bg.deinit(gpa);
        self.ring.deinit();
        gpa.destroy(self.bg);
        gpa.destroy(self.ring);
    }
};

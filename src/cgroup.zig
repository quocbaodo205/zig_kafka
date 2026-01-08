pub const CGroup = struct {
    const Self = @This();

    group_id: u32,
    topic_id: u32,
    offset: usize,

    pub fn new(group_id: u32, topic_id: u32, offset: usize) !Self {
        return Self{
            .group_id = group_id,
            .topic_id = topic_id,
            .offset = offset,
        };
    }
};

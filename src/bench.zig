const std = @import("std");
const zstd = @cImport(@cInclude("zstd.h"));
const zdict = @cImport(@cInclude("zdict.h"));
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const PAGE_SIZE = 1 << 20;

pub fn main() !void {
    const alloc = std.heap.page_allocator;

    const indices = try read_file(alloc, "input.index", null);
    defer alloc.free(indices);

    var parsed_indices = ArrayList(usize).init(alloc);
    defer parsed_indices.deinit();

    var index_iter = std.mem.splitScalar(u8, indices[1..], ' ');

    while (index_iter.next()) |index| {
        const idx = try std.fmt.parseInt(usize, index, 10);
        try parsed_indices.append(idx);
    }

    if (parsed_indices.items.len == 0) {
        @panic("empty index file");
    }

    var input_sizes = try alloc.alloc(usize, parsed_indices.items.len);
    defer alloc.free(input_sizes);

    var max_input_size: usize = 0;
    var input_start_offset: usize = 0;

    for (parsed_indices.items, 0..) |offset, i| {
        const sz = offset - input_start_offset;
        input_start_offset = offset;

        max_input_size = @max(max_input_size, sz);

        input_sizes[i] = sz;

        if (sz > PAGE_SIZE) {
            @panic("there is an input with size bigger than PAGE_SIZE\n");
        }
    }

    std.mem.sort(usize, input_sizes, {}, std.sort.asc(usize));

    const median_input_size = input_sizes[input_sizes.len / 2];

    const data_size = parsed_indices.items[parsed_indices.items.len - 1];

    const data = try read_file(alloc, "input.data", data_size + 1024);
    defer alloc.free(data);

    var start: usize = 0;

    var pages = ArrayList([]const u8).init(alloc);
    defer pages.deinit();

    for (parsed_indices.items) |offset| {
        if (offset - start >= PAGE_SIZE) {
            try pages.append(data[start..offset]);
            start = offset;
        }
    }

    if (start < data_size) {
        try pages.append(data[start..data_size]);
    }

    const scratch = try alloc.alloc(u8, zstd.ZSTD_compressBound(PAGE_SIZE));
    defer alloc.free(scratch);

    var compressed_size: usize = 0;

    const dict_buffer = try alloc.alloc(u8, data_size / 100);
    defer alloc.free(dict_buffer);

    const d_len = zdict.ZDICT_trainFromBuffer(dict_buffer.ptr, @intCast(dict_buffer.len), data.ptr, input_sizes.ptr, @intCast(input_sizes.len));

    if (zdict.ZDICT_isError(d_len) != 0) {
        @panic("failed to compress");
    }

    const dict_ref = dict_buffer[0..d_len];

    std.debug.print("dict_len={d}\n", .{dict_ref.len});

    for (pages.items) |page| {
        const c_len = zstd.ZSTD_compress(scratch.ptr, scratch.len, page.ptr, page.len, 8);
        if (zstd.ZSTD_isError(c_len) != 0) {
            @panic("failed to compress");
        }
        compressed_size += c_len;
    }

    const stats = Stats{
        .num_transactions = parsed_indices.items.len,
        .average_input_size = data_size / parsed_indices.items.len,
        .median_input_size = median_input_size,
        .max_input_size = max_input_size,
        .num_pages = pages.items.len,
        .total_size = data_size,
        .compressed_size = compressed_size,
        .dictionary_size = 0,
    };

    std.debug.print("{}\n", .{stats});
}

const Stats = struct {
    num_transactions: usize,
    average_input_size: usize,
    median_input_size: usize,
    max_input_size: usize,
    num_pages: usize,
    total_size: usize,
    compressed_size: usize,
    dictionary_size: usize,
};

fn read_file(alloc: Allocator, path: []const u8, size_hint: ?usize) ![]u8 {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    return try file.readToEndAllocOptions(
        alloc,
        std.math.maxInt(usize),
        size_hint,
        64,
        null,
    );
}

const std = @import("std");
const zstd = @cImport(@cInclude("zstd.h"));
const zdict = @cImport(@cInclude("zdict.h"));
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

pub fn main() !void {
    const page_sizes = [_]usize{ 1 << 15, 1 << 17, 1 << 19, 1 << 20, 1 << 22 };

    for (page_sizes) |page_size| {
        try run_config(page_size);
    }
}

fn run_config(page_size: usize) !void {
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

        // if (sz > page_size) {
        //     @panic("there is an input with size bigger than page_size\n");
        // }
    }

    const data_size = parsed_indices.items[parsed_indices.items.len - 1];

    const data = try read_file(alloc, "input.data", data_size + 1024);
    defer alloc.free(data);

    var start: usize = 0;

    var pages = ArrayList([]const u8).init(alloc);
    defer pages.deinit();

    for (parsed_indices.items) |offset| {
        if (offset - start >= page_size) {
            try pages.append(data[start..offset]);
            start = offset;
        }
    }

    if (start < data_size) {
        try pages.append(data[start..data_size]);
    }

    const scratch = try alloc.alloc(u8, zstd.ZSTD_compressBound(@max(1 << 20, page_size)));
    defer alloc.free(scratch);

    const dict_buffer = try alloc.alloc(u8, 256 * 1024);
    defer alloc.free(dict_buffer);

    // const page_sizes = try alloc.alloc(usize, pages.items.len);
    // defer alloc.free(page_sizes);

    // for (pages.items, 0..) |page, i| {
    //     page_sizes[i] = page.len;
    // }

    const d_len = zdict.ZDICT_trainFromBuffer(dict_buffer.ptr, @intCast(dict_buffer.len), data.ptr, input_sizes.ptr, @intCast(input_sizes.len));

    if (zdict.ZDICT_isError(d_len) != 0) {
        @panic("failed to compress");
    }

    const dict_ref = dict_buffer[0..d_len];

    var regular_compressed_size: usize = 0;
    for (pages.items) |page| {
        const c_len = zstd.ZSTD_compress(scratch.ptr, scratch.len, page.ptr, page.len, 8);
        if (zstd.ZSTD_isError(c_len) != 0) {
            @panic("failed to compress");
        }
        regular_compressed_size += c_len;
    }

    const c_dict = zstd.ZSTD_createCDict(dict_ref.ptr, dict_ref.len, 8);
    defer {
        _ = zstd.ZSTD_freeCDict(c_dict);
    }

    const c_ctx = zstd.ZSTD_createCCtx();
    defer {
        _ = zstd.ZSTD_freeCCtx(c_ctx);
    }

    var dictionary_compressed_size: usize = 0;
    for (pages.items) |page| {
        const c_len = zstd.ZSTD_compress_usingCDict(c_ctx, scratch.ptr, scratch.len, page.ptr, page.len, c_dict);
        if (zstd.ZSTD_isError(c_len) != 0) {
            @panic("failed to compress");
        }
        dictionary_compressed_size += c_len;
    }

    std.mem.sort(usize, input_sizes, {}, std.sort.asc(usize));
    const median_input_size = input_sizes[input_sizes.len / 2];

    const stats = Stats{
        .num_transactions = parsed_indices.items.len,
        .average_input_size = data_size / parsed_indices.items.len,
        .median_input_size = median_input_size,
        .max_input_size = max_input_size,
        .num_pages = pages.items.len,
        .total_size = data_size,
        .regular_compressed_size = regular_compressed_size,
        .dictionary_size = dict_ref.len,
        .dictionary_compressed_size = dictionary_compressed_size,
    };

    std.debug.print("PAGE_SIZE={d}: {}\n", .{ page_size, stats });
}

const Stats = struct {
    num_transactions: usize,
    average_input_size: usize,
    median_input_size: usize,
    max_input_size: usize,
    num_pages: usize,
    total_size: usize,
    regular_compressed_size: usize,
    dictionary_size: usize,
    dictionary_compressed_size: usize,
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

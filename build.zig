const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_source_file = b.path("src/bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    const zstd_dependency = b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
    });
    bench.linkLibrary(zstd_dependency.artifact("zstd"));
    const lz4_dependency = b.dependency("lz4", .{
        .target = target,
        .optimize = optimize,
    });
    bench.linkLibrary(lz4_dependency.artifact("lz4"));

    const run_cmd = b.addRunArtifact(bench);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("runbench", "Run the benchmark app");
    run_step.dependOn(&run_cmd.step);
}

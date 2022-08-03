const std = @import("std");

/// List of errors to control error flow
const ControlErrors = error{
    /// The current random values aren't working, try with a different set of random values.
    PropTestDiscard,
};

const RunOptions = struct {
    allocator: std.mem.Allocator = std.testing.allocator,
    cache_path: []const u8 = "zig-cache/test-cases",
    max_iterations: usize = 100,
};

pub fn Generator(comptime Input: type) type {
    return struct {
        create: fn (std.mem.Allocator, std.rand.Random) anyerror!Input,
        destroy: fn (Input, std.mem.Allocator) void,
        shrink: fn (Input, std.mem.Allocator, std.rand.Random, tactic: u32) anyerror!Result,
        print: fn (Input) void,

        pub const Result = union(enum) {
            shrunk: Input,
            dead_end,
            no_more_tactics,
        };
    };
}

pub fn run(src: std.builtin.SourceLocation, run_options: RunOptions, comptime Input: type, generator: Generator(Input), testFn: fn (Input) anyerror!void) !void {
    var cache = try std.fs.cwd().makeOpenPath(run_options.cache_path, .{});
    defer cache.close();

    const test_name = &cacheName(src);
    const seed = try getInputU64(cache, test_name);

    // TODO: switch on return type
    var iterations: usize = 0;
    while (iterations < run_options.max_iterations) : (iterations += 1) {
        var prng = std.rand.DefaultPrng.init(seed + iterations);
        const input = try generator.create(run_options.allocator, prng.random());
        //var rand_recorder = RandomRecorder{ .allocator = run_options.allocator, .parent = prng.random() };
        //defer rand_recorder.deinit();

        testFn(input) catch |initial_error| switch (initial_error) {
            error.PropTestDiscard => continue,
            else => {
                std.debug.print("initial error = {}\n", .{initial_error});
                // Try to shrink test case
                var tactic: u32 = 0;
                var current_input = input;
                var current_error = initial_error;
                while (true) {
                    var new_prng = std.rand.DefaultPrng.init(seed + iterations);
                    const new_input = switch (try generator.shrink(current_input, run_options.allocator, new_prng.random(), tactic)) {
                        .shrunk => |new| new,
                        .dead_end => {
                            std.debug.print("dead end\n", .{});
                            tactic += 1;
                            continue;
                        },
                        .no_more_tactics => {
                            std.debug.print("no more tactics!\n", .{});
                            break;
                        },
                    };
                    if (testFn(new_input)) {
                        // The test succeeded, so our simplification didn't work.
                        // Change tactics and continue.
                        std.debug.print("error was removed, reverting\n", .{});
                        tactic += 1;
                        generator.destroy(new_input, run_options.allocator);
                    } else |e| if (e == initial_error) {
                        std.debug.print("new error = {}\n", .{e});
                        // We got the same error back out! Continue simplifying with the new input, resetting the tactics we're using
                        tactic = 0;
                        generator.destroy(current_input, run_options.allocator);
                        current_input = new_input;
                        current_error = e;
                    } else {
                        std.debug.print("error changed ({}), reverting\n", .{e});
                        tactic += 1;
                    }
                }

                // Print input
                std.debug.print("{s} failed with input:\n", .{src.fn_name});
                generator.print(current_input);
                generator.destroy(current_input, run_options.allocator);
                return current_error;
            },
        };

        generator.destroy(input, run_options.allocator);
    }

    cleanTestCache(cache, test_name);
}

/// Get an u64 and store it in the test cases cache. This allows us to redo the same test case if it fails.
pub fn getInputU64(cache: std.fs.Dir, test_name: []const u8) !u64 {
    const test_case = cache.readFileAlloc(std.testing.allocator, test_name, @sizeOf(u64)) catch |e| switch (e) {
        error.FileTooBig => {
            std.debug.print("Test case in cache too large\n", .{});
            return error.UnexpectedValueInCache;
        },

        // Generate a test case
        error.FileNotFound => {
            const new_test_case = std.crypto.random.int(u64);
            try cache.writeFile(test_name, std.mem.asBytes(&new_test_case));
            return new_test_case;
        },

        else => return e,
    };
    defer std.testing.allocator.free(test_case);
    if (test_case.len < @sizeOf(u64)) {
        std.debug.print("Test case in cache too small\n", .{});
        return error.UnexpectedValueInCache;
    }

    return @bitCast(u64, test_case[0..@sizeOf(u64)].*);
}

/// The test succeeded, we remove the cached input and do a different test case
pub fn cleanTestCache(cache: std.fs.Dir, test_name: []const u8) void {
    cache.deleteFile(test_name) catch return;
}

pub const CACHE_NAME_LEN = std.base64.url_safe_no_pad.Encoder.calcSize(16);

pub fn cacheName(src: std.builtin.SourceLocation) [CACHE_NAME_LEN]u8 {
    var hash: [16]u8 = undefined;
    std.crypto.hash.Blake3.hash(src.fn_name, &hash, .{});

    var name: [std.base64.url_safe_no_pad.Encoder.calcSize(16)]u8 = undefined;
    _ = std.base64.url_safe_no_pad.Encoder.encode(&name, &hash);

    return name;
}

// A random interface that logs the values it returns for use later
const RandomRecorder = struct {
    allocator: std.mem.Allocator,
    parent: std.rand.Random,
    record: std.SegmentedList(u8, 0) = .{},
    num_bytes_not_recorded: usize = 0,

    pub fn deinit(this: *@This()) void {
        this.record.deinit(this.allocator);
        if (this.num_bytes_not_recorded > 0) {
            std.debug.print("RandomRecorder missed recording {} bytes", .{this.num_bytes_not_recorded});
        }
    }

    pub fn random(this: *@This()) std.rand.Random {
        return std.rand.Random.init(this, fill);
    }

    pub fn fill(this: *@This(), buf: []u8) void {
        this.parent.bytes(buf);
        this.record.appendSlice(this.allocator, buf) catch {
            this.num_bytes_not_recorded += buf.len;
        };
    }
};

// A random interface that logs the values it returns for use later
const RandomReplayer = struct {
    record: *const std.SegmentedList(u8, 0),
    index: usize = 0,

    /// The maximum index to return recorded data from. Panics if more data is requested.
    max_index: usize,

    /// The index where we want to start simplifying the data. Replayer will return 0 after this.
    begin_simplify_at_index: ?usize = null,

    pub fn init(recorder: *const RandomRecorder) @This() {
        return @This(){
            .record = &recorder.record,
            .max_index = recorder.record.count(),
        };
    }

    pub fn random(this: *@This()) std.rand.Random {
        return std.rand.Random.init(this, fill);
    }

    pub fn fill(this: *@This(), buf: []u8) void {
        for (buf) |*c| {
            if (this.index >= this.max_index) {
                std.debug.panic("Random data requested beyond recorded data", .{});
            }
            if (this.index >= this.begin_simplify_at_index) {
                c.* = 0;
            } else {
                c.* = this.record.at(this.index);
            }
            this.index += 1;
        }
    }
};

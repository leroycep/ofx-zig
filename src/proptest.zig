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

pub fn Result(comptime Input: type) type {
    return union(enum) {
        shrunk: Input,
        dead_end,
        no_more_tactics,
    };
}

pub fn Generator(comptime Input: type) type {
    return struct {
        create: fn (std.mem.Allocator, std.rand.Random) anyerror!Input,
        destroy: fn (Input, std.mem.Allocator) void,
        shrink: fn (Input, std.mem.Allocator, std.rand.Random, tactic: u32) anyerror!Result(Input),
        print: fn (Input) void,
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
                // Try to shrink test case
                var tactic: u32 = 0;
                var current_input = input;
                var current_error = initial_error;
                while (true) {
                    var new_prng = std.rand.DefaultPrng.init(seed + iterations);
                    const new_input = switch (try generator.shrink(current_input, run_options.allocator, new_prng.random(), tactic)) {
                        .shrunk => |new| new,
                        .dead_end => {
                            tactic += 1;
                            continue;
                        },
                        .no_more_tactics => {
                            break;
                        },
                    };
                    if (testFn(new_input)) {
                        // The test succeeded, so our simplification didn't work.
                        // Change tactics and continue.
                        tactic += 1;
                        generator.destroy(new_input, run_options.allocator);
                    } else |e| if (e == initial_error) {
                        // We got the same error back out! Continue simplifying with the new input, resetting the tactics we're using
                        tactic = 0;
                        generator.destroy(current_input, run_options.allocator);
                        current_input = new_input;
                        current_error = e;
                    } else {
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

pub fn String(comptime T: type, comptime options: struct {
    ranges: []const Range(T) = &.{.{ .min_max = .{ 0, std.math.maxInt(T) } }},
    min_len: usize = 0,
    max_len: usize = 50 * 1024,
}) type {
    const StringCharacter = Character(T, options.ranges);

    return struct {
        pub fn generator() Generator([]const u8) {
            return .{
                .create = create,
                .destroy = destroy,
                .shrink = shrink,
                .print = print,
            };
        }

        pub fn create(allocator: std.mem.Allocator, rand: std.rand.Random) ![]const T {
            const buf = try allocator.alloc(u8, rand.intRangeLessThan(usize, options.min_len, options.max_len));
            for (buf) |*element| {
                element.* = try StringCharacter.create(allocator, rand);
            }
            return buf;
        }

        pub fn destroy(buf: []const T, allocator: std.mem.Allocator) void {
            allocator.free(buf);
        }

        const Tactic = enum(u32) {
            take_front_half,
            take_back_half,
            simplify,
            simplify_front_half,
            simplify_back_half,
            remove_last_char,
            remove_first_char,
            simplify_last_char,
            simplify_first_char,
            _,
        };

        const Res = Result([]const T);
        pub fn shrink(buf: []const T, allocator: std.mem.Allocator, rand: std.rand.Random, tactic: u32) !Res {
            if (buf.len <= options.min_len) return Res.no_more_tactics;
            switch (@intToEnum(Tactic, tactic)) {
                .take_front_half => {
                    if (buf.len / 2 < options.min_len) return Res.dead_end;
                    return Res{ .shrunk = try allocator.dupe(T, buf[0 .. buf.len / 2]) };
                },
                .take_back_half => {
                    if (buf.len / 2 < options.min_len) return Res.dead_end;
                    return Res{ .shrunk = try allocator.dupe(T, buf[buf.len / 2 ..]) };
                },
                .simplify => {
                    const new = try allocator.dupe(T, buf);
                    for (new) |*element| {
                        switch (try StringCharacter.shrink(element.*, allocator, rand, 0)) {
                            .shrunk => |new_char| element.* = new_char,
                            .dead_end, .no_more_tactics => return Res.dead_end,
                        }
                    }
                    return Res{ .shrunk = new };
                },
                .simplify_front_half => {
                    const new = try allocator.dupe(T, buf);
                    for (new[0 .. buf.len / 2]) |*element| {
                        switch (try StringCharacter.shrink(element.*, allocator, rand, 0)) {
                            .shrunk => |new_char| element.* = new_char,
                            .dead_end, .no_more_tactics => return Res.dead_end,
                        }
                    }
                    return Res{ .shrunk = new };
                },
                .simplify_back_half => {
                    const new = try allocator.dupe(T, buf);
                    for (new[new.len / 2 ..]) |*element| {
                        switch (try StringCharacter.shrink(element.*, allocator, rand, 0)) {
                            .shrunk => |new_char| element.* = new_char,
                            .dead_end, .no_more_tactics => return Res.dead_end,
                        }
                    }
                    return Res{ .shrunk = new };
                },
                .remove_last_char => return Res{ .shrunk = try allocator.dupe(u8, buf[0 .. buf.len - 1]) },
                .remove_first_char => return Res{ .shrunk = try allocator.dupe(u8, buf[1..]) },

                .simplify_last_char => {
                    switch (try StringCharacter.shrink(buf[buf.len - 1], allocator, rand, 0)) {
                        .shrunk => |new_char| {
                            const new = try allocator.dupe(T, buf);
                            new[buf.len - 1] = new_char;
                            return Res{ .shrunk = new };
                        },
                        .dead_end, .no_more_tactics => {
                            return Res.dead_end;
                        },
                    }
                },
                .simplify_first_char => {
                    switch (try StringCharacter.shrink(buf[0], allocator, rand, 0)) {
                        .shrunk => |new_char| {
                            const new = try allocator.dupe(T, buf);
                            new[0] = new_char;
                            return Res{ .shrunk = new };
                        },
                        .dead_end, .no_more_tactics => {
                            return Res.dead_end;
                        },
                    }
                },

                _ => return Res.no_more_tactics,
            }
        }

        pub fn print(ascii: []const u8) void {
            std.debug.print("{s}\n", .{ascii});
        }
    };
}

pub fn Range(comptime T: type) type {
    return union(enum) {
        list: []const T,
        min_max: [2]T,

        pub fn valueAt(this: @This(), index: usize) T {
            switch (this) {
                .list => |l| return l[index],
                .min_max => |r| return r[0] + @intCast(T, index),
            }
        }

        pub fn size(this: @This()) usize {
            switch (this) {
                .list => |l| return l.len,
                .min_max => |r| {
                    std.debug.assert(r[0] < r[1]);
                    return r[1] - r[0] + 1;
                },
            }
        }
    };
}

pub fn Character(comptime T: type, comptime ranges: []const Range(T)) type {
    std.debug.assert(ranges.len > 0);
    // TODO: Ensure that none of the ranges overlap
    var total: usize = 0;
    for (ranges) |range| {
        total += range.size();
    }
    const total_number_of_characters = total;
    return struct {
        fn create(_: std.mem.Allocator, rand: std.rand.Random) !T {
            const idx = rand.uintLessThan(usize, total_number_of_characters);
            var range_start: usize = 0;
            for (ranges) |range| {
                const range_end = range_start + range.size();
                if (range_start <= idx and idx < range_end) {
                    return range.valueAt(idx - range_start);
                }
                range_start = range_end;
            } else {
                std.debug.panic("index not in ranges! {}", .{idx});
            }
        }

        fn destroy(_: T, _: std.mem.Allocator) void {}

        const Tactic = enum(u32) {
            change_to_first,
            _,
        };

        const Res = Result(T);
        fn shrink(current: T, _: std.mem.Allocator, _: std.rand.Random, tactic: u32) !Res {
            switch (@intToEnum(Tactic, tactic)) {
                .change_to_first => {
                    const first = ranges[0].valueAt(0);
                    if (current == first) return Res.dead_end;
                    return Res{ .shrunk = first };
                },
                _ => return Res.no_more_tactics,
            }
        }

        fn print(value: T) void {
            std.debug.print("\'{'}\' ({}, 0x{x})\n", .{ std.zig.fmtEscapes(value), value, value });
        }
    };
}

const std = @import("std");
const proptest = @import("./proptest.zig");

pub const Document = struct {};

pub const Event = union(enum) {
    flat_element: Loc,
    text: Loc,
    // Generic start/close; check the loc for more details
    start_other: Loc,
    close_other: Loc,

    start_fi,
    close_fi,
    fid: Loc,
    org: Loc,

    curdef: Loc,
    bankid: Loc,
    acctid: Loc,
    accttype: Loc,

    start_stmttrn,
    close_stmttrn,
    trntype: Loc,
    dtposted: Loc,
    trnamt: Loc,
    fitid: Loc,
    name: Loc,
    memo: Loc,

    start_balance: BalanceKind,
    close_balance: BalanceKind,
    balamt: Loc,
    dtasof: Loc,

    const BalanceKind = enum {
        ledger,
        available,

        fn elementName(this: @This()) []const u8 {
            return switch (this) {
                .ledger => "LEDGERBAL",
                .available => "AVAILBAL",
            };
        }
    };

    pub fn isStart(this: @This()) bool {
        return switch (this) {
            .start_other,
            .start_fi,
            .start_stmttrn,
            .start_balance,
            => true,
            else => false,
        };
    }

    pub fn isClose(this: @This()) bool {
        return switch (this) {
            .close_other,
            .close_fi,
            .close_stmttrn,
            .close_balance,
            => true,
            else => false,
        };
    }

    pub fn getClose(this: @This()) ?@This() {
        return switch (this) {
            .start_fi => return .close_fi,
            .start_stmttrn => return .close_stmttrn,
            .start_other => |loc| return @This(){ .close_other = loc },
            .start_balance => |kind| return @This(){ .close_balance = kind },
            else => return null,
        };
    }

    pub fn renderSGML(this: @This(), src: []const u8, writer: anytype) !void {
        switch (this) {
            .start_fi => try writer.writeAll("<FI>"),
            .close_fi => try writer.writeAll("</FI>"),

            .start_stmttrn => try writer.writeAll("<SMTTRN>"),
            .close_stmttrn => try writer.writeAll("</SMTTRN>"),

            .bankid => |loc| try writer.print("<BANKID>{s}", .{loc.text(src)}),
            .acctid => |loc| try writer.print("<ACCTID>{s}", .{loc.text(src)}),

            .fid => |loc| try writer.print("<FID>{s}", .{loc.text(src)}),
            .org => |loc| try writer.print("<ORG>{s}", .{loc.text(src)}),
            .curdef => |loc| try writer.print("<CURDEF>{s}", .{loc.text(src)}),
            .accttype => |loc| try writer.print("<ACCTTYPE>{s}", .{loc.text(src)}),
            .trntype => |loc| try writer.print("<TRNTYPE>{s}", .{loc.text(src)}),
            .dtposted => |loc| try writer.print("<DTPOSTED>{s}", .{loc.text(src)}),
            .trnamt => |loc| try writer.print("<TRNAMT>{s}", .{loc.text(src)}),
            .fitid => |loc| try writer.print("<FITID>{s}", .{loc.text(src)}),
            .name => |loc| try writer.print("<NAME>{s}", .{loc.text(src)}),
            .memo => |loc| try writer.print("<MEMO>{s}", .{loc.text(src)}),
            .balamt => |loc| try writer.print("<BALAMT>{s}", .{loc.text(src)}),
            .dtasof => |loc| try writer.print("<DTASOF>{s}", .{loc.text(src)}),

            .start_other => |loc| try writer.print("<{s}>", .{loc.text(src)}),
            .close_other => |loc| try writer.print("</{s}>", .{loc.text(src)}),
            .flat_element => |loc| try writer.print("<{s}>", .{loc.text(src)}),

            .text => |loc| try writer.print("{s}", .{loc.text(src)}),

            .start_balance => |bal_kind| try writer.print("<{s}>", .{bal_kind.elementName()}),
            .close_balance => |bal_kind| try writer.print("<{s}>", .{bal_kind.elementName()}),
        }
    }

    pub fn eql(a: @This(), a_src: []const u8, b: @This(), b_src: []const u8) bool {
        if (std.meta.activeTag(a) != b) return false;
        return switch (a) {
            .start_balance => |kind| kind == b.start_balance,
            .start_other => |loc| std.mem.eql(u8, loc.text(a_src), b.start_other.text(b_src)),
            else => true,
        };
    }

    pub fn fmtWithSrc(this: @This(), src: []const u8) FmtWithSrc {
        return FmtWithSrc{ .src = src, .event = this };
    }

    pub const FmtWithSrc = struct {
        src: []const u8,
        event: Event,

        pub fn format(
            this: @This(),
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            _ = fmt;
            _ = options;
            switch (this.event) {
                .start_fi,
                .close_fi,
                .start_stmttrn,
                .close_stmttrn,
                => try writer.print("{s}", .{std.meta.tagName(this.event)}),

                .bankid,
                .acctid,
                => try writer.print("{s} [REDACTED]", .{
                    std.meta.tagName(this.event),
                }),

                .fid,
                .org,
                .curdef,
                .accttype,
                .trntype,
                .dtposted,
                .trnamt,
                .fitid,
                .name,
                .memo,
                .text,
                .start_other,
                .close_other,
                .flat_element,
                .balamt,
                .dtasof,
                => |loc| try writer.print("{s} \"{}\"", .{
                    std.meta.tagName(this.event),
                    std.zig.fmtEscapes(loc.text(this.src)),
                }),

                .start_balance,
                .close_balance,
                => |balance_kind| try writer.print("{s} {s}", .{
                    std.meta.tagName(this.event),
                    std.meta.tagName(balance_kind),
                }),
            }
        }
    };
};

pub fn parse(allocator: std.mem.Allocator, src: []const u8) ![]Event {
    std.debug.assert(src.len <= std.math.maxInt(u32));

    const tokens = try tokenize(allocator, src);
    defer allocator.free(tokens);

    var events = std.ArrayList(Event).init(allocator);
    defer events.deinit();

    var cursor = Cursor{ .tokens = tokens, .index = 0 };

    // Skip text until we get to the `<OFX>`
    while (cursor.eat(.text)) |_| {}

    _ = cursor.eat(.start_ofx_tag) orelse return error.InvalidSyntax;

    while (true) {
        if (cursor.eat(.close_ofx_tag)) |_| {
            break;
        } else if (cursor.eat(.eof)) |_| {
            break;
        } else if (try parseAggregate(allocator, src, &cursor)) |aggregate| {
            defer allocator.free(aggregate.children);
            try events.append(.{ .start_other = aggregate.name });
            try events.appendSlice(aggregate.children);
            try events.append(.{ .close_other = aggregate.name });
        } else if (try parseElement(src, &cursor)) |element| {
            try events.append(.{ .flat_element = element.name });
            if (element.value) |val| try events.append(.{ .text = val });
        } else return error.Unexpected;
    }

    return events.toOwnedSlice();
}

const CONTAINER_ELEMENTS = [_][]const u8{
    "OFX",
    "SIGNONMSGSRSV1",
    "SONRS",
    "STATUS",
    "STMTTRNRS",
    "STMTTRN",
    "FI",
    "BANKACCTFROM",
    "BANKTRANLIST",
    "LEDGERBAL",
    "AVAILBAL",
    "STMTRS",
    "BANKMSGSRSV1",
    "CCACCTFROM",
    "CCSTMTRS",
    "CCSTMTTRNRS",
    "CREDITCARDMSGSRSV1",
    "CCACCTFROM",
};

const Aggregate = struct {
    name: Loc,
    children: []Event,
};

fn parseAggregate(allocator: std.mem.Allocator, src: []const u8, cursor: *Cursor) anyerror!?Aggregate {
    const start = cursor.*;
    errdefer cursor.* = start;

    _ = cursor.eat(.angle_start) orelse return error.UnexpectedToken;
    const start_name_token = cursor.eat(.text) orelse return error.UnexpectedToken;
    _ = cursor.eat(.angle_close) orelse return error.UnexpectedToken;

    var children = std.ArrayList(Event).init(allocator);
    defer children.deinit();
    while (cursor.peek()) |token| {
        switch (token.tag) {
            .eof, .close_ofx_tag => {
                cursor.* = start;
                return null;
            },

            .text => {
                try children.append(.{ .text = token.loc });
                _ = cursor.next();
            },
            .angle_start => {
                if (try parseAggregate(allocator, src, cursor)) |aggregate| {
                    defer allocator.free(aggregate.children);
                    try children.append(.{ .start_other = aggregate.name });
                    try children.appendSlice(aggregate.children);
                    try children.append(.{ .close_other = aggregate.name });
                } else if (try parseElement(src, cursor)) |element| {
                    try children.append(.{ .flat_element = element.name });
                    if (element.value) |val| try children.append(.{ .text = val });
                } else return error.Unexpected;
            },
            .angle_slash => {
                _ = cursor.next();
                const close_name_token = cursor.eat(.text) orelse return error.UnexpectedToken;
                _ = cursor.eat(.angle_close) orelse return error.UnexpectedToken;

                const is_match = std.mem.eql(u8, cursor.tokens[start_name_token].text(src), cursor.tokens[close_name_token].text(src));
                if (!is_match) {
                    cursor.* = start;
                    return null;
                }

                return Aggregate{
                    .name = cursor.tokens[start_name_token].loc,
                    .children = children.toOwnedSlice(),
                };
            },
            .start_ofx_tag, .angle_close => return error.InvalidSyntax,
        }
    }
    std.debug.panic("Shouldn't exit while loop; eof should come before end", .{});
}

const Element = struct {
    name: Loc,
    value: ?Loc,
};

fn parseElement(src: []const u8, cursor: *Cursor) !?Element {
    const start = cursor.*;
    errdefer cursor.* = start;

    _ = src;

    _ = cursor.eat(.angle_start) orelse return null;
    const name_tok_idx = cursor.eat(.text) orelse return null;
    _ = cursor.eat(.angle_close) orelse return null;

    const value_loc = mergeText(cursor);

    return Element{
        .name = cursor.tokens[name_tok_idx].loc,
        .value = value_loc,
    };
}

fn mergeText(cursor: *Cursor) ?Loc {
    const start = cursor.*;
    errdefer cursor.* = start;

    const first_tok_idx = cursor.eat(.text) orelse return null;
    var loc = cursor.tokens[first_tok_idx].loc;
    while (cursor.eat(.text)) |tok| {
        loc.end = cursor.tokens[tok].loc.end;
    }

    return loc;
}

const Cursor = struct {
    tokens: []Token,
    index: usize,

    pub fn peek(this: *@This()) ?Token {
        if (this.index >= this.tokens.len) return null;
        return this.tokens[this.index];
    }

    pub fn next(this: *@This()) ?Token {
        if (this.index >= this.tokens.len) return null;
        defer this.index += 1;
        return this.tokens[this.index];
    }

    pub fn eat(this: *@This(), expected_tag: Token.Tag) ?u32 {
        if (this.index >= this.tokens.len) return null;
        if (this.tokens[this.index].tag == expected_tag) {
            defer this.index += 1;
            return @intCast(u32, this.index);
        }
        return null;
    }
};

pub const Loc = struct {
    start: u32,
    end: u32,

    pub fn text(this: @This(), src: []const u8) []const u8 {
        return src[this.start..this.end];
    }
};

pub const Token = struct {
    loc: Loc,
    tag: Tag,

    pub const Tag = enum {
        eof,
        text,

        // `<OFX>`
        start_ofx_tag,
        // `</OFX>`
        close_ofx_tag,

        // `</`
        angle_slash,
        angle_start,
        angle_close,
    };

    pub fn text(this: @This(), src: []const u8) []const u8 {
        return this.loc.text(src);
    }

    pub fn fmtWithSrc(this: @This(), src: []const u8) FmtWithSrc {
        return FmtWithSrc{ .src = src, .token = this };
    }

    pub const FmtWithSrc = struct {
        src: []const u8,
        token: Token,

        pub fn format(
            this: @This(),
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            _ = fmt;
            _ = options;
            try writer.print("{s} \"{}\"", .{
                std.meta.tagName(this.token.tag),
                std.zig.fmtEscapes(this.token.text(this.src)),
            });
        }
    };
};

pub fn tokenize(allocator: std.mem.Allocator, src: []const u8) ![]Token {
    var tokens = std.ArrayList(Token).init(allocator);
    defer tokens.deinit();

    var pos: u32 = 0;
    while (true) {
        var token = try getToken(src, pos);
        try tokens.append(token);

        pos = token.loc.end;
        if (token.tag == .eof) {
            break;
        }
    }

    return tokens.toOwnedSlice();
}

pub fn getToken(src: []const u8, pos: u32) !Token {
    const State = enum {
        default,
        lt,
        lt_o,
        lt_of,
        lt_ofx,
        lt_s,
        lt_s_o,
        lt_s_of,
        lt_s_ofx,
        text,
    };

    var state = State.default;
    var token = Token{
        .tag = .eof,
        .loc = .{
            .start = pos,
            .end = undefined,
        },
    };

    var i = pos;
    while (i < src.len) {
        const c = src[i];
        switch (state) {
            .default => switch (c) {
                '<' => {
                    token.tag = .angle_start;
                    i += 1;
                    token.loc.end = i;
                    state = .lt;
                },
                '>' => {
                    token.tag = .angle_close;
                    i += 1;
                    break;
                },
                '\r', '\n' => {
                    i += 1;
                    token.loc.start = i;
                },
                else => {
                    token.tag = .text;
                    state = .text;
                    i += 1;
                },
            },
            .lt => switch (c) {
                'O' => {
                    state = .lt_o;
                    i += 1;
                },
                '/' => {
                    state = .lt_s;
                    i += 1;
                    token.loc.end = i;
                    token.tag = .angle_slash;
                },
                else => return token,
            },
            .lt_o => switch (c) {
                'F' => {
                    state = .lt_of;
                    i += 1;
                },
                else => return token,
            },
            .lt_of => switch (c) {
                'X' => {
                    state = .lt_ofx;
                    i += 1;
                },
                else => return token,
            },
            .lt_ofx => switch (c) {
                '>' => {
                    token.tag = .start_ofx_tag;
                    i += 1;
                    token.loc.end = i;
                    return token;
                },
                else => return token,
            },
            .lt_s => switch (c) {
                'O' => {
                    state = .lt_s_o;
                    i += 1;
                },
                else => return token,
            },
            .lt_s_o => switch (c) {
                'F' => {
                    state = .lt_s_of;
                    i += 1;
                },
                else => return token,
            },
            .lt_s_of => switch (c) {
                'X' => {
                    state = .lt_s_ofx;
                    i += 1;
                },
                else => return token,
            },
            .lt_s_ofx => switch (c) {
                '>' => {
                    token.tag = .close_ofx_tag;
                    i += 1;
                    token.loc.end = i;
                    return token;
                },
                else => return token,
            },
            .text => switch (c) {
                '<', '>', '\r', '\n' => break,
                else => i += 1,
            },
        }
    }

    token.loc.end = i;

    return token;
}

test "parse empty <OFX>" {
    try testEncodeDecode(.{
        .names = "OFX",
        .events = &.{},
    });
}

test "parse <T><O>" {
    try testEncodeDecode(.{
        .names = "TO",
        .events = &.{
            .{ .start_other = .{ .start = 0, .end = 1 } },
            .{ .start_other = .{ .start = 1, .end = 2 } },
            .{ .close_other = .{ .start = 1, .end = 2 } },
            .{ .close_other = .{ .start = 0, .end = 1 } },
        },
    });
}

test "property: does not crash" {
    const AsciiDocument = proptest.String(u8, .{ .ranges = &.{
        .{ .list = "\n\t\r" },
        .{ .min_max = .{ ' ', '~' } },
    } });
    try proptest.run(@src(), .{}, []const u8, AsciiDocument.generator(), testDoesNotCrash);
}

fn testDoesNotCrash(test_case: []const u8) !void {
    // Run parse. Errors are fine, but it shouldn't crash.
    const events = parse(std.testing.allocator, test_case) catch return;
    defer std.testing.allocator.free(events);
}

test "property: encode decode" {
    try proptest.run(@src(), .{ .print_value = true }, RandDocument, RandDocument.generator(), testEncodeDecode);
}

fn testEncodeDecode(document: RandDocument) !void {
    var document_as_text = std.ArrayList(u8).init(std.testing.allocator);
    defer document_as_text.deinit();
    try renderRandDocumentAsText(document, document_as_text.writer());

    const parsed_events = try parse(std.testing.allocator, document_as_text.items);
    defer std.testing.allocator.free(parsed_events);

    const min = std.math.min(document.events.len, parsed_events.len);
    for (document.events[0..min]) |expected_event, i| {
        const parsed_event = parsed_events[i];
        if (!expected_event.eql(document.names, parsed_event, document_as_text.items)) {
            std.debug.print(
                \\Parsed events first differs at index {}
                \\
                \\Expected: {}
                \\  Parsed: {}
                \\
            , .{ i, expected_event.fmtWithSrc(document.names), parsed_event.fmtWithSrc(document_as_text.items) });
            return error.TestExpectedEqual;
        }
    }
    // TODO? Better way to compare?
    try std.testing.expectEqual(document.events.len, parsed_events.len);
}

fn renderRandDocumentAsText(doc: RandDocument, writer: anytype) !void {
    try writer.writeAll("<OFX>\n");
    for (doc.events) |event| {
        try event.renderSGML(doc.names, writer);
        try writer.writeAll("\n");
    }
    try writer.writeAll("</OFX>\n");
}

const RandDocument = struct {
    names: []const u8,
    events: []const Event,

    fn generator() proptest.Generator(@This()) {
        return .{
            .create = create,
            .destroy = destroy,
            .shrink = shrink,
            .print = print,
        };
    }

    fn create(allocator: std.mem.Allocator, rand: std.rand.Random) !@This() {
        var names = std.ArrayList(u8).init(allocator);
        var events = std.ArrayList(Event).init(allocator);
        defer {
            names.deinit();
            events.deinit();
        }

        const num_children = rand.intRangeAtMost(usize, 1, 4);
        var i: usize = 0;
        while (i < num_children) : (i += 1) {
            if (rand.boolean()) {
                try randAggregate(rand, &names, &events, 16);
            } else {
                try randElement(rand, &names, &events);
            }
        }

        return RandDocument{
            .names = names.toOwnedSlice(),
            .events = events.toOwnedSlice(),
        };
    }

    fn destroy(this: @This(), allocator: std.mem.Allocator) void {
        allocator.free(this.events);
        allocator.free(this.names);
    }

    const Tactic = enum(u32) {
        remove_text,
        remove_last_half,
        remove_first_aggregate,
        remove_last_aggregate,
        simplify_other_tags,
        simplify_other_tags1,
        simplify_other_tags2,
        simplify_other_tags3,
        simplify_other_tags4,
        simplify_other_tags5,
        simplify_other_tags6,
        simplify_other_tags7,
        simplify_other_tags8,
        _,
    };

    fn shrink(this: @This(), allocator: std.mem.Allocator, rand: std.rand.Random, tactic: u32) !proptest.Result(@This()) {
        const Res = proptest.Result(@This());

        var new_names = std.ArrayList(u8).init(allocator);
        var new_events = std.ArrayList(Event).init(allocator);
        defer {
            new_names.deinit();
            new_events.deinit();
        }
        switch (@intToEnum(Tactic, tactic)) {
            .remove_text => {
                try new_names.appendSlice(this.names);
                for (this.events) |event| {
                    if (event == .text) continue;
                    try new_events.append(event);
                }
            },
            .remove_last_half => {
                try new_names.appendSlice(this.names);

                var stack = std.ArrayList(Event).init(allocator);
                defer stack.deinit();

                for (this.events[0 .. this.events.len / 2]) |event| {
                    try new_events.append(event);
                    if (event.getClose()) |close_event| {
                        try stack.append(close_event);
                    } else if (event.isClose()) {
                        _ = stack.pop();
                    }
                }
                while (stack.popOrNull()) |close_event| {
                    try new_events.append(close_event);
                }
            },
            .simplify_other_tags,
            .simplify_other_tags1,
            .simplify_other_tags2,
            .simplify_other_tags3,
            .simplify_other_tags4,
            .simplify_other_tags5,
            .simplify_other_tags6,
            .simplify_other_tags7,
            .simplify_other_tags8,
            => {
                var loc_updates = std.AutoHashMap(Loc, Loc).init(allocator);
                defer loc_updates.deinit();

                for (this.events) |event| {
                    switch (event) {
                        // Values
                        .text,
                        .fid,
                        .org,
                        .curdef,
                        .bankid,
                        .acctid,
                        .accttype,
                        .trntype,
                        .dtposted,
                        .trnamt,
                        .fitid,
                        .name,
                        .memo,
                        .balamt,
                        .dtasof,
                        => |loc| {
                            const gop = try loc_updates.getOrPut(loc);
                            if (!gop.found_existing) {
                                if (std.mem.eql(u8, "OFX", loc.text(this.names))) {
                                    gop.value_ptr.* = try appendTextLoc(&new_names, loc.text("OFX"));
                                } else switch (try ValueText.shrink(loc.text(this.names), allocator, rand, tactic - @enumToInt(Tactic.simplify_other_tags))) {
                                    .shrunk => |value_text_simplified| {
                                        defer allocator.free(value_text_simplified);
                                        gop.value_ptr.* = try appendTextLoc(&new_names, value_text_simplified);
                                    },
                                    .dead_end, .no_more_tactics => {
                                        gop.value_ptr.* = try appendTextLoc(&new_names, loc.text(this.names));
                                    },
                                }
                            }
                            try new_events.append(switch (event) {
                                .text => .{ .text = gop.value_ptr.* },
                                .fid => .{ .fid = gop.value_ptr.* },
                                .org => .{ .org = gop.value_ptr.* },
                                .curdef => .{ .curdef = gop.value_ptr.* },
                                .bankid => .{ .bankid = gop.value_ptr.* },
                                .acctid => .{ .acctid = gop.value_ptr.* },
                                .accttype => .{ .accttype = gop.value_ptr.* },
                                .trntype => .{ .trntype = gop.value_ptr.* },
                                .dtposted => .{ .dtposted = gop.value_ptr.* },
                                .trnamt => .{ .trnamt = gop.value_ptr.* },
                                .fitid => .{ .fitid = gop.value_ptr.* },
                                .name => .{ .name = gop.value_ptr.* },
                                .memo => .{ .memo = gop.value_ptr.* },
                                .balamt => .{ .balamt = gop.value_ptr.* },
                                .dtasof => .{ .dtasof = gop.value_ptr.* },
                                else => std.debug.panic("nested switch should be a subset", .{}),
                            });
                        },

                        // Tags
                        .flat_element,
                        .start_other,
                        .close_other,
                        => |loc| {
                            const gop = try loc_updates.getOrPut(loc);
                            if (!gop.found_existing) {
                                if (std.mem.eql(u8, "OFX", loc.text(this.names))) {
                                    gop.value_ptr.* = try appendTextLoc(&new_names, loc.text("OFX"));
                                } else switch (try TagName.shrink(loc.text(this.names), allocator, rand, tactic - @enumToInt(Tactic.simplify_other_tags))) {
                                    .shrunk => |tag_name_simplified| {
                                        defer allocator.free(tag_name_simplified);
                                        gop.value_ptr.* = try appendTextLoc(&new_names, tag_name_simplified);
                                    },
                                    .dead_end, .no_more_tactics => {
                                        gop.value_ptr.* = try appendTextLoc(&new_names, loc.text(this.names));
                                    },
                                }
                            }
                            try new_events.append(switch (event) {
                                .flat_element => .{ .flat_element = gop.value_ptr.* },
                                .start_other => .{ .start_other = gop.value_ptr.* },
                                .close_other => .{ .close_other = gop.value_ptr.* },
                                else => std.debug.panic("nested switch should be a subset", .{}),
                            });
                        },
                        else => try new_events.append(event),
                    }
                }
            },

            .remove_first_aggregate => {
                var index: usize = 0;
                while (index < this.events.len and !this.events[index].isStart()) : (index += 1) {}

                var level: usize = 0;
                while (index < this.events.len) : (index += 1) {
                    if (this.events[index].isStart()) {
                        level += 1;
                    } else {
                        level -= 1;
                    }

                    if (level == 0) break;
                }

                try new_names.appendSlice(this.names);
                try new_events.appendSlice(this.events[index..]);
            },
            .remove_last_aggregate => {
                var index: usize = 0;
                while (index < this.events.len and !this.events[this.events.len - index - 1].isClose()) : (index += 1) {}

                var level: usize = 0;
                while (index < this.events.len) : (index += 1) {
                    if (this.events[this.events.len - index - 1].isClose()) {
                        level += 1;
                    } else {
                        level -= 1;
                    }

                    if (level == 0) break;
                }

                try new_names.appendSlice(this.names);
                try new_events.appendSlice(this.events[0..index]);
            },

            _ => return Res.no_more_tactics,
        }

        for (new_events.items) |new_event, i| {
            if (!new_event.eql(new_names.items, this.events[i], this.names)) {
                break;
            }
        } else {
            return Res.dead_end;
        }

        return Res{
            .shrunk = @This(){
                .names = new_names.toOwnedSlice(),
                .events = new_events.toOwnedSlice(),
            },
        };
    }

    fn print(this: @This()) void {
        const stderr = std.io.getStdErr();
        stderr.writer().print("events len = {}; names len = {}\n", .{ this.events.len, this.names.len }) catch return;
        renderRandDocumentAsText(this, stderr.writer()) catch return;
        stderr.writeAll("\n") catch return;
    }
};

/// Append some text to an arraylist and return the start and end as a location
fn appendTextLoc(out: *std.ArrayList(u8), text: []const u8) !Loc {
    const start = out.items.len;
    try out.appendSlice(text);
    const end = out.items.len;
    return Loc{ .start = @intCast(u32, start), .end = @intCast(u32, end) };
}

fn appendNLoc(out: *std.ArrayList(u8), n: usize) !Loc {
    const start = out.items.len;
    try out.appendNTimes(undefined, n);
    const end = out.items.len;
    return Loc{ .start = @intCast(u32, start), .end = @intCast(u32, end) };
}

fn randAggregate(rand: std.rand.Random, names: *std.ArrayList(u8), events: *std.ArrayList(Event), max_children: usize) anyerror!void {
    const name = try randTagName(rand, names);

    try events.append(.{ .start_other = name });

    const num_children = rand.uintAtMost(usize, max_children);
    var i: usize = 0;
    while (i < num_children) : (i += 1) {
        if (rand.boolean()) {
            try randAggregate(rand, names, events, max_children / 2);
        } else {
            try randElement(rand, names, events);
        }
    }

    try events.append(.{ .close_other = name });
}

fn randElement(rand: std.rand.Random, names: *std.ArrayList(u8), events: *std.ArrayList(Event)) !void {
    try events.append(.{ .flat_element = try randTagName(rand, names) });
    const value = try ValueText.create(names.allocator, rand);
    defer ValueText.destroy(value, names.allocator);
    try events.append(.{ .text = try appendTextLoc(names, value) });
}

fn randTagName(rand: std.rand.Random, names: *std.ArrayList(u8)) !Loc {
    const loc = try appendNLoc(names, rand.intRangeLessThan(u8, 1, 64));
    randTagNameText(rand, names.items[loc.start..loc.end]);
    return loc;
}

/// Fill the given buffer with random characters
fn randTagNameText(rand: std.rand.Random, buf: []u8) void {
    if (buf.len == 0) return;

    const NAME_START_CHARS = ":_" ++ rangeAtMostToArray(u8, 'A', 'Z');
    const NAME_CHARS = NAME_START_CHARS ++ "-." ++ rangeAtMostToArray(u8, '0', '9');

    buf[0] = randCodepoint(u8, rand, NAME_START_CHARS);

    for (buf) |*c| {
        c.* = randCodepoint(u8, rand, NAME_CHARS);
    }
}

const TagName = proptest.String(u8, .{
    .min_len = 1,
    .max_len = 64,
    .ranges = &.{
        .{ .min_max = .{ 'A', 'Z' } },
        .{ .min_max = .{ '0', '9' } },
        .{ .list = ":_-." },
    },
});

// All ASCII characters except the angle brackets (`<` and `>`, 0x3C and 0x3E)
const ValueText = proptest.String(u8, .{
    .min_len = 1,
    .max_len = 1000,
    .ranges = &.{
        .{ .min_max = .{ ' ', ';' } },
        .{ .min_max = .{ '?', '~' } },
        .{ .list = "\n\t\r=" },
    },
});

/// Fill a buffer with random elements from an alphabet, biasing towards the first 256 elements
fn randCodepoint(comptime T: type, rand: std.rand.Random, alphabet: []const T) T {
    if (alphabet.len <= 256 or rand.int(u3) == 0) {
        // 1/8th of the time, pull from the larger alphabet
        return alphabet[rand.uintLessThan(usize, alphabet.len)];
    } else {
        // 7/8th of the time, pull from the first 256 elements in alphabet
        return alphabet[0..256][rand.int(u8)];
    }
}

fn rangeAtMostToArray(comptime T: type, comptime min: T, comptime max: T) [max - min + 1]T {
    var buf: [max - min + 1]T = undefined;
    var value = min;
    for (buf) |*c| {
        c.* = value;
        value += 1;
    }
    return buf;
}

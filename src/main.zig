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

    // Skip text until we get to the first `<`
    while (cursor.eat(.text) catch null) |_| {}

    while (true) {
        if (cursor.eat(.eof)) |_| {
            break;
        } else |_| try parseContainerElement(src, &cursor, &events);
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

fn parseContainerElement(src: []const u8, cursor: *Cursor, events: *std.ArrayList(Event)) anyerror!void {
    const start = cursor.*;
    const events_start_len = events.items.len;
    errdefer {
        cursor.* = start;
        events.shrinkRetainingCapacity(events_start_len);
    }

    const element_start_event = try parseElementStart(src, cursor, events);

    if (events.items[element_start_event] == .start_other) {
        const element_name = events.items[element_start_event].start_other.text(src);

        for (CONTAINER_ELEMENTS) |container_name| {
            if (std.mem.eql(u8, element_name, container_name)) {
                break;
            }
        } else return error.NotAContainerElement;
    }

    while (cursor.peek()) |token| {
        switch (token.tag) {
            .eof => break,

            .text, .forward_slash => {
                try events.append(.{ .text = token.loc });
                _ = cursor.next();
            },
            .angle_start => {
                const before_parse_end = cursor.*;
                const before_parse_end_events_len = events.items.len;
                errdefer {
                    cursor.* = before_parse_end;
                    events.shrinkRetainingCapacity(before_parse_end_events_len);
                }

                if (parseElementEnd(src, cursor, events)) |element_end_event| {
                    const start_element = events.items[element_start_event];
                    const close_element = events.items[element_end_event];
                    const is_match = switch (start_element) {
                        .start_stmttrn => close_element == .close_stmttrn,
                        .start_fi => close_element == .close_fi,
                        .start_balance => close_element == .close_balance and start_element.start_balance == close_element.close_balance,
                        .start_other => |loc| close_element == .close_other and std.mem.eql(u8, loc.text(src), close_element.close_other.text(src)),
                        else => false,
                    };
                    if (!is_match) {
                        std.debug.print("{s}:{} element doesn't match ({} != {})\n", .{ @src().file, @src().line, start_element, close_element });
                        try events.append(events.items[element_end_event]);
                        events.items[element_end_event] = start_element.getClose().?;
                    }
                    break;
                } else |_| if (parseContainerElement(src, cursor, events)) {
                    //
                } else |_| if (parsePropertyElement(src, cursor, events)) |_| {
                    //
                } else |_| if (parseFlatElement(src, cursor, events)) {
                    //
                } else |e| {
                    return e;
                }
            },
            .angle_close => return error.InvalidSyntax,
        }
    }
}

fn parsePropertyElement(src: []const u8, cursor: *Cursor, events: *std.ArrayList(Event)) anyerror!u32 {
    const start = cursor.*;
    const events_start_len = events.items.len;
    errdefer {
        cursor.* = start;
        events.shrinkRetainingCapacity(events_start_len);
    }
    _ = src;

    _ = try cursor.eat(.angle_start);
    const element_name_token_idx = try cursor.eat(.text);
    _ = try cursor.eat(.angle_close);
    const value_loc = try mergeText(src, cursor, events);

    const element_name = cursor.tokens[element_name_token_idx].loc.text(src);
    const event_index = events.items.len;
    if (std.mem.eql(u8, element_name, "TRNTYPE")) {
        try events.append(.{ .trntype = value_loc });
    } else if (std.mem.eql(u8, element_name, "DTPOSTED")) {
        try events.append(.{ .dtposted = value_loc });
    } else if (std.mem.eql(u8, element_name, "TRNAMT")) {
        try events.append(.{ .trnamt = value_loc });
    } else if (std.mem.eql(u8, element_name, "FITID")) {
        try events.append(.{ .fitid = value_loc });
    } else if (std.mem.eql(u8, element_name, "NAME")) {
        try events.append(.{ .name = value_loc });
    } else if (std.mem.eql(u8, element_name, "MEMO")) {
        try events.append(.{ .memo = value_loc });
    } else if (std.mem.eql(u8, element_name, "BANKID")) {
        try events.append(.{ .bankid = value_loc });
    } else if (std.mem.eql(u8, element_name, "ACCTID")) {
        try events.append(.{ .acctid = value_loc });
    } else if (std.mem.eql(u8, element_name, "ACCTTYPE")) {
        try events.append(.{ .accttype = value_loc });
    } else if (std.mem.eql(u8, element_name, "CURDEF")) {
        try events.append(.{ .curdef = value_loc });
    } else if (std.mem.eql(u8, element_name, "BALAMT")) {
        try events.append(.{ .balamt = value_loc });
    } else if (std.mem.eql(u8, element_name, "DTASOF")) {
        try events.append(.{ .dtasof = value_loc });
    } else if (std.mem.eql(u8, element_name, "ORG")) {
        try events.append(.{ .org = value_loc });
    } else if (std.mem.eql(u8, element_name, "FID")) {
        try events.append(.{ .fid = value_loc });
    } else {
        return error.UnrecognizedPropertyName;
    }
    return @intCast(u32, event_index);
}

fn mergeText(src: []const u8, cursor: *Cursor, events: *std.ArrayList(Event)) anyerror!Loc {
    const start = cursor.*;
    const events_start_len = events.items.len;
    errdefer {
        cursor.* = start;
        events.shrinkRetainingCapacity(events_start_len);
    }
    _ = src;

    const first_tok_idx = cursor.eat(.text) catch cursor.eat(.forward_slash) catch |e| return e;
    var loc = cursor.tokens[first_tok_idx].loc;
    while (cursor.peek()) |tok| {
        switch (tok.tag) {
            .text, .forward_slash => {
                _ = cursor.next();
                loc.end = tok.loc.end;
            },
            else => break,
        }
    }

    return loc;
}

fn parseFlatElement(src: []const u8, cursor: *Cursor, events: *std.ArrayList(Event)) anyerror!void {
    const start = cursor.*;
    const events_start_len = events.items.len;
    errdefer {
        cursor.* = start;
        events.shrinkRetainingCapacity(events_start_len);
    }
    _ = src;

    _ = try cursor.eat(.angle_start);
    const element_name_token_idx = try cursor.eat(.text);
    _ = try cursor.eat(.angle_close);

    try events.append(.{ .flat_element = cursor.tokens[element_name_token_idx].loc });

    while (cursor.peek()) |token| {
        switch (token.tag) {
            .eof, .angle_start => break,

            .text, .forward_slash => {
                try events.append(.{ .text = token.loc });
                _ = cursor.next();
            },

            .angle_close => return error.InvalidSyntax,
        }
    }
}

fn parseElementStart(src: []const u8, cursor: *Cursor, events: *std.ArrayList(Event)) !u32 {
    const start = cursor.*;
    const events_start_len = events.items.len;
    errdefer {
        cursor.* = start;
        events.shrinkRetainingCapacity(events_start_len);
    }
    _ = src;

    _ = try cursor.eat(.angle_start);
    const element_name_token_idx = try cursor.eat(.text);
    _ = try cursor.eat(.angle_close);

    const event_index = events.items.len;
    const element_name = cursor.tokens[element_name_token_idx].text(src);
    if (std.mem.eql(u8, element_name, "STMTTRN")) {
        try events.append(.start_stmttrn);
    } else if (std.mem.eql(u8, element_name, "LEDGERBAL")) {
        try events.append(.{ .start_balance = .ledger });
    } else if (std.mem.eql(u8, element_name, "AVAILBAL")) {
        try events.append(.{ .start_balance = .available });
    } else if (std.mem.eql(u8, element_name, "FI")) {
        try events.append(.start_fi);
    } else {
        try events.append(.{ .start_other = cursor.tokens[element_name_token_idx].loc });
    }

    return @intCast(u32, event_index);
}

fn parseElementEnd(src: []const u8, cursor: *Cursor, events: *std.ArrayList(Event)) !u32 {
    const start = cursor.*;
    const events_start_len = events.items.len;
    errdefer {
        cursor.* = start;
        events.shrinkRetainingCapacity(events_start_len);
    }
    _ = src;

    _ = try cursor.eat(.angle_start);
    _ = try cursor.eat(.forward_slash);
    const element_name_token_idx = try cursor.eat(.text);
    _ = try cursor.eat(.angle_close);

    const event_index = events.items.len;
    const element_name = cursor.tokens[element_name_token_idx].text(src);
    if (std.mem.eql(u8, element_name, "STMTTRN")) {
        try events.append(.close_stmttrn);
    } else if (std.mem.eql(u8, element_name, "LEDGERBAL")) {
        try events.append(.{ .close_balance = .ledger });
    } else if (std.mem.eql(u8, element_name, "AVAILBAL")) {
        try events.append(.{ .close_balance = .available });
    } else if (std.mem.eql(u8, element_name, "FI")) {
        try events.append(.close_fi);
    } else {
        try events.append(.{ .close_other = cursor.tokens[element_name_token_idx].loc });

        for (CONTAINER_ELEMENTS) |container_name| {
            if (std.mem.eql(u8, element_name, container_name)) {
                break;
            }
        } else {
            std.debug.print("Element name {s} not in list of containers!\n", .{element_name});
        }
    }

    return @intCast(u32, event_index);
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

    pub fn eat(this: *@This(), expected_tag: Token.Tag) !u32 {
        if (this.index >= this.tokens.len) return error.UnexpectedEOF;
        if (this.tokens[this.index].tag == expected_tag) {
            defer this.index += 1;
            return @intCast(u32, this.index);
        }
        return error.UnexpectedToken;
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
        forward_slash,
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
                    break;
                },
                '>' => {
                    token.tag = .angle_close;
                    i += 1;
                    break;
                },
                '/' => {
                    token.tag = .forward_slash;
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
            .text => switch (c) {
                '<', '/', '>', '\r', '\n' => break,
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
        .events = &.{
            .{ .start_other = Loc{ .start = 0, .end = 3 } },
            .{ .close_other = Loc{ .start = 0, .end = 3 } },
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
    try proptest.run(@src(), .{}, RandDocument, RandDocument.generator(), testEncodeDecode);
}

fn testEncodeDecode(document: RandDocument) !void {
    const document_as_text = try renderRandDocumentAsText(std.testing.allocator, document);
    defer std.testing.allocator.free(document_as_text);

    const parsed_events = try parse(std.testing.allocator, document_as_text);
    defer std.testing.allocator.free(parsed_events);

    try std.testing.expectEqual(document.events.len, parsed_events.len);
    for (document.events) |expected_event, i| {
        const parsed_event = parsed_events[i];
        if (!expected_event.eql(document.names, parsed_event, document_as_text)) {
            std.debug.print(
                \\Parsed events first differs at index {}
                \\
                \\Expected: {}
                \\  Parsed: {}
                \\
            , .{ i, expected_event.fmtWithSrc(document.names), parsed_event.fmtWithSrc(document_as_text) });
            return error.TestExpectedEqual;
        }
    }
}

fn renderRandDocumentAsText(allocator: std.mem.Allocator, doc: RandDocument) ![]const u8 {
    var src = std.ArrayList(u8).init(allocator);
    defer src.deinit();
    for (doc.events) |event| {
        try event.renderSGML(doc.names, src.writer());
    }
    return src.toOwnedSlice();
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

        const ofx_loc = try appendTextLoc(&names, "OFX");
        try events.append(.{ .start_other = ofx_loc });

        const num_children = rand.intRangeAtMost(usize, 1, 4);
        var i: usize = 0;
        while (i < num_children) : (i += 1) {
            if (rand.boolean()) {
                try randAggregate(rand, &names, &events, 32);
            } else {
                try randElement(rand, &names, &events);
            }
        }

        try events.append(.{ .close_other = ofx_loc });

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
        remove_all_but_first_two,
        remove_text,
        remove_last_half,
        simplify_other_tags,
        simplify_other_tags1,
        simplify_other_tags2,
        _,
    };

    fn shrink(this: @This(), allocator: std.mem.Allocator, rand: std.rand.Random, tactic: u32) !proptest.Result(@This()) {
        const Res = proptest.Result(@This());
        if (this.events.len <= 2) return Res.no_more_tactics;

        var new_names = std.ArrayList(u8).init(allocator);
        defer new_names.deinit();
        var new_events = std.ArrayList(Event).init(allocator);
        defer new_events.deinit();
        switch (@intToEnum(Tactic, tactic)) {
            .remove_all_but_first_two => {
                if (this.events.len <= 4) return Res.dead_end;
                try new_names.appendSlice(this.names);
                var level: usize = 0;
                for (this.events[0..2]) |event| {
                    try new_events.append(event);
                    if (event.isStart()) {
                        level += 1;
                    } else if (event.isClose()) {
                        level -= 1;
                    }
                }

                var next_level_to_keep = level - 1;
                for (this.events[2..]) |event| {
                    if (event.isStart()) {
                        level += 1;
                    } else if (event.isClose()) {
                        level -= 1;
                    }
                    if (level == next_level_to_keep) {
                        try new_events.append(event);
                        next_level_to_keep -|= 1;
                    }
                }
            },
            .remove_text => {
                try new_names.appendSlice(this.names);
                for (this.events) |event| {
                    if (event == .text) continue;
                    try new_events.append(event);
                }
            },
            .remove_last_half => {
                try new_names.appendSlice(this.names);
                var level: usize = 0;
                for (this.events[0 .. this.events.len / 2]) |event| {
                    if (event.isStart()) {
                        level += 1;
                    } else if (event.isClose()) {
                        level -= 1;
                    }
                }

                try new_events.appendSlice(this.events[0 .. this.events.len / 2]);

                var next_level_to_keep = level - 1;
                for (this.events[this.events.len / 2 ..]) |event| {
                    if (event.isStart()) {
                        level += 1;
                    } else if (event.isClose()) {
                        level -= 1;
                    }
                    if (level == next_level_to_keep) {
                        try new_events.append(event);
                        next_level_to_keep -|= 1;
                    }
                }
            },
            .simplify_other_tags, .simplify_other_tags1, .simplify_other_tags2 => {
                var loc_updates = std.AutoHashMap(Loc, Loc).init(allocator);
                defer loc_updates.deinit();

                for (this.events) |event| {
                    switch (event) {
                        .flat_element,
                        .text,
                        .start_other,
                        .close_other,
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
                                } else switch (try TagName.shrink(loc.text(this.names), allocator, rand, tactic - @enumToInt(Tactic.simplify_other_tags))) {
                                    .shrunk => |tag_name_simplified| {
                                        gop.value_ptr.* = try appendTextLoc(&new_names, tag_name_simplified);
                                        allocator.free(tag_name_simplified);
                                    },
                                    .dead_end, .no_more_tactics => {
                                        gop.value_ptr.* = try appendTextLoc(&new_names, loc.text(this.names));
                                    },
                                }
                            }
                            try new_events.append(switch (event) {
                                .flat_element => .{ .flat_element = gop.value_ptr.* },
                                .text => .{ .text = gop.value_ptr.* },
                                .start_other => .{ .start_other = gop.value_ptr.* },
                                .close_other => .{ .close_other = gop.value_ptr.* },
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
                        else => try new_events.append(event),
                    }
                }
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

        std.debug.assert(std.mem.eql(u8, "OFX", new_events.items[0].start_other.text(this.names)));
        std.debug.assert(std.mem.eql(u8, "OFX", new_events.items[new_events.items.len - 1].close_other.text(this.names)));
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
        for (this.events) |event| {
            event.renderSGML(this.names, stderr.writer()) catch return;
            //stderr.writer().print(" ; {} ; {}", .{ event, event.fmtWithSrc(this.names) }) catch return;
            stderr.writeAll("\n") catch return;
        }
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
    try events.append(.{ .flat_element = try randValue(rand, names) });
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
    .ranges = &.{
        .{ .min_max = .{ 'A', 'Z' } },
        .{ .min_max = .{ '0', '9' } },
        .{ .list = ":_-." },
    },
});

fn randValue(rand: std.rand.Random, names: *std.ArrayList(u8)) !Loc {
    const loc = try appendNLoc(names, rand.intRangeLessThan(usize, 1, 1024));
    try randValueText(rand, names.items[loc.start..loc.end]);
    return loc;
}

fn randValueText(rand: std.rand.Random, buf: []u8) !void {
    // All ASCII characters except the angle brackets (`<` and `>`, 0x3C and 0x3E)
    const VALUE_ALPHABET = "\n\t\r=" ++
        rangeAtMostToArray(u8, ' ', ';') ++
        rangeAtMostToArray(u8, '?', '~');

    for (buf) |*c| {
        c.* = randCodepoint(u8, rand, VALUE_ALPHABET);
    }
}

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

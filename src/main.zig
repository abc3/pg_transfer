const std = @import("std");
const pg = @import("pg");
const clap = @import("clap");

const CONNECTION_TIMEOUT_MS: u32 = 30_000;

const Config = struct {
    workers: u32,
    source_conn_str: []const u8,
    dest_conn_str: []const u8,
    table_name: []const u8,
    buffer_mb: u32,
    batch_size: usize,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) !Self {
        const params = comptime clap.parseParamsComptime(
            \\-h, --help                    Display this help and exit.
            \\-w, --workers <u32>           Number of worker threads (default: CPU count).
            \\-s, --source <str>            Source Postgres connection string.
            \\-d, --destination <str>       Destination Postgres connection string.
            \\-t, --table <str>             Table name to export.
            \\-b, --buffer <u32>            Batch buffer size in MB (default: 1).
            \\
        );

        var diag = clap.Diagnostic{};
        var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
            .diagnostic = &diag,
            .allocator = allocator,
        }) catch |err| switch (err) {
            error.InvalidArgument, error.MissingValue => {
                diag.report(std.io.getStdErr().writer(), err) catch {};
                return err;
            },
            else => return err,
        };
        defer res.deinit();

        if (res.args.help != 0) {
            try clap.help(std.io.getStdErr().writer(), clap.Help, &params, .{});
            std.process.exit(0);
        }

        const source_conn_str = res.args.source orelse {
            std.debug.print("Error: --source connection string is required\n", .{});
            return error.MissingSourceConnection;
        };

        const dest_conn_str = res.args.destination orelse {
            std.debug.print("Error: --destination connection string is required\n", .{});
            return error.MissingDestinationConnection;
        };

        const table_name = res.args.table orelse {
            std.debug.print("Error: --table name is required\n", .{});
            return error.MissingTableName;
        };

        const default_workers = @as(u32, @intCast(std.Thread.getCpuCount() catch 4));
        const workers = res.args.workers orelse default_workers;

        const buffer_mb = res.args.buffer orelse 1;
        const batch_size = @as(usize, buffer_mb) * 1024 * 1024;

        return Self{
            .workers = workers,
            .source_conn_str = source_conn_str,
            .dest_conn_str = dest_conn_str,
            .table_name = table_name,
            .buffer_mb = buffer_mb,
            .batch_size = batch_size,
        };
    }

    pub fn print(self: Self) void {
        std.debug.print("Starting with {} workers\n", .{self.workers});
        std.debug.print("Source: {s}\n", .{self.source_conn_str});
        std.debug.print("Destination: {s}\n", .{self.dest_conn_str});
        std.debug.print("Table: {s}\n", .{self.table_name});
        std.debug.print("Buffer size: {}MB\n", .{self.buffer_mb});
    }
};

const DBHandler = struct {
    source_pool: *pg.Pool,
    dest_pool: *pg.Pool,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        const source_uri = std.Uri.parse(config.source_conn_str) catch |e| {
            std.debug.print("Source URI parse error: {}\n", .{e});
            return e;
        };
        const dest_uri = std.Uri.parse(config.dest_conn_str) catch |e| {
            std.debug.print("Destination URI parse error: {}\n", .{e});
            return e;
        };

        var source_pool = try pg.Pool.initUri(allocator, source_uri, @as(u16, @intCast(config.workers)), CONNECTION_TIMEOUT_MS);
        errdefer source_pool.deinit();

        var dest_pool = try pg.Pool.initUri(allocator, dest_uri, @as(u16, @intCast(config.workers)), CONNECTION_TIMEOUT_MS);
        errdefer dest_pool.deinit();

        return Self{
            .source_pool = source_pool,
            .dest_pool = dest_pool,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.source_pool.deinit();
        self.dest_pool.deinit();
    }

    pub fn getTablePages(self: *Self, table_name: []const u8) !u32 {
        const pages_query =
            "SELECT (pg_relation_size($1::regclass) / current_setting('block_size')::int) AS pages";

        var pages_result = try self.source_pool.query(pages_query, .{table_name});
        defer pages_result.deinit();

        var total_pages: u32 = 0;
        if (try pages_result.next()) |row| {
            total_pages = @intCast(row.get(i64, 0));
        }

        return total_pages;
    }
};

fn sendSimpleQuery(alloc: std.mem.Allocator, conn: anytype, sql: []const u8) !void {
    const msg_len: u32 = @intCast(4 + sql.len + 1);
    const total_len: usize = 1 + @as(usize, msg_len);
    var query_msg = try alloc.alloc(u8, total_len);
    defer alloc.free(query_msg);

    query_msg[0] = 'Q';
    std.mem.writeInt(u32, query_msg[1..5], msg_len, .big);
    @memcpy(query_msg[5 .. 5 + sql.len], sql);
    query_msg[5 + sql.len] = 0;

    try conn.write(query_msg);
}

const Worker = struct {
    worker_id: u32,
    total_pages: u32,
    pages_per_worker: u32,
    remainder: u32,
    source_pool: *pg.Pool,
    dest_pool: *pg.Pool,
    allocator: std.mem.Allocator,
    buffer: []u8,
    table_name: []const u8,
    batch_size: usize,
};

fn workerThread(worker: Worker) !void {
    const start_page = worker.worker_id * worker.pages_per_worker + @min(worker.worker_id, worker.remainder);
    var end_page = start_page + worker.pages_per_worker;
    if (worker.worker_id < worker.remainder) {
        end_page += 1;
    }

    end_page = @min(end_page, worker.total_pages);

    std.debug.print("Worker {} processing pages {} to {} (total: {})\n", .{ worker.worker_id, start_page, end_page, end_page - start_page });

    if (start_page >= end_page) {
        std.debug.print("Worker {} has no work to do\n", .{worker.worker_id});
        return;
    }

    var source_conn = try worker.source_pool.acquire();
    defer source_conn.release();
    var dest_conn = try worker.dest_pool.acquire();
    defer dest_conn.release();

    _ = source_conn.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", .{}) catch |e| {
        std.debug.print("Failed to start source transaction: {}\n", .{e});
        return;
    };

    // TODO: Maybe set Postgres optimizations, did not make any difference so far ¯\_(ツ)_/¯
    // _ = dest_conn.exec("SET synchronous_commit = off", .{}) catch {};
    // _ = dest_conn.exec("SET work_mem = '256MB'", .{}) catch {};
    // _ = source_conn.exec("SET work_mem = '256MB'", .{}) catch {};

    const copy_sql = try std.fmt.allocPrint(worker.allocator,
        \\COPY (
        \\    SELECT * FROM {s} 
        \\    WHERE ctid >= '({},0)' 
        \\    AND ctid < '({},0)'
        \\) TO STDOUT WITH (FORMAT binary)
    , .{ worker.table_name, start_page, end_page });
    defer worker.allocator.free(copy_sql);
    try sendSimpleQuery(worker.allocator, source_conn, copy_sql);

    const dest_copy_sql = try std.fmt.allocPrint(worker.allocator, "COPY {s} FROM STDIN WITH (FORMAT binary)", .{worker.table_name});
    defer worker.allocator.free(dest_copy_sql);
    try sendSimpleQuery(worker.allocator, dest_conn, dest_copy_sql);

    while (true) {
        const m = dest_conn.read() catch |e| {
            std.debug.print("dest read error: {}\n", .{e});
            return;
        };
        if (m.type == PgMessageType.CopyInResponse) break;
    }

    var frames_processed: usize = 0;
    var batch_buffer = worker.buffer;
    var buffer_pos: usize = 0;

    while (true) {
        const msg = source_conn.read() catch {
            break;
        };

        switch (msg.type) {
            PgMessageType.CopyData => {
                const payload = msg.data;
                frames_processed += 1;
                if (payload.len == 0) continue;

                const header_size = 5;
                const total_msg_size = header_size + payload.len;

                if (total_msg_size > worker.batch_size) {
                    if (buffer_pos > 0) {
                        dest_conn.write(batch_buffer[0..buffer_pos]) catch |e| {
                            std.debug.print("batch write error: {}\n", .{e});
                            return;
                        };
                        buffer_pos = 0;
                    }
                    var header: [5]u8 = .{ PgMessageType.CopyData, 0, 0, 0, 0 };
                    const len_u32_direct: u32 = @intCast(4 + payload.len);
                    std.mem.writeInt(u32, header[1..5], len_u32_direct, .big);
                    dest_conn.write(&header) catch |e| {
                        std.debug.print("direct header write error: {}\n", .{e});
                        return;
                    };
                    dest_conn.write(payload) catch |e| {
                        std.debug.print("direct payload write error: {}\n", .{e});
                        return;
                    };
                    continue;
                }

                if (buffer_pos + total_msg_size > worker.batch_size) {
                    if (buffer_pos > 0) {
                        dest_conn.write(batch_buffer[0..buffer_pos]) catch |e| {
                            std.debug.print("batch write error: {}\n", .{e});
                            return;
                        };
                        buffer_pos = 0;
                    }
                }

                batch_buffer[buffer_pos] = PgMessageType.CopyData;
                const len_u32: u32 = @intCast(4 + payload.len);
                std.mem.writeInt(u32, batch_buffer[buffer_pos + 1 .. buffer_pos + 5][0..4], len_u32, .big);
                buffer_pos += header_size;

                @memcpy(batch_buffer[buffer_pos .. buffer_pos + payload.len], payload);
                buffer_pos += payload.len;
            },
            PgMessageType.ReadyForQuery => {
                if (buffer_pos > 0) {
                    dest_conn.write(batch_buffer[0..buffer_pos]) catch |e| {
                        std.debug.print("final batch write error: {}\n", .{e});
                        return;
                    };
                }
                break;
            },
            else => {},
        }
    }

    var done_msg: [5]u8 = .{ PgMessageType.CopyDoneDest, 0, 0, 0, 4 };
    dest_conn.write(&done_msg) catch |e| {
        std.debug.print("CopyDone error: {}\n", .{e});
        return;
    };

    while (true) {
        const m = dest_conn.read() catch {
            break;
        };
        if (m.type == PgMessageType.ReadyForQuery) break;
    }

    std.debug.print("Worker {} completed: {} pages, {} frames\n", .{ worker.worker_id, end_page - start_page, frames_processed });

    _ = source_conn.exec("COMMIT", .{}) catch |e| {
        std.debug.print("Failed to commit source transaction: {}\n", .{e});
    };
}

const PgMessageType = struct {
    const CopyData = 'd';
    const CopyDoneSource = 'c';
    const CopyDoneDest = 'c';
    const CopyFail = 'f';
    const CopyInResponse = 'G';
    const CopyOutResponse = 'H';
    const CommandComplete = 'C';
    const ReadyForQuery = 'Z';
};

fn transferData(allocator: std.mem.Allocator, config: Config, db_handler: *DBHandler, total_pages: u32) !void {
    const pages_per_worker = total_pages / config.workers;
    const remainder_pages = total_pages % config.workers;

    std.debug.print("Total pages: {}, {} pages per worker (+ remainder: {})\n", .{ total_pages, pages_per_worker, remainder_pages });

    var worker_buffers = try allocator.alloc([]u8, config.workers);
    defer {
        for (worker_buffers) |buffer| {
            allocator.free(buffer);
        }
        allocator.free(worker_buffers);
    }

    for (0..config.workers) |i| {
        worker_buffers[i] = try allocator.alloc(u8, config.batch_size);
    }

    std.debug.print("Pre-allocated {} buffers of {}MB each\n", .{ config.workers, config.batch_size / (1024 * 1024) });

    var threads = try allocator.alloc(std.Thread, config.workers);
    defer allocator.free(threads);

    var i: u32 = 0;
    while (i < config.workers) : (i += 1) {
        const worker = Worker{
            .worker_id = i,
            .total_pages = total_pages,
            .pages_per_worker = pages_per_worker,
            .remainder = remainder_pages,
            .source_pool = db_handler.source_pool,
            .dest_pool = db_handler.dest_pool,
            .allocator = allocator,
            .buffer = worker_buffers[i],
            .table_name = config.table_name,
            .batch_size = config.batch_size,
        };
        threads[i] = try std.Thread.spawn(.{}, workerThread, .{worker});
    }

    for (threads) |thread| {
        thread.join();
    }

    std.debug.print("All workers completed\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config = try Config.init(allocator);
    config.print();

    var db_handler = try DBHandler.init(allocator, config);
    defer db_handler.deinit();

    const total_pages = try db_handler.getTablePages(config.table_name);
    std.debug.print("Total pages: {}\n", .{total_pages});

    if (total_pages == 0) {
        std.debug.print("No pages found or table is empty\n", .{});
        return;
    }

    try transferData(allocator, config, &db_handler, total_pages);
}

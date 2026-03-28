# discodb

A relational database implemented entirely on top of Discord's primitives.

## What is discodb?

discodb treats Discord as a distributed storage backend:

- **Messages** → Row storage / WAL entries
- **Channels** → Segments (heap files)
- **Categories** → Table namespaces
- **Roles** → Free Space Map (tracking free slots)
- **Forum channels** → B-Tree indexes
- **Threads** → TOAST/overflow storage
- **Attachments** → Blob storage

Built in Go, discodb exposes a PostgreSQL-compatible wire protocol, so you can connect using `psql`:

```bash
psql -h localhost -d discorddb
```

## Key Characteristics

- **Log-structured**: Append-only design, not heap-based
- **Eventually consistent**: Built on Discord's eventual consistency with transactional overlays
- **API-bound**: All I/O is network-bound and rate-limited
- **WAL-first**: Write-Ahead Logging ensures durability and recoverability
- **MVCC**: Multi-Version Concurrency Control via append-only row versions

## Why?

It sounded funny, I won't lie. Also, Discord provides primitives that, while not designed for database use, can be leveraged as storage surface:

| Discord Primitive | Database Concept |
|------------------|------------------|
| Guild | Database |
| Category | Table namespace |
| Channel | Segment (heap file) |
| Message | Row / WAL entry |
| Embed | Column storage |
| Thread | TOAST / overflow |
| Attachment | Blob storage |
| Role | FSM page |
| Forum Channel | B-Tree index |

The challenge is building correctness guarantees (ACID properties) on top of an API that wasn't designed for this purpose.

## Architecture

```
Client (psql)
    ↓
Wire Protocol Layer
    ↓
SQL Layer (Parser / Planner / Optimizer)
    ↓
Execution Engine (Pull-based iterators)
    ↓
Storage Engine (Discord HAL)
    ↓
Discord API
```

Your standard database with Discord as the underlying storage engine.

### Core Components

- **`discord`** - Discord API client (HAL layer)
- **`storage`** - Row encoding, segment layout, TOAST
- **`wal`** - Write-Ahead Logging with idempotency
- **`mvcc`** - Multi-Version Concurrency Control
- **`fsm`** - Free Space Map via role encoding
- **`index`** - Forum-based B-Tree indexes
- **`catalog`** - System catalog (tables, columns, indexes)
- **`scheduler`** - IO scheduler with 5-token rate limiting
- **`executor`** - Pull-based query execution
- **`sql`** - SQL parser and planner
- **`wire`** - PostgreSQL wire protocol

## Building

```bash
go build ./...
```

## Configuration

discodb requires a Discord bot token and configured guild. You can run it with a JSON config file, environment variables, or both.

```bash
go run ./cmd/discodb -config ./config/discodb.example.json -print-config
go run ./cmd/discodb -config ./config/discodb.example.json -check-config
go run ./cmd/discodb -config ./config/discodb.example.json -serve -addr :55432
```

The effective load order is:

- built-in defaults
- JSON file from `-config` or `DISCODB_CONFIG`
- environment overrides such as `DISCORD_BOT_TOKEN`, `DISCORD_BOT_TOKEN_WAL`, `DISCORD_BOT_TOKEN_HEAP`, `DISCORD_BOT_TOKEN_INDEX`, `DISCORD_BOT_TOKEN_CATALOG`, `DISCORD_BOT_TOKEN_OVERFLOW`, `DISCORD_GUILD_ID`, `DISCODB_LOG_LEVEL`, and `DISCODB_LOG_FILE`

The example file lives at [`config/discodb.example.json`](/home/winit/code/work/lasect/discodb/config/discodb.example.json).

The scheduler model uses five token classes from [`ai-docs/06_scheduler.md`](/home/winit/code/work/lasect/discodb/ai-docs/06_scheduler.md): `wal`, `heap`, `index`, `catalog`, and `overflow`.

`DISCORD_BOT_TOKEN` is treated as a compatibility shortcut and is copied to all five token classes unless a per-class env var overrides one of them.

## Status

discodb is in active development. I work on it in my free time and I post updates on my [X/Twitter](https://x.com/hiwinit).

## License

GNU General Public License v3.0 - see [LICENSE](LICENSE)

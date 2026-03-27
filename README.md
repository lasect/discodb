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

Built in Rust, discodb exposes a PostgreSQL-compatible wire protocol, so you can connect using `psql`:

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
cargo build --release
```

## Configuration

discodb requires a Discord bot token and configured guild. See `crates/config` for configuration options.

## Status

discodb is in active development. I work on it in my free time and I post updates on my [X/Twitter](https://x.com/hiwinit).

## License

GNU General Public License v3.0 - see [LICENSE](LICENSE)

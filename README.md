# SaturnDB

A realtime event-sourced database written in Rust.

- **Sub-millisecond push** to all subscribers on every write
- **Append-only event log** — full history, nothing deleted
- **Redis-inspired protocol** — but with history, realtime push, and stream-level access control
- **Single binary** — no JVM, no Zookeeper, no dependencies

## Features

| Category | What's included |
|---|---|
| **Core** | Append-only WAL, in-memory index, O(1) reads, group commit |
| **Realtime** | Pub/sub fan-out, pattern subscriptions (`users:*`), sub-ms push |
| **Queues** | Durable at-least-once job queues, long-poll DEQUEUE, visibility timeout |
| **Subscriptions** | `SUBSCRIBE FROM <id>` — replay history then stream live, never miss an event |
| **Triggers** | `WHEN <pattern> [WHERE ...] THEN EMIT/ENQUEUE` — reactive rules, server-side |
| **Geo** | Haversine radius search, auto-indexed from EMIT payloads with `lat`/`lng` |
| **Transactions** | `BEGIN / COMMIT / ROLLBACK` — ACID, cross-shard rejected |
| **Auth** | Token roles (Admin/Writer/Reader), namespaces, JWT (HS256), stream ownership |
| **Clustering** | Consistent hash ring, gossip failure detection, scatter-gather |
| **Storage** | Snapshots + WAL replay, history compaction, EXPIRE, DEL |

## Performance

Single node, loopback, async WAL:

| Operation | Throughput | p50 |
|---|---|---|
| EMIT | ~55,000 ops/s | 0.057ms |
| GET  | ~58,000 ops/s | 0.05ms |

## Quick Start

```bash
git clone https://github.com/Even2B/saturndb
cd saturndb
cargo build --release
./target/release/saturn-server
```

Default config (no `saturn.toml` needed for local dev):
- TCP: `127.0.0.1:7379`
- WebSocket: `127.0.0.1:7380`
- Admin token: `saturn-admin-secret`

## Protocol

Text-based, Redis-inspired. Connect via TCP or WebSocket.

```
AUTH saturn-admin-secret
OK

EMIT users:1 {"name":"alice","role":"rider"}
OK

GET users:1
VALUE {"name":"alice","role":"rider"}

WATCH users:*
WATCHING users:*
# → EVENT users:1 {...} pushed on every write

SUBSCRIBE users:* FROM 0
SUBSCRIBED users:* 1
SEVENT users:1 1 {"name":"alice","role":"rider"}
LIVE
# → SEVENT ... streamed live from here, cursor-resumable
```

## Durable Queues

```
ENQUEUE jobs:email {"to":"user@example.com"}
OK 1001

DEQUEUE jobs:email WAIT 30    ← long-poll, blocks until job arrives
JOB 1001 {"to":"user@example.com"}

ACK jobs:email 1001
OK
```

## Reactive Triggers

Fire server-side logic on writes — no app code needed:

```
# Enqueue a dispatch job whenever a ride is requested
WHEN ride:* WHERE status = requested THEN ENQUEUE jobs:dispatch {"ride":"$stream_id"}

# Mirror completed orders to an audit log
WHEN order:* WHERE status = completed THEN EMIT audit:log {"order":"$stream","ts":"$ts"}
```

## Geospatial

```
EMIT driver:42 {"lat":40.7128,"lng":-74.0060,"status":"available"}

GEO NEAR driver:* 40.7128 -74.0060 5.0
GEO_RESULT driver:42 40.7128 -74.0060 0.0
GEO_END
```

## Config (`saturn.toml`)

```toml
[server]
host    = "0.0.0.0"
port    = 7379
ws_port = 7380
tls     = false

[auth]
admin_token = "your-admin-secret"

[auth.tokens]
"app-token" = { role = "writer", namespaces = ["users", "rides"] }

[auth.jwt]
secret     = "your-jwt-secret"
role       = "writer"
namespaces = ["user:{sub}"]

[storage]
log_path          = "saturn.log"
snapshot_interval = 1000
wal_mode          = "async"   # or "sync" for durability
```

## SDKs

| Language | Location |
|---|---|
| JavaScript | `sdk/js/saturn.js` |
| Python | `sdk/python/saturn.py` |
| Dart/Flutter | `sdk/dart/saturn.dart` |

All SDKs: auto-reconnect, re-subscribe on reconnect, exponential backoff.

## Documentation

Full docs at **[even2b.github.io/saturndb](https://even2b.github.io/saturndb)**

## License

MIT

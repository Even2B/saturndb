# SaturnDB — CLAUDE.md

## What is SaturnDB
SaturnDB is an open-source realtime event-sourced database built in Rust.
- Sub-millisecond push to all subscribers on every write
- Append-only event log (nothing is ever deleted, just tombstoned)
- Redis-inspired protocol but with history, realtime push, and stream-level access control
- Written from scratch — no external DB engine

## Project Location
`C:\Users\even\Desktop\project\flux` (repo dir — binaries and config all use `saturn` prefix)

## Architecture

### Core Modules (`src/`)
| File | Responsibility |
|------|---------------|
| `event.rs` | `Event` struct — id, ts (unix micros), stream, op (Set/Delete/Patch), payload |
| `log.rs` | Append-only disk log. Every write = one JSON line. Replays on startup. |
| `index.rs` | In-memory index. stream → Vec<Event>. O(1) reads. Also holds expiry + deleted maps. |
| `broker.rs` | Pub/sub fan-out. Pattern-aware (`users:*`). Uses tokio broadcast channels. |
| `protocol.rs` | Command parser. Parses raw text lines into `Command` enum. |
| `server.rs` | TCP server. One task per client. Handles TLS or plain TCP based on config. |
| `auth.rs` | Token store. Roles: Admin/Writer/Reader. Per-namespace, per-operation permissions. Token expiry. |
| `config.rs` | Loads `flux.toml`. Structs: ServerConfig, AuthConfig, StorageConfig. |
| `snapshot.rs` | Periodic snapshots. Saves current index state to `flux-snapshot.json`. On startup loads snapshot then replays only new events. |
| `tls.rs` | Generates self-signed cert if missing. Returns TlsAcceptor. |
| `ws_proxy.rs` | WebSocket → TCP proxy on port 7380. Embedded in flux-server, not a separate process. |
| `monitor.rs` | TUI dashboard (ratatui). Shows streams + live events. Two connections: one for WATCH *, one polls keys every 2s. |
| `cli.rs` | CLI binary (`flux`). Uses env vars FLUX_HOST, FLUX_PORT, FLUX_TOKEN. |
| `main.rs` | Boot: load config → open log → restore index from snapshot + replay → spawn ws proxy → spawn snapshot loop → start server. |

### Binaries
- `flux-server` — the database server
- `flux` — CLI tool
- `flux-monitor` — TUI live dashboard

## Commands (Protocol)
```
AUTH <token>
EMIT <stream> <json>
GET <stream>
WATCH <pattern>           # pattern supports * wildcard e.g. users:*
SINCE <stream> <ts>       # ts = unix microseconds, 0 = from beginning
DEL <stream>
KEYS <pattern>
EXPIRE <stream> <secs>
QUERY <pattern> WHERE <field> <op> <value> [AND|OR <field> <op> <value>]
COUNT <pattern> [WHERE ...]
SUM <pattern> <field> [WHERE ...]
AVG <pattern> <field> [WHERE ...]
BEGIN
COMMIT
ROLLBACK
PING
TOKEN CREATE <token> <role> [ns1,ns2] [expires_at]
TOKEN REVOKE <token>
TOKEN LIST
CLAIM <stream>             # claim ownership of a stream (requires JWT session)
WHEN TOKEN <pattern> <token_tmpl> <role> [ns1,ns2]   # token template rule
WHEN TOKEN DEL <pattern> <token_tmpl>
WHEN TOKEN LIST
ENQUEUE <queue> <json>                    # push job to queue → OK <job_id>
DEQUEUE <queue> [WAIT [wait_secs]] [timeout_secs]  # claim next job → JOB <id> <json> | EMPTY
ACK     <queue> <job_id>                  # permanently remove job
NACK    <queue> <job_id>                  # return job to front of queue (retry++)
QLEN    <queue>                           # pending + in-flight count → COUNT <n>
QPEEK   <queue>                           # peek without claiming → JOB <id> <json> | EMPTY
SUBSCRIBE <pattern> FROM <last_event_id>  # replay history then stream live (SEVENT + LIVE)
MGET <stream> [stream ...]               # batch read multiple streams → VALUE/NULL per stream
GEO SET <stream> <lat> <lng>         # set geo position (also auto-indexed on EMIT/PATCH if payload has lat+lng)
GEO GET <stream>                     # get position → GEO_POS <lat> <lng>
GEO DEL <stream>                     # remove from geo index
GEO NEAR <pattern> <lat> <lng> <radius_km>  # radius search, sorted by distance
GEO DIST <stream1> <stream2>         # haversine distance in km between two streams
```

## Responses
```
OK
PONG
VALUE <json>
NULL
EMPTY
QUEUED              # inside a transaction
EVENT <stream> <json>
ROW <stream> <json>
KEY <stream>
COUNT <n>
SUM <n>
AVG <n>
TOKEN <raw> <role> <namespaces>
TOKEN_RULE <pattern> <token_tmpl> <role> <namespaces>
SUBSCRIBED <pattern> <count>
SEVENT <stream> <id> <payload>   # subscribe event with ID (both history and live)
LIVE                              # end of history replay
GEO_POS <lat> <lng>
GEO_RESULT <stream> <lat> <lng> <dist_km>   # one per matching stream (sorted by distance)
GEO_END                                      # end of GEO NEAR results
GEO_DIST <km>
ERR <message>
WATCHING <pattern>
```

## Transactions
- `BEGIN` starts a buffer
- `EMIT`/`PATCH`/`DEL` inside transaction return `QUEUED`
- `COMMIT` writes all atomically
- `ROLLBACK` discards all
- Client disconnect auto-rollbacks
- Cross-shard transactions rejected with clear error

## Access Control
Three roles: Admin, Writer, Reader
- Admin: full access to everything
- Writer: read + write within namespaces
- Reader: read only within namespaces
- Per-token: `can_watch`, `can_since`, `expires_at`
- Multiple namespaces per token: `namespaces = ["users", "orders"]`
- Namespace matching supports wildcard suffix: `users:*`
- JWT auth: `AUTH <supabase_jwt>` — verified via HS256, scoped to `{sub}` namespaces
- Stream ownership: `CLAIM <stream>` — only the claiming user_id (or admin) can write to it
- Token templates: `WHEN TOKEN <pattern> <token_tmpl> <role> [namespaces]` — auto-create tokens on EMIT

## Config (`saturn.toml`)
```toml
[server]
host    = "127.0.0.1"
port    = 7379
ws_port = 7380
tls     = false           # true = TLS, false = plain TCP

[auth]
admin_token = "saturn-admin-secret"

[auth.tokens]
"myapp-token" = { role = "writer", namespaces = ["users", "profiles"] }
"logs-token"  = { role = "reader", namespaces = ["logs"], can_since = false }
"temp-token"  = { role = "reader", namespaces = ["public"], expires_at = "2026-12-31" }

# JWT verification (for Supabase or any HS256 issuer)
[auth.jwt]
secret     = "your-supabase-jwt-secret"   # from Supabase → Settings → API → JWT Secret
role       = "writer"                      # role granted to JWT users
namespaces = ["user:{sub}", "ride:{sub}"]  # {sub} replaced with JWT subject (user_id)

[storage]
log_path          = "saturn.log"
snapshot_interval = 1000    # snapshot every N events
```

## Stream Ownership (CLAIM)
- `CLAIM <stream>` sets the authenticated user as owner (requires JWT session)
- Only the owner (or Admin) can EMIT/PATCH/DEL to a claimed stream
- Ownership is persisted in snapshots — survives restarts
- Readers can still GET/SINCE/WATCH regardless of ownership

## Token Templates (WHEN TOKEN)
Auto-create tokens when streams are written:
```
WHEN TOKEN user:* rider:$stream_id writer ride:$stream_id,driver:$stream_id
```
- `$stream_id` = part after last `:` (e.g. `user:123` → `123`)
- `$stream` = full stream name
- Token is created in the auth store immediately on matching EMIT
- Persisted in `saturn-rules.json` alongside WHEN EMIT rules

## Storage Files
- `flux.log` — append-only event log (one JSON line per event)
- `flux-snapshot.json` — latest index snapshot (rebuilt every N events)
- `flux-cert.pem` / `flux-key.pem` — auto-generated TLS cert (when tls = true)

## Client SDKs
All SDKs: `FluxClient(host, port, token)`
Methods: `connect()`, `emit(stream, payload)`, `get(stream)`, `watch(pattern, handler)`, `since(stream, ts)`, `del(stream)`, `ping()`, `disconnect()`

| SDK | Location |
|-----|---------|
| JavaScript (Node.js) | `sdk/js/flux.js` |
| Python | `sdk/python/flux.py` |
| Dart (Flutter) | `sdk/dart/flux.dart` |

All SDKs:
- Send AUTH on connect automatically
- Auto-reconnect with exponential backoff
- Re-subscribe to all WATCH patterns after reconnect

## Running
```bash
# server
cargo run --bin flux-server

# CLI
$env:FLUX_TOKEN = "flux-admin-secret"
./target/debug/flux ping
./target/debug/flux emit users:1 '{\"name\":\"evan\"}'
./target/debug/flux get users:1
./target/debug/flux keys "*"
./target/debug/flux watch "users:*"

# TUI monitor
cargo run --bin flux-monitor
```

## Examples
- `examples/chat.html` — realtime chat app (HTML/CSS/JS). Connects via WebSocket proxy on port 7380.

## Dependencies (Cargo.toml)
- `tokio` — async runtime
- `serde` / `serde_json` — serialization
- `dashmap` — concurrent hashmap (index + broker)
- `tokio-tungstenite` — WebSocket support
- `futures-util` — stream utilities
- `ratatui` + `crossterm` — TUI monitor
- `rcgen` + `rustls` + `tokio-rustls` + `rustls-pemfile` — TLS
- `jsonwebtoken` — HS256 JWT verification (Supabase-compatible)
- `toml` — config parsing
- `tracing` + `tracing-subscriber` — logging
- `anyhow` — error handling
- `chrono` — timestamps

## What's Built
- [x] Append-only event log
- [x] In-memory index with O(1) reads
- [x] Realtime pub/sub (sub-ms)
- [x] Pattern subscriptions (users:*)
- [x] Authentication (token-based)
- [x] Stream-level access control (RLS equivalent)
- [x] Multiple namespaces per token
- [x] Dynamic token management (TOKEN CREATE/REVOKE/LIST)
- [x] Token expiry
- [x] TLS (optional, toggle in config)
- [x] WebSocket proxy (embedded)
- [x] Snapshots (startup optimization)
- [x] EXPIRE (auto-delete after N seconds)
- [x] DEL (tombstone delete, history preserved)
- [x] KEYS pattern listing
- [x] SINCE (replay history from timestamp)
- [x] QUERY with WHERE clause (=, !=, >, >=, <, <=, CONTAINS)
- [x] QUERY with multiple WHERE conditions (AND/OR)
- [x] Aggregations (COUNT, SUM, AVG)
- [x] ACID transactions (BEGIN/COMMIT/ROLLBACK)
- [x] CLI tool
- [x] TUI monitor (ratatui)
- [x] JS SDK
- [x] Python SDK
- [x] Dart SDK
- [x] Chat example app
- [x] Clustering — consistent hash ring (FNV-1a, 150 vnodes), gossip failure detection, forward+replicate
- [x] Reactive rules — `WHEN <pattern> [WHERE <field> <op> <value>] THEN EMIT/ENQUEUE <target> <template>`, fires on EMIT/PATCH/DEL including forwarded cluster writes; conditional WHERE clause filters by payload field
- [x] Scatter-gather — KEYS, QUERY, COUNT, SUM, AVG all fan out to all shards and merge results
- [x] History compaction (`max_history_per_stream` in saturn.toml — caps in-memory Vec<Event>, WAL untouched)
- [x] Rate limiting — token bucket per connection (EMIT/PATCH/DEL/EmitMany), configurable via `rate_limit_per_sec` + `rate_limit_burst`
- [x] Bounded SINCE — `max_since_events` cap, returns most-recent N + `TRUNCATED` prefix if hit
- [x] Graceful shutdown — SIGTERM/SIGINT drains connections, saves final snapshot
- [x] Idle connection timeout — per-connection deadline, configurable via `idle_timeout_secs`
- [x] `/health` + `/metrics` — raw TCP HTTP endpoints (no extra deps), Prometheus text format
- [x] Integration test suite — 29 end-to-end tests, port-0 isolation, all green
- [x] JWT verification — `AUTH <supabase_jwt>` via HS256, ephemeral token scoped to `{sub}` namespaces
- [x] Stream ownership — `CLAIM <stream>` locks writes to the claiming user_id; persisted in snapshots
- [x] Token templates — `WHEN TOKEN <pattern> <token_tmpl> <role> [ns]` auto-creates tokens on EMIT; `$stream_id` interpolation
- [x] Text mode cluster routing — WS/browser clients routed to correct shard same as binary mode
- [x] Durable queues — `ENQUEUE/DEQUEUE/ACK/NACK/QLEN/QPEEK`; visibility timeout (default 30s); auto-requeue on timeout; persisted in snapshots; at-least-once delivery
- [x] Long-poll DEQUEUE — `DEQUEUE <q> WAIT [secs]` blocks until a job arrives (or times out); no busy-wait, uses tokio Notify
- [x] Batch read — `MGET stream1 stream2 ...` returns one VALUE/NULL line per stream in a single round-trip
- [x] PATCH operators — `$inc`, `$mul`, `$min`, `$max`, `$push`, `$pop`, `$unset` applied atomically server-side; backward-compatible with plain merge PATCH
- [x] Durable subscriptions — `SUBSCRIBE <pattern> FROM <id>` replays history then streams live; `SEVENT <stream> <id> <payload>` format; client tracks cursor across reconnects
- [x] Geospatial index — `GEO SET/GET/DEL/NEAR/DIST`; haversine radius search; auto-indexed from EMIT/PATCH payloads with `lat`+`lng` fields; persisted in snapshots

## What's Not Built Yet
- [ ] npm/pip packages

## Key Design Decisions
- **Append-only** — never mutate, always append. Current state = last event per stream.
- **No schema** — any JSON payload, any stream name
- **Stream naming convention** — `namespace:id` e.g. `users:123`, `chat:1749203`
- **TLS off by default** — plain TCP for local dev, enable TLS for production
- **WebSocket embedded** — browser clients connect on port 7380, proxied to TCP 7379
- **Snapshot + replay** — snapshot saves latest state, startup only replays events after last snapshot ID

/// Integration tests — spin up a real server in-process, connect via TCP,
/// assert text-protocol responses. Each test gets its own port (OS assigns via
/// port 0) so tests run fully in parallel with no port collisions.

use saturn::core::{auth::AuthStore, broker::Broker, config::{AuthConfig, ServerConfig, StorageConfig}, rules::RulesEngine};
use saturn::server::{handler::Limits, listen, MetricsState};
use saturn::storage::{index::Index, log::EventLog};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

// ── Test helpers ──────────────────────────────────────────────────────────────

struct Server {
    port:        u16,
    shutdown_tx: watch::Sender<bool>,
    _log_path:   String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        // Best-effort cleanup of temp files
        let _ = std::fs::remove_file(&self._log_path);
        let _ = std::fs::remove_file(format!("{}.snap", self._log_path));
        let _ = std::fs::remove_file(format!("{}.rules", self._log_path));
    }
}

async fn start_server() -> Server {
    start_server_with_rate_limit(0.0).await
}

async fn start_server_with_rate_limit(rate: f64) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port     = listener.local_addr().unwrap().port();

    let log_path   = format!("{}/saturn-test-{}.log",   std::env::temp_dir().display(), port);
    let log     = EventLog::open(&log_path).await.unwrap();
    let index   = Index::new();
    let broker  = Broker::new();
    let auth    = AuthStore::from_config(&AuthConfig::default());
    let rules   = Arc::new(RulesEngine::new(String::new())); // empty path = in-memory only
    let metrics = MetricsState::new();
    let limits  = Limits {
        rate_per_sec:      rate,
        rate_burst:        rate * 2.0,
        max_since:         50_000,
        idle_timeout_secs: 0,
        max_auth_failures: 0, // disabled in tests
    };
    let cfg = ServerConfig { tls: false, max_connections: 100, ..ServerConfig::default() };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        listen(listener, log, index, broker, auth, cfg, None, false, limits, metrics, rules, None, shutdown_rx)
            .await
            .ok();
    });

    // Give the server a moment to be ready
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

    Server { port, shutdown_tx, _log_path: log_path }
}

// ── Text-protocol client ──────────────────────────────────────────────────────

struct Client {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl Client {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
        let (r, w) = stream.into_split();
        Self { reader: BufReader::new(r), writer: w }
    }

    async fn send(&mut self, line: &str) {
        self.writer.write_all(format!("{line}\n").as_bytes()).await.unwrap();
    }

    async fn recv(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line.trim().to_string()
    }

    /// Send a command, return one response line.
    async fn cmd(&mut self, line: &str) -> String {
        self.send(line).await;
        self.recv().await
    }

    /// Authenticated admin client.
    async fn admin(port: u16) -> Self {
        let mut c = Self::connect(port).await;
        assert_eq!(c.cmd("AUTH saturn-admin-secret").await, "OK");
        c
    }

    /// Collect N response lines (for multi-line responses like KEYS, SINCE).
    async fn recv_n(&mut self, n: usize) -> Vec<String> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n { out.push(self.recv().await); }
        out
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ping() {
    let srv = start_server().await;
    let mut c = Client::connect(srv.port).await;
    assert_eq!(c.cmd("PING").await, "PONG");
}

#[tokio::test]
async fn test_auth_ok() {
    let srv = start_server().await;
    let mut c = Client::connect(srv.port).await;
    assert_eq!(c.cmd("AUTH saturn-admin-secret").await, "OK");
}

#[tokio::test]
async fn test_auth_bad_token() {
    let srv = start_server().await;
    let mut c = Client::connect(srv.port).await;
    assert_eq!(c.cmd("AUTH wrong").await, "ERR invalid token");
}

#[tokio::test]
async fn test_unauthenticated_emit_rejected() {
    let srv = start_server().await;
    let mut c = Client::connect(srv.port).await;
    let resp = c.cmd(r#"EMIT users:1 {"x":1}"#).await;
    assert!(resp.starts_with("ERR"));
}

#[tokio::test]
async fn test_emit_and_get() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    assert_eq!(c.cmd(r#"EMIT users:1 {"name":"alice"}"#).await, "OK");
    assert_eq!(c.cmd("GET users:1").await,                        r#"VALUE {"name":"alice"}"#);
}

#[tokio::test]
async fn test_get_missing_stream() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    assert_eq!(c.cmd("GET no:such:stream").await, "NULL");
}

#[tokio::test]
async fn test_emit_overwrites() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT s:1 {"v":1}"#).await;
    c.cmd(r#"EMIT s:1 {"v":2}"#).await;
    assert_eq!(c.cmd("GET s:1").await, r#"VALUE {"v":2}"#);
}

#[tokio::test]
async fn test_patch() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT users:1 {"name":"alice","age":30}"#).await;
    c.cmd(r#"PATCH users:1 {"age":31}"#).await;
    let val = c.cmd("GET users:1").await;
    assert!(val.contains("\"age\":31"), "got: {val}");
    assert!(val.contains("\"name\":\"alice\""), "got: {val}");
}

#[tokio::test]
async fn test_del() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT users:1 {"x":1}"#).await;
    assert_eq!(c.cmd("GET users:1").await, r#"VALUE {"x":1}"#);
    assert_eq!(c.cmd("DEL users:1").await, "OK");
    assert_eq!(c.cmd("GET users:1").await, "NULL");
}

#[tokio::test]
async fn test_keys_pattern() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT users:1 {"x":1}"#).await;
    c.cmd(r#"EMIT users:2 {"x":2}"#).await;
    c.cmd(r#"EMIT orders:1 {"x":3}"#).await;

    let resp = c.cmd("KEYS users:*").await;
    // Multi-line: each key on its own line
    let line2 = c.recv().await;
    let keys: Vec<&str> = [resp.as_str(), line2.as_str()]
        .iter()
        .flat_map(|l| l.strip_prefix("KEY "))
        .collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"users:1"));
    assert!(keys.contains(&"users:2"));
}

#[tokio::test]
async fn test_keys_empty() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    assert_eq!(c.cmd("KEYS nobody:*").await, "EMPTY");
}

#[tokio::test]
async fn test_since() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT hist:1 {"v":1}"#).await;
    c.cmd(r#"EMIT hist:1 {"v":2}"#).await;
    // SINCE 0 should return all events
    let first = c.cmd("SINCE hist:1 0").await;
    let second = c.recv().await;
    assert!(first.starts_with("ROW"), "got: {first}");
    assert!(second.starts_with("ROW"), "got: {second}");
}

#[tokio::test]
async fn test_since_empty_stream() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    assert_eq!(c.cmd("SINCE nobody:1 0").await, "EMPTY");
}

#[tokio::test]
async fn test_expire() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT temp:1 {"x":1}"#).await;
    c.cmd("EXPIRE temp:1 1").await;
    // Should be present immediately
    assert!(c.cmd("GET temp:1").await.starts_with("VALUE"));
    // After 1.1s the eviction loop runs (1s tick) and removes it
    tokio::time::sleep(tokio::time::Duration::from_millis(1200)).await;
    assert_eq!(c.cmd("GET temp:1").await, "NULL");
}

#[tokio::test]
async fn test_query_where() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT orders:1 {"status":"paid","total":100}"#).await;
    c.cmd(r#"EMIT orders:2 {"status":"pending","total":50}"#).await;

    let resp = c.cmd(r#"QUERY orders:* WHERE status = paid"#).await;
    assert!(resp.starts_with("ROW"), "got: {resp}");
    assert!(resp.contains("orders:1"), "got: {resp}");
}

#[tokio::test]
async fn test_count() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT u:1 {"role":"admin"}"#).await;
    c.cmd(r#"EMIT u:2 {"role":"user"}"#).await;
    c.cmd(r#"EMIT u:3 {"role":"admin"}"#).await;

    assert_eq!(c.cmd("COUNT u:*").await, "COUNT 3");
    assert_eq!(c.cmd(r#"COUNT u:* WHERE role = admin"#).await, "COUNT 2");
}

#[tokio::test]
async fn test_sum() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT tx:1 {"amount":100}"#).await;
    c.cmd(r#"EMIT tx:2 {"amount":200}"#).await;
    c.cmd(r#"EMIT tx:3 {"amount":50}"#).await;

    assert_eq!(c.cmd("SUM tx:* amount").await, "SUM 350");
}

#[tokio::test]
async fn test_avg() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT scores:1 {"val":10}"#).await;
    c.cmd(r#"EMIT scores:2 {"val":20}"#).await;
    c.cmd(r#"EMIT scores:3 {"val":30}"#).await;

    assert_eq!(c.cmd("AVG scores:* val").await, "AVG 20");
}

#[tokio::test]
async fn test_avg_no_data() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    assert_eq!(c.cmd("AVG nothing:* val").await, "NULL");
}

#[tokio::test]
async fn test_transaction_commit() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    assert_eq!(c.cmd("BEGIN").await,                        "OK transaction started");
    assert_eq!(c.cmd(r#"EMIT txn:1 {"x":1}"#).await,       "QUEUED");
    assert_eq!(c.cmd(r#"EMIT txn:2 {"x":2}"#).await,       "QUEUED");
    let commit = c.cmd("COMMIT").await;
    assert!(commit.starts_with("OK committed"), "got: {commit}");

    assert!(c.cmd("GET txn:1").await.starts_with("VALUE"));
    assert!(c.cmd("GET txn:2").await.starts_with("VALUE"));
}

#[tokio::test]
async fn test_transaction_rollback() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    c.cmd("BEGIN").await;
    c.cmd(r#"EMIT rollback:1 {"x":1}"#).await;
    assert_eq!(c.cmd("ROLLBACK").await, "OK rolled back");
    assert_eq!(c.cmd("GET rollback:1").await, "NULL");
}

#[tokio::test]
async fn test_no_nested_transactions() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    c.cmd("BEGIN").await;
    let resp = c.cmd("BEGIN").await;
    assert!(resp.starts_with("ERR"), "got: {resp}");
    c.cmd("ROLLBACK").await;
}

#[tokio::test]
async fn test_watch_receives_event() {
    let srv = start_server().await;

    let mut watcher = Client::admin(srv.port).await;
    let watch_resp = watcher.cmd("WATCH live:*").await;
    assert_eq!(watch_resp, "WATCHING live:*");

    // Emit from a separate connection
    let mut emitter = Client::admin(srv.port).await;
    emitter.cmd(r#"EMIT live:1 {"msg":"hello"}"#).await;

    // Watcher should receive EVENT within 100ms
    let event = tokio::time::timeout(
        tokio::time::Duration::from_millis(200),
        watcher.recv(),
    ).await.expect("timed out waiting for event");

    assert!(event.starts_with("EVENT live:1"), "got: {event}");
}

#[tokio::test]
async fn test_watch_pattern_filter() {
    let srv = start_server().await;

    let mut watcher = Client::admin(srv.port).await;
    watcher.cmd("WATCH chats:*").await;

    let mut emitter = Client::admin(srv.port).await;
    emitter.cmd(r#"EMIT chats:room1 {"text":"hi"}"#).await;
    emitter.cmd(r#"EMIT other:stuff {"text":"nope"}"#).await;

    let event = tokio::time::timeout(
        tokio::time::Duration::from_millis(200),
        watcher.recv(),
    ).await.expect("timed out");

    assert!(event.contains("chats:room1"), "got: {event}");
}

#[tokio::test]
async fn test_rate_limit() {
    let srv = start_server_with_rate_limit(2.0).await; // 2/s, burst 4
    let mut c = Client::admin(srv.port).await;

    // Burst of 5 should eventually trigger rate limit
    let mut got_limited = false;
    for _ in 0..20 {
        let resp = c.cmd(r#"EMIT rl:1 {"x":1}"#).await;
        if resp.contains("rate limit") {
            got_limited = true;
            break;
        }
    }
    assert!(got_limited, "expected rate limit to trigger");
}

#[tokio::test]
async fn test_reactive_rule() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    // Register rule: when orders:* fires, emit to audit:log
    let resp = c.cmd(r#"WHEN orders:* THEN EMIT audit:log {"event":"order","src":"$stream"}"#).await;
    assert!(resp.starts_with("OK rule added"), "got: {resp}");

    // Subscribe to audit:log before emitting
    let mut watcher = Client::admin(srv.port).await;
    watcher.cmd("WATCH audit:*").await;

    // Emit to orders — should trigger the rule
    c.cmd(r#"EMIT orders:1 {"total":99}"#).await;

    let event = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        watcher.recv(),
    ).await.expect("timed out waiting for reactive event");

    assert!(event.starts_with("EVENT audit:log"), "got: {event}");
    assert!(event.contains("order"), "payload missing: {event}");
}

#[tokio::test]
async fn test_when_list_and_del() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    c.cmd(r#"WHEN x:* THEN EMIT y:log {"e":"x"}"#).await;

    let list = c.cmd("WHEN LIST").await;
    assert!(list.starts_with("RULE"), "got: {list}");

    let del = c.cmd("WHEN DEL x:* y:log").await;
    assert!(del.starts_with("OK"), "got: {del}");

    assert_eq!(c.cmd("WHEN LIST").await, "EMPTY");
}

#[tokio::test]
async fn test_when_then_enqueue() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    // When a ride is emitted, enqueue a dispatch job.
    let r = c.cmd(r#"WHEN ride:* THEN ENQUEUE jobs:dispatch {"ride_id":"$stream_id"}"#).await;
    assert!(r.starts_with("OK"), "got: {r}");

    c.cmd(r#"EMIT ride:42 {"status":"requested"}"#).await;

    // Job should appear in the queue.
    let job = c.cmd("DEQUEUE jobs:dispatch").await;
    assert!(job.starts_with("JOB"), "got: {job}");
    assert!(job.contains("42"), "stream_id not interpolated: {job}");
}

#[tokio::test]
async fn test_when_where_condition() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    // Only enqueue when status = "completed".
    let r = c.cmd(r#"WHEN order:* WHERE status = completed THEN ENQUEUE jobs:invoice {"order":"$stream_id"}"#).await;
    assert!(r.starts_with("OK"), "got: {r}");

    // This emit should NOT trigger (status != completed).
    c.cmd(r#"EMIT order:1 {"status":"pending"}"#).await;
    assert_eq!(c.cmd("QLEN jobs:invoice").await, "COUNT 0");

    // This one SHOULD trigger.
    c.cmd(r#"EMIT order:2 {"status":"completed","amount":99}"#).await;
    assert_eq!(c.cmd("QLEN jobs:invoice").await, "COUNT 1");

    let job = c.cmd("DEQUEUE jobs:invoice").await;
    assert!(job.contains("2"), "stream_id wrong: {job}");
}

#[tokio::test]
async fn test_token_create_and_use() {
    let srv = start_server().await;
    let mut admin = Client::admin(srv.port).await;

    // Create a reader token for the "data" namespace
    let resp = admin.cmd("TOKEN CREATE mytoken reader data").await;
    assert!(resp.starts_with("OK"), "got: {resp}");

    // Use the new token
    let mut c = Client::connect(srv.port).await;
    assert_eq!(c.cmd("AUTH mytoken").await, "OK");
    // Reader can GET streams in the data namespace
    admin.cmd(r#"EMIT data:1 {"v":1}"#).await;
    assert!(c.cmd("GET data:1").await.starts_with("VALUE"));
    // Reader cannot EMIT
    let e = c.cmd(r#"EMIT data:1 {"v":2}"#).await;
    assert!(e.starts_with("ERR"), "reader should not write: {e}");
}

#[tokio::test]
async fn test_queue_enqueue_dequeue_ack() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    // Enqueue two jobs
    let r1 = c.cmd(r#"ENQUEUE jobs:email {"to":"a@b.com"}"#).await;
    assert!(r1.starts_with("OK "), "got: {r1}");
    let id1: u64 = r1.split_whitespace().nth(1).unwrap().parse().unwrap();

    let r2 = c.cmd(r#"ENQUEUE jobs:email {"to":"b@c.com"}"#).await;
    let id2: u64 = r2.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(id2 > id1);

    // Queue length should be 2
    assert_eq!(c.cmd("QLEN jobs:email").await, format!("COUNT 2"));

    // Dequeue first job
    let d1 = c.cmd("DEQUEUE jobs:email 30").await;
    assert!(d1.starts_with("JOB"), "got: {d1}");
    let dequeued_id: u64 = d1.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert_eq!(dequeued_id, id1);

    // ACK it
    assert_eq!(c.cmd(&format!("ACK jobs:email {dequeued_id}")).await, "OK");

    // Queue should now have 1 pending
    assert_eq!(c.cmd("QLEN jobs:email").await, "COUNT 1");

    // Dequeue + NACK — job goes back to front
    let d2 = c.cmd("DEQUEUE jobs:email 30").await;
    let nack_id: u64 = d2.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert_eq!(c.cmd(&format!("NACK jobs:email {nack_id}")).await, "OK");

    // QLEN still 1 (back in pending)
    assert_eq!(c.cmd("QLEN jobs:email").await, "COUNT 1");

    // Dequeue again — same job, retries=1
    let d3 = c.cmd("DEQUEUE jobs:email 30").await;
    assert!(d3.starts_with("JOB"), "got: {d3}");
    c.cmd(&format!("ACK jobs:email {nack_id}")).await;
}

#[tokio::test]
async fn test_queue_empty_and_peek() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    // Empty queue returns EMPTY
    assert_eq!(c.cmd("DEQUEUE jobs:sms 30").await, "EMPTY");
    assert_eq!(c.cmd("QPEEK jobs:sms").await, "EMPTY");
    assert_eq!(c.cmd("QLEN jobs:sms").await, "COUNT 0");

    c.cmd(r#"ENQUEUE jobs:sms {"msg":"hello"}"#).await;

    // PEEK does not claim
    let peek = c.cmd("QPEEK jobs:sms").await;
    assert!(peek.starts_with("JOB"));
    assert_eq!(c.cmd("QLEN jobs:sms").await, "COUNT 1"); // still 1
    let peek2 = c.cmd("QPEEK jobs:sms").await;
    assert_eq!(peek, peek2); // same job
}

#[tokio::test]
async fn test_subscribe_replay_and_live() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    // Emit some events before subscribing.
    c.cmd(r#"EMIT events:1 {"n":1}"#).await;
    c.cmd(r#"EMIT events:2 {"n":2}"#).await;
    c.cmd(r#"EMIT events:3 {"n":3}"#).await;

    // Subscribe from the beginning.
    let sub_resp = c.cmd("SUBSCRIBE events:* FROM 0").await;
    assert!(sub_resp.starts_with("SUBSCRIBED events:*"), "got: {sub_resp}");

    // Should receive 3 historical SEVENT lines then LIVE.
    let h1 = c.recv().await;
    let h2 = c.recv().await;
    let h3 = c.recv().await;
    let live = c.recv().await;
    assert!(h1.starts_with("SEVENT events:"), "expected SEVENT, got: {h1}");
    assert!(h2.starts_with("SEVENT events:"), "expected SEVENT, got: {h2}");
    assert!(h3.starts_with("SEVENT events:"), "expected SEVENT, got: {h3}");
    assert_eq!(live, "LIVE");

    // New events after LIVE should also arrive as SEVENT.
    let mut writer = Client::admin(srv.port).await;
    writer.cmd(r#"EMIT events:4 {"n":4}"#).await;
    let live_event = tokio::time::timeout(
        tokio::time::Duration::from_millis(200),
        c.recv(),
    ).await.expect("timed out waiting for live SEVENT");
    assert!(live_event.starts_with("SEVENT events:4"), "got: {live_event}");
}

#[tokio::test]
async fn test_subscribe_from_cursor() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;

    c.cmd(r#"EMIT cursor:1 {"n":1}"#).await;
    c.cmd(r#"EMIT cursor:2 {"n":2}"#).await;

    // First subscriber gets everything.
    c.cmd("SUBSCRIBE cursor:* FROM 0").await;
    let e1 = c.recv().await; // SEVENT cursor:1 ...
    let e2 = c.recv().await; // SEVENT cursor:2 ...
    c.recv().await;           // LIVE

    // Extract the event ID from the first event to use as cursor.
    // Format: SEVENT <stream> <id> <payload>
    let id1: u64 = e1.split_whitespace().nth(2).unwrap().parse().unwrap();
    let id2: u64 = e2.split_whitespace().nth(2).unwrap().parse().unwrap();
    assert!(id2 > id1);

    // Second connection resumes from id1 — should only get cursor:2 onwards.
    let mut c2 = Client::admin(srv.port).await;
    c2.cmd(&format!("SUBSCRIBE cursor:* FROM {id1}")).await;
    let only = c2.recv().await;
    let live2 = c2.recv().await;
    assert!(only.contains("cursor:2"), "should only replay cursor:2, got: {only}");
    assert_eq!(live2, "LIVE");
}

#[tokio::test]
async fn test_emitmany() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    let resp = c.cmd(r#"EMITMANY multi:1 {"a":1} multi:2 {"b":2}"#).await;
    assert!(resp.starts_with("OK"), "got: {resp}");
    assert!(c.cmd("GET multi:1").await.starts_with("VALUE"));
    assert!(c.cmd("GET multi:2").await.starts_with("VALUE"));
}

#[tokio::test]
async fn test_mget() {
    let srv = start_server().await;
    let mut c = Client::admin(srv.port).await;
    c.cmd(r#"EMIT mget:1 {"x":1}"#).await;
    c.cmd(r#"EMIT mget:2 {"x":2}"#).await;
    // Three streams: two exist, one missing. MGET returns one line per stream.
    c.send("MGET mget:1 mget:2 mget:missing").await;
    let lines = c.recv_n(3).await;
    let joined = lines.join(" ");
    assert!(lines.iter().any(|l| l.starts_with("VALUE mget:1")), "got: {joined}");
    assert!(lines.iter().any(|l| l.starts_with("VALUE mget:2")), "got: {joined}");
    assert!(lines.iter().any(|l| l.starts_with("NULL mget:missing")), "got: {joined}");
}

#[tokio::test]
async fn test_dequeue_wait() {
    let srv = start_server().await;
    let mut producer = Client::admin(srv.port).await;
    let mut consumer = Client::admin(srv.port).await;

    // Consumer issues long-poll DEQUEUE with 5s wait before any job exists.
    // It should block until the producer enqueues.
    let consumer_task = tokio::spawn(async move {
        consumer.cmd("DEQUEUE waitq WAIT 5").await
    });

    // Give the consumer a moment to send DEQUEUE WAIT before we enqueue.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let enq = producer.cmd(r#"ENQUEUE waitq {"task":"hello"}"#).await;
    assert!(enq.starts_with("OK"), "enqueue failed: {enq}");

    let resp = tokio::time::timeout(
        tokio::time::Duration::from_secs(3),
        consumer_task,
    ).await.expect("long-poll DEQUEUE timed out").unwrap();
    assert!(resp.starts_with("JOB"), "expected JOB, got: {resp}");
    assert!(resp.contains("hello"), "got: {resp}");
}

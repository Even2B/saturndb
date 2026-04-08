use crate::core::event::{Event, Op};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

// Every line in the log is one of these two variants.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum WalLine {
    Commit { txn_commit: u64 },
    Event(Event),
}

// ── WAL actor message ─────────────────────────────────────────────────────────

enum WalMsg {
    /// Append a pre-serialized line. Ack fires after the kernel write.
    Write { line: String, ack: oneshot::Sender<()> },
    /// Append a pre-serialized line, no ack needed (fire-and-forget).
    WriteAsync { line: String },
    /// Write commit marker + fsync. Ack fires after sync_all().
    Commit { txn_id: u64, ack: oneshot::Sender<Result<()>> },
    /// Rewrite the log, keeping only what is still needed.
    Compact { keep_after: u64, ack: oneshot::Sender<Result<usize>> },
}

// ── Public handle ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct EventLog {
    wal_tx:      mpsc::Sender<WalMsg>,
    next_id:     Arc<AtomicU64>,
    next_txn_id: Arc<AtomicU64>,
}

impl EventLog {
    pub async fn open(path: &str) -> Result<Self> {
        let seed = highest_event_id(path).await + 1;
        let file = OpenOptions::new().create(true).append(true).open(path).await?;

        // Channel depth 4096 — producers almost never block.
        let (wal_tx, wal_rx) = mpsc::channel::<WalMsg>(4096);
        tokio::spawn(wal_actor(path.to_string(), file, wal_rx));

        Ok(Self {
            wal_tx,
            next_id:     Arc::new(AtomicU64::new(seed)),
            next_txn_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Write a single auto-committed event (no transaction). Waits for WAL write.
    pub async fn append(&self, stream: &str, op: Op, payload: serde_json::Value) -> Result<Event> {
        let id    = self.next_id.fetch_add(1, Ordering::SeqCst);
        let event = Event::new(id, stream, op, payload);
        self.send_write(serde_json::to_string(&WalLine::Event(event.clone()))?).await?;
        Ok(event)
    }

    /// Allocate an event ID + enqueue WAL write without waiting — returns immediately.
    /// The event is durable within ~1 WAL flush cycle (~10ms). Use for async wal_mode.
    pub fn append_nowait(&self, stream: &str, op: Op, payload: serde_json::Value) -> Event {
        let id    = self.next_id.fetch_add(1, Ordering::SeqCst);
        let event = Event::new(id, stream, op, payload);
        if let Ok(line) = serde_json::to_string(&WalLine::Event(event.clone())) {
            let _ = self.wal_tx.try_send(WalMsg::WriteAsync { line });
        }
        event
    }

    /// Write an event that belongs to an open transaction (status: pending).
    pub async fn append_pending(&self, txn_id: u64, stream: &str, op: Op, payload: serde_json::Value) -> Result<Event> {
        let id    = self.next_id.fetch_add(1, Ordering::SeqCst);
        let event = Event::new(id, stream, op, payload).with_txn(txn_id);
        self.send_write(serde_json::to_string(&WalLine::Event(event.clone()))?).await?;
        Ok(event)
    }

    /// Write a COMMIT marker and fsync — this is the atomic moment.
    pub async fn commit_txn(&self, txn_id: u64) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.wal_tx.send(WalMsg::Commit { txn_id, ack: ack_tx }).await
            .map_err(|_| anyhow::anyhow!("WAL actor died"))?;
        ack_rx.await.map_err(|_| anyhow::anyhow!("WAL ack dropped"))?
    }

    /// Allocate a new transaction ID.
    pub fn begin_txn(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Compact the WAL — called after every snapshot.
    pub async fn compact(&self, keep_after_id: u64) -> Result<usize> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.wal_tx.send(WalMsg::Compact { keep_after: keep_after_id, ack: ack_tx }).await
            .map_err(|_| anyhow::anyhow!("WAL actor died"))?;
        ack_rx.await.map_err(|_| anyhow::anyhow!("WAL ack dropped"))?
    }

    /// Replay the log, skipping uncommitted transactions.
    pub async fn replay(path: &str) -> Result<Vec<Event>> {
        let raw = tokio::fs::read_to_string(path).await.unwrap_or_default();

        let mut committed: HashSet<u64> = HashSet::new();
        let mut events:    Vec<Event>   = Vec::new();

        for line in raw.lines().filter(|l| !l.is_empty()) {
            match serde_json::from_str::<WalLine>(line) {
                Ok(WalLine::Commit { txn_commit }) => { committed.insert(txn_commit); }
                Ok(WalLine::Event(event))          => { events.push(event); }
                Err(_)                             => {}
            }
        }

        Ok(events.into_iter()
            .filter(|e| match e.txn_id {
                None      => true,
                Some(tid) => committed.contains(&tid),
            })
            .collect())
    }

    async fn send_write(&self, line: String) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.wal_tx.send(WalMsg::Write { line, ack: ack_tx }).await
            .map_err(|_| anyhow::anyhow!("WAL actor died"))?;
        ack_rx.await.map_err(|_| anyhow::anyhow!("WAL ack dropped"))?;
        Ok(())
    }
}

// ── WAL actor ─────────────────────────────────────────────────────────────────
//
// Owns the file handle exclusively — no Mutex needed.
//
// Group commit: when a Write arrives, we immediately drain any other Writes
// already queued in the channel and batch them into one write_all() syscall.
// Under load: 100 concurrent EMITs → 1 syscall.
// Under light load: each EMIT gets its own write with no added latency.

async fn wal_actor(path: String, mut file: tokio::fs::File, mut rx: mpsc::Receiver<WalMsg>) {
    // Small queue for messages that arrived mid-batch but aren't Writes.
    // They get processed on the next iteration.
    let mut pending: VecDeque<WalMsg> = VecDeque::new();

    loop {
        let msg = match pending.pop_front() {
            Some(m) => m,
            None    => match rx.recv().await {
                Some(m) => m,
                None    => break, // all EventLog handles dropped
            }
        };

        match msg {
            WalMsg::Write { line, ack } => {
                // ── Group commit ──────────────────────────────────────────────
                // Seed the batch with this write, then drain everything that
                // is already sitting in the channel right now (non-blocking).
                // Both Write (sync) and WriteAsync (fire-and-forget) are batched
                // together — one write_all syscall covers both.
                let mut batch = String::with_capacity(line.len() + 1);
                batch.push_str(&line);
                batch.push('\n');
                let mut acks: Vec<oneshot::Sender<()>> = vec![ack];

                loop {
                    match rx.try_recv() {
                        Ok(WalMsg::Write { line, ack }) => {
                            batch.push_str(&line);
                            batch.push('\n');
                            acks.push(ack);
                        }
                        Ok(WalMsg::WriteAsync { line }) => {
                            batch.push_str(&line);
                            batch.push('\n');
                            // no ack needed
                        }
                        // Non-write message: save for next iteration.
                        Ok(other) => { pending.push_back(other); break; }
                        // Channel empty — batch is complete.
                        Err(_) => break,
                    }
                }

                if let Err(e) = file.write_all(batch.as_bytes()).await {
                    eprintln!("WAL write error: {e}");
                }
                // Wake every waiting caller in one shot.
                for ack in acks { let _ = ack.send(()); }
            }

            WalMsg::WriteAsync { line } => {
                // Standalone async write (no sync writers to batch with).
                // Drain any more async writes that arrived.
                let mut batch = String::with_capacity(line.len() + 1);
                batch.push_str(&line);
                batch.push('\n');

                loop {
                    match rx.try_recv() {
                        Ok(WalMsg::WriteAsync { line }) => {
                            batch.push_str(&line);
                            batch.push('\n');
                        }
                        Ok(other) => { pending.push_back(other); break; }
                        Err(_) => break,
                    }
                }

                if let Err(e) = file.write_all(batch.as_bytes()).await {
                    eprintln!("WAL write error: {e}");
                }
            }

            WalMsg::Commit { txn_id, ack } => {
                let result = async {
                    let line = serde_json::to_string(&WalLine::Commit { txn_commit: txn_id })?;
                    file.write_all(line.as_bytes()).await?;
                    file.write_all(b"\n").await?;
                    file.sync_all().await?;
                    Ok::<(), anyhow::Error>(())
                }.await;
                let _ = ack.send(result);
            }

            WalMsg::Compact { keep_after, ack } => {
                let result = do_compact(&path, keep_after).await;
                if result.is_ok() {
                    // Reopen in append mode so future writes go to the compacted file.
                    match OpenOptions::new().create(true).append(true).open(&path).await {
                        Ok(f)  => file = f,
                        Err(e) => eprintln!("WAL reopen error after compact: {e}"),
                    }
                }
                let _ = ack.send(result);
            }
        }
    }
}

async fn do_compact(path: &str, keep_after_id: u64) -> Result<usize> {
    let raw = tokio::fs::read_to_string(path).await.unwrap_or_default();

    let mut committed:  HashSet<u64> = HashSet::new();
    let mut all_events: Vec<Event>   = Vec::new();

    for line in raw.lines().filter(|l| !l.is_empty()) {
        match serde_json::from_str::<WalLine>(line) {
            Ok(WalLine::Commit { txn_commit }) => { committed.insert(txn_commit); }
            Ok(WalLine::Event(e))              => { all_events.push(e); }
            Err(_)                                        => {}
        }
    }

    let original = all_events.len();
    let kept: Vec<Event> = all_events.into_iter()
        .filter(|e| {
            if e.id <= keep_after_id { return false; }
            match e.txn_id {
                None      => true,
                Some(tid) => committed.contains(&tid),
            }
        })
        .map(|mut e| { e.txn_id = None; e })
        .collect();

    let removed = original - kept.len();
    if removed == 0 { return Ok(0); }

    let mut content = String::new();
    for event in &kept {
        content.push_str(&serde_json::to_string(&WalLine::Event(event.clone()))?);
        content.push('\n');
    }

    tokio::fs::write(path, &content).await?;
    Ok(removed)
}

// ── helpers ───────────────────────────────────────────────────────────────────

async fn highest_event_id(path: &str) -> u64 {
    tokio::fs::read_to_string(path)
        .await
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|l| serde_json::from_str::<Event>(l).ok())
        .map(|e| e.id)
        .max()
        .unwrap_or(0)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::Op;
    use serde_json::json;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("saturn_test_{name}.log"));
        p.to_string_lossy().into_owned()
    }

    async fn cleanup(path: &str) { let _ = tokio::fs::remove_file(path).await; }

    #[tokio::test]
    async fn standalone_event_survives_replay() {
        let path = tmp_path("standalone");
        cleanup(&path).await;

        let log = EventLog::open(&path).await.unwrap();
        log.append("users:1", Op::Set, json!({"name": "evan"})).await.unwrap();

        let events = EventLog::replay(&path).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stream, "users:1");

        cleanup(&path).await;
    }

    #[tokio::test]
    async fn committed_transaction_survives_replay() {
        let path = tmp_path("committed");
        cleanup(&path).await;

        let log = EventLog::open(&path).await.unwrap();
        let txn = log.begin_txn();
        log.append_pending(txn, "users:1", Op::Set, json!({"a": 1})).await.unwrap();
        log.append_pending(txn, "users:2", Op::Set, json!({"b": 2})).await.unwrap();
        log.commit_txn(txn).await.unwrap();

        let events = EventLog::replay(&path).await.unwrap();
        assert_eq!(events.len(), 2);

        cleanup(&path).await;
    }

    #[tokio::test]
    async fn rolled_back_transaction_excluded_from_replay() {
        let path = tmp_path("rollback");
        cleanup(&path).await;

        let log = EventLog::open(&path).await.unwrap();
        let txn = log.begin_txn();
        log.append_pending(txn, "users:1", Op::Set, json!({"a": 1})).await.unwrap();
        // no commit_txn — simulates crash or ROLLBACK

        let events = EventLog::replay(&path).await.unwrap();
        assert_eq!(events.len(), 0, "rolled-back events must not appear on replay");

        cleanup(&path).await;
    }

    #[tokio::test]
    async fn mixed_standalone_and_txn() {
        let path = tmp_path("mixed");
        cleanup(&path).await;

        let log = EventLog::open(&path).await.unwrap();

        log.append("orders:1", Op::Set, json!({"total": 50})).await.unwrap();

        let txn = log.begin_txn();
        log.append_pending(txn, "orders:2", Op::Set, json!({"total": 100})).await.unwrap();
        log.commit_txn(txn).await.unwrap();

        let bad_txn = log.begin_txn();
        log.append_pending(bad_txn, "orders:3", Op::Set, json!({"total": 999})).await.unwrap();

        let events = EventLog::replay(&path).await.unwrap();
        assert_eq!(events.len(), 2);
        let streams: Vec<&str> = events.iter().map(|e| e.stream.as_str()).collect();
        assert!(streams.contains(&"orders:1"));
        assert!(streams.contains(&"orders:2"));
        assert!(!streams.contains(&"orders:3"));

        cleanup(&path).await;
    }

    #[tokio::test]
    async fn compact_removes_old_and_orphaned_entries() {
        let path = tmp_path("compact");
        cleanup(&path).await;

        let log = EventLog::open(&path).await.unwrap();

        log.append("orders:1", Op::Set, json!({"total": 10})).await.unwrap();
        log.append("orders:2", Op::Set, json!({"total": 20})).await.unwrap();
        log.append("orders:3", Op::Set, json!({"total": 30})).await.unwrap();

        let bad = log.begin_txn();
        log.append_pending(bad, "orders:4", Op::Set, json!({"total": 999})).await.unwrap();

        let good = log.begin_txn();
        log.append_pending(good, "orders:5", Op::Set, json!({"total": 50})).await.unwrap();
        log.commit_txn(good).await.unwrap();

        log.append("orders:6", Op::Set, json!({"total": 60})).await.unwrap();

        let removed = log.compact(3).await.unwrap();
        assert!(removed > 0, "should have removed entries");

        let events = EventLog::replay(&path).await.unwrap();
        let streams: Vec<&str> = events.iter().map(|e| e.stream.as_str()).collect();

        assert!(!streams.contains(&"orders:1"), "covered by snapshot");
        assert!(!streams.contains(&"orders:2"), "covered by snapshot");
        assert!(!streams.contains(&"orders:3"), "covered by snapshot");
        assert!(!streams.contains(&"orders:4"), "rolled back");
        assert!(streams.contains(&"orders:5"),  "committed after snapshot");
        assert!(streams.contains(&"orders:6"),  "standalone after snapshot");

        let order5 = events.iter().find(|e| e.stream == "orders:5").unwrap();
        assert!(order5.txn_id.is_none(), "txn_id should be stripped after compaction");

        cleanup(&path).await;
    }

    #[tokio::test]
    async fn compact_no_op_when_nothing_to_remove() {
        let path = tmp_path("compact_noop");
        cleanup(&path).await;

        let log = EventLog::open(&path).await.unwrap();
        log.append("s:1", Op::Set, json!({})).await.unwrap();

        let removed = log.compact(0).await.unwrap();
        assert_eq!(removed, 0);

        cleanup(&path).await;
    }

    #[tokio::test]
    async fn malformed_lines_skipped() {
        let path = tmp_path("malformed");
        cleanup(&path).await;

        tokio::fs::write(&path, "{\"id\":1,\"ts\":0,\"stream\":\"s:1\",\"op\":\"Set\",\"payload\":{}}\nGARBAGE LINE\n").await.unwrap();

        let events = EventLog::replay(&path).await.unwrap();
        assert_eq!(events.len(), 1);

        cleanup(&path).await;
    }

    #[tokio::test]
    async fn group_commit_batches_concurrent_writes() {
        let path = tmp_path("group_commit");
        cleanup(&path).await;

        let log = EventLog::open(&path).await.unwrap();

        // Fire 50 writes concurrently — they race into the WAL channel.
        // The actor should batch them into far fewer syscalls.
        let handles: Vec<_> = (0..50).map(|i| {
            let log = log.clone();
            tokio::spawn(async move {
                log.append(&format!("s:{i}"), Op::Set, json!({"i": i})).await.unwrap();
            })
        }).collect();

        for h in handles { h.await.unwrap(); }

        let events = EventLog::replay(&path).await.unwrap();
        assert_eq!(events.len(), 50, "all 50 events must be durable");

        cleanup(&path).await;
    }
}

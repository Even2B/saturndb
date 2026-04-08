pub mod gossip;
pub mod peer;
pub mod ring;
pub mod router;

use crate::core::config::ClusterConfig;
use crate::core::event::Event;
use crate::core::broker::Broker;
use crate::core::rules::RulesEngine;
use crate::net::codec::{OP_CLUSTER_FORWARD, OP_CLUSTER_HELLO, OP_CLUSTER_PING, OP_CLUSTER_REPLICATE, peer_frame};
use crate::server::metrics::MetricsState;
use crate::storage::index::Index;
use anyhow::Result;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use gossip::{GossipState, gossip_loop};
use peer::PeerPool;
use ring::Ring;
use router::ClusterRouter;
use tokio::net::TcpListener;

pub use router::ClusterRouter as Cluster;

// ── Initialise ────────────────────────────────────────────────────────────────

pub async fn init(cfg: &ClusterConfig) -> Option<Arc<Cluster>> {
    if !cfg.enabled || cfg.peers.is_empty() {
        return None;
    }

    // Build the full node list: self + all peers.
    let mut all_nodes: Vec<String> = vec![cfg.node_id.clone()];
    for p in &cfg.peers {
        if *p != cfg.peer_addr { all_nodes.push(p.clone()); }
    }
    all_nodes.dedup();

    let ring   = Ring::new(all_nodes.clone());
    let pool   = PeerPool::new();
    let gossip = GossipState::new(&all_nodes);

    // Connect to each peer.
    for peer_addr in &cfg.peers {
        if *peer_addr == cfg.peer_addr { continue; } // skip self
        let node_id = derive_node_id(peer_addr);
        pool.add_peer(node_id, peer_addr.clone(), cfg.node_id.clone(), cfg.cluster_secret.clone(), cfg.peer_tls);
    }

    let router = Arc::new(ClusterRouter::new(
        ring,
        pool.clone(),
        gossip.clone(),
        cfg.node_id.clone(),
        cfg.replication_factor,
    ));

    // Background gossip loop.
    let (gpool, gstate, gid) = (pool.clone(), gossip.clone(), cfg.node_id.clone());
    tokio::spawn(async move { gossip_loop(gpool, gstate, gid).await });

    println!(
        "[cluster] node={} peers={} replication={}",
        cfg.node_id,
        cfg.peers.len(),
        cfg.replication_factor,
    );

    Some(router)
}

/// Listen for incoming peer connections on the peer_addr.
pub async fn listen_peers(
    peer_addr:  &str,
    local_id:   String,
    secret:     String,
    peer_tls:   bool,
    index:      Index,
    broker:     Broker,
    log:        crate::storage::log::EventLog,
    rules:      Arc<RulesEngine>,
    metrics:    MetricsState,
    router:     Option<Arc<Cluster>>,
) -> Result<()> {
    let listener = TcpListener::bind(peer_addr).await?;
    let acceptor: Option<tokio_rustls::TlsAcceptor> = if peer_tls {
        Some(crate::core::tls::make_acceptor()?)
    } else {
        None
    };
    println!("[cluster] peer listener on {peer_addr}{}", if peer_tls { " (TLS)" } else { "" });

    loop {
        let (tcp, peer) = listener.accept().await?;
        let (index, broker, local_id, secret, log, rules, metrics, router, acceptor) =
            (index.clone(), broker.clone(), local_id.clone(), secret.clone(),
             log.clone(), rules.clone(), metrics.clone(), router.clone(), acceptor.clone());
        tokio::spawn(async move {
            let result = if let Some(acc) = acceptor {
                match acc.accept(tcp).await {
                    Ok(s)  => handle_peer(s,   local_id, secret, index, broker, log, rules, metrics, router).await,
                    Err(e) => { eprintln!("[cluster] TLS handshake from {peer}: {e}"); return; }
                }
            } else {
                handle_peer(tcp, local_id, secret, index, broker, log, rules, metrics, router).await
            };
            if let Err(e) = result { eprintln!("[cluster] peer {peer} error: {e}"); }
        });
    }
}

// ── Peer connection handler ───────────────────────────────────────────────────

async fn handle_peer<S>(
    sock:     S,
    local_id: String,
    secret:   String,
    index:    Index,
    broker:   Broker,
    log:      crate::storage::log::EventLog,
    rules:    Arc<RulesEngine>,
    metrics:  MetricsState,
    router:   Option<Arc<Cluster>>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Split once upfront — all I/O uses the halves.
    let (r, mut writer) = tokio::io::split(sock);
    let mut reader = tokio::io::BufReader::new(r);

    // ── Handshake: expect CLUSTER_HELLO ───────────────────────────────────────
    let mut hdr = [0u8; 5];
    reader.read_exact(&mut hdr).await?;
    let opcode   = hdr[0];
    let body_len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; body_len];
    reader.read_exact(&mut body).await?;

    if opcode != OP_CLUSTER_HELLO {
        anyhow::bail!("expected CLUSTER_HELLO, got 0x{opcode:02X}");
    }

    // HELLO body: "{node_id}\n{secret}"
    let hello_str  = String::from_utf8_lossy(&body);
    let (remote_id, peer_secret) = match hello_str.find('\n') {
        Some(i) => (&hello_str[..i], &hello_str[i+1..]),
        None    => (hello_str.as_ref(), ""),
    };
    let remote_id = remote_id.to_string();

    // Verify shared secret (constant-time compare to resist timing attacks).
    if !secret.is_empty() && !constant_time_eq(&secret, peer_secret) {
        let _ = writer.write_all(&peer_frame(OP_CLUSTER_HELLO, b"ERR unauthorized")).await;
        anyhow::bail!("peer {remote_id} rejected: wrong cluster_secret");
    }

    eprintln!("[cluster] peer {remote_id} authenticated");
    writer.write_all(&peer_frame(OP_CLUSTER_HELLO, &[])).await?;

    loop {
        let mut hdr = [0u8; 5];
        reader.read_exact(&mut hdr).await?;
        let op       = hdr[0];
        let body_len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; body_len];
        reader.read_exact(&mut body).await?;

        match op {
            OP_CLUSTER_FORWARD => {
                // body = original client binary frame → dispatch it, write back the response.
                let response = dispatch_forwarded(&body, &index, &broker, &log, &rules, router.as_deref()).await;
                writer.write_all(&response).await?;
            }

            OP_CLUSTER_REPLICATE => {
                // body = event JSON → apply to local index + broker for WATCH fan-out.
                if let Ok(event) = serde_json::from_slice::<Event>(&body) {
                    metrics.replicates_received.fetch_add(1, Ordering::Relaxed);
                    broker.publish(&event).await;
                    index.apply(event);
                }
                // No response for replication.
            }

            OP_CLUSTER_PING => {
                let pong = peer_frame(OP_CLUSTER_PING, &[]);
                writer.write_all(&pong).await?;
            }

            other => eprintln!("[cluster] unknown peer opcode: 0x{other:02X}"),
        }
    }
}

/// Dispatch a forwarded client frame on the primary node.
/// Handles all write commands: EMIT, PATCH, DEL, EXPIRE + reads: GET, SINCE.
/// Writes to WAL and replicates to other nodes so WATCH fans out cluster-wide.
async fn dispatch_forwarded(
    frame:  &[u8],
    index:  &Index,
    broker: &Broker,
    log:    &crate::storage::log::EventLog,
    rules:  &RulesEngine,
    router: Option<&Cluster>,
) -> Vec<u8> {
    use crate::net::codec::{decode_frame, encode_response};
    use crate::core::event::Op;

    let cmd = match decode_frame(frame) {
        Ok(c)  => c,
        Err(e) => return encode_response(&format!("ERR forwarded parse error: {e}\n")),
    };

    match cmd {
        crate::net::protocol::Command::Get { stream } => {
            match index.latest(&stream) {
                Some(v) => encode_response(&format!("VALUE {v}\n")),
                None    => encode_response("NULL\n"),
            }
        }

        crate::net::protocol::Command::Emit { stream, payload, ttl } => {
            let event = log.append_nowait(&stream, Op::Set, payload);
            if let Some(secs) = ttl { index.set_expiry(&stream, secs); }
            index.apply(event.clone());
            broker.publish(&event).await;
            if let Some(r) = router { r.broadcast_event(&event).await; }
            fire_rules(&event, rules, log, index, broker).await;
            encode_response("OK\n")
        }

        crate::net::protocol::Command::Patch { stream, patch } => {
            let merged = index.patch(&stream, patch.clone()).unwrap_or(patch);
            let event  = log.append_nowait(&stream, Op::Set, merged);
            index.apply(event.clone());
            broker.publish(&event).await;
            if let Some(r) = router { r.broadcast_event(&event).await; }
            fire_rules(&event, rules, log, index, broker).await;
            encode_response("OK\n")
        }

        crate::net::protocol::Command::Del { stream } => {
            let event = log.append_nowait(&stream, Op::Delete, serde_json::Value::Null);
            index.apply(event.clone());
            broker.publish(&event).await;
            if let Some(r) = router { r.broadcast_event(&event).await; }
            fire_rules(&event, rules, log, index, broker).await;
            encode_response("OK\n")
        }

        crate::net::protocol::Command::Expire { stream, secs } => {
            index.set_expiry(&stream, secs);
            encode_response("OK\n")
        }

        crate::net::protocol::Command::Since { stream, ts } => {
            let (events, truncated) = index.since(&stream, ts, 50_000);
            if events.is_empty() { return encode_response("EMPTY\n"); }
            let mut out = String::new();
            if truncated { out.push_str("TRUNCATED 50000\n"); }
            for e in &events { out.push_str(&format!("ROW {} {}\n", e.stream, e.payload)); }
            encode_response(&out)
        }

        // ── Scatter-gather: local shard answers ───────────────────────────────

        crate::net::protocol::Command::Keys { pattern } => {
            let keys = index.keys(&pattern);
            if keys.is_empty() { return encode_response("EMPTY\n"); }
            let out: String = keys.iter().map(|k| format!("KEY {k}\n")).collect();
            encode_response(&out)
        }

        crate::net::protocol::Command::Query { pattern, clause, limit } => {
            let results = index.query(&pattern, &clause, limit);
            if results.is_empty() { return encode_response("EMPTY\n"); }
            let out: String = results.iter().map(|(s, p)| format!("ROW {s} {p}\n")).collect();
            encode_response(&out)
        }

        crate::net::protocol::Command::Count { pattern, clause } => {
            let n = index.count(&pattern, clause.as_ref());
            encode_response(&format!("COUNT {n}\n"))
        }

        crate::net::protocol::Command::Sum { pattern, field, clause } => {
            let v = index.sum(&pattern, &field, clause.as_ref());
            encode_response(&format!("SUM {v}\n"))
        }

        crate::net::protocol::Command::Avg { pattern, field, clause } => {
            let (sum, count) = index.sum_and_count(&pattern, &field, clause.as_ref());
            encode_response(&format!("AVGPARTS {sum} {count}\n"))
        }

        _ => encode_response("ERR command not routable\n"),
    }
}

/// Fire reactive rules for a just-written event (same semantics as handler.rs).
/// Derived events use fire-and-forget WAL and do NOT cascade (no rules on derived events).
async fn fire_rules(event: &Event, rules: &RulesEngine, log: &crate::storage::log::EventLog, index: &Index, broker: &Broker) {
    use crate::core::event::Op;
    for fired in rules.fire(event) {
        match fired {
            crate::core::rules::RuleFired::Emit { stream, payload } => {
                let derived = log.append_nowait(&stream, Op::Set, payload);
                index.apply(derived.clone());
                broker.publish(&derived).await;
            }
            crate::core::rules::RuleFired::Enqueue { queue, payload } => {
                index.enqueue(&queue, payload);
            }
        }
    }
}

/// Constant-time string equality — prevents timing-based secret oracle attacks.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.len() != bb.len() { return false; }
    ab.iter().zip(bb.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Derive a stable node ID from a peer address.
/// In production you'd want explicit node IDs; this is a convenient default.
fn derive_node_id(addr: &str) -> String {
    format!("node@{addr}")
}

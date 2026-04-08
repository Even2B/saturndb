// ClusterRouter: decides where each command lives and handles forwarding/replication.
//
// For every stream-targeted command (EMIT, GET, DEL, etc.):
//   1. Compute primary node from consistent hash ring.
//   2. If primary == self  → handle locally, then async-replicate to replicas.
//   3. If primary != self  → forward raw client frame to primary, return its response.
//
// For fan-out commands (KEYS, QUERY, WATCH) → always handle locally.
// Scatter-gather across shards is a future V2 feature.
//
// Event replication doubles as WATCH fan-out: when we replicate an event to
// another node, that node applies it to its local broker, firing any WATCH
// subscribers connected there.

use crate::cluster::{gossip::GossipState, peer::PeerPool, ring::Ring};
use crate::core::event::Event;
use crate::net::codec::OP_CLUSTER_REPLICATE;
use anyhow::{anyhow, Result};
use std::sync::Arc;

#[derive(Clone)]
pub struct ClusterRouter {
    ring:               Arc<Ring>,
    pool:               PeerPool,
    gossip:             GossipState,
    pub local_id:       String,
    replication_factor: usize,
}

impl ClusterRouter {
    pub fn new(
        ring:               Ring,
        pool:               PeerPool,
        gossip:             GossipState,
        local_id:           String,
        replication_factor: usize,
    ) -> Self {
        Self {
            ring:   Arc::new(ring),
            pool,
            gossip,
            local_id,
            replication_factor,
        }
    }

    // ── Routing ───────────────────────────────────────────────────────────────

    /// True if this node is the primary for `stream`.
    pub fn is_local(&self, stream: &str) -> bool {
        let live = self.gossip.live_nodes();
        self.primary_live(stream, &live)
            .map(|id| id == self.local_id)
            .unwrap_or(true) // single-node fallback
    }

    /// Forward a raw client binary frame to the owning node.
    ///
    /// Tries ring candidates in order (primary first, then replicas).
    /// On TCP failure, moves to the next live candidate automatically —
    /// one retry per candidate until all live non-self nodes are exhausted.
    /// This handles the window between gossip heartbeats where a node has
    /// died but gossip hasn't marked it dead yet.
    pub async fn forward(&self, stream: &str, raw_frame: Vec<u8>) -> Result<Vec<u8>> {
        let live       = self.gossip.live_nodes();
        // Walk every ring position so we always have a fallback order.
        let candidates = self.ring.replicas(stream, self.ring.nodes().len());

        let mut last_err = anyhow!("no live candidate for stream {stream}");

        for candidate in candidates {
            if candidate == self.local_id         { continue; } // never forward to self
            if !live.iter().any(|l| l == candidate) { continue; } // skip gossip-dead nodes

            match self.pool.forward(candidate, raw_frame.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e)   => {
                    // TCP-level failure — node may have just died between heartbeats.
                    // Log and try the next ring candidate.
                    eprintln!("[cluster] forward to {candidate} failed ({e}) — trying next candidate");
                    last_err = e;
                }
            }
        }

        Err(last_err)
    }

    // ── Replication / WATCH fan-out ───────────────────────────────────────────

    /// After handling a write locally, replicate the event to other nodes.
    ///
    /// Replication serves two purposes:
    ///   1. Data durability: replica nodes store a copy of the event.
    ///   2. WATCH fan-out: nodes with WATCH subscribers on this stream
    ///      receive the event and push it to those clients.
    ///
    /// We broadcast to ALL other nodes (not just replicas) so WATCH works
    /// regardless of which node the subscriber is connected to.
    pub async fn broadcast_event(&self, event: &Event) {
        let event_bytes = match serde_json::to_vec(event) {
            Ok(b)  => b,
            Err(e) => { eprintln!("[cluster] replication serialization error: {e}"); return; }
        };

        // Build a cluster replicate frame: [OP_CLUSTER_REPLICATE][4B len][event_bytes]
        let frame = {
            let mut f = Vec::with_capacity(5 + event_bytes.len());
            f.push(OP_CLUSTER_REPLICATE);
            f.extend_from_slice(&(event_bytes.len() as u32).to_be_bytes());
            f.extend_from_slice(&event_bytes);
            f
        };

        // Send to all other nodes concurrently.
        let all_nodes = self.pool.node_ids();
        let tasks: Vec<_> = all_nodes
            .into_iter()
            .filter(|id| id != &self.local_id)
            .map(|id| {
                let pool  = self.pool.clone();
                let frame = frame.clone();
                tokio::spawn(async move {
                    pool.replicate_to(&id, frame).await;
                })
            })
            .collect();

        // Fire-and-forget: don't await the tasks. If a node is down, the
        // replicate silently drops; the gossip loop will detect it.
        drop(tasks);
    }

    // ── Scatter-gather ────────────────────────────────────────────────────────

    /// Send `frame` to every other live node in parallel and return their
    /// decoded text responses. Used for KEYS, QUERY, COUNT, SUM fan-out.
    pub async fn scatter(&self, frame: Vec<u8>) -> Vec<String> {
        let peers: Vec<String> = self.pool.node_ids()
            .into_iter()
            .filter(|id| id != &self.local_id)
            .filter(|id| self.gossip.is_alive(id))
            .collect();

        let mut set = tokio::task::JoinSet::new();
        for peer_id in peers {
            let pool  = self.pool.clone();
            let frame = frame.clone();
            set.spawn(async move { pool.forward(&peer_id, frame).await });
        }

        let mut results = Vec::new();
        while let Some(res) = set.join_next().await {
            if let Ok(Ok(resp_frame)) = res {
                // Response frame: [1B tag][4B len][body]  — body is the text payload.
                if resp_frame.len() > 5 {
                    if let Ok(text) = String::from_utf8(resp_frame[5..].to_vec()) {
                        results.push(text);
                    }
                }
            }
        }
        results
    }

    // ── Cluster status ────────────────────────────────────────────────────────

    /// Returns (node_id, is_alive) for every configured peer (excludes self).
    pub fn peer_statuses(&self) -> Vec<(String, bool)> {
        self.pool.node_ids().into_iter()
            .map(|id| { let alive = self.gossip.is_alive(&id); (id, alive) })
            .collect()
    }

    /// Number of configured peers (excludes self).
    pub fn peer_count(&self) -> usize { self.pool.node_ids().len() }

    /// Number of currently live peers (excludes self).
    pub fn live_peer_count(&self) -> usize {
        self.pool.node_ids().iter().filter(|id| self.gossip.is_alive(id)).count()
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn primary_live<'a>(&'a self, stream: &str, live: &[String]) -> Option<&'a str> {
        // Walk ring, skip dead nodes.
        let candidates = self.ring.replicas(stream, self.ring.nodes().len());
        for candidate in candidates {
            if live.contains(&candidate.to_string()) {
                return Some(candidate);
            }
        }
        // All nodes dead? Fall back to ring primary (best effort).
        self.ring.primary(stream)
    }
}

// Node liveness tracking via periodic heartbeats.
//
// Each node pings all peers every HEARTBEAT_SECS.
// A node is marked dead after DEAD_THRESHOLD_SECS of no successful ping.
// Dead nodes are excluded from ring routing — the next live node takes over.

use crate::cluster::peer::PeerPool;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{interval, Duration};

const HEARTBEAT_SECS:     u64 = 1;
const DEAD_THRESHOLD_SECS: u64 = 5;

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

#[derive(Clone)]
pub struct GossipState {
    /// node_id → last successful ping timestamp (unix secs)
    last_seen: Arc<DashMap<String, u64>>,
}

impl GossipState {
    pub fn new(peers: &[String]) -> Self {
        let last_seen = Arc::new(DashMap::new());
        let now = now_secs();
        for p in peers {
            last_seen.insert(p.clone(), now);
        }
        Self { last_seen }
    }

    pub fn mark_alive(&self, node_id: &str) {
        self.last_seen.insert(node_id.to_string(), now_secs());
    }

    pub fn is_alive(&self, node_id: &str) -> bool {
        self.last_seen
            .get(node_id)
            .map(|t| now_secs().saturating_sub(*t) < DEAD_THRESHOLD_SECS)
            .unwrap_or(false)
    }

    pub fn live_nodes(&self) -> Vec<String> {
        self.last_seen
            .iter()
            .filter(|e| now_secs().saturating_sub(*e.value()) < DEAD_THRESHOLD_SECS)
            .map(|e| e.key().clone())
            .collect()
    }
}

/// Spawns the gossip heartbeat loop. Runs forever in the background.
pub async fn gossip_loop(pool: PeerPool, state: GossipState, local_id: String) {
    let mut ticker = interval(Duration::from_secs(HEARTBEAT_SECS));
    loop {
        ticker.tick().await;
        // Always mark self alive so live_nodes() always includes this node.
        state.mark_alive(&local_id);
        let peers = pool.node_ids();
        for peer_id in &peers {
            if peer_id == &local_id { continue; }
            let alive = pool.ping(peer_id).await;
            if alive {
                state.mark_alive(peer_id);
            } else {
                // Only log on transition to dead (not every heartbeat).
                if state.is_alive(peer_id) {
                    eprintln!("[cluster] peer {peer_id} appears dead — excluding from ring");
                }
            }
        }
    }
}

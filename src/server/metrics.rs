use crate::storage::index::Index;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MetricsState {
    pub connections:         Arc<AtomicUsize>,
    /// Commands forwarded to another node (binary-mode cluster routing).
    pub forwards_total:      Arc<AtomicU64>,
    /// Events received via cluster replication (OP_CLUSTER_REPLICATE).
    pub replicates_received: Arc<AtomicU64>,
    start:                   Instant,
    cluster:                 Option<Arc<crate::cluster::Cluster>>,
}

impl MetricsState {
    pub fn new() -> Self {
        Self {
            connections:         Arc::new(AtomicUsize::new(0)),
            forwards_total:      Arc::new(AtomicU64::new(0)),
            replicates_received: Arc::new(AtomicU64::new(0)),
            start:               Instant::now(),
            cluster:             None,
        }
    }

    /// Attach cluster state for reporting. Call after cluster::init().
    pub fn set_cluster(&mut self, cluster: Option<Arc<crate::cluster::Cluster>>) {
        self.cluster = cluster;
    }

    pub fn uptime_secs(&self) -> u64 { self.start.elapsed().as_secs() }
}

// ── HTTP listener ─────────────────────────────────────────────────────────────

pub async fn listen(port: u16, state: MetricsState, index: Index) {
    if port == 0 { return; }
    let addr     = format!("0.0.0.0:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(l)  => { println!("metrics on http://{addr}  (/health  /metrics)"); l }
        Err(e) => { eprintln!("metrics listener failed: {e}"); return; }
    };
    loop {
        let Ok((mut sock, _)) = listener.accept().await else { continue };
        let state = state.clone();
        let index = index.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let n = match sock.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _              => return,
            };
            let req        = String::from_utf8_lossy(&buf[..n]);
            let first_line = req.lines().next().unwrap_or("");
            let (status, ctype, body) = route(first_line, &state, &index);
            let resp = format!(
                "HTTP/1.1 {status}\r\n\
                 Content-Type: {ctype}\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}

// ── Route ─────────────────────────────────────────────────────────────────────

fn route(req: &str, state: &MetricsState, index: &Index) -> (&'static str, &'static str, String) {
    let streams     = index.stream_count();
    let connections = state.connections.load(Ordering::Relaxed);
    let uptime      = state.uptime_secs();
    let forwards    = state.forwards_total.load(Ordering::Relaxed);
    let replicates  = state.replicates_received.load(Ordering::Relaxed);

    if req.starts_with("GET /metrics") {
        let body = prometheus_metrics(streams, connections, uptime, forwards, replicates, &state.cluster);
        ("200 OK", "text/plain; version=0.0.4; charset=utf-8", body)
    } else if req.starts_with("GET /health") {
        let body = health_json(streams, connections, uptime, &state.cluster);
        ("200 OK", "application/json", body)
    } else {
        ("404 Not Found", "text/plain", "Not Found\n".to_string())
    }
}

// ── Prometheus text format ────────────────────────────────────────────────────

fn prometheus_metrics(
    streams:     usize,
    connections: usize,
    uptime:      u64,
    forwards:    u64,
    replicates:  u64,
    cluster:     &Option<Arc<crate::cluster::Cluster>>,
) -> String {
    let mut out = format!(
        "# HELP saturn_streams_total Active streams in the index\n\
         # TYPE saturn_streams_total gauge\n\
         saturn_streams_total {streams}\n\
         # HELP saturn_connections_active Active client connections\n\
         # TYPE saturn_connections_active gauge\n\
         saturn_connections_active {connections}\n\
         # HELP saturn_uptime_seconds Seconds since server start\n\
         # TYPE saturn_uptime_seconds counter\n\
         saturn_uptime_seconds {uptime}\n\
         # HELP saturn_forwards_total Commands forwarded to another shard\n\
         # TYPE saturn_forwards_total counter\n\
         saturn_forwards_total {forwards}\n\
         # HELP saturn_replicates_received_total Events received via cluster replication\n\
         # TYPE saturn_replicates_received_total counter\n\
         saturn_replicates_received_total {replicates}\n"
    );

    let cluster_enabled = if cluster.is_some() { 1 } else { 0 };
    out.push_str(&format!(
        "# HELP saturn_cluster_enabled 1 if this node is part of a cluster\n\
         # TYPE saturn_cluster_enabled gauge\n\
         saturn_cluster_enabled {cluster_enabled}\n"
    ));

    if let Some(c) = cluster {
        let peers_configured = c.peer_count();
        let peers_live       = c.live_peer_count();
        out.push_str(&format!(
            "# HELP saturn_cluster_peers_configured Total configured peer nodes\n\
             # TYPE saturn_cluster_peers_configured gauge\n\
             saturn_cluster_peers_configured {peers_configured}\n\
             # HELP saturn_cluster_peers_live Currently reachable peer nodes\n\
             # TYPE saturn_cluster_peers_live gauge\n\
             saturn_cluster_peers_live {peers_live}\n"
        ));
        // Per-peer liveness gauge with node_id label.
        out.push_str(
            "# HELP saturn_cluster_peer_live 1 if peer is reachable, 0 if dead\n\
             # TYPE saturn_cluster_peer_live gauge\n"
        );
        for (id, alive) in c.peer_statuses() {
            let v = if alive { 1 } else { 0 };
            out.push_str(&format!("saturn_cluster_peer_live{{node_id=\"{id}\"}} {v}\n"));
        }
    }

    out
}

// ── Health JSON ───────────────────────────────────────────────────────────────

fn health_json(
    streams:     usize,
    connections: usize,
    uptime:      u64,
    cluster:     &Option<Arc<crate::cluster::Cluster>>,
) -> String {
    let cluster_block = match cluster {
        None => "\"cluster\":null".to_string(),
        Some(c) => {
            let node_id      = &c.local_id;
            let peers_cfg    = c.peer_count();
            let peers_live   = c.live_peer_count();
            let peer_arr: String = c.peer_statuses().iter()
                .map(|(id, alive)| {
                    let status = if *alive { "live" } else { "dead" };
                    format!("{{\"id\":\"{id}\",\"status\":\"{status}\"}}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "\"cluster\":{{\"node_id\":\"{node_id}\",\
                    \"peers_configured\":{peers_cfg},\
                    \"peers_live\":{peers_live},\
                    \"peers\":[{peer_arr}]}}"
            )
        }
    };

    format!(
        "{{\"status\":\"ok\",\"streams\":{streams},\
          \"connections\":{connections},\
          \"uptime_secs\":{uptime},\
          {cluster_block}}}\n"
    )
}

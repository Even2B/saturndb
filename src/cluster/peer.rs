// Pipelined binary connections to peer nodes.
//
// Each peer gets one persistent TCP connection.
// Frames are written back-to-back; responses are matched via FIFO.
// On disconnect, pending requests fail and the connection auto-reconnects.

use crate::net::codec::{OP_CLUSTER_FORWARD, OP_CLUSTER_HELLO, OP_CLUSTER_PING, OP_CLUSTER_PONG, peer_frame};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};

const RECONNECT_MS: u64 = 500;
const READ_BUF: usize   = 64 * 1024;

// ── Per-peer message ──────────────────────────────────────────────────────────

enum PeerMsg {
    /// Forward a client frame to this peer; await the response frame.
    Forward { frame: Vec<u8>, ack: oneshot::Sender<Result<Vec<u8>>> },
    /// Fire-and-forget: replicate an event to this peer (no response expected).
    Replicate { bytes: Vec<u8> },
    /// Liveness ping; await pong frame.
    Ping { ack: oneshot::Sender<bool> },
}

// ── Single peer connection ────────────────────────────────────────────────────

async fn peer_task(addr: String, local_id: String, secret: String, peer_tls: bool, mut rx: mpsc::Receiver<PeerMsg>) {
    loop {
        let err = connect_and_run(&addr, &local_id, &secret, peer_tls, &mut rx).await;
        eprintln!("[cluster] peer {addr} disconnected: {err} — retrying in {RECONNECT_MS}ms");
        sleep(Duration::from_millis(RECONNECT_MS)).await;
    }
}

/// Connect to a peer, perform the HELLO handshake, then drive the pipeline.
/// Returns the error that caused the disconnect.
async fn connect_and_run(
    addr:     &str,
    local_id: &str,
    secret:   &str,
    peer_tls: bool,
    rx:       &mut mpsc::Receiver<PeerMsg>,
) -> anyhow::Error {
    let mut sock = match TcpStream::connect(addr).await {
        Ok(s)  => s,
        Err(e) => return anyhow!("connect error: {e}"),
    };
    let _ = sock.set_nodelay(true);

    let hello_body = format!("{local_id}\n{secret}");
    let hello      = peer_frame(OP_CLUSTER_HELLO, hello_body.as_bytes());

    if peer_tls {
        let connector = match crate::core::tls::make_peer_connector() {
            Ok(c)  => c,
            Err(e) => return anyhow!("TLS connector: {e}"),
        };
        let stream = match connector.connect(crate::core::tls::peer_server_name(), sock).await {
            Ok(s)  => s,
            Err(e) => return anyhow!("TLS handshake: {e}"),
        };
        // Split once — read half goes into BufReader, write half used directly.
        let (r, mut w) = tokio::io::split(stream);
        let mut br = tokio::io::BufReader::new(r);
        if let Err(e) = w.write_all(&hello).await { return anyhow!("write hello: {e}"); }
        let mut ack = [0u8; 5];
        if let Err(e) = br.read_exact(&mut ack).await { return anyhow!("read ack: {e}"); }
        eprintln!("[cluster] connected to peer {addr} (TLS)");
        run_pipeline(addr, br, w, rx).await
    } else {
        let (r, mut w) = sock.into_split();
        let mut br = tokio::io::BufReader::new(r);
        if let Err(e) = w.write_all(&hello).await { return anyhow!("write hello: {e}"); }
        let mut ack = [0u8; 5];
        if let Err(e) = br.read_exact(&mut ack).await { return anyhow!("read ack: {e}"); }
        eprintln!("[cluster] connected to peer {addr}");
        run_pipeline(addr, br, w, rx).await
    }
}

/// Drives a connected peer. Generic over the read/write halves so it works
/// with both plain TCP and TLS streams. Returns the error that ended the loop.
async fn run_pipeline<R, W>(
    addr:   &str,
    mut reader: R,
    mut writer: W,
    rx:     &mut mpsc::Receiver<PeerMsg>,
) -> anyhow::Error
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // Pending request acks in FIFO order (only Forward+Ping need a response).
    let mut pending: std::collections::VecDeque<oneshot::Sender<Result<Vec<u8>>>> =
        std::collections::VecDeque::new();

    // Read and write run in parallel via select!.
    let mut read_buf  = vec![0u8; READ_BUF];
    let mut read_pos  = 0usize;

    loop {
        tokio::select! {
            // ── Incoming message from caller ──────────────────────────────────
            msg = rx.recv() => {
                let msg = match msg {
                    Some(m) => m,
                    None    => return anyhow!("peer task channel closed"),
                };
                match msg {
                    PeerMsg::Forward { frame, ack } => {
                        let wrapped = peer_frame(OP_CLUSTER_FORWARD, &frame);
                        if let Err(e) = writer.write_all(&wrapped).await {
                            let _ = ack.send(Err(anyhow!("{e}")));
                            return anyhow!("write error: {e}");
                        }
                        pending.push_back(ack);
                    }
                    PeerMsg::Replicate { bytes } => {
                        // Fire and forget — no ack queued.
                        if let Err(e) = writer.write_all(&bytes).await {
                            return anyhow!("write error: {e}");
                        }
                    }
                    PeerMsg::Ping { ack } => {
                        let ping = peer_frame(OP_CLUSTER_PING, &[]);
                        if let Err(e) = writer.write_all(&ping).await {
                            let _ = ack.send(false);
                            return anyhow!("write error: {e}");
                        }
                        // Wrap in a Forward ack — pong response will resolve it.
                        let (tx, rx_inner) = oneshot::channel::<Result<Vec<u8>>>();
                        pending.push_back(tx);
                        tokio::spawn(async move {
                            let ok = rx_inner.await.is_ok();
                            let _ = ack.send(ok);
                        });
                    }
                }
            }

            // ── Incoming bytes from peer ──────────────────────────────────────
            result = reader.read(&mut read_buf[read_pos..]) => {
                let n = match result {
                    Ok(0) | Err(_) => return anyhow!("peer {addr} closed connection"),
                    Ok(n) => n,
                };
                read_pos += n;

                // Parse all complete response frames from the buffer.
                let mut off = 0;
                while read_pos - off >= 5 {
                    let body_len = u32::from_be_bytes(
                        read_buf[off+1..off+5].try_into().unwrap()
                    ) as usize;
                    let total = 5 + body_len;
                    if read_pos - off < total { break; }

                    let frame = read_buf[off..off + total].to_vec();
                    off += total;

                    if let Some(waiter) = pending.pop_front() {
                        let _ = waiter.send(Ok(frame));
                    }
                }
                // Compact read buffer.
                if off > 0 {
                    read_buf.copy_within(off..read_pos, 0);
                    read_pos -= off;
                }
            }
        }
    }
}

// ── Pool of all peer connections ──────────────────────────────────────────────

#[derive(Clone)]
pub struct PeerPool {
    /// node_id → sender into that peer's task
    peers: Arc<DashMap<String, mpsc::Sender<PeerMsg>>>,
}

impl PeerPool {
    pub fn new() -> Self {
        Self { peers: Arc::new(DashMap::new()) }
    }

    /// Spawn a background task for a peer and register it in the pool.
    pub fn add_peer(&self, node_id: String, peer_addr: String, local_id: String, secret: String, peer_tls: bool) {
        let (tx, rx) = mpsc::channel::<PeerMsg>(4096);
        self.peers.insert(node_id.clone(), tx);
        tokio::spawn(peer_task(peer_addr, local_id, secret, peer_tls, rx));
    }

    /// Forward a client binary frame to a specific node; await its response.
    pub async fn forward(&self, node_id: &str, frame: Vec<u8>) -> Result<Vec<u8>> {
        let tx = self.peers.get(node_id)
            .ok_or_else(|| anyhow!("unknown peer: {node_id}"))?
            .clone();
        let (ack_tx, ack_rx) = oneshot::channel();
        tx.send(PeerMsg::Forward { frame, ack: ack_tx }).await
            .map_err(|_| anyhow!("peer {node_id} channel dead"))?;
        ack_rx.await.map_err(|_| anyhow!("peer {node_id} dropped ack"))?
    }

    /// Replicate an event to a specific node (fire-and-forget).
    /// `frame` must already be a complete OP_CLUSTER_REPLICATE peer frame.
    pub async fn replicate_to(&self, node_id: &str, frame: Vec<u8>) {
        if let Some(tx) = self.peers.get(node_id) {
            // Send the frame as-is — it's already framed by broadcast_event.
            let _ = tx.send(PeerMsg::Replicate { bytes: frame }).await;
        }
    }

    /// Ping a peer; returns true if it responds within the timeout.
    pub async fn ping(&self, node_id: &str) -> bool {
        let tx = match self.peers.get(node_id) {
            Some(t) => t.clone(),
            None    => return false,
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        if tx.send(PeerMsg::Ping { ack: ack_tx }).await.is_err() { return false; }
        tokio::time::timeout(Duration::from_secs(3), ack_rx)
            .await
            .map(|r| r.unwrap_or(false))
            .unwrap_or(false)
    }

    pub fn node_ids(&self) -> Vec<String> {
        self.peers.iter().map(|e| e.key().clone()).collect()
    }
}

use crate::cluster::Cluster;
use crate::core::{auth::AuthStore, broker::Broker, tls};
use crate::core::config::ServerConfig;
use crate::core::rules::RulesEngine;
use crate::server::audit::AuditLog;
use crate::server::handler::{handle, Limits};
use crate::server::metrics::MetricsState;
use crate::storage::{index::Index, log::EventLog};
use anyhow::Result;
use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

pub async fn listen(
    listener:    TcpListener,
    log:         EventLog,
    index:       Index,
    broker:      Broker,
    auth:        AuthStore,
    cfg:         ServerConfig,
    cluster:     Option<Arc<Cluster>>,
    wal_async:   bool,
    limits:      Limits,
    metrics:     MetricsState,
    rules:       Arc<RulesEngine>,
    audit:       Option<AuditLog>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let semaphore    = Arc::new(Semaphore::new(cfg.max_connections));
    // Per-IP connection rate limiter: (tokens, last_refill)
    let ip_limiters: Arc<DashMap<IpAddr, (f64, std::time::Instant)>> = Arc::new(DashMap::new());
    let ip_rate      = cfg.max_conn_per_ip_per_sec;
    let ip_burst     = cfg.max_conn_burst_per_ip;
    let mut tasks: JoinSet<()> = JoinSet::new();

    if cfg.tls {
        let acceptor = tls::make_acceptor()?;
        println!("saturn listening on {} (TLS, max_connections={})", listener.local_addr()?, cfg.max_connections);
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (tcp, peer) = res?;
                    if ip_rate > 0.0 && !check_ip_rate(&ip_limiters, peer.ip(), ip_rate, ip_burst) {
                        eprintln!("connection rate limit exceeded for {} — rejecting", peer.ip());
                        let mut t = tcp;
                        let _ = tokio::io::AsyncWriteExt::write_all(&mut t, b"ERR connection rate limit exceeded\n").await;
                        continue;
                    }
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p)  => p,
                        Err(_) => {
                            eprintln!("connection limit reached — rejecting {peer}");
                            let mut t = tcp;
                            let _ = tokio::io::AsyncWriteExt::write_all(&mut t, b"ERR server at connection limit\n").await;
                            continue;
                        }
                    };
                    let (log, index, broker, auth, acceptor, cluster, srx, counter, rules, m, al) =
                        (log.clone(), index.clone(), broker.clone(), auth.clone(),
                         acceptor.clone(), cluster.clone(), shutdown_rx.clone(),
                         metrics.connections.clone(), rules.clone(), metrics.clone(), audit.clone());
                    counter.fetch_add(1, Ordering::Relaxed);
                    tasks.spawn(async move {
                        match acceptor.accept(tcp).await {
                            Ok(s)  => {
                                if let Err(e) = handle(s, log, index, broker, auth, cluster, wal_async, limits, rules, m, al, srx).await {
                                    eprintln!("client error: {e}");
                                }
                            }
                            Err(e) => eprintln!("TLS handshake failed: {e}"),
                        }
                        counter.fetch_sub(1, Ordering::Relaxed);
                        drop(permit);
                    });
                }
                _ = shutdown_rx.changed() => break,
            }
            while tasks.try_join_next().is_some() {}
        }
    } else {
        println!("saturn listening on {} (max_connections={})", listener.local_addr()?, cfg.max_connections);
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (tcp, peer) = res?;
                    if ip_rate > 0.0 && !check_ip_rate(&ip_limiters, peer.ip(), ip_rate, ip_burst) {
                        eprintln!("connection rate limit exceeded for {} — rejecting", peer.ip());
                        let mut t = tcp;
                        let _ = tokio::io::AsyncWriteExt::write_all(&mut t, b"ERR connection rate limit exceeded\n").await;
                        continue;
                    }
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p)  => p,
                        Err(_) => {
                            eprintln!("connection limit reached — rejecting {peer}");
                            let mut t = tcp;
                            let _ = tokio::io::AsyncWriteExt::write_all(&mut t, b"ERR server at connection limit\n").await;
                            continue;
                        }
                    };
                    println!("client connected: {peer}");
                    let (log, index, broker, auth, cluster, srx, counter, rules, m, al) =
                        (log.clone(), index.clone(), broker.clone(), auth.clone(),
                         cluster.clone(), shutdown_rx.clone(), metrics.connections.clone(),
                         rules.clone(), metrics.clone(), audit.clone());
                    counter.fetch_add(1, Ordering::Relaxed);
                    tasks.spawn(async move {
                        if let Err(e) = handle(tcp, log, index, broker, auth, cluster, wal_async, limits, rules, m, al, srx).await {
                            eprintln!("client error: {e}");
                        }
                        counter.fetch_sub(1, Ordering::Relaxed);
                        drop(permit);
                    });
                }
                _ = shutdown_rx.changed() => break,
            }
            while tasks.try_join_next().is_some() {}
        }
    }

    // Drain remaining connections (30s timeout).
    let remaining = tasks.len();
    if remaining > 0 {
        println!("[shutdown] draining {remaining} connection(s)...");
        let drain = async { while tasks.join_next().await.is_some() {} };
        if tokio::time::timeout(tokio::time::Duration::from_secs(30), drain).await.is_err() {
            eprintln!("[shutdown] drain timed out — forcing close");
            tasks.abort_all();
        }
    }
    println!("[shutdown] listener closed");
    Ok(())
}

/// Token-bucket check for per-IP connection rate limiting.
/// Returns true if the connection is allowed, false if rate-limited.
fn check_ip_rate(
    limiters: &DashMap<IpAddr, (f64, std::time::Instant)>,
    ip:       IpAddr,
    rate:     f64,
    burst:    f64,
) -> bool {
    let mut entry = limiters.entry(ip).or_insert_with(|| (burst, std::time::Instant::now()));
    let (tokens, last) = entry.value_mut();
    let elapsed = last.elapsed().as_secs_f64();
    *tokens = (*tokens + elapsed * rate).min(burst);
    *last   = std::time::Instant::now();
    if *tokens >= 1.0 {
        *tokens -= 1.0;
        true
    } else {
        false
    }
}

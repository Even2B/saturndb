#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::Result;
use saturn::core::{auth::AuthStore, broker::Broker, config::Config, rules::RulesEngine};
use saturn::server::{self, AuditLog, handler::Limits, listen_metrics, MetricsState};
use saturn::storage::{index::Index, log::EventLog, snapshot};
use saturn::cluster;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cfg    = Config::load()?;
    let log    = EventLog::open(&cfg.storage.log_path).await?;
    let index  = Index::new().with_max_history(cfg.storage.max_history_per_stream);
    let broker = Broker::new();
    let auth   = AuthStore::from_config(&cfg.auth);

    restore(&index, &cfg.storage.log_path).await?;

    let rules = Arc::new(RulesEngine::new(cfg.storage.rules_path.clone()));
    let n = rules.load().await;
    if n > 0 { println!("loaded {n} reactive rule(s) from {}", cfg.storage.rules_path); }

    // ── Background tasks ──────────────────────────────────────────────────────

    tokio::spawn({
        let (i, l, interval) = (index.clone(), log.clone(), cfg.storage.snapshot_interval);
        async move { snapshot_loop(&i, &l, interval).await }
    });

    tokio::spawn({
        let i = index.clone();
        async move { eviction_loop(&i).await }
    });

    // ── Cluster ───────────────────────────────────────────────────────────────

    let mut metrics = MetricsState::new();
    let cluster = cluster::init(&cfg.cluster).await;
    metrics.set_cluster(cluster.clone());

    if cfg.cluster.enabled {
        let (peer_addr, local_id, secret, peer_tls, i2, b2, l2, ru2, m2, r2) = (
            cfg.cluster.peer_addr.clone(),
            cfg.cluster.node_id.clone(),
            cfg.cluster.cluster_secret.clone(),
            cfg.cluster.peer_tls,
            index.clone(),
            broker.clone(),
            log.clone(),
            rules.clone(),
            metrics.clone(),
            cluster.clone(),
        );
        tokio::spawn(async move {
            if let Err(e) = cluster::listen_peers(&peer_addr, local_id, secret, peer_tls, i2, b2, l2, ru2, m2, r2).await {
                eprintln!("[cluster] peer listener error: {e}");
            }
        });
    }

    // ── WebSocket proxy ───────────────────────────────────────────────────────

    tokio::spawn({
        let (ws_port, saturn_port, host) =
            (cfg.server.ws_port, cfg.server.port, cfg.server.host.clone());
        async move {
            if let Err(e) = server::ws_proxy::listen(ws_port, saturn_port, &host).await {
                eprintln!("ws proxy error: {e}");
            }
        }
    });

    // ── Client listener ───────────────────────────────────────────────────────

    let wal_async = cfg.storage.wal_mode == "async";
    if wal_async { println!("WAL mode: async (fire-and-forget)"); }

    let limits = Limits {
        rate_per_sec:      cfg.storage.rate_limit_per_sec,
        rate_burst:        cfg.storage.rate_limit_burst,
        max_since:         cfg.storage.max_since_events,
        idle_timeout_secs: cfg.server.idle_timeout_secs,
        max_auth_failures: cfg.server.max_auth_failures,
    };

    let audit = if cfg.storage.audit_log.is_empty() {
        None
    } else {
        match AuditLog::open(&cfg.storage.audit_log).await {
            Ok(al) => { println!("audit log: {}", cfg.storage.audit_log); Some(al) }
            Err(e) => { eprintln!("failed to open audit log: {e}"); None }
        }
    };
    println!(
        "rate limit: {}/s burst={} | max SINCE: {} events | idle timeout: {}s",
        limits.rate_per_sec, limits.rate_burst, limits.max_since,
        if limits.idle_timeout_secs == 0 { "disabled".to_string() } else { limits.idle_timeout_secs.to_string() }
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    tokio::spawn({
        let (m, i, port) = (metrics.clone(), index.clone(), cfg.server.metrics_port);
        async move { listen_metrics(port, m, i).await }
    });

    let addr     = format!("{}:{}", cfg.server.host, cfg.server.port);
    let tcp_listener = TcpListener::bind(&addr).await?;

    let listener_index = index.clone();
    let listener_log   = log.clone();
    let server_cfg     = cfg.server.clone();
    let listener_task  = tokio::spawn(async move {
        server::listen(
            tcp_listener, listener_log, listener_index, broker, auth,
            server_cfg, cluster, wal_async, limits, metrics, rules, audit, shutdown_rx,
        ).await
    });

    // ── Wait for shutdown signal ──────────────────────────────────────────────

    wait_for_signal().await;
    println!("[shutdown] signal received — draining connections");

    let _ = shutdown_tx.send(true);

    match tokio::time::timeout(
        tokio::time::Duration::from_secs(35),
        listener_task,
    ).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => eprintln!("[shutdown] listener error: {e}"),
        Ok(Err(e))     => eprintln!("[shutdown] listener task panic: {e}"),
        Err(_)         => eprintln!("[shutdown] drain timed out"),
    }

    // ── Final snapshot ────────────────────────────────────────────────────────

    let last_id = index.last_event_id();
    match snapshot::save(&index, last_id).await {
        Ok(()) => println!("[shutdown] final snapshot saved (event {last_id})"),
        Err(e) => eprintln!("[shutdown] snapshot error: {e}"),
    }

    println!("[shutdown] goodbye");
    Ok(())
}

// ── Signal handling ───────────────────────────────────────────────────────────

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv()          => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

// ── Startup restore ───────────────────────────────────────────────────────────

async fn restore(index: &Index, log_path: &str) -> Result<()> {
    let last_id    = snapshot::load(index).await?;
    let all_events = EventLog::replay(log_path).await?;
    let new_events: Vec<_> = all_events.into_iter().filter(|e| e.id > last_id).collect();
    let count = new_events.len();
    for event in new_events { index.apply(event); }
    println!("replayed {count} new events after snapshot");
    Ok(())
}

// ── Background loops ──────────────────────────────────────────────────────────

async fn eviction_loop(index: &Index) {
    let mut t = tokio::time::interval(tokio::time::Duration::from_secs(1));
    loop {
        t.tick().await;
        let n = index.evict_expired();
        if n > 0 { println!("evicted {n} expired stream{}", if n == 1 { "" } else { "s" }); }
        let r = index.requeue_timed_out();
        if r > 0 { println!("requeued {r} timed-out job{}", if r == 1 { "" } else { "s" }); }
    }
}

async fn snapshot_loop(index: &Index, log: &EventLog, interval: u64) {
    let mut last_id = index.last_event_id();
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        let current = index.last_event_id();
        if current.saturating_sub(last_id) >= interval {
            match snapshot::save(index, current).await {
                Err(e) => eprintln!("snapshot error: {e}"),
                Ok(()) => {
                    last_id = current;
                    match log.compact(current).await {
                        Ok(0) => {}
                        Ok(n) => println!("compacted WAL — removed {n} entries"),
                        Err(e) => eprintln!("compaction error: {e}"),
                    }
                }
            }
        }
    }
}

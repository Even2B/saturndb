use crate::cluster::Cluster;
use crate::core::{
    auth::{AuthStore, Role, Token},
    broker::Broker,
    event::{Event, Op},
    rules::RulesEngine,
};
use crate::net::{codec, protocol::Command};
use crate::server::audit::AuditLog;
use crate::server::metrics::MetricsState;
use crate::storage::{index::Index, log::EventLog};
use anyhow::Result;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

// ── Limits ────────────────────────────────────────────────────────────────────

/// Per-server limits passed into every connection handler.
#[derive(Clone, Copy)]
pub struct Limits {
    pub rate_per_sec:      f64,   // sustained writes/sec per connection (0 = disabled)
    pub rate_burst:        f64,   // burst capacity
    pub max_since:         usize, // max events per SINCE response
    pub idle_timeout_secs: u64,   // inactivity timeout in seconds (0 = disabled)
    pub max_auth_failures: u32,   // drop connection after N consecutive failures (0 = disabled)
}

// ── Token-bucket rate limiter ─────────────────────────────────────────────────

struct RateLimiter {
    tokens:   f64,
    capacity: f64,
    rate:     f64,           // tokens added per second
    last:     std::time::Instant,
    disabled: bool,
}

impl RateLimiter {
    fn new(capacity: f64, rate: f64) -> Self {
        Self {
            tokens:   capacity,
            capacity,
            rate,
            last:     std::time::Instant::now(),
            disabled: rate == 0.0,
        }
    }

    /// Try to consume `cost` tokens. Returns false → rate limited.
    fn try_consume(&mut self, cost: f64) -> bool {
        if self.disabled { return true; }
        let now     = std::time::Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last   = now;
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn handle<S>(
    stream:      S,
    log:         EventLog,
    index:       Index,
    broker:      Broker,
    auth:        AuthStore,
    cluster:     Option<Arc<Cluster>>,
    wal_async:   bool,
    limits:      Limits,
    rules:       Arc<RulesEngine>,
    metrics:     MetricsState,
    audit:       Option<AuditLog>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut buf = BufReader::new(reader);

    // Protocol detection: peek at first byte.
    // Binary client opcodes: 0x01–0x17 (< 0x20)
    // Text commands: ASCII letters (>= 0x41)
    let binary = {
        let peek = buf.fill_buf().await?;
        if peek.is_empty() { return Ok(()); }
        peek[0] < 0x20
    };

    let mut token:         Option<Token>                                    = None;
    let mut tx_buf:        Option<(u64, u64, Vec<crate::core::event::Event>)> = None;
    let mut rate_limiter:  RateLimiter = RateLimiter::new(limits.rate_burst, limits.rate_per_sec);
    let mut auth_failures: u32 = 0;
    let (watch_tx, mut watch_rx) = mpsc::channel::<crate::core::event::Event>(1024);
    let (ctrl_tx,  mut ctrl_rx)  = mpsc::channel::<String>(32);
    // sub_tx carries historical + live events for SUBSCRIBE (None = LIVE marker)
    let (sub_tx, mut sub_rx) = mpsc::channel::<Option<crate::core::event::Event>>(4096);
    // bdeq_tx carries long-poll DEQUEUE results (JOB ... or EMPTY)
    let (bdeq_tx, mut bdeq_rx) = mpsc::channel::<String>(16);

    let idle_dur = (limits.idle_timeout_secs > 0)
        .then(|| tokio::time::Duration::from_secs(limits.idle_timeout_secs));

    if binary {
        // ── Binary mode ───────────────────────────────────────────────────────
        loop {
            tokio::select! {
                result = codec::read_command_raw(&mut buf) => {
                    match result {
                        Err(e) if is_eof(&e) => break,
                        Err(e) => return Err(e),
                        Ok((cmd, raw_frame)) => {
                            // ── Cluster routing ───────────────────────────────
                            if let Some(ref router) = cluster {
                                if let Some(stream_name) = cmd.primary_stream() {
                                    if !router.is_local(stream_name) {
                                        // Reject cross-shard ops inside a transaction — we
                                        // can't atomically commit across nodes.
                                        if tx_buf.is_some() {
                                            writer.write_all(&codec::encode_response(
                                                &format!("ERR cross-shard transaction not supported: \
                                                    stream {stream_name} lives on a different node — \
                                                    ROLLBACK and retry on the owning node\n")
                                            )).await?;
                                            continue;
                                        }
                                        match router.forward(stream_name, raw_frame).await {
                                            Ok(resp) => {
                                                metrics.forwards_total.fetch_add(1, Ordering::Relaxed);
                                                writer.write_all(&resp).await?;
                                                continue;
                                            }
                                            Err(e)   => {
                                                writer.write_all(
                                                    &codec::encode_response(&format!("ERR cluster forward failed: {e}\n"))
                                                ).await?;
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }

                            // ── Local dispatch ────────────────────────────────
                            let (audit_op, audit_stream) = audit_info(&cmd);
                            let resp = dispatch(
                                cmd, &log, &index, &broker, &auth,
                                &mut token, &watch_tx, &ctrl_tx, &sub_tx.clone(), &bdeq_tx, &mut tx_buf,
                                &cluster, wal_async, &mut rate_limiter, &limits, &rules,
                            ).await;
                            let ok = !resp.starts_with("ERR");
                            if audit_op == "AUTH" {
                                if ok { auth_failures = 0; }
                                else {
                                    auth_failures += 1;
                                    if limits.max_auth_failures > 0 && auth_failures >= limits.max_auth_failures {
                                        writer.write_all(&codec::encode_response(&resp)).await?;
                                        writer.write_all(&codec::encode_response("ERR too many failed authentications — connection closed\n")).await?;
                                        break;
                                    }
                                }
                            }
                            if !audit_op.is_empty() {
                                if let Some(ref al) = audit {
                                    let user   = token.as_ref().map(|t| t.user_id.as_deref().unwrap_or("token")).unwrap_or("-");
                                    let stream = audit_stream.as_deref().unwrap_or("-");
                                    al.log(audit_op, user, stream, ok);
                                }
                            }
                            if !resp.is_empty() {
                                writer.write_all(&codec::encode_response(&resp)).await?;
                            }
                        }
                    }
                }

                Some(event) = watch_rx.recv() => {
                    let msg = format!("EVENT {} {}\n", event.stream, event.payload);
                    writer.write_all(&codec::encode_response(&msg)).await?;
                }

                Some(maybe_event) = sub_rx.recv() => {
                    match maybe_event {
                        Some(e) => {
                            let msg = format!("SEVENT {} {} {}\n", e.stream, e.id, e.payload);
                            writer.write_all(&codec::encode_response(&msg)).await?;
                        }
                        None => {
                            writer.write_all(&codec::encode_response("LIVE\n")).await?;
                        }
                    }
                }

                Some(resp) = bdeq_rx.recv() => {
                    writer.write_all(&codec::encode_response(&resp)).await?;
                }

                Some(err) = ctrl_rx.recv() => {
                    writer.write_all(&codec::encode_response(&format!("ERR {err}\n"))).await?;
                    break;
                }

                _ = shutdown_rx.changed() => break,

                _ = async {
                    if let Some(d) = idle_dur { tokio::time::sleep(d).await }
                    else { std::future::pending::<()>().await }
                } => {
                    let _ = writer.write_all(&codec::encode_response("ERR idle timeout\n")).await;
                    break;
                }
            }
        }
    } else {
        // ── Text mode (ws_proxy, telnet, legacy clients) ───────────────────────
        let mut lines = buf.lines();
        loop {
            tokio::select! {
                line = lines.next_line() => {
                    match line? {
                        None => break,
                        Some(line) => {
                            let mut audit_op: &'static str = "";
                            let mut audit_stream: Option<String> = None;
                            let resp = match Command::parse(&line) {
                                Ok(cmd) => {
                                    (audit_op, audit_stream) = audit_info(&cmd);
                                    // Text mode cluster routing: forward to primary if needed.
                                    if let Some(ref router) = cluster {
                                        if let Some(stream_name) = cmd.primary_stream() {
                                            if !router.is_local(stream_name) {
                                                if tx_buf.is_some() {
                                                    format!("ERR cross-shard transaction not supported: \
                                                        stream {stream_name} lives on a different node — \
                                                        ROLLBACK and retry on the owning node\n")
                                                } else if let Some(frame) = encode_for_forward(&cmd) {
                                                    match router.forward(stream_name, frame).await {
                                                        Ok(resp) if resp.len() > 5 =>
                                                            format!("{}\n", String::from_utf8_lossy(&resp[5..])),
                                                        Ok(_)    => "ERR empty forward response\n".to_string(),
                                                        Err(e)   => format!("ERR cluster forward failed: {e}\n"),
                                                    }
                                                } else {
                                                    dispatch(
                                                        cmd, &log, &index, &broker, &auth,
                                                        &mut token, &watch_tx, &ctrl_tx, &sub_tx.clone(), &bdeq_tx, &mut tx_buf,
                                                        &cluster, wal_async, &mut rate_limiter, &limits, &rules,
                                                    ).await
                                                }
                                            } else {
                                                dispatch(
                                                    cmd, &log, &index, &broker, &auth,
                                                    &mut token, &watch_tx, &ctrl_tx, &sub_tx.clone(), &bdeq_tx, &mut tx_buf,
                                                    &cluster, wal_async, &mut rate_limiter, &limits, &rules,
                                                ).await
                                            }
                                        } else {
                                            dispatch(
                                                cmd, &log, &index, &broker, &auth,
                                                &mut token, &watch_tx, &ctrl_tx, &sub_tx.clone(), &bdeq_tx, &mut tx_buf,
                                                &cluster, wal_async, &mut rate_limiter, &limits, &rules,
                                            ).await
                                        }
                                    } else {
                                        dispatch(
                                            cmd, &log, &index, &broker, &auth,
                                            &mut token, &watch_tx, &ctrl_tx, &sub_tx.clone(), &bdeq_tx, &mut tx_buf,
                                            &cluster, wal_async, &mut rate_limiter, &limits, &rules,
                                        ).await
                                    }
                                }
                                Err(e) => format!("ERR {e}\n"),
                            };
                            let ok = !resp.starts_with("ERR");
                            if audit_op == "AUTH" {
                                if ok { auth_failures = 0; }
                                else {
                                    auth_failures += 1;
                                    if limits.max_auth_failures > 0 && auth_failures >= limits.max_auth_failures {
                                        writer.write_all(resp.as_bytes()).await?;
                                        writer.write_all(b"ERR too many failed authentications - connection closed\n").await?;
                                        break;
                                    }
                                }
                            }
                            if !audit_op.is_empty() {
                                if let Some(ref al) = audit {
                                    let user   = token.as_ref().map(|t| t.user_id.as_deref().unwrap_or("token")).unwrap_or("-");
                                    let stream = audit_stream.as_deref().unwrap_or("-");
                                    al.log(audit_op, user, stream, ok);
                                }
                            }
                            if !resp.is_empty() {
                                writer.write_all(resp.as_bytes()).await?;
                            }
                        }
                    }
                }

                Some(event) = watch_rx.recv() => {
                    let msg = format!("EVENT {} {}\n", event.stream, event.payload);
                    writer.write_all(msg.as_bytes()).await?;
                }

                Some(maybe_event) = sub_rx.recv() => {
                    match maybe_event {
                        Some(e) => {
                            let msg = format!("SEVENT {} {} {}\n", e.stream, e.id, e.payload);
                            writer.write_all(msg.as_bytes()).await?;
                        }
                        None => {
                            writer.write_all(b"LIVE\n").await?;
                        }
                    }
                }

                Some(resp) = bdeq_rx.recv() => {
                    writer.write_all(resp.as_bytes()).await?;
                }

                Some(err) = ctrl_rx.recv() => {
                    writer.write_all(format!("ERR {err}\n").as_bytes()).await?;
                    break;
                }

                _ = shutdown_rx.changed() => break,

                _ = async {
                    if let Some(d) = idle_dur { tokio::time::sleep(d).await }
                    else { std::future::pending::<()>().await }
                } => {
                    let _ = writer.write_all(b"ERR idle timeout\n").await;
                    break;
                }
            }
        }
    }

    if tx_buf.is_some() {
        eprintln!("client disconnected with open transaction — rolled back");
    }
    Ok(())
}

/// Returns (op_name, stream) for auditable commands. Empty op_name = skip logging.
fn audit_info(cmd: &Command) -> (&'static str, Option<String>) {
    match cmd {
        Command::Auth  { .. }          => ("AUTH",         None),
        Command::Emit  { stream, .. }  => ("EMIT",         Some(stream.clone())),
        Command::Patch { stream, .. }  => ("PATCH",        Some(stream.clone())),
        Command::Del   { stream }      => ("DEL",          Some(stream.clone())),
        Command::Claim { stream }      => ("CLAIM",        Some(stream.clone())),
        Command::TokenCreate { .. }    => ("TOKEN_CREATE", None),
        Command::TokenRevoke { raw }   => ("TOKEN_REVOKE", Some(raw.clone())),
        _                              => ("",             None),
    }
}

/// Encode a routable command as a binary frame for cluster forwarding.
/// Returns None for commands that can't be forwarded (shouldn't happen if called correctly).
fn encode_for_forward(cmd: &Command) -> Option<Vec<u8>> {
    match cmd {
        Command::Emit   { stream, payload, ttl } => Some(codec::encode_emit(stream, payload, *ttl)),
        Command::Patch  { stream, patch }        => Some(codec::encode_patch(stream, patch)),
        Command::Get    { stream }               => Some(codec::encode_get(stream)),
        Command::Del    { stream }               => Some(codec::encode_del(stream)),
        Command::Since  { stream, ts }           => Some(codec::encode_since(stream, *ts)),
        Command::Expire { stream, secs }         => Some(codec::encode_expire(stream, *secs)),
        _ => None,
    }
}

fn is_eof(e: &anyhow::Error) -> bool {
    e.downcast_ref::<std::io::Error>()
        .map(|e| e.kind() == std::io::ErrorKind::UnexpectedEof)
        .unwrap_or(false)
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

/// Fire reactive rules for a just-written event. No cascading (derived events don't trigger more rules).
/// Derived events always use fire-and-forget WAL writes to avoid blocking the originating command.
async fn fire_rules(event: &Event, rules: &RulesEngine, log: &EventLog, index: &Index, broker: &Broker) {
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

/// Auto-index geo position when a payload contains numeric `lat` + `lng` fields.
fn try_geo_index(index: &Index, stream: &str, payload: &serde_json::Value) {
    if let (Some(lat), Some(lng)) = (
        payload.get("lat").and_then(|v| v.as_f64()),
        payload.get("lng").and_then(|v| v.as_f64()),
    ) {
        index.geo_set(stream, lat, lng);
    }
}

/// Fire token template rules — auto-create tokens when streams are written.
fn fire_token_rules(event: &Event, rules: &RulesEngine, auth: &AuthStore) {
    for (raw, role_str, namespaces) in rules.fire_token_rules(event) {
        let role = match role_str.to_lowercase().as_str() {
            "writer" => Role::Writer,
            _        => Role::Reader,
        };
        auth.create(raw, Token {
            role,
            namespaces,
            can_watch:  true,
            can_since:  true,
            expires_at: None,
            blocked:    false,
            user_id:    None,
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    cmd:          Command,
    log:          &EventLog,
    index:        &Index,
    broker:       &Broker,
    auth:         &AuthStore,
    token:        &mut Option<Token>,
    watch_tx:     &mpsc::Sender<crate::core::event::Event>,
    ctrl_tx:      &mpsc::Sender<String>,
    sub_tx:       &mpsc::Sender<Option<crate::core::event::Event>>,
    bdeq_tx:      &mpsc::Sender<String>,
    tx_buf:       &mut Option<(u64, u64, Vec<crate::core::event::Event>)>,
    cluster:      &Option<Arc<Cluster>>,
    wal_async:    bool,
    rate_limiter: &mut RateLimiter,
    limits:       &Limits,
    rules:        &RulesEngine,
) -> String {
    match cmd {
        Command::Ping => "PONG\n".to_string(),

        Command::Auth { token: raw } => {
            if let Some(t) = auth.verify(&raw) {
                *token = Some(t);
                "OK\n".to_string()
            } else if let Some(t) = auth.verify_jwt(&raw) {
                *token = Some(t);
                "OK\n".to_string()
            } else {
                "ERR invalid token\n".to_string()
            }
        }

        Command::Begin => {
            if tx_buf.is_some() { return "ERR transaction already open\n".to_string(); }
            let txn_id   = log.begin_txn();
            let snapshot = index.last_event_id();
            *tx_buf = Some((txn_id, snapshot, Vec::new()));
            "OK transaction started\n".to_string()
        }

        Command::Commit => {
            let Some((txn_id, snapshot_id, buf)) = tx_buf.take() else {
                return "ERR no transaction open\n".to_string();
            };
            let write_set: Vec<String> = buf.iter().map(|e| e.stream.clone()).collect();
            if let Some(conflict) = index.conflict_check(&write_set, snapshot_id) {
                return format!("ERR conflict on stream {conflict} — transaction aborted\n");
            }
            if let Err(e) = log.commit_txn(txn_id).await {
                return format!("ERR commit failed: {e}\n");
            }
            let count = buf.len();
            for event in buf {
                if let Some(router) = cluster { router.broadcast_event(&event).await; }
                broker.publish(&event).await;
                index.apply(event);
            }
            format!("OK committed {count} operations\n")
        }

        Command::Rollback => {
            if tx_buf.take().is_none() { return "ERR no transaction open\n".to_string(); }
            "OK rolled back\n".to_string()
        }

        Command::Emit { stream, payload, ttl } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &stream) { return "ERR forbidden\n".to_string(); }
            if !rate_limiter.try_consume(1.0) { return "ERR rate limit exceeded\n".to_string(); }
            if let Some(owner) = index.get_owner(&stream) {
                if !matches!(t.role, Role::Admin) && t.user_id.as_deref() != Some(owner.as_str()) {
                    return "ERR forbidden: stream is owned by another user\n".to_string();
                }
            }
            if let Some((txn_id, _, buf)) = tx_buf {
                if let Some(router) = cluster {
                    if !router.is_local(&stream) {
                        return format!("ERR cross-shard transaction not supported: \
                            stream {stream} lives on a different node — \
                            ROLLBACK and retry on the owning node\n");
                    }
                }
                match log.append_pending(*txn_id, &stream, Op::Set, payload).await {
                    Ok(event) => { buf.push(event); }
                    Err(e)    => return format!("ERR {e}\n"),
                }
                return "QUEUED\n".to_string();
            }
            if wal_async {
                let event = log.append_nowait(&stream, Op::Set, payload);
                if let Some(secs) = ttl { index.set_expiry(&event.stream, secs); }
                if let Some(router) = cluster { router.broadcast_event(&event).await; }
                broker.publish(&event).await;
                try_geo_index(index, &event.stream, &event.payload);
                index.apply(event.clone());
                fire_rules(&event, rules, log, index, broker).await;
                fire_token_rules(&event, rules, auth);
                "OK\n".to_string()
            } else {
                match log.append(&stream, Op::Set, payload).await {
                    Ok(event) => {
                        if let Some(secs) = ttl { index.set_expiry(&event.stream, secs); }
                        if let Some(router) = cluster { router.broadcast_event(&event).await; }
                        broker.publish(&event).await;
                        try_geo_index(index, &event.stream, &event.payload);
                        index.apply(event.clone());
                        fire_rules(&event, rules, log, index, broker).await;
                fire_token_rules(&event, rules, auth);
                        "OK\n".to_string()
                    }
                    Err(e) => format!("ERR {e}\n"),
                }
            }
        }

        Command::EmitMany { entries } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            for (stream, _) in &entries {
                if !auth.can_write(t, stream) { return format!("ERR forbidden on {stream}\n"); }
            }
            if !rate_limiter.try_consume(entries.len() as f64) { return "ERR rate limit exceeded\n".to_string(); }
            let count = entries.len();
            let (local_entries, remote_entries): (Vec<_>, Vec<_>) = entries.into_iter()
                .partition(|(stream, _)| cluster.as_ref().map(|r| r.is_local(stream)).unwrap_or(true));
            // Forward non-local entries to their primary shards.
            if let Some(router) = cluster {
                for (stream, payload) in remote_entries {
                    let frame = codec::encode_emit(&stream, &payload, None);
                    if let Err(e) = router.forward(&stream, frame).await {
                        return format!("ERR forward failed for {stream}: {e}\n");
                    }
                }
            }
            if wal_async {
                for (stream, payload) in local_entries {
                    let event = log.append_nowait(&stream, Op::Set, payload);
                    if let Some(router) = cluster { router.broadcast_event(&event).await; }
                    broker.publish(&event).await;
                    index.apply(event);
                }
            } else {
                for (stream, payload) in local_entries {
                    match log.append(&stream, Op::Set, payload).await {
                        Ok(event) => {
                            if let Some(router) = cluster { router.broadcast_event(&event).await; }
                            broker.publish(&event).await;
                            index.apply(event);
                        }
                        Err(e) => return format!("ERR {e}\n"),
                    }
                }
            }
            format!("OK {count} emitted\n")
        }

        Command::Patch { stream, patch } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &stream) { return "ERR forbidden\n".to_string(); }
            if !rate_limiter.try_consume(1.0) { return "ERR rate limit exceeded\n".to_string(); }
            if let Some(owner) = index.get_owner(&stream) {
                if !matches!(t.role, Role::Admin) && t.user_id.as_deref() != Some(owner.as_str()) {
                    return "ERR forbidden: stream is owned by another user\n".to_string();
                }
            }
            if let Some((txn_id, _, buf)) = tx_buf {
                if let Some(router) = cluster {
                    if !router.is_local(&stream) {
                        return format!("ERR cross-shard transaction not supported: \
                            stream {stream} lives on a different node — \
                            ROLLBACK and retry on the owning node\n");
                    }
                }
                let merged = index.patch(&stream, patch.clone()).unwrap_or(patch);
                match log.append_pending(*txn_id, &stream, Op::Set, merged).await {
                    Ok(event) => { buf.push(event); }
                    Err(e)    => return format!("ERR {e}\n"),
                }
                return "QUEUED\n".to_string();
            }
            let merged = index.patch(&stream, patch.clone()).unwrap_or(patch);
            if wal_async {
                let event = log.append_nowait(&stream, Op::Set, merged);
                if let Some(router) = cluster { router.broadcast_event(&event).await; }
                broker.publish(&event).await;
                try_geo_index(index, &event.stream, &event.payload);
                index.apply(event.clone());
                fire_rules(&event, rules, log, index, broker).await;
                fire_token_rules(&event, rules, auth);
                "OK\n".to_string()
            } else {
                match log.append(&stream, Op::Set, merged).await {
                    Ok(event) => {
                        if let Some(router) = cluster { router.broadcast_event(&event).await; }
                        broker.publish(&event).await;
                        try_geo_index(index, &event.stream, &event.payload);
                        index.apply(event.clone());
                        fire_rules(&event, rules, log, index, broker).await;
                fire_token_rules(&event, rules, auth);
                        "OK\n".to_string()
                    }
                    Err(e) => format!("ERR {e}\n"),
                }
            }
        }

        Command::Watch { pattern, filter } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_watch(t, &pattern) { return "ERR forbidden\n".to_string(); }
            let mut rx   = broker.subscribe(&pattern, filter).await;
            let tx       = watch_tx.clone();
            let ctrl     = ctrl_tx.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => { if tx.send(event).await.is_err() { break; } }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            let _ = ctrl.send(format!("lagged: {n} events dropped — reconnect")).await;
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
            format!("WATCHING {pattern}\n")
        }

        Command::Get { stream } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &stream) { return "ERR forbidden\n".to_string(); }
            match index.latest(&stream) {
                Some(val) => format!("VALUE {val}\n"),
                None      => "NULL\n".to_string(),
            }
        }

        Command::Del { stream } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &stream) { return "ERR forbidden\n".to_string(); }
            if !rate_limiter.try_consume(1.0) { return "ERR rate limit exceeded\n".to_string(); }
            if let Some(owner) = index.get_owner(&stream) {
                if !matches!(t.role, Role::Admin) && t.user_id.as_deref() != Some(owner.as_str()) {
                    return "ERR forbidden: stream is owned by another user\n".to_string();
                }
            }
            if let Some((txn_id, _, buf)) = tx_buf {
                if let Some(router) = cluster {
                    if !router.is_local(&stream) {
                        return format!("ERR cross-shard transaction not supported: \
                            stream {stream} lives on a different node — \
                            ROLLBACK and retry on the owning node\n");
                    }
                }
                match log.append_pending(*txn_id, &stream, Op::Delete, serde_json::Value::Null).await {
                    Ok(event) => { buf.push(event); }
                    Err(e)    => return format!("ERR {e}\n"),
                }
                return "QUEUED\n".to_string();
            }
            if wal_async {
                let event = log.append_nowait(&stream, Op::Delete, serde_json::Value::Null);
                if let Some(router) = cluster { router.broadcast_event(&event).await; }
                broker.publish(&event).await;
                index.apply(event.clone());
                fire_rules(&event, rules, log, index, broker).await;
                fire_token_rules(&event, rules, auth);
                "OK\n".to_string()
            } else {
                match log.append(&stream, Op::Delete, serde_json::Value::Null).await {
                    Ok(event) => {
                        if let Some(router) = cluster { router.broadcast_event(&event).await; }
                        broker.publish(&event).await;
                        index.apply(event.clone());
                        fire_rules(&event, rules, log, index, broker).await;
                fire_token_rules(&event, rules, auth);
                        "OK\n".to_string()
                    }
                    Err(e) => format!("ERR {e}\n"),
                }
            }
        }

        Command::Expire { stream, secs } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &stream) { return "ERR forbidden\n".to_string(); }
            index.set_expiry(&stream, secs);
            format!("OK expires in {secs}s\n")
        }

        Command::Keys { pattern } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &pattern) { return "ERR forbidden\n".to_string(); }
            let mut keys: std::collections::HashSet<String> =
                index.keys(&pattern).into_iter().collect();
            if let Some(router) = cluster {
                for text in router.scatter(codec::encode_keys(&pattern)).await {
                    for line in text.lines() {
                        if let Some(k) = line.strip_prefix("KEY ") { keys.insert(k.to_string()); }
                    }
                }
            }
            if keys.is_empty() { return "EMPTY\n".to_string(); }
            let mut sorted: Vec<String> = keys.into_iter().collect();
            sorted.sort();
            sorted.iter().map(|k| format!("KEY {k}\n")).collect()
        }

        Command::Query { pattern, clause, limit } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &pattern) { return "ERR forbidden\n".to_string(); }
            // Use IndexMap-style dedup: first occurrence wins (local preferred).
            let mut seen = std::collections::HashSet::new();
            let mut rows: Vec<(String, serde_json::Value)> = Vec::new();
            for (s, v) in index.query(&pattern, &clause, limit) {
                seen.insert(s.clone());
                rows.push((s, v));
            }
            if let Some(router) = cluster {
                for text in router.scatter(codec::encode_query(&pattern, &clause, limit)).await {
                    for line in text.lines() {
                        if let Some(rest) = line.strip_prefix("ROW ") {
                            if let Some(sp) = rest.find(' ') {
                                let stream = &rest[..sp];
                                if seen.insert(stream.to_string()) {
                                    if let Ok(v) = serde_json::from_str(&rest[sp+1..]) {
                                        rows.push((stream.to_string(), v));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if rows.is_empty() { return "EMPTY\n".to_string(); }
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            if let Some(n) = limit { rows.truncate(n); }
            rows.iter().map(|(s, v)| format!("ROW {s} {v}\n")).collect()
        }

        Command::Count { pattern, clause } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &pattern) { return "ERR forbidden\n".to_string(); }
            let mut total = index.count(&pattern, clause.as_ref());
            if let Some(router) = cluster {
                for text in router.scatter(codec::encode_count(&pattern, clause.as_ref())).await {
                    for line in text.lines() {
                        if let Some(n) = line.strip_prefix("COUNT ") {
                            total += n.trim().parse::<usize>().unwrap_or(0);
                        }
                    }
                }
            }
            format!("COUNT {total}\n")
        }

        Command::Sum { pattern, field, clause } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &pattern) { return "ERR forbidden\n".to_string(); }
            let mut total = index.sum(&pattern, &field, clause.as_ref());
            if let Some(router) = cluster {
                for text in router.scatter(codec::encode_sum(&pattern, &field, clause.as_ref())).await {
                    for line in text.lines() {
                        if let Some(n) = line.strip_prefix("SUM ") {
                            total += n.trim().parse::<f64>().unwrap_or(0.0);
                        }
                    }
                }
            }
            format!("SUM {total}\n")
        }

        Command::Avg { pattern, field, clause } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &pattern) { return "ERR forbidden\n".to_string(); }
            let (mut total_sum, mut total_count) = index.sum_and_count(&pattern, &field, clause.as_ref());
            if let Some(router) = cluster {
                for text in router.scatter(codec::encode_avg(&pattern, &field, clause.as_ref())).await {
                    for line in text.lines() {
                        if let Some(rest) = line.strip_prefix("AVGPARTS ") {
                            let mut parts = rest.split_whitespace();
                            let s = parts.next().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
                            let n = parts.next().and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
                            total_sum   += s;
                            total_count += n;
                        }
                    }
                }
            }
            if total_count == 0 { "NULL\n".to_string() }
            else { format!("AVG {}\n", total_sum / total_count as f64) }
        }

        Command::Since { stream, ts } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_since(t, &stream) { return "ERR forbidden\n".to_string(); }
            let (events, truncated) = index.since(&stream, ts, limits.max_since);
            if events.is_empty() { return "EMPTY\n".to_string(); }
            let mut out = String::new();
            if truncated {
                out.push_str(&format!("TRUNCATED {}\n", limits.max_since));
            }
            for e in &events {
                out.push_str(&format!("ROW {} {}\n", e.stream, e.payload));
            }
            out
        }

        Command::TokenCreate { raw, role, namespaces, expires_at } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            let new_token = crate::core::auth::Token {
                role: match role.to_lowercase().as_str() {
                    "writer" => crate::core::auth::Role::Writer,
                    _        => crate::core::auth::Role::Reader,
                },
                namespaces,
                can_watch:  true,
                can_since:  true,
                expires_at: expires_at.as_deref().and_then(crate::core::auth::parse_date_secs_pub),
                blocked:    false,
                user_id:    None,
            };
            auth.create(raw.clone(), new_token);
            format!("OK token {raw} created\n")
        }

        Command::TokenRevoke { raw } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            if auth.revoke(&raw) { format!("OK token {raw} revoked\n") }
            else { format!("ERR token {raw} not found\n") }
        }

        Command::TokenBlock { raw } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            if auth.block(&raw) { format!("OK token {raw} blocked\n") }
            else { format!("ERR token {raw} not found\n") }
        }

        Command::TokenUnblock { raw } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            if auth.unblock(&raw) { format!("OK token {raw} unblocked\n") }
            else { format!("ERR token {raw} not found\n") }
        }

        Command::TokenList => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            let tokens = auth.list();
            if tokens.is_empty() { return "EMPTY\n".to_string(); }
            tokens.iter().map(|(raw, role, ns)| {
                let ns_str: String = if ns.is_empty() { "*".to_string() } else { ns.join(",") };
                format!("TOKEN {raw} {role} {ns_str}\n")
            }).collect()
        }

        // ── Reactive rules ────────────────────────────────────────────────────

        Command::WhenAdd { pattern, target, template, condition } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            let rule = crate::core::rules::Rule {
                pattern:   pattern.clone(),
                action:    crate::core::rules::ActionType::Emit,
                target:    Some(target.clone()),
                queue:     None,
                template,
                condition: condition.map(|(field, op, value)|
                    crate::core::rules::RuleCondition { field, op, value }),
            };
            rules.add(rule);
            format!("OK rule added: {pattern} → {target}\n")
        }

        Command::WhenEnqueueAdd { pattern, queue, template, condition } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            let rule = crate::core::rules::Rule {
                pattern:   pattern.clone(),
                action:    crate::core::rules::ActionType::Enqueue,
                target:    None,
                queue:     Some(queue.clone()),
                template,
                condition: condition.map(|(field, op, value)|
                    crate::core::rules::RuleCondition { field, op, value }),
            };
            rules.add(rule);
            format!("OK rule added: {pattern} → ENQUEUE {queue}\n")
        }

        Command::WhenDel { pattern, key } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            if rules.remove(&pattern, &key) {
                format!("OK rule removed: {pattern} → {key}\n")
            } else {
                "ERR rule not found\n".to_string()
            }
        }

        Command::WhenList => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if t.role != crate::core::auth::Role::Admin { return "ERR forbidden — admin only\n".to_string(); }
            let list = rules.list();
            if list.is_empty() { return "EMPTY\n".to_string(); }
            list.iter().map(|r| {
                let cond = r.condition.as_ref()
                    .map(|c| format!(" WHERE {} {} {}", c.field, c.op, c.value))
                    .unwrap_or_default();
                match r.action {
                    crate::core::rules::ActionType::Emit =>
                        format!("RULE {}{} → EMIT {} {}\n",
                            r.pattern, cond,
                            r.target.as_deref().unwrap_or("?"), r.template),
                    crate::core::rules::ActionType::Enqueue =>
                        format!("RULE {}{} → ENQUEUE {} {}\n",
                            r.pattern, cond,
                            r.queue.as_deref().unwrap_or("?"), r.template),
                }
            }).collect()
        }

        // ── Stream ownership ──────────────────────────────────────────────────

        Command::Claim { stream } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            let Some(ref uid) = t.user_id else {
                return "ERR CLAIM requires a JWT-authenticated session (no user_id on this token)\n".to_string();
            };
            if !auth.can_write(t, &stream) { return "ERR forbidden\n".to_string(); }
            index.claim(&stream, uid.clone());
            format!("OK claimed {stream} for {uid}\n")
        }

        // ── Token templates ───────────────────────────────────────────────────

        Command::WhenTokenAdd { pattern, token_tmpl, role, namespaces } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !matches!(t.role, Role::Admin) { return "ERR forbidden — admin only\n".to_string(); }
            rules.add_token_rule(crate::core::rules::TokenRule {
                pattern: pattern.clone(), token_tmpl: token_tmpl.clone(), role, namespaces,
            });
            format!("OK token rule added: {pattern} → {token_tmpl}\n")
        }

        Command::WhenTokenDel { pattern, token_tmpl } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !matches!(t.role, Role::Admin) { return "ERR forbidden — admin only\n".to_string(); }
            if rules.remove_token_rule(&pattern, &token_tmpl) {
                format!("OK token rule removed: {pattern} → {token_tmpl}\n")
            } else {
                "ERR token rule not found\n".to_string()
            }
        }

        Command::WhenTokenList => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !matches!(t.role, Role::Admin) { return "ERR forbidden — admin only\n".to_string(); }
            let list = rules.list_token_rules();
            if list.is_empty() { return "EMPTY\n".to_string(); }
            list.iter().map(|r| {
                let ns = r.namespaces.join(",");
                format!("TOKEN_RULE {} {} {} {}\n", r.pattern, r.token_tmpl, r.role, ns)
            }).collect()
        }

        // ── Geospatial ────────────────────────────────────────────────────────

        Command::GeoSet { stream, lat, lng } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &stream) { return "ERR forbidden\n".to_string(); }
            index.geo_set(&stream, lat, lng);
            "OK\n".to_string()
        }

        Command::GeoGet { stream } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &stream) { return "ERR forbidden\n".to_string(); }
            match index.geo_get(&stream) {
                Some((lat, lng)) => format!("GEO_POS {lat} {lng}\n"),
                None             => "NULL\n".to_string(),
            }
        }

        Command::GeoDel { stream } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &stream) { return "ERR forbidden\n".to_string(); }
            index.geo_del(&stream);
            "OK\n".to_string()
        }

        Command::GeoNear { pattern, lat, lng, radius_km } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &pattern) { return "ERR forbidden\n".to_string(); }
            let results = index.geo_near(&pattern, lat, lng, radius_km);
            if results.is_empty() { return "GEO_END\n".to_string(); }
            let mut out = String::new();
            for (stream, slat, slng, dist) in &results {
                out.push_str(&format!("GEO_RESULT {stream} {slat} {slng} {dist:.6}\n"));
            }
            out.push_str("GEO_END\n");
            out
        }

        Command::GeoDist { stream1, stream2 } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &stream1) || !auth.can_read(t, &stream2) {
                return "ERR forbidden\n".to_string();
            }
            match index.geo_dist(&stream1, &stream2) {
                Some(km) => format!("GEO_DIST {km:.6}\n"),
                None     => "ERR one or both streams have no geo position\n".to_string(),
            }
        }

        // ── Durable queues ────────────────────────────────────────────────────

        Command::Enqueue { queue, payload } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &queue) { return "ERR forbidden\n".to_string(); }
            if !rate_limiter.try_consume(1.0) { return "ERR rate limit exceeded\n".to_string(); }
            let id = index.enqueue(&queue, payload);
            format!("OK {id}\n")
        }

        Command::Dequeue { queue, timeout_secs, wait_secs } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &queue) { return "ERR forbidden\n".to_string(); }
            // Try immediate dequeue first (covers both modes).
            if let Some(job) = index.dequeue(&queue, timeout_secs) {
                return format!("JOB {} {}\n", job.id, job.payload);
            }
            // If no job and long-poll requested, spawn background waiter.
            if let Some(ws) = wait_secs {
                let notify  = index.get_queue_notify(&queue);
                let idx     = index.clone();
                let tx      = bdeq_tx.clone();
                tokio::spawn(async move {
                    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(ws);
                    loop {
                        match tokio::time::timeout_at(deadline, notify.notified()).await {
                            Err(_) => { let _ = tx.send("EMPTY\n".to_string()).await; break; }
                            Ok(_)  => {
                                if let Some(job) = idx.dequeue(&queue, timeout_secs) {
                                    let _ = tx.send(format!("JOB {} {}\n", job.id, job.payload)).await;
                                    break;
                                }
                                // Another waiter got it first; loop back and wait again.
                            }
                        }
                    }
                });
                String::new() // response will arrive via bdeq_rx
            } else {
                "EMPTY\n".to_string()
            }
        }

        Command::Ack { queue, job_id } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &queue) { return "ERR forbidden\n".to_string(); }
            if index.ack(job_id) { "OK\n".to_string() }
            else { "ERR job not found or already acked\n".to_string() }
        }

        Command::Nack { queue, job_id } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_write(t, &queue) { return "ERR forbidden\n".to_string(); }
            if index.nack(job_id) { "OK\n".to_string() }
            else { "ERR job not found\n".to_string() }
        }

        Command::Qlen { queue } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &queue) { return "ERR forbidden\n".to_string(); }
            format!("COUNT {}\n", index.qlen(&queue))
        }

        Command::Qpeek { queue } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_read(t, &queue) { return "ERR forbidden\n".to_string(); }
            match index.qpeek(&queue) {
                Some(job) => format!("JOB {} {}\n", job.id, job.payload),
                None      => "EMPTY\n".to_string(),
            }
        }

        // ── Batch read ────────────────────────────────────────────────────────

        Command::Mget { streams } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            let mut out = String::new();
            for stream in streams {
                if !auth.can_read(t, &stream) {
                    out.push_str(&format!("ERR forbidden: {stream}\n"));
                    continue;
                }
                match index.latest(&stream) {
                    Some(payload) => out.push_str(&format!("VALUE {stream} {payload}\n")),
                    None          => out.push_str(&format!("NULL {stream}\n")),
                }
            }
            out
        }

        // ── Durable subscriptions ─────────────────────────────────────────────

        Command::Subscribe { pattern, from_id } => {
            let Some(t) = token else { return "ERR unauthenticated\n".to_string(); };
            if !auth.can_watch(t, &pattern) { return "ERR forbidden\n".to_string(); }
            if !auth.can_since(t, &pattern) { return "ERR forbidden: replay not allowed on this token\n".to_string(); }

            // Subscribe to live events FIRST — so no events slip through before replay.
            let mut live_rx = broker.subscribe(&pattern, None).await;

            // Collect in-memory history.
            let history = index.since_pattern(&pattern, from_id, limits.max_since);
            let replayed = history.len();

            // Pump history → LIVE marker → live events through sub_tx.
            let tx = sub_tx.clone();
            tokio::spawn(async move {
                for event in history {
                    if tx.send(Some(event)).await.is_err() { return; }
                }
                // Signal end of history.
                if tx.send(None).await.is_err() { return; }
                // Forward live events.
                loop {
                    match live_rx.recv().await {
                        Ok(event) => {
                            if tx.send(Some(event)).await.is_err() { break; }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // Too many events buffered — signal client then stop.
                            let _ = tx.send(None).await; // extra LIVE acts as warning marker
                            eprintln!("[subscribe] lagged: {n} events dropped");
                            break;
                        }
                        Err(_) => break,
                    }
                }
            });

            format!("SUBSCRIBED {pattern} {replayed}\n")
        }
    }
}

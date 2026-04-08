use crate::net::protocol::{Command, Condition, Logic, WhereClause};
use anyhow::{bail, Result};
use std::io::Cursor;
use tokio::io::AsyncReadExt;

/// Hard cap on JSON payload size. Protects against OOM via oversized EMIT.
/// 1 MB default — raise in config if you need larger payloads.
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024; // 1 MB

// ── Request opcodes ───────────────────────────────────────────────────────────

pub const OP_PING:          u8 = 0x01;
pub const OP_AUTH:          u8 = 0x02;
pub const OP_EMIT:          u8 = 0x03;
pub const OP_EMITMANY:      u8 = 0x04;
pub const OP_PATCH:         u8 = 0x05;
pub const OP_WATCH:         u8 = 0x06;
pub const OP_GET:           u8 = 0x07;
pub const OP_SINCE:         u8 = 0x08;
pub const OP_DEL:           u8 = 0x09;
pub const OP_KEYS:          u8 = 0x0A;
pub const OP_EXPIRE:        u8 = 0x0B;
pub const OP_QUERY:         u8 = 0x0C;
pub const OP_COUNT:         u8 = 0x0D;
pub const OP_SUM:           u8 = 0x0E;
pub const OP_AVG:           u8 = 0x0F;
pub const OP_TOKEN_CREATE:  u8 = 0x10;
pub const OP_TOKEN_REVOKE:  u8 = 0x11;
pub const OP_TOKEN_BLOCK:   u8 = 0x12;
pub const OP_TOKEN_UNBLOCK: u8 = 0x13;
pub const OP_TOKEN_LIST:    u8 = 0x14;
pub const OP_BEGIN:         u8 = 0x15;
pub const OP_COMMIT:        u8 = 0x16;
pub const OP_ROLLBACK:      u8 = 0x17;
pub const OP_WHEN_ADD:        u8 = 0x18;
pub const OP_WHEN_DEL:        u8 = 0x19;
pub const OP_WHEN_LIST:       u8 = 0x1A;
pub const OP_CLAIM:           u8 = 0x1B;
pub const OP_WHEN_TOKEN_ADD:  u8 = 0x1C;
pub const OP_WHEN_TOKEN_DEL:  u8 = 0x1D;
pub const OP_WHEN_TOKEN_LIST: u8 = 0x1E;
pub const OP_GEO_SET:         u8 = 0x1F;
pub const OP_GEO_GET:         u8 = 0x20;
pub const OP_GEO_DEL:         u8 = 0x21;
pub const OP_GEO_NEAR:        u8 = 0x22;
pub const OP_GEO_DIST:        u8 = 0x23;
pub const OP_SUBSCRIBE:       u8 = 0x24;
pub const OP_ENQUEUE:         u8 = 0x25;
pub const OP_DEQUEUE:         u8 = 0x26;
pub const OP_ACK:             u8 = 0x27;
pub const OP_NACK:            u8 = 0x28;
pub const OP_QLEN:            u8 = 0x29;
pub const OP_QPEEK:           u8 = 0x2A;
pub const OP_MGET:            u8 = 0x2B;

// ── Cluster opcodes (peer-to-peer only, >= 0xC0) ──────────────────────────────

pub const OP_CLUSTER_HELLO:   u8 = 0xC0;
pub const OP_CLUSTER_FORWARD: u8 = 0xC1;
pub const OP_CLUSTER_REPLICATE: u8 = 0xC2;
pub const OP_CLUSTER_PING:    u8 = 0xC3;
pub const OP_CLUSTER_PONG:    u8 = 0xC4;

// ── Response tags ─────────────────────────────────────────────────────────────

pub const TAG_PONG:     u8 = 0x01;
pub const TAG_OK:       u8 = 0x02;
pub const TAG_ERR:      u8 = 0x03;
pub const TAG_VALUE:    u8 = 0x04;
pub const TAG_NULL:     u8 = 0x05;
pub const TAG_EVENT:    u8 = 0x06;
pub const TAG_WATCHING: u8 = 0x07;
pub const TAG_ROW:      u8 = 0x08;
pub const TAG_KEY:      u8 = 0x09;
pub const TAG_EMPTY:    u8 = 0x0A;
pub const TAG_COUNT:    u8 = 0x0B;
pub const TAG_SUM:      u8 = 0x0C;
pub const TAG_AVG:      u8 = 0x0D;
pub const TAG_TOKEN:    u8 = 0x0E;
pub const TAG_QUEUED:   u8 = 0x0F;
pub const TAG_RULE:     u8 = 0x10;
pub const TAG_AVGPARTS:    u8 = 0x11;
pub const TAG_TOKEN_RULE:  u8 = 0x12;
pub const TAG_GEO_POS:     u8 = 0x13;
pub const TAG_GEO_RESULT:  u8 = 0x14;
pub const TAG_GEO_END:     u8 = 0x15;
pub const TAG_GEO_DIST:    u8 = 0x16;
pub const TAG_SUBSCRIBED:  u8 = 0x17;
pub const TAG_SEVENT:      u8 = 0x18;
pub const TAG_LIVE:        u8 = 0x19;
pub const TAG_JOB:         u8 = 0x1A;

// ── Read a binary request frame ───────────────────────────────────────────────
//
// Returns (Command, raw_frame_bytes).
// raw_frame_bytes is the complete wire bytes — used for cluster forwarding
// so we don't need to re-encode the command.

pub async fn read_command_raw<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<(Command, Vec<u8>)> {
    let mut hdr = [0u8; 5];
    r.read_exact(&mut hdr).await?;
    let opcode   = hdr[0];
    let body_len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body).await?;

    // Reconstruct full frame for potential forwarding.
    let mut raw = Vec::with_capacity(5 + body_len);
    raw.extend_from_slice(&hdr);
    raw.extend_from_slice(&body);

    let cmd = decode(opcode, &body)?;
    Ok((cmd, raw))
}

fn decode(op: u8, body: &[u8]) -> Result<Command> {
    let mut c = Cursor::new(body);
    match op {
        OP_PING       => Ok(Command::Ping),
        OP_BEGIN      => Ok(Command::Begin),
        OP_COMMIT     => Ok(Command::Commit),
        OP_ROLLBACK   => Ok(Command::Rollback),
        OP_TOKEN_LIST => Ok(Command::TokenList),

        OP_AUTH => Ok(Command::Auth { token: rs(&mut c)? }),

        OP_EMIT => {
            let stream  = rs(&mut c)?;
            let payload = rjson(&mut c)?;
            let ttl     = if ru8(&mut c)? == 1 { Some(ru64(&mut c)?) } else { None };
            Ok(Command::Emit { stream, payload, ttl })
        }

        OP_EMITMANY => {
            let count = ru16(&mut c)? as usize;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count { entries.push((rs(&mut c)?, rjson(&mut c)?)); }
            Ok(Command::EmitMany { entries })
        }

        OP_PATCH => Ok(Command::Patch { stream: rs(&mut c)?, patch: rjson(&mut c)? }),

        OP_WATCH => {
            let pattern = rs(&mut c)?;
            let filter  = if ru8(&mut c)? == 1 { Some(rwhere(&mut c)?) } else { None };
            Ok(Command::Watch { pattern, filter })
        }

        OP_GET    => Ok(Command::Get    { stream:  rs(&mut c)? }),
        OP_DEL    => Ok(Command::Del    { stream:  rs(&mut c)? }),
        OP_KEYS   => Ok(Command::Keys   { pattern: rs(&mut c)? }),

        OP_SINCE => { let s = rs(&mut c)?; let ts = ru64(&mut c)?; Ok(Command::Since { stream: s, ts }) }
        OP_EXPIRE => { let s = rs(&mut c)?; let n = ru64(&mut c)?; Ok(Command::Expire { stream: s, secs: n }) }

        OP_QUERY => {
            let pattern = rs(&mut c)?;
            let clause  = rwhere(&mut c)?;
            let limit   = if ru8(&mut c)? == 1 { Some(ru64(&mut c)? as usize) } else { None };
            Ok(Command::Query { pattern, clause, limit })
        }
        OP_COUNT => {
            let pattern = rs(&mut c)?;
            let clause  = if ru8(&mut c)? == 1 { Some(rwhere(&mut c)?) } else { None };
            Ok(Command::Count { pattern, clause })
        }
        OP_SUM => {
            let pattern = rs(&mut c)?; let field = rs(&mut c)?;
            let clause  = if ru8(&mut c)? == 1 { Some(rwhere(&mut c)?) } else { None };
            Ok(Command::Sum { pattern, field, clause })
        }
        OP_AVG => {
            let pattern = rs(&mut c)?; let field = rs(&mut c)?;
            let clause  = if ru8(&mut c)? == 1 { Some(rwhere(&mut c)?) } else { None };
            Ok(Command::Avg { pattern, field, clause })
        }

        OP_TOKEN_CREATE => {
            let raw = rs(&mut c)?; let role = rs(&mut c)?;
            let ns_count = ru8(&mut c)? as usize;
            let mut namespaces = Vec::with_capacity(ns_count);
            for _ in 0..ns_count { namespaces.push(rs(&mut c)?); }
            let expires_at = if ru8(&mut c)? == 1 { Some(rs(&mut c)?) } else { None };
            Ok(Command::TokenCreate { raw, role, namespaces, expires_at })
        }

        OP_TOKEN_REVOKE  => Ok(Command::TokenRevoke  { raw: rs(&mut c)? }),
        OP_TOKEN_BLOCK   => Ok(Command::TokenBlock   { raw: rs(&mut c)? }),
        OP_TOKEN_UNBLOCK => Ok(Command::TokenUnblock { raw: rs(&mut c)? }),

        OP_WHEN_ADD  => Ok(Command::WhenAdd  { pattern: rs(&mut c)?, target: rs(&mut c)?, template: rs(&mut c)?, condition: None }),
        OP_WHEN_DEL  => Ok(Command::WhenDel  { pattern: rs(&mut c)?, key: rs(&mut c)? }),
        OP_WHEN_LIST => Ok(Command::WhenList),

        OP_CLAIM           => Ok(Command::Claim { stream: rs(&mut c)? }),
        OP_WHEN_TOKEN_LIST => Ok(Command::WhenTokenList),
        OP_WHEN_TOKEN_DEL  => Ok(Command::WhenTokenDel  { pattern: rs(&mut c)?, token_tmpl: rs(&mut c)? }),
        OP_WHEN_TOKEN_ADD  => {
            let pattern    = rs(&mut c)?;
            let token_tmpl = rs(&mut c)?;
            let role       = rs(&mut c)?;
            let ns_count   = ru8(&mut c)? as usize;
            let mut namespaces = Vec::with_capacity(ns_count);
            for _ in 0..ns_count { namespaces.push(rs(&mut c)?); }
            Ok(Command::WhenTokenAdd { pattern, token_tmpl, role, namespaces })
        }

        OP_GEO_SET => {
            let stream = rs(&mut c)?;
            let lat    = rf64(&mut c)?;
            let lng    = rf64(&mut c)?;
            Ok(Command::GeoSet { stream, lat, lng })
        }
        OP_GEO_GET  => Ok(Command::GeoGet  { stream:  rs(&mut c)? }),
        OP_GEO_DEL  => Ok(Command::GeoDel  { stream:  rs(&mut c)? }),
        OP_GEO_NEAR => {
            let pattern   = rs(&mut c)?;
            let lat       = rf64(&mut c)?;
            let lng       = rf64(&mut c)?;
            let radius_km = rf64(&mut c)?;
            Ok(Command::GeoNear { pattern, lat, lng, radius_km })
        }
        OP_GEO_DIST => {
            let stream1 = rs(&mut c)?;
            let stream2 = rs(&mut c)?;
            Ok(Command::GeoDist { stream1, stream2 })
        }

        OP_SUBSCRIBE => {
            let pattern = rs(&mut c)?;
            let from_id = ru64(&mut c)?;
            Ok(Command::Subscribe { pattern, from_id })
        }

        OP_ENQUEUE => {
            let queue   = rs(&mut c)?;
            let payload = rjson(&mut c)?;
            Ok(Command::Enqueue { queue, payload })
        }
        OP_DEQUEUE => {
            let queue        = rs(&mut c)?;
            let timeout_secs = ru64(&mut c)?;
            // optional wait_secs: 0 means None (no long-poll)
            let remaining = c.get_ref().len().saturating_sub(c.position() as usize);
            let wait_raw = if remaining >= 8 { Some(ru64(&mut c)?) } else { None };
            let wait_secs = wait_raw.filter(|&w| w > 0);
            Ok(Command::Dequeue { queue, timeout_secs, wait_secs })
        }
        OP_ACK  => { let queue = rs(&mut c)?; let job_id = ru64(&mut c)?; Ok(Command::Ack  { queue, job_id }) }
        OP_NACK => { let queue = rs(&mut c)?; let job_id = ru64(&mut c)?; Ok(Command::Nack { queue, job_id }) }
        OP_QLEN  => Ok(Command::Qlen  { queue: rs(&mut c)? }),
        OP_QPEEK => Ok(Command::Qpeek { queue: rs(&mut c)? }),
        OP_MGET => {
            let count = ru64(&mut c)? as usize;
            let mut streams = Vec::with_capacity(count);
            for _ in 0..count { streams.push(rs(&mut c)?); }
            Ok(Command::Mget { streams })
        }

        other => bail!("unknown binary opcode: 0x{other:02X}"),
    }
}

// ── Encode a response as a binary frame ──────────────────────────────────────

pub fn encode_response(response: &str) -> Vec<u8> {
    let body = response.trim_end_matches('\n').as_bytes();
    let tag  = match response.split_ascii_whitespace().next().unwrap_or("") {
        "PONG"     => TAG_PONG,
        "OK"       => TAG_OK,
        "ERR"      => TAG_ERR,
        "VALUE"    => TAG_VALUE,
        "NULL"     => TAG_NULL,
        "EVENT"    => TAG_EVENT,
        "WATCHING" => TAG_WATCHING,
        "ROW"      => TAG_ROW,
        "KEY"      => TAG_KEY,
        "EMPTY"    => TAG_EMPTY,
        "COUNT"    => TAG_COUNT,
        "SUM"      => TAG_SUM,
        "AVG"      => TAG_AVG,
        "TOKEN"    => TAG_TOKEN,
        "QUEUED"     => TAG_QUEUED,
        "RULE"       => TAG_RULE,
        "AVGPARTS"   => TAG_AVGPARTS,
        "TOKEN_RULE" => TAG_TOKEN_RULE,
        "GEO_POS"    => TAG_GEO_POS,
        "GEO_RESULT" => TAG_GEO_RESULT,
        "GEO_END"    => TAG_GEO_END,
        "GEO_DIST"   => TAG_GEO_DIST,
        "SUBSCRIBED" => TAG_SUBSCRIBED,
        "SEVENT"     => TAG_SEVENT,
        "LIVE"       => TAG_LIVE,
        "JOB"        => TAG_JOB,
        _          => TAG_ERR,
    };
    let mut frame = Vec::with_capacity(5 + body.len());
    frame.push(tag);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(body);
    frame
}

// ── Build a cluster peer frame ────────────────────────────────────────────────

/// Decode a complete raw client binary frame (5-byte header + body) into a Command.
/// Used by cluster forwarding to re-parse a frame on the receiving node.
pub fn decode_frame(frame: &[u8]) -> anyhow::Result<Command> {
    if frame.len() < 5 { anyhow::bail!("frame too short") }
    decode(frame[0], &frame[5..])
}

// ── Frame encoders (used for scatter-gather) ──────────────────────────────────

fn ws(s: &str) -> Vec<u8> {
    let mut b = Vec::with_capacity(2 + s.len());
    b.extend_from_slice(&(s.len() as u16).to_be_bytes());
    b.extend_from_slice(s.as_bytes());
    b
}

fn encode_where(clause: &WhereClause) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(matches!(clause.logic, Logic::Or) as u8);
    b.push(clause.conditions.len() as u8);
    for c in &clause.conditions {
        b.extend(ws(&c.field));
        b.extend(ws(&c.op));
        b.extend(ws(&c.value));
    }
    b
}

fn wrap(opcode: u8, body: Vec<u8>) -> Vec<u8> {
    let mut f = Vec::with_capacity(5 + body.len());
    f.push(opcode);
    f.extend_from_slice(&(body.len() as u32).to_be_bytes());
    f.extend(body);
    f
}

pub fn encode_emit(stream: &str, payload: &serde_json::Value, ttl: Option<u64>) -> Vec<u8> {
    let payload_bytes = serde_json::to_vec(payload).unwrap_or_default();
    let mut body = ws(stream);
    body.extend_from_slice(&(payload_bytes.len() as u32).to_be_bytes());
    body.extend_from_slice(&payload_bytes);
    match ttl {
        None    => body.push(0),
        Some(t) => { body.push(1); body.extend_from_slice(&t.to_be_bytes()); }
    }
    wrap(OP_EMIT, body)
}

pub fn encode_patch(stream: &str, patch: &serde_json::Value) -> Vec<u8> {
    let patch_bytes = serde_json::to_vec(patch).unwrap_or_default();
    let mut body = ws(stream);
    body.extend_from_slice(&(patch_bytes.len() as u32).to_be_bytes());
    body.extend_from_slice(&patch_bytes);
    wrap(OP_PATCH, body)
}

pub fn encode_get(stream: &str)                    -> Vec<u8> { wrap(OP_GET,    ws(stream)) }
pub fn encode_del(stream: &str)                    -> Vec<u8> { wrap(OP_DEL,    ws(stream)) }
pub fn encode_since(stream: &str, ts: u64)         -> Vec<u8> { let mut b = ws(stream); b.extend_from_slice(&ts.to_be_bytes());   wrap(OP_SINCE,  b) }
pub fn encode_expire(stream: &str, secs: u64)      -> Vec<u8> { let mut b = ws(stream); b.extend_from_slice(&secs.to_be_bytes()); wrap(OP_EXPIRE, b) }

pub fn encode_keys(pattern: &str) -> Vec<u8> {
    wrap(OP_KEYS, ws(pattern))
}

pub fn encode_query(pattern: &str, clause: &WhereClause, limit: Option<usize>) -> Vec<u8> {
    let mut body = ws(pattern);
    body.extend(encode_where(clause));
    if let Some(n) = limit { body.push(1); body.extend((n as u64).to_be_bytes()); }
    else { body.push(0); }
    wrap(OP_QUERY, body)
}

pub fn encode_count(pattern: &str, clause: Option<&WhereClause>) -> Vec<u8> {
    let mut body = ws(pattern);
    if let Some(c) = clause { body.push(1); body.extend(encode_where(c)); }
    else { body.push(0); }
    wrap(OP_COUNT, body)
}

pub fn encode_sum(pattern: &str, field: &str, clause: Option<&WhereClause>) -> Vec<u8> {
    let mut body = ws(pattern);
    body.extend(ws(field));
    if let Some(c) = clause { body.push(1); body.extend(encode_where(c)); }
    else { body.push(0); }
    wrap(OP_SUM, body)
}

pub fn encode_avg(pattern: &str, field: &str, clause: Option<&WhereClause>) -> Vec<u8> {
    let mut body = ws(pattern);
    body.extend(ws(field));
    if let Some(c) = clause { body.push(1); body.extend(encode_where(c)); }
    else { body.push(0); }
    wrap(OP_AVG, body)
}

pub fn encode_geo_near(pattern: &str, lat: f64, lng: f64, radius_km: f64) -> Vec<u8> {
    let mut body = ws(pattern);
    body.extend_from_slice(&lat.to_be_bytes());
    body.extend_from_slice(&lng.to_be_bytes());
    body.extend_from_slice(&radius_km.to_be_bytes());
    wrap(OP_GEO_NEAR, body)
}

pub fn peer_frame(opcode: u8, body: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(5 + body.len());
    f.push(opcode);
    f.extend_from_slice(&(body.len() as u32).to_be_bytes());
    f.extend_from_slice(body);
    f
}

// ── Field readers ─────────────────────────────────────────────────────────────

fn ru8(c: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut b = [0u8; 1]; std::io::Read::read_exact(c, &mut b)?; Ok(b[0])
}
fn ru16(c: &mut Cursor<&[u8]>) -> Result<u16> {
    let mut b = [0u8; 2]; std::io::Read::read_exact(c, &mut b)?; Ok(u16::from_be_bytes(b))
}
fn ru64(c: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut b = [0u8; 8]; std::io::Read::read_exact(c, &mut b)?; Ok(u64::from_be_bytes(b))
}
fn rs(c: &mut Cursor<&[u8]>) -> Result<String> {
    let len = ru16(c)? as usize;
    let mut buf = vec![0u8; len];
    std::io::Read::read_exact(c, &mut buf)?;
    Ok(String::from_utf8(buf)?)
}
fn rjson(c: &mut Cursor<&[u8]>) -> Result<serde_json::Value> {
    let len = { let mut b = [0u8; 4]; std::io::Read::read_exact(c, &mut b)?; u32::from_be_bytes(b) } as usize;
    if len > MAX_PAYLOAD_BYTES {
        bail!("payload too large ({len} bytes, max {MAX_PAYLOAD_BYTES})");
    }
    let mut buf = vec![0u8; len];
    std::io::Read::read_exact(c, &mut buf)?;
    Ok(serde_json::from_slice(&buf)?)
}
fn rf64(c: &mut Cursor<&[u8]>) -> Result<f64> {
    let mut b = [0u8; 8]; std::io::Read::read_exact(c, &mut b)?; Ok(f64::from_be_bytes(b))
}
fn rwhere(c: &mut Cursor<&[u8]>) -> Result<WhereClause> {
    let logic = if ru8(c)? == 1 { Logic::Or } else { Logic::And };
    let count = ru8(c)? as usize;
    let mut conditions = Vec::with_capacity(count);
    for _ in 0..count {
        conditions.push(Condition { field: rs(c)?, op: rs(c)?, value: rs(c)? });
    }
    Ok(WhereClause { conditions, logic })
}

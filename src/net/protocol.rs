use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct Condition {
    pub field: String,
    pub op:    String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub enum Logic { And, Or }

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
    pub logic:      Logic,
}

impl WhereClause {
    pub fn single(field: String, op: String, value: String) -> Self {
        Self { conditions: vec![Condition { field, op, value }], logic: Logic::And }
    }
}

#[derive(Debug)]
pub enum Command {
    Auth      { token: String },
    Emit      { stream: String, payload: serde_json::Value, ttl: Option<u64> },
    EmitMany  { entries: Vec<(String, serde_json::Value)> },
    Patch     { stream: String, patch: serde_json::Value },
    Watch     { pattern: String, filter: Option<WhereClause> },
    Get       { stream: String },
    Since     { stream: String, ts: u64 },
    Del       { stream: String },
    Keys      { pattern: String },
    Expire    { stream: String, secs: u64 },
    Query     { pattern: String, clause: WhereClause, limit: Option<usize> },
    Count     { pattern: String, clause: Option<WhereClause> },
    Sum       { pattern: String, field: String, clause: Option<WhereClause> },
    Avg       { pattern: String, field: String, clause: Option<WhereClause> },
    TokenCreate  { raw: String, role: String, namespaces: Vec<String>, expires_at: Option<String> },
    TokenRevoke  { raw: String },
    TokenBlock   { raw: String },
    TokenUnblock { raw: String },
    TokenList,
    Begin,
    Commit,
    Rollback,
    Ping,
    WhenAdd  { pattern: String, target: String, template: String, condition: Option<(String,String,String)> },
    WhenEnqueueAdd { pattern: String, queue: String, template: String, condition: Option<(String,String,String)> },
    WhenDel  { pattern: String, key: String },
    WhenList,
    /// Claim ownership of a stream — only this user_id (or admin) can write to it.
    Claim { stream: String },
    /// Auto-create a token when a stream matching `pattern` is written.
    WhenTokenAdd { pattern: String, token_tmpl: String, role: String, namespaces: Vec<String> },
    WhenTokenDel { pattern: String, token_tmpl: String },
    WhenTokenList,
    // ── Geospatial ────────────────────────────────────────────────────────────
    GeoSet  { stream: String, lat: f64, lng: f64 },
    GeoGet  { stream: String },
    GeoDel  { stream: String },
    GeoNear { pattern: String, lat: f64, lng: f64, radius_km: f64 },
    GeoDist { stream1: String, stream2: String },
    // ── Durable subscriptions ─────────────────────────────────────────────────
    /// Resume from `from_id`. Replays history then streams live events.
    /// `from_id = 0` replays all available in-memory history.
    Subscribe { pattern: String, from_id: u64 },
    // ── Durable queues ────────────────────────────────────────────────────────
    Enqueue { queue: String, payload: serde_json::Value },
    /// `timeout_secs` = visibility timeout (default 30s). `wait_secs` = long-poll (None = no wait).
    Dequeue { queue: String, timeout_secs: u64, wait_secs: Option<u64> },
    Ack     { queue: String, job_id: u64 },
    Nack    { queue: String, job_id: u64 },
    /// Pending + in-flight count.
    Qlen  { queue: String },
    /// Peek without claiming.
    Qpeek { queue: String },
    // ── Batch read ────────────────────────────────────────────────────────────
    /// Read multiple streams in one round-trip.
    Mget { streams: Vec<String> },
}

impl Command {
    /// Returns the single stream this command targets — used by the cluster router
    /// to decide which node should handle the request.
    /// Returns None for commands that fan-out (KEYS, QUERY, WATCH, etc.).
    pub fn primary_stream(&self) -> Option<&str> {
        match self {
            Command::Emit   { stream, .. } => Some(stream),
            Command::Patch  { stream, .. } => Some(stream),
            Command::Get    { stream }     => Some(stream),
            Command::Del    { stream }     => Some(stream),
            Command::Since  { stream, .. } => Some(stream),
            Command::Expire { stream, .. } => Some(stream),
            _ => None,
        }
    }

    pub fn parse(line: &str) -> Result<Self> {
        let line = line.trim();
        let (cmd, rest) = split_first_word(line);

        match cmd.to_uppercase().as_str() {
            "AUTH" => {
                let token = rest.trim().to_string();
                if token.is_empty() { bail!("AUTH requires a token") }
                Ok(Command::Auth { token })
            }

            "EMIT" => {
                let (stream, rest2) = split_first_word(rest);
                if stream.is_empty() { bail!("EMIT requires a stream name") }
                // check for TTL suffix: EMIT stream {...} TTL 60
                let (json_part, ttl) = parse_ttl_suffix(rest2);
                let json_part = json_part.trim();
                if json_part.len() > crate::net::codec::MAX_PAYLOAD_BYTES {
                    bail!("payload too large ({} bytes, max {})", json_part.len(), crate::net::codec::MAX_PAYLOAD_BYTES);
                }
                let payload = serde_json::from_str(json_part)?;
                Ok(Command::Emit { stream: stream.to_string(), payload, ttl })
            }

            "EMITMANY" => {
                // EMITMANY stream1 {json1} stream2 {json2} ...
                let mut entries = Vec::new();
                let mut remaining = rest;
                while !remaining.trim().is_empty() {
                    let (stream, rest2) = split_first_word(remaining);
                    if stream.is_empty() { break; }
                    let (json, rest3) = split_json(rest2)?;
                    entries.push((stream.to_string(), serde_json::from_str(&json)?));
                    remaining = rest3;
                }
                if entries.is_empty() { bail!("EMITMANY requires at least one stream+json pair") }
                Ok(Command::EmitMany { entries })
            }

            "PATCH" => {
                let (stream, json) = split_first_word(rest);
                if stream.is_empty() { bail!("PATCH requires a stream name") }
                let patch = serde_json::from_str(json)?;
                Ok(Command::Patch { stream: stream.to_string(), patch })
            }

            "WATCH" => {
                let (pattern, rest2) = split_first_word(rest);
                if pattern.is_empty() { bail!("WATCH requires a pattern") }
                let filter = if rest2.to_uppercase().starts_with("WHERE") {
                    let after = rest2[5..].trim();
                    Some(parse_where(after)?)
                } else {
                    None
                };
                Ok(Command::Watch { pattern: pattern.to_string(), filter })
            }

            "GET" => {
                let stream = rest.trim().to_string();
                if stream.is_empty() { bail!("GET requires a stream name") }
                Ok(Command::Get { stream })
            }

            "SINCE" => {
                let (stream, ts_str) = split_first_word(rest);
                if stream.is_empty() { bail!("SINCE requires a stream and timestamp") }
                let ts = ts_str.trim().parse::<u64>()?;
                Ok(Command::Since { stream: stream.to_string(), ts })
            }

            "DEL" => {
                let stream = rest.trim().to_string();
                if stream.is_empty() { bail!("DEL requires a stream name") }
                Ok(Command::Del { stream })
            }

            "KEYS" => {
                let pattern = rest.trim().to_string();
                if pattern.is_empty() { bail!("KEYS requires a pattern") }
                Ok(Command::Keys { pattern })
            }

            "EXPIRE" => {
                let (stream, secs_str) = split_first_word(rest);
                if stream.is_empty() { bail!("EXPIRE requires a stream and seconds") }
                let secs = secs_str.trim().parse::<u64>()?;
                Ok(Command::Expire { stream: stream.to_string(), secs })
            }

            "QUERY" => {
                // QUERY pattern WHERE cond [AND|OR cond] [LIMIT n]
                let (pattern, rest2) = split_first_word(rest);
                if pattern.is_empty() { bail!("QUERY <pattern> WHERE <field> <op> <value>") }
                let (where_kw, rest3) = split_first_word(rest2);
                if where_kw.to_uppercase() != "WHERE" { bail!("expected WHERE") }
                let (clause, rest4) = parse_where_with_rest(rest3)?;
                let limit = parse_limit(rest4);
                Ok(Command::Query { pattern: pattern.to_string(), clause, limit })
            }

            "COUNT" => {
                let (pattern, rest2) = split_first_word(rest);
                if pattern.is_empty() { bail!("COUNT requires a pattern") }
                let clause = parse_optional_where(rest2)?;
                Ok(Command::Count { pattern: pattern.to_string(), clause })
            }

            "SUM" => {
                let (pattern, rest2) = split_first_word(rest);
                let (field,   rest3) = split_first_word(rest2);
                if pattern.is_empty() || field.is_empty() { bail!("SUM <pattern> <field> [WHERE ...]") }
                let clause = parse_optional_where(rest3)?;
                Ok(Command::Sum { pattern: pattern.to_string(), field: field.to_string(), clause })
            }

            "AVG" => {
                let (pattern, rest2) = split_first_word(rest);
                let (field,   rest3) = split_first_word(rest2);
                if pattern.is_empty() || field.is_empty() { bail!("AVG <pattern> <field> [WHERE ...]") }
                let clause = parse_optional_where(rest3)?;
                Ok(Command::Avg { pattern: pattern.to_string(), field: field.to_string(), clause })
            }

            "TOKEN" => {
                let (sub, rest2) = split_first_word(rest);
                match sub.to_uppercase().as_str() {
                    "CREATE" => {
                        let (raw,  rest3) = split_first_word(rest2);
                        let (role, rest4) = split_first_word(rest3);
                        let (ns,   exp  ) = split_first_word(rest4);
                        if raw.is_empty() || role.is_empty() { bail!("TOKEN CREATE <token> <role> [namespaces] [expires_at]") }
                        let namespaces = if ns.is_empty() { vec![] } else {
                            ns.split(',').map(|s| s.trim().to_string()).collect()
                        };
                        let expires_at = if exp.trim().is_empty() { None } else { Some(exp.trim().to_string()) };
                        Ok(Command::TokenCreate { raw: raw.to_string(), role: role.to_string(), namespaces, expires_at })
                    }
                    "REVOKE" => {
                        let raw = rest2.trim().to_string();
                        if raw.is_empty() { bail!("TOKEN REVOKE <token>") }
                        Ok(Command::TokenRevoke { raw })
                    }
                    "BLOCK" => {
                        let raw = rest2.trim().to_string();
                        if raw.is_empty() { bail!("TOKEN BLOCK <token>") }
                        Ok(Command::TokenBlock { raw })
                    }
                    "UNBLOCK" => {
                        let raw = rest2.trim().to_string();
                        if raw.is_empty() { bail!("TOKEN UNBLOCK <token>") }
                        Ok(Command::TokenUnblock { raw })
                    }
                    "LIST" => Ok(Command::TokenList),
                    other  => bail!("unknown TOKEN subcommand: {other}"),
                }
            }

            "CLAIM" => {
                let stream = rest.trim().to_string();
                if stream.is_empty() { bail!("CLAIM <stream>") }
                Ok(Command::Claim { stream })
            }

            "WHEN" => {
                let (sub, rest2) = split_first_word(rest);
                match sub.to_uppercase().as_str() {
                    "LIST" => Ok(Command::WhenList),
                    "DEL"  => {
                        let (pattern, key) = split_first_word(rest2);
                        if pattern.is_empty() || key.is_empty() {
                            bail!("WHEN DEL <pattern> <target|queue>")
                        }
                        Ok(Command::WhenDel {
                            pattern: pattern.to_string(),
                            key:     key.trim().to_string(),
                        })
                    }
                    "TOKEN" => {
                        let (sub2, rest3) = split_first_word(rest2);
                        match sub2.to_uppercase().as_str() {
                            "LIST" => Ok(Command::WhenTokenList),
                            "DEL"  => {
                                let (pattern, token_tmpl) = split_first_word(rest3);
                                if pattern.is_empty() || token_tmpl.is_empty() {
                                    bail!("WHEN TOKEN DEL <pattern> <token_tmpl>")
                                }
                                Ok(Command::WhenTokenDel {
                                    pattern:    pattern.to_string(),
                                    token_tmpl: token_tmpl.trim().to_string(),
                                })
                            }
                            _ if !sub2.is_empty() => {
                                // WHEN TOKEN <pattern> <token_tmpl> <role> <ns1>[,<ns2>]
                                let (token_tmpl, rest4) = split_first_word(rest3);
                                let (role,       ns_str) = split_first_word(rest4);
                                if sub2.is_empty() || token_tmpl.is_empty() || role.is_empty() {
                                    bail!("WHEN TOKEN <pattern> <token_tmpl> <role> [namespaces]")
                                }
                                let namespaces = if ns_str.trim().is_empty() { vec![] } else {
                                    ns_str.trim().split(',').map(|s| s.trim().to_string()).collect()
                                };
                                Ok(Command::WhenTokenAdd {
                                    pattern:    sub2.to_string(),
                                    token_tmpl: token_tmpl.to_string(),
                                    role:       role.to_string(),
                                    namespaces,
                                })
                            }
                            _ => bail!("WHEN TOKEN LIST | WHEN TOKEN DEL | WHEN TOKEN <pattern> <token_tmpl> <role> [namespaces]"),
                        }
                    }
                    _ if !sub.is_empty() => {
                        // WHEN <pattern> [WHERE <field> <op> <value>] THEN EMIT <target> <template>
                        // WHEN <pattern> [WHERE <field> <op> <value>] THEN ENQUEUE <queue> <template>
                        let pattern = sub.to_string();

                        // Check for optional WHERE clause before THEN.
                        let (condition, rest_after_where) = {
                            let (kw, after) = split_first_word(rest2);
                            if kw.to_uppercase() == "WHERE" {
                                let (field, after2) = split_first_word(after);
                                let (op,    after3) = split_first_word(after2);
                                let (value, after4) = split_first_word(after3);
                                if field.is_empty() || op.is_empty() || value.is_empty() {
                                    bail!("WHEN <pattern> WHERE <field> <op> <value> THEN ...")
                                }
                                (Some((field.to_string(), op.to_string(), value.to_string())), after4)
                            } else {
                                (None, rest2)
                            }
                        };

                        let (then_kw, rest3) = split_first_word(rest_after_where);
                        if then_kw.to_uppercase() != "THEN" { bail!("expected THEN") }
                        let (action_kw, rest4) = split_first_word(rest3);
                        match action_kw.to_uppercase().as_str() {
                            "EMIT" => {
                                let (target, template) = split_first_word(rest4);
                                if target.is_empty() || template.is_empty() {
                                    bail!("WHEN <pattern> THEN EMIT <target> <template>")
                                }
                                Ok(Command::WhenAdd {
                                    pattern,
                                    target:    target.to_string(),
                                    template:  template.trim().to_string(),
                                    condition,
                                })
                            }
                            "ENQUEUE" => {
                                let (queue, template) = split_first_word(rest4);
                                if queue.is_empty() || template.is_empty() {
                                    bail!("WHEN <pattern> THEN ENQUEUE <queue> <template>")
                                }
                                Ok(Command::WhenEnqueueAdd {
                                    pattern,
                                    queue:    queue.to_string(),
                                    template: template.trim().to_string(),
                                    condition,
                                })
                            }
                            _ => bail!("expected EMIT or ENQUEUE after THEN"),
                        }
                    }
                    _ => bail!("WHEN LIST | WHEN DEL | WHEN TOKEN ... | WHEN <pattern> THEN EMIT/ENQUEUE"),
                }
            }

            "GEO" => {
                let (sub, rest2) = split_first_word(rest);
                match sub.to_uppercase().as_str() {
                    "SET" => {
                        let (stream, rest3) = split_first_word(rest2);
                        let (lat_s, rest4)  = split_first_word(rest3);
                        let lng_s = rest4.trim();
                        if stream.is_empty() || lat_s.is_empty() || lng_s.is_empty() {
                            bail!("GEO SET <stream> <lat> <lng>")
                        }
                        Ok(Command::GeoSet {
                            stream: stream.to_string(),
                            lat: lat_s.parse()?,
                            lng: lng_s.parse()?,
                        })
                    }
                    "GET" => {
                        let stream = rest2.trim().to_string();
                        if stream.is_empty() { bail!("GEO GET <stream>") }
                        Ok(Command::GeoGet { stream })
                    }
                    "DEL" => {
                        let stream = rest2.trim().to_string();
                        if stream.is_empty() { bail!("GEO DEL <stream>") }
                        Ok(Command::GeoDel { stream })
                    }
                    "NEAR" => {
                        let (pattern, rest3) = split_first_word(rest2);
                        let (lat_s,   rest4) = split_first_word(rest3);
                        let (lng_s,   rest5) = split_first_word(rest4);
                        let radius_s = rest5.trim();
                        if pattern.is_empty() || lat_s.is_empty() || lng_s.is_empty() || radius_s.is_empty() {
                            bail!("GEO NEAR <pattern> <lat> <lng> <radius_km>")
                        }
                        Ok(Command::GeoNear {
                            pattern: pattern.to_string(),
                            lat: lat_s.parse()?,
                            lng: lng_s.parse()?,
                            radius_km: radius_s.parse()?,
                        })
                    }
                    "DIST" => {
                        let (s1, s2) = split_first_word(rest2);
                        let s2 = s2.trim();
                        if s1.is_empty() || s2.is_empty() { bail!("GEO DIST <stream1> <stream2>") }
                        Ok(Command::GeoDist { stream1: s1.to_string(), stream2: s2.to_string() })
                    }
                    other => bail!("unknown GEO subcommand: {other}"),
                }
            }

            "SUBSCRIBE" => {
                let (pattern, rest2) = split_first_word(rest);
                if pattern.is_empty() { bail!("SUBSCRIBE <pattern> FROM <last_event_id>") }
                let (kw, id_str) = split_first_word(rest2);
                let from_id = if kw.to_uppercase() == "FROM" {
                    id_str.trim().parse::<u64>()?
                } else {
                    0 // default: replay all in-memory history
                };
                Ok(Command::Subscribe { pattern: pattern.to_string(), from_id })
            }

            "ENQUEUE" => {
                let (queue, json) = split_first_word(rest);
                if queue.is_empty() { bail!("ENQUEUE <queue> <json>") }
                Ok(Command::Enqueue { queue: queue.to_string(), payload: serde_json::from_str(json.trim())? })
            }
            "DEQUEUE" => {
                // DEQUEUE <queue> [WAIT [wait_secs]] [timeout_secs]
                // DEQUEUE <queue> WAIT         → long-poll, default 30s wait, default vis timeout
                // DEQUEUE <queue> WAIT 10      → long-poll, 10s wait
                // DEQUEUE <queue> WAIT 10 60   → long-poll, 10s wait, 60s vis timeout
                // DEQUEUE <queue> 60           → immediate, 60s vis timeout
                let (queue, rest2) = split_first_word(rest);
                if queue.is_empty() { bail!("DEQUEUE <queue> [WAIT [wait_secs] [timeout_secs]]") }
                let rest2 = rest2.trim();
                let (wait_secs, timeout_secs) = if rest2.to_uppercase().starts_with("WAIT") {
                    let after_wait = rest2[4..].trim();
                    let mut nums = after_wait.split_whitespace();
                    let ws  = nums.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(30);
                    let vis = nums.next().and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(crate::storage::index::DEFAULT_VISIBILITY_SECS);
                    (Some(ws), vis)
                } else {
                    let vis = rest2.parse::<u64>().unwrap_or(crate::storage::index::DEFAULT_VISIBILITY_SECS);
                    (None, vis)
                };
                Ok(Command::Dequeue { queue: queue.to_string(), timeout_secs, wait_secs })
            }
            "ACK" => {
                let (queue, id_str) = split_first_word(rest);
                if queue.is_empty() { bail!("ACK <queue> <job_id>") }
                Ok(Command::Ack { queue: queue.to_string(), job_id: id_str.trim().parse()? })
            }
            "NACK" => {
                let (queue, id_str) = split_first_word(rest);
                if queue.is_empty() { bail!("NACK <queue> <job_id>") }
                Ok(Command::Nack { queue: queue.to_string(), job_id: id_str.trim().parse()? })
            }
            "QLEN"  => {
                let queue = rest.trim().to_string();
                if queue.is_empty() { bail!("QLEN <queue>") }
                Ok(Command::Qlen { queue })
            }
            "QPEEK" => {
                let queue = rest.trim().to_string();
                if queue.is_empty() { bail!("QPEEK <queue>") }
                Ok(Command::Qpeek { queue })
            }

            "MGET" => {
                let streams: Vec<String> = rest.split_whitespace().map(|s| s.to_string()).collect();
                if streams.is_empty() { bail!("MGET <stream> [stream ...]") }
                Ok(Command::Mget { streams })
            }

            "BEGIN"    => Ok(Command::Begin),
            "COMMIT"   => Ok(Command::Commit),
            "ROLLBACK" => Ok(Command::Rollback),
            "PING"     => Ok(Command::Ping),
            other      => bail!("unknown command: {other}"),
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

pub fn split_first_word(s: &str) -> (&str, &str) {
    let s = s.trim();
    match s.find(char::is_whitespace) {
        Some(i) => (&s[..i], s[i..].trim_start()),
        None    => (s, ""),
    }
}

fn parse_ttl_suffix(s: &str) -> (&str, Option<u64>) {
    // look for TTL <n> at the end
    if let Some(i) = s.to_uppercase().rfind(" TTL ") {
        let ttl_str = s[i + 5..].trim();
        if let Ok(n) = ttl_str.parse::<u64>() {
            return (&s[..i], Some(n));
        }
    }
    (s, None)
}

fn split_json(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    if !s.starts_with('{') { bail!("expected JSON object") }
    let mut depth = 0usize;
    let mut in_str = false;
    let mut escape = false;
    for (i, c) in s.char_indices() {
        if escape { escape = false; continue; }
        if c == '\\' && in_str { escape = true; continue; }
        if c == '"' { in_str = !in_str; continue; }
        if in_str { continue; }
        if c == '{' { depth += 1; }
        if c == '}' {
            depth -= 1;
            if depth == 0 {
                return Ok((s[..=i].to_string(), s[i+1..].trim_start()));
            }
        }
    }
    bail!("unclosed JSON object")
}

fn parse_where(s: &str) -> Result<WhereClause> {
    let (clause, _) = parse_where_with_rest(s)?;
    Ok(clause)
}

fn parse_where_with_rest(s: &str) -> Result<(WhereClause, &str)> {
    let (field, rest) = split_first_word(s);
    let (op,    rest) = split_first_word(rest);
    let (val,   rest) = split_first_word(rest);
    if field.is_empty() || op.is_empty() || val.is_empty() {
        bail!("WHERE requires <field> <op> <value>")
    }
    let value = val.trim_matches('"').to_string();
    let mut conditions = vec![Condition { field: field.to_string(), op: op.to_string(), value }];
    let mut logic = Logic::And;
    let mut remaining = rest;

    loop {
        let (connector, rest2) = split_first_word(remaining);
        match connector.to_uppercase().as_str() {
            "AND" => {
                logic = Logic::And;
                let (f, r2) = split_first_word(rest2);
                let (o, r3) = split_first_word(r2);
                let (v, r4) = split_first_word(r3);
                if f.is_empty() || o.is_empty() || v.is_empty() { break; }
                conditions.push(Condition { field: f.to_string(), op: o.to_string(), value: v.trim_matches('"').to_string() });
                remaining = r4;
            }
            "OR" => {
                logic = Logic::Or;
                let (f, r2) = split_first_word(rest2);
                let (o, r3) = split_first_word(r2);
                let (v, r4) = split_first_word(r3);
                if f.is_empty() || o.is_empty() || v.is_empty() { break; }
                conditions.push(Condition { field: f.to_string(), op: o.to_string(), value: v.trim_matches('"').to_string() });
                remaining = r4;
            }
            _ => break,
        }
    }

    Ok((WhereClause { conditions, logic }, remaining))
}

fn parse_optional_where(s: &str) -> Result<Option<WhereClause>> {
    let (kw, rest) = split_first_word(s);
    if kw.to_uppercase() == "WHERE" {
        Ok(Some(parse_where(rest)?))
    } else {
        Ok(None)
    }
}

fn parse_limit(s: &str) -> Option<usize> {
    let (kw, rest) = split_first_word(s);
    if kw.to_uppercase() == "LIMIT" {
        rest.trim().parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Command { Command::parse(s).unwrap() }

    #[test]
    fn ping() { assert!(matches!(parse("PING"), Command::Ping)); }

    #[test]
    fn auth() {
        let Command::Auth { token } = parse("AUTH mytoken") else { panic!() };
        assert_eq!(token, "mytoken");
    }

    #[test]
    fn emit_basic() {
        let Command::Emit { stream, payload, ttl } = parse(r#"EMIT users:1 {"name":"evan"}"#) else { panic!() };
        assert_eq!(stream, "users:1");
        assert_eq!(payload["name"], "evan");
        assert!(ttl.is_none());
    }

    #[test]
    fn emit_with_ttl() {
        let Command::Emit { ttl, .. } = parse(r#"EMIT sessions:1 {"x":1} TTL 60"#) else { panic!() };
        assert_eq!(ttl, Some(60));
    }

    #[test]
    fn patch() {
        let Command::Patch { stream, patch } = parse(r#"PATCH users:1 {"status":"online"}"#) else { panic!() };
        assert_eq!(stream, "users:1");
        assert_eq!(patch["status"], "online");
    }

    #[test]
    fn get() {
        let Command::Get { stream } = parse("GET users:1") else { panic!() };
        assert_eq!(stream, "users:1");
    }

    #[test]
    fn del() {
        let Command::Del { stream } = parse("DEL users:1") else { panic!() };
        assert_eq!(stream, "users:1");
    }

    #[test]
    fn keys() {
        let Command::Keys { pattern } = parse("KEYS users:*") else { panic!() };
        assert_eq!(pattern, "users:*");
    }

    #[test]
    fn since() {
        let Command::Since { stream, ts } = parse("SINCE users:1 1234567890") else { panic!() };
        assert_eq!(stream, "users:1");
        assert_eq!(ts, 1234567890);
    }

    #[test]
    fn expire() {
        let Command::Expire { stream, secs } = parse("EXPIRE users:1 3600") else { panic!() };
        assert_eq!(stream, "users:1");
        assert_eq!(secs, 3600);
    }

    #[test]
    fn watch_no_filter() {
        let Command::Watch { pattern, filter } = parse("WATCH users:*") else { panic!() };
        assert_eq!(pattern, "users:*");
        assert!(filter.is_none());
    }

    #[test]
    fn watch_with_filter() {
        let Command::Watch { filter, .. } = parse(r#"WATCH users:* WHERE status = online"#) else { panic!() };
        let f = filter.unwrap();
        assert_eq!(f.conditions[0].field, "status");
        assert_eq!(f.conditions[0].op,    "=");
        assert_eq!(f.conditions[0].value, "online");
    }

    #[test]
    fn query_single_where() {
        let Command::Query { pattern, clause, limit } = parse(r#"QUERY users:* WHERE role = admin"#) else { panic!() };
        assert_eq!(pattern, "users:*");
        assert_eq!(clause.conditions.len(), 1);
        assert_eq!(clause.conditions[0].field, "role");
        assert!(limit.is_none());
    }

    #[test]
    fn query_and_limit() {
        let Command::Query { clause, limit, .. } = parse(r#"QUERY users:* WHERE age > 18 AND role = admin LIMIT 5"#) else { panic!() };
        assert_eq!(clause.conditions.len(), 2);
        assert_eq!(limit, Some(5));
    }

    #[test]
    fn count_no_where() {
        let Command::Count { pattern, clause } = parse("COUNT users:*") else { panic!() };
        assert_eq!(pattern, "users:*");
        assert!(clause.is_none());
    }

    #[test]
    fn count_with_where() {
        let Command::Count { clause, .. } = parse(r#"COUNT users:* WHERE role = admin"#) else { panic!() };
        assert!(clause.is_some());
    }

    #[test]
    fn sum_and_avg() {
        let Command::Sum { pattern, field, .. } = parse("SUM orders:* total") else { panic!() };
        assert_eq!(pattern, "orders:*");
        assert_eq!(field, "total");

        let Command::Avg { field, .. } = parse("AVG orders:* total") else { panic!() };
        assert_eq!(field, "total");
    }

    #[test]
    fn emitmany() {
        let Command::EmitMany { entries } = parse(r#"EMITMANY users:1 {"a":1} users:2 {"b":2}"#) else { panic!() };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "users:1");
        assert_eq!(entries[1].0, "users:2");
    }

    #[test]
    fn transactions() {
        assert!(matches!(parse("BEGIN"),    Command::Begin));
        assert!(matches!(parse("COMMIT"),   Command::Commit));
        assert!(matches!(parse("ROLLBACK"), Command::Rollback));
    }

    #[test]
    fn token_commands() {
        let Command::TokenCreate { raw, role, .. } = parse("TOKEN CREATE mytoken writer users,orders") else { panic!() };
        assert_eq!(raw, "mytoken");
        assert_eq!(role, "writer");

        let Command::TokenRevoke { raw } = parse("TOKEN REVOKE mytoken") else { panic!() };
        assert_eq!(raw, "mytoken");

        assert!(matches!(parse("TOKEN LIST"), Command::TokenList));
    }

    #[test]
    fn unknown_command_errors() {
        assert!(Command::parse("FOOBAR").is_err());
    }

    #[test]
    fn missing_args_errors() {
        assert!(Command::parse("EMIT").is_err());
        assert!(Command::parse("GET").is_err());
        assert!(Command::parse("AUTH").is_err());
    }
}

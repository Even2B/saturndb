use crate::core::event::{Event, Op};
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ── Durable queue types ───────────────────────────────────────────────────────

pub const DEFAULT_VISIBILITY_SECS: u64 = 30;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct QueueJob {
    pub id:      u64,
    pub payload: serde_json::Value,
    pub retries: u32,
}

/// In-memory only — not serialized; converted back to pending on snapshot restore.
struct InflightJob {
    queue:      String,
    job:        QueueJob,
    timeout_at: u64, // unix secs
}

fn eval_op(field_val: &serde_json::Value, op: &str, value: &str) -> bool {
    match op {
        "="  | "==" => field_val.as_str().map(|s| s == value).unwrap_or_else(|| field_val.to_string().trim_matches('"') == value),
        "!=" | "<>" => field_val.as_str().map(|s| s != value).unwrap_or_else(|| field_val.to_string().trim_matches('"') != value),
        ">"  => {
            let a = field_val.as_f64().or_else(|| field_val.as_str()?.parse().ok());
            let b = value.parse::<f64>().ok();
            matches!((a, b), (Some(a), Some(b)) if a > b)
        }
        ">=" => {
            let a = field_val.as_f64().or_else(|| field_val.as_str()?.parse().ok());
            let b = value.parse::<f64>().ok();
            matches!((a, b), (Some(a), Some(b)) if a >= b)
        }
        "<"  => {
            let a = field_val.as_f64().or_else(|| field_val.as_str()?.parse().ok());
            let b = value.parse::<f64>().ok();
            matches!((a, b), (Some(a), Some(b)) if a < b)
        }
        "<=" => {
            let a = field_val.as_f64().or_else(|| field_val.as_str()?.parse().ok());
            let b = value.parse::<f64>().ok();
            matches!((a, b), (Some(a), Some(b)) if a <= b)
        }
        "CONTAINS" | "contains" => {
            field_val.as_str().map(|s| s.contains(value)).unwrap_or(false)
        }
        _ => false,
    }
}

fn matches_pattern(pattern: &str, stream: &str) -> bool {
    if pattern == "*"    { return true; }
    if pattern == stream { return true; }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return stream.starts_with(prefix);
    }
    false
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// Apply a PATCH payload to a base object, supporting both plain merge and operators.
///
/// A field value is treated as an operator if it is a JSON object whose keys all start with `$`.
///
/// Supported operators:
///   `{"$inc": N}`          — add N to a numeric field (creates field at 0 + N if missing)
///   `{"$mul": N}`          — multiply field by N
///   `{"$min": N}`          — set to min(current, N)
///   `{"$max": N}`          — set to max(current, N)
///   `{"$push": V}`         — append V to an array field (creates array if missing)
///   `{"$pop": "first"|"last"}` — remove element from array (defaults to "last")
///   `{"$unset": true}`     — delete the field
fn apply_patch_ops(base: serde_json::Value, patch: serde_json::Value) -> serde_json::Value {
    let mut obj = match base {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    let serde_json::Value::Object(fields) = patch else { return serde_json::Value::Object(obj); };

    for (k, v) in fields {
        // Operator block: all keys start with '$'
        if let serde_json::Value::Object(ref ops) = v {
            if !ops.is_empty() && ops.keys().all(|key| key.starts_with('$')) {
                if let Some(n) = ops.get("$inc") {
                    let delta = n.as_f64().unwrap_or(1.0);
                    let cur   = obj.get(&k).and_then(|v| v.as_f64()).unwrap_or(0.0);
                    obj.insert(k, serde_json::json!(cur + delta));
                } else if let Some(n) = ops.get("$mul") {
                    let factor = n.as_f64().unwrap_or(1.0);
                    let cur    = obj.get(&k).and_then(|v| v.as_f64()).unwrap_or(0.0);
                    obj.insert(k, serde_json::json!(cur * factor));
                } else if let Some(n) = ops.get("$min") {
                    let n   = n.as_f64().unwrap_or(f64::MAX);
                    let cur = obj.get(&k).and_then(|v| v.as_f64()).unwrap_or(f64::MAX);
                    obj.insert(k, serde_json::json!(f64::min(cur, n)));
                } else if let Some(n) = ops.get("$max") {
                    let n   = n.as_f64().unwrap_or(f64::MIN);
                    let cur = obj.get(&k).and_then(|v| v.as_f64()).unwrap_or(f64::MIN);
                    obj.insert(k, serde_json::json!(f64::max(cur, n)));
                } else if let Some(val) = ops.get("$push") {
                    let arr = obj.entry(k).or_insert_with(|| serde_json::Value::Array(vec![]));
                    if let serde_json::Value::Array(a) = arr { a.push(val.clone()); }
                } else if let Some(side) = ops.get("$pop") {
                    let from_front = side.as_str().unwrap_or("last") == "first";
                    if let Some(serde_json::Value::Array(a)) = obj.get_mut(&k) {
                        if from_front { if !a.is_empty() { a.remove(0); } }
                        else          { a.pop(); }
                    }
                } else if ops.contains_key("$unset") {
                    obj.remove(&k);
                }
                // unknown operator → skip silently
                continue;
            }
        }
        // Plain merge
        obj.insert(k, v);
    }
    serde_json::Value::Object(obj)
}

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

#[derive(Clone)]
pub struct Index {
    streams:     Arc<DashMap<String, Vec<Event>>>,
    deleted:     Arc<DashMap<String, bool>>,
    expiry:      Arc<DashMap<String, u64>>,
    owners:      Arc<DashMap<String, String>>,
    geo_index:   Arc<DashMap<String, (f64, f64)>>,
    // ── Durable queues ────────────────────────────────────────────────────────
    queues:       Arc<DashMap<String, VecDeque<QueueJob>>>,
    inflight:     Arc<DashMap<u64, InflightJob>>,
    next_job_id:  Arc<AtomicU64>,
    queue_notify: Arc<DashMap<String, Arc<tokio::sync::Notify>>>,
    max_history: Option<usize>,
}

impl Index {
    pub fn new() -> Self {
        Self {
            streams:     Arc::new(DashMap::new()),
            deleted:     Arc::new(DashMap::new()),
            expiry:      Arc::new(DashMap::new()),
            owners:      Arc::new(DashMap::new()),
            geo_index:   Arc::new(DashMap::new()),
            queues:      Arc::new(DashMap::new()),
            inflight:    Arc::new(DashMap::new()),
            next_job_id:  Arc::new(AtomicU64::new(1)),
            queue_notify: Arc::new(DashMap::new()),
            max_history: None,
        }
    }

    pub fn with_max_history(mut self, max: Option<usize>) -> Self {
        self.max_history = max;
        self
    }

    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    pub fn apply(&self, event: Event) {
        match event.op {
            Op::Delete => {
                self.deleted.insert(event.stream.clone(), true);
                self.expiry.remove(&event.stream);
                let mut v = self.streams.entry(event.stream.clone()).or_default();
                v.push(event);
                self.compact(&mut v);
            }
            _ => {
                self.deleted.remove(&event.stream);
                let mut v = self.streams.entry(event.stream.clone()).or_default();
                v.push(event);
                self.compact(&mut v);
            }
        }
    }

    fn compact(&self, v: &mut Vec<Event>) {
        if let Some(max) = self.max_history {
            if v.len() > max {
                v.drain(0..v.len() - max);
            }
        }
    }

    pub fn set_expiry(&self, stream: &str, secs: u64) {
        self.expiry.insert(stream.to_string(), now_secs() + secs);
    }

    pub fn latest(&self, stream: &str) -> Option<serde_json::Value> {
        if self.is_gone(stream) { return None; }
        self.streams
            .get(stream)
            .and_then(|events| events.last().map(|e| e.payload.clone()))
    }

    pub fn is_deleted(&self, stream: &str) -> bool {
        self.deleted.get(stream).map(|v| *v).unwrap_or(false)
    }

    pub fn is_expired(&self, stream: &str) -> bool {
        self.expiry.get(stream).map(|exp| now_secs() >= *exp).unwrap_or(false)
    }

    pub fn is_gone(&self, stream: &str) -> bool {
        self.is_deleted(stream) || self.is_expired(stream)
    }

    /// Returns `(events, truncated)`. `truncated` is true when the result hit `limit`.
    pub fn since(&self, stream: &str, since_ts: u64, limit: usize) -> (Vec<Event>, bool) {
        let all: Vec<Event> = self.streams
            .get(stream)
            .map(|events| events.iter().filter(|e| e.ts >= since_ts).cloned().collect())
            .unwrap_or_default();
        let truncated = all.len() > limit;
        // Return the most-recent `limit` events so the caller always gets current state.
        let events = if truncated { all[all.len() - limit..].to_vec() } else { all };
        (events, truncated)
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let mut keys: Vec<String> = self.streams
            .iter()
            .filter(|e| !self.is_gone(e.key()) && matches_pattern(pattern, e.key()))
            .map(|e| e.key().clone())
            .collect();
        keys.sort();
        keys
    }

    pub fn history(&self, stream: &str) -> Vec<Event> {
        self.streams
            .get(stream)
            .map(|events| events.clone())
            .unwrap_or_default()
    }

    pub fn patch(&self, stream: &str, patch: serde_json::Value) -> Option<serde_json::Value> {
        let entry = self.streams.entry(stream.to_string()).or_default();
        let base = entry.last()
            .map(|e| e.payload.clone())
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
        Some(apply_patch_ops(base, patch))
    }

    pub fn query(&self, pattern: &str, clause: &crate::net::protocol::WhereClause, limit: Option<usize>) -> Vec<(String, serde_json::Value)> {
        let mut results: Vec<(String, serde_json::Value)> = self.streams.iter()
            .filter(|e| !self.is_gone(e.key()) && matches_pattern(pattern, e.key()))
            .filter_map(|e| {
                let payload = e.last()?.payload.clone();
                let matched = match clause.logic {
                    crate::net::protocol::Logic::And => clause.conditions.iter().all(|c| {
                        payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                    }),
                    crate::net::protocol::Logic::Or => clause.conditions.iter().any(|c| {
                        payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                    }),
                };
                if matched { Some((e.key().clone(), payload)) } else { None }
            })
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        if let Some(n) = limit { results.truncate(n); }
        results
    }

    pub fn count(&self, pattern: &str, clause: Option<&crate::net::protocol::WhereClause>) -> usize {
        self.streams.iter()
            .filter(|e| !self.is_gone(e.key()) && matches_pattern(pattern, e.key()))
            .filter(|e| {
                let Some(clause) = clause else { return true; };
                let Some(ev) = e.last() else { return false; };
                let payload = &ev.payload;
                match clause.logic {
                    crate::net::protocol::Logic::And => clause.conditions.iter().all(|c| {
                        payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                    }),
                    crate::net::protocol::Logic::Or => clause.conditions.iter().any(|c| {
                        payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                    }),
                }
            })
            .count()
    }

    pub fn sum(&self, pattern: &str, field: &str, clause: Option<&crate::net::protocol::WhereClause>) -> f64 {
        self.streams.iter()
            .filter(|e| !self.is_gone(e.key()) && matches_pattern(pattern, e.key()))
            .filter_map(|e| {
                let ev = e.last()?;
                let payload = &ev.payload;
                if let Some(clause) = clause {
                    let matched = match clause.logic {
                        crate::net::protocol::Logic::And => clause.conditions.iter().all(|c| {
                            payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                        }),
                        crate::net::protocol::Logic::Or => clause.conditions.iter().any(|c| {
                            payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                        }),
                    };
                    if !matched { return None; }
                }
                payload.get(field)?.as_f64()
            })
            .sum()
    }

    pub fn avg(&self, pattern: &str, field: &str, clause: Option<&crate::net::protocol::WhereClause>) -> Option<f64> {
        let vals: Vec<f64> = self.streams.iter()
            .filter(|e| !self.is_gone(e.key()) && matches_pattern(pattern, e.key()))
            .filter_map(|e| {
                let ev = e.last()?;
                let payload = &ev.payload;
                if let Some(clause) = clause {
                    let matched = match clause.logic {
                        crate::net::protocol::Logic::And => clause.conditions.iter().all(|c| {
                            payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                        }),
                        crate::net::protocol::Logic::Or => clause.conditions.iter().any(|c| {
                            payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                        }),
                    };
                    if !matched { return None; }
                }
                payload.get(field)?.as_f64()
            })
            .collect();
        if vals.is_empty() { None } else { Some(vals.iter().sum::<f64>() / vals.len() as f64) }
    }

    /// Returns all events across all streams matching `pattern` with id > `from_id`,
    /// sorted by event id. Capped at `limit` (most-recent events if truncated).
    pub fn since_pattern(&self, pattern: &str, from_id: u64, limit: usize) -> Vec<Event> {
        let mut events: Vec<Event> = self.streams.iter()
            .filter(|e| !self.is_gone(e.key()) && matches_pattern(pattern, e.key()))
            .flat_map(|e| {
                e.value().iter()
                    .filter(|ev| ev.id > from_id)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect();
        events.sort_by_key(|e| e.id);
        if events.len() > limit { events.drain(0..events.len() - limit); }
        events
    }

    /// Returns (sum, count) for the matching field — used by cluster AVG scatter-gather.
    pub fn sum_and_count(&self, pattern: &str, field: &str, clause: Option<&crate::net::protocol::WhereClause>) -> (f64, usize) {
        let vals: Vec<f64> = self.streams.iter()
            .filter(|e| !self.is_gone(e.key()) && matches_pattern(pattern, e.key()))
            .filter_map(|e| {
                let ev = e.last()?;
                let payload = &ev.payload;
                if let Some(clause) = clause {
                    let matched = match clause.logic {
                        crate::net::protocol::Logic::And => clause.conditions.iter().all(|c| {
                            payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                        }),
                        crate::net::protocol::Logic::Or => clause.conditions.iter().any(|c| {
                            payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
                        }),
                    };
                    if !matched { return None; }
                }
                payload.get(field)?.as_f64()
            })
            .collect();
        (vals.iter().sum(), vals.len())
    }

    pub fn restore_expiry(&self, stream: &str, exp: u64) {
        self.expiry.insert(stream.to_string(), exp);
    }

    /// Claim ownership of a stream. Only the owner (or admin) can write to it.
    pub fn claim(&self, stream: &str, user_id: String) {
        self.owners.insert(stream.to_string(), user_id);
    }

    /// Returns the owner user_id if the stream is claimed.
    pub fn get_owner(&self, stream: &str) -> Option<String> {
        self.owners.get(stream).map(|r| r.clone())
    }

    pub fn snapshot_owners(&self) -> std::collections::HashMap<String, String> {
        self.owners.iter().map(|e| (e.key().clone(), e.value().clone())).collect()
    }

    pub fn restore_owner(&self, stream: &str, user_id: String) {
        self.owners.insert(stream.to_string(), user_id);
    }

    // ── Durable queues ────────────────────────────────────────────────────────

    /// Return (or create) the Notify handle for a queue — used by long-poll DEQUEUE.
    pub fn get_queue_notify(&self, queue: &str) -> Arc<tokio::sync::Notify> {
        self.queue_notify.entry(queue.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone()
    }

    /// Append a job to the back of the queue. Returns the new job id.
    pub fn enqueue(&self, queue: &str, payload: serde_json::Value) -> u64 {
        let id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        self.queues.entry(queue.to_string()).or_default()
            .push_back(QueueJob { id, payload, retries: 0 });
        // Wake any waiting long-poll DEQUEUE on this queue.
        if let Some(n) = self.queue_notify.get(queue) { n.notify_one(); }
        id
    }

    /// Claim the next pending job. It becomes invisible for `timeout_secs` seconds.
    /// Returns `None` if the queue is empty.
    pub fn dequeue(&self, queue: &str, timeout_secs: u64) -> Option<QueueJob> {
        let job = self.queues.get_mut(queue)?.pop_front()?;
        self.inflight.insert(job.id, InflightJob {
            queue:      queue.to_string(),
            job:        job.clone(),
            timeout_at: now_secs() + timeout_secs,
        });
        Some(job)
    }

    /// Permanently remove a claimed job. Returns false if not found / wrong queue.
    pub fn ack(&self, job_id: u64) -> bool {
        self.inflight.remove(&job_id).is_some()
    }

    /// Return a claimed job to the front of its queue. Increments retry count.
    pub fn nack(&self, job_id: u64) -> bool {
        if let Some((_, inf)) = self.inflight.remove(&job_id) {
            let mut job = inf.job;
            job.retries += 1;
            self.queues.entry(inf.queue).or_default().push_front(job);
            true
        } else {
            false
        }
    }

    /// Pending + in-flight count for a queue.
    pub fn qlen(&self, queue: &str) -> usize {
        let pending  = self.queues.get(queue).map(|q| q.len()).unwrap_or(0);
        let inflight = self.inflight.iter().filter(|e| e.value().queue == queue).count();
        pending + inflight
    }

    /// Peek at the next pending job without claiming it.
    pub fn qpeek(&self, queue: &str) -> Option<QueueJob> {
        self.queues.get(queue)?.front().cloned()
    }

    /// Re-queue any in-flight jobs whose visibility timeout has expired.
    /// Called from the eviction loop every second.
    pub fn requeue_timed_out(&self) -> usize {
        let now = now_secs();
        let expired: Vec<u64> = self.inflight.iter()
            .filter(|e| now >= e.value().timeout_at)
            .map(|e| *e.key())
            .collect();
        let count = expired.len();
        for id in expired {
            if let Some((_, inf)) = self.inflight.remove(&id) {
                let mut job = inf.job;
                job.retries += 1;
                self.queues.entry(inf.queue).or_default().push_front(job);
            }
        }
        count
    }

    pub fn snapshot_queues(&self) -> std::collections::HashMap<String, Vec<QueueJob>> {
        // Pending jobs
        let mut map: std::collections::HashMap<String, Vec<QueueJob>> = self.queues.iter()
            .map(|e| (e.key().clone(), e.value().iter().cloned().collect()))
            .collect();
        // In-flight jobs also become pending on restore (workers must re-process)
        for entry in self.inflight.iter() {
            let inf = entry.value();
            let mut job = inf.job.clone();
            job.retries += 1;
            map.entry(inf.queue.clone()).or_default().push(job);
        }
        map
    }

    pub fn restore_queue(&self, queue: &str, jobs: Vec<QueueJob>) {
        let mut cur_max = self.next_job_id.load(Ordering::Relaxed);
        for job in jobs {
            if job.id >= cur_max { cur_max = job.id + 1; }
            self.queues.entry(queue.to_string()).or_default().push_back(job);
        }
        self.next_job_id.store(cur_max, Ordering::Relaxed);
    }

    // ── Geospatial ────────────────────────────────────────────────────────────

    pub fn geo_set(&self, stream: &str, lat: f64, lng: f64) {
        self.geo_index.insert(stream.to_string(), (lat, lng));
    }

    pub fn geo_get(&self, stream: &str) -> Option<(f64, f64)> {
        self.geo_index.get(stream).map(|r| *r)
    }

    pub fn geo_del(&self, stream: &str) {
        self.geo_index.remove(stream);
    }

    /// Returns `(stream, lat, lng, dist_km)` sorted by distance, filtered to within `radius_km`.
    pub fn geo_near(&self, pattern: &str, lat: f64, lng: f64, radius_km: f64) -> Vec<(String, f64, f64, f64)> {
        let mut results: Vec<(String, f64, f64, f64)> = self.geo_index.iter()
            .filter(|e| matches_pattern(pattern, e.key()))
            .filter_map(|e| {
                let (slat, slng) = *e.value();
                let dist = haversine_km(lat, lng, slat, slng);
                if dist <= radius_km { Some((e.key().clone(), slat, slng, dist)) } else { None }
            })
            .collect();
        results.sort_by(|a, b| a.3.partial_cmp(&b.3).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    pub fn geo_dist(&self, stream1: &str, stream2: &str) -> Option<f64> {
        let (lat1, lng1) = self.geo_get(stream1)?;
        let (lat2, lng2) = self.geo_get(stream2)?;
        Some(haversine_km(lat1, lng1, lat2, lng2))
    }

    pub fn snapshot_geo(&self) -> std::collections::HashMap<String, (f64, f64)> {
        self.geo_index.iter().map(|e| (e.key().clone(), *e.value())).collect()
    }

    pub fn restore_geo(&self, stream: &str, lat: f64, lng: f64) {
        self.geo_index.insert(stream.to_string(), (lat, lng));
    }

    pub fn snapshot_streams(&self) -> std::collections::HashMap<String, serde_json::Value> {
        self.streams.iter()
            .filter(|e| !self.is_gone(e.key()))
            .filter_map(|e| e.last().map(|ev| (e.key().clone(), ev.payload.clone())))
            .collect()
    }

    pub fn snapshot_deleted(&self) -> Vec<String> {
        self.deleted.iter()
            .filter(|e| *e.value())
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn snapshot_expiry(&self) -> std::collections::HashMap<String, u64> {
        self.expiry.iter()
            .filter(|e| now_secs() < *e.value())
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    pub fn next_id(&self) -> u64 {
        self.last_event_id() + 1
    }

    pub fn last_event_id(&self) -> u64 {
        self.streams.iter()
            .filter_map(|e| e.last().map(|ev| ev.id))
            .max()
            .unwrap_or(0)
    }

    /// Removes all streams whose TTL has passed from every map.
    /// Returns how many streams were evicted.
    /// Before this existed, expired streams were just hidden (is_gone = true)
    /// but their event history stayed in RAM forever.
    pub fn evict_expired(&self) -> usize {
        let now = now_secs();
        let expired: Vec<String> = self.expiry
            .iter()
            .filter(|e| now >= *e.value())
            .map(|e| e.key().clone())
            .collect();

        for stream in &expired {
            self.streams.remove(stream);
            self.expiry.remove(stream);
            self.deleted.remove(stream);
            self.geo_index.remove(stream);
        }

        expired.len()
    }

    /// Returns the ID of the latest event written to a stream (0 if none).
    pub fn stream_version(&self, stream: &str) -> u64 {
        self.streams.get(stream)
            .and_then(|events| events.last().map(|e| e.id))
            .unwrap_or(0)
    }

    /// Checks whether any of the given streams were modified after `since_id`.
    /// Returns the first conflicting stream name, or None if all are clean.
    pub fn conflict_check(&self, streams: &[String], since_id: u64) -> Option<String> {
        streams.iter()
            .find(|s| self.stream_version(s) > since_id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::Op;
    use crate::net::protocol::{Condition, Logic, WhereClause};
    use serde_json::json;

    fn make_index() -> Index { Index::new() }

    fn emit(index: &Index, id: u64, stream: &str, payload: serde_json::Value) {
        index.apply(Event::new(id, stream, Op::Set, payload));
    }

    fn where_clause(field: &str, op: &str, value: &str) -> WhereClause {
        WhereClause {
            conditions: vec![Condition { field: field.into(), op: op.into(), value: value.into() }],
            logic: Logic::And,
        }
    }

    // ── basic read/write ──────────────────────────────────────────────────────

    #[test]
    fn set_and_get() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"name": "evan"}));
        assert_eq!(idx.latest("users:1").unwrap()["name"], "evan");
    }

    #[test]
    fn latest_returns_last_event() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"name": "evan"}));
        emit(&idx, 2, "users:1", json!({"name": "updated"}));
        assert_eq!(idx.latest("users:1").unwrap()["name"], "updated");
    }

    #[test]
    fn delete_hides_stream() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"name": "evan"}));
        idx.apply(Event::new(2, "users:1", Op::Delete, json!(null)));
        assert!(idx.latest("users:1").is_none());
        assert!(idx.is_deleted("users:1"));
    }

    #[test]
    fn missing_stream_returns_none() {
        let idx = make_index();
        assert!(idx.latest("users:99").is_none());
    }

    // ── keys ─────────────────────────────────────────────────────────────────

    #[test]
    fn keys_wildcard() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({}));
        emit(&idx, 2, "users:2", json!({}));
        emit(&idx, 3, "orders:1", json!({}));
        let keys = idx.keys("users:*");
        assert_eq!(keys, vec!["users:1", "users:2"]);
    }

    #[test]
    fn keys_excludes_deleted() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({}));
        idx.apply(Event::new(2, "users:1", Op::Delete, json!(null)));
        assert!(idx.keys("users:*").is_empty());
    }

    // ── since ─────────────────────────────────────────────────────────────────

    #[test]
    fn since_returns_events_after_ts() {
        let idx = make_index();
        let e1 = Event { id: 1, ts: 100, stream: "s:1".into(), op: Op::Set, payload: json!({"v":1}), txn_id: None };
        let e2 = Event { id: 2, ts: 200, stream: "s:1".into(), op: Op::Set, payload: json!({"v":2}), txn_id: None };
        idx.apply(e1);
        idx.apply(e2);
        let (results, _) = idx.since("s:1", 150, 50_000);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].payload["v"], 2);
    }

    // ── patch ─────────────────────────────────────────────────────────────────

    #[test]
    fn patch_merges_fields() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"name": "evan", "age": 25}));
        let merged = idx.patch("users:1", json!({"age": 26, "role": "admin"})).unwrap();
        assert_eq!(merged["name"], "evan");   // preserved
        assert_eq!(merged["age"],  26);       // updated
        assert_eq!(merged["role"], "admin");  // added
    }

    #[test]
    fn patch_inc_creates_and_increments() {
        let idx = make_index();
        // $inc on missing field starts at 0
        let r = idx.patch("c:1", json!({"views": {"$inc": 1}})).unwrap();
        assert_eq!(r["views"], 1.0);
        // $inc again
        emit(&idx, 1, "c:1", r);
        let r2 = idx.patch("c:1", json!({"views": {"$inc": 5}})).unwrap();
        assert_eq!(r2["views"], 6.0);
    }

    #[test]
    fn patch_inc_decrement() {
        let idx = make_index();
        emit(&idx, 1, "c:1", json!({"score": 10}));
        let r = idx.patch("c:1", json!({"score": {"$inc": -3}})).unwrap();
        assert_eq!(r["score"], 7.0);
    }

    #[test]
    fn patch_mul() {
        let idx = make_index();
        emit(&idx, 1, "c:1", json!({"price": 100}));
        let r = idx.patch("c:1", json!({"price": {"$mul": 1.1}})).unwrap();
        assert!((r["price"].as_f64().unwrap() - 110.0).abs() < 0.001);
    }

    #[test]
    fn patch_min_max() {
        let idx = make_index();
        emit(&idx, 1, "c:1", json!({"temp": 20}));
        let r = idx.patch("c:1", json!({"temp": {"$max": 25}})).unwrap();
        assert_eq!(r["temp"], 25.0);
        emit(&idx, 2, "c:1", r);
        let r2 = idx.patch("c:1", json!({"temp": {"$min": 18}})).unwrap();
        assert_eq!(r2["temp"], 18.0);
    }

    #[test]
    fn patch_push_and_pop() {
        let idx = make_index();
        emit(&idx, 1, "list:1", json!({"items": [1, 2]}));
        let r = idx.patch("list:1", json!({"items": {"$push": 3}})).unwrap();
        assert_eq!(r["items"], json!([1, 2, 3]));
        emit(&idx, 2, "list:1", r);
        let r2 = idx.patch("list:1", json!({"items": {"$pop": "first"}})).unwrap();
        assert_eq!(r2["items"], json!([2, 3]));
    }

    #[test]
    fn patch_unset() {
        let idx = make_index();
        emit(&idx, 1, "u:1", json!({"name": "evan", "temp": "delete_me"}));
        let r = idx.patch("u:1", json!({"temp": {"$unset": true}})).unwrap();
        assert_eq!(r["name"], "evan");
        assert!(r.get("temp").is_none());
    }

    // ── query ─────────────────────────────────────────────────────────────────

    #[test]
    fn query_eq() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"role": "admin"}));
        emit(&idx, 2, "users:2", json!({"role": "reader"}));
        let results = idx.query("users:*", &where_clause("role", "=", "admin"), None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "users:1");
    }

    #[test]
    fn query_gt() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"age": 25}));
        emit(&idx, 2, "users:2", json!({"age": 15}));
        let results = idx.query("users:*", &where_clause("age", ">", "18"), None);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn query_and() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"role": "admin", "age": 30}));
        emit(&idx, 2, "users:2", json!({"role": "admin", "age": 15}));
        let clause = WhereClause {
            conditions: vec![
                Condition { field: "role".into(), op: "=".into(), value: "admin".into() },
                Condition { field: "age".into(),  op: ">".into(), value: "18".into() },
            ],
            logic: Logic::And,
        };
        let results = idx.query("users:*", &clause, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "users:1");
    }

    #[test]
    fn query_or() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"role": "admin"}));
        emit(&idx, 2, "users:2", json!({"role": "writer"}));
        emit(&idx, 3, "users:3", json!({"role": "reader"}));
        let clause = WhereClause {
            conditions: vec![
                Condition { field: "role".into(), op: "=".into(), value: "admin".into() },
                Condition { field: "role".into(), op: "=".into(), value: "writer".into() },
            ],
            logic: Logic::Or,
        };
        let results = idx.query("users:*", &clause, None);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn query_limit() {
        let idx = make_index();
        for i in 1..=5 { emit(&idx, i, &format!("users:{i}"), json!({"age": 25})); }
        let results = idx.query("users:*", &where_clause("age", "=", "25"), Some(3));
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn query_contains() {
        let idx = make_index();
        emit(&idx, 1, "posts:1", json!({"title": "hello world"}));
        emit(&idx, 2, "posts:2", json!({"title": "goodbye"}));
        let results = idx.query("posts:*", &where_clause("title", "CONTAINS", "world"), None);
        assert_eq!(results.len(), 1);
    }

    // ── count / sum / avg ─────────────────────────────────────────────────────

    #[test]
    fn count_all() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({}));
        emit(&idx, 2, "users:2", json!({}));
        assert_eq!(idx.count("users:*", None), 2);
    }

    #[test]
    fn count_with_filter() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({"role": "admin"}));
        emit(&idx, 2, "users:2", json!({"role": "reader"}));
        let clause = where_clause("role", "=", "admin");
        assert_eq!(idx.count("users:*", Some(&clause)), 1);
    }

    #[test]
    fn sum_field() {
        let idx = make_index();
        emit(&idx, 1, "orders:1", json!({"total": 100.0}));
        emit(&idx, 2, "orders:2", json!({"total": 50.0}));
        assert_eq!(idx.sum("orders:*", "total", None), 150.0);
    }

    #[test]
    fn avg_field() {
        let idx = make_index();
        emit(&idx, 1, "orders:1", json!({"total": 100.0}));
        emit(&idx, 2, "orders:2", json!({"total": 50.0}));
        assert_eq!(idx.avg("orders:*", "total", None), Some(75.0));
    }

    #[test]
    fn avg_empty_returns_none() {
        let idx = make_index();
        assert!(idx.avg("orders:*", "total", None).is_none());
    }

    // ── isolation / conflict detection ────────────────────────────────────────

    #[test]
    fn evict_expired_frees_memory() {
        let idx = make_index();
        emit(&idx, 1, "sessions:1", json!({"user": "evan"}));
        emit(&idx, 2, "sessions:2", json!({"user": "sara"}));

        // expire sessions:1 immediately (0 seconds from now)
        idx.expiry.insert("sessions:1".to_string(), 0);

        assert_eq!(idx.evict_expired(), 1);
        assert!(idx.latest("sessions:1").is_none());  // evicted
        assert!(idx.streams.get("sessions:1").is_none()); // actually freed from RAM
        assert!(idx.latest("sessions:2").is_some());  // untouched
    }

    #[test]
    fn evict_only_removes_expired() {
        let idx = make_index();
        emit(&idx, 1, "sessions:1", json!({}));
        emit(&idx, 2, "sessions:2", json!({}));

        // sessions:1 expires far in the future
        idx.set_expiry("sessions:1", 99999);

        assert_eq!(idx.evict_expired(), 0); // nothing expired yet
        assert!(idx.latest("sessions:1").is_some());
        assert!(idx.latest("sessions:2").is_some());
    }

    #[test]
    fn conflict_check_no_conflict() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({}));
        let snapshot = idx.last_event_id(); // snapshot = 1
        // no writes after snapshot
        assert!(idx.conflict_check(&["users:1".to_string()], snapshot).is_none());
    }

    #[test]
    fn conflict_check_detects_conflict() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({}));
        let snapshot = idx.last_event_id(); // snapshot = 1
        emit(&idx, 2, "users:1", json!({"updated": true})); // another write after snapshot
        assert_eq!(
            idx.conflict_check(&["users:1".to_string()], snapshot),
            Some("users:1".to_string())
        );
    }

    #[test]
    fn conflict_check_different_stream_no_conflict() {
        let idx = make_index();
        emit(&idx, 1, "users:1", json!({}));
        let snapshot = idx.last_event_id();
        emit(&idx, 2, "users:2", json!({})); // different stream written after snapshot
        // we only wrote to users:1, so no conflict
        assert!(idx.conflict_check(&["users:1".to_string()], snapshot).is_none());
    }
}

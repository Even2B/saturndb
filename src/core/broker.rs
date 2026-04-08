use crate::core::event::Event;
use crate::net::protocol::{Logic, WhereClause};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

const CHANNEL_CAPACITY: usize = 1024;

struct Subscription {
    pattern: String,
    filter:  Option<WhereClause>,
    tx:      broadcast::Sender<Event>,
}

#[derive(Clone)]
pub struct Broker {
    subscriptions: Arc<RwLock<Vec<Subscription>>>,
}

impl Broker {
    pub fn new() -> Self {
        Self { subscriptions: Arc::new(RwLock::new(Vec::new())) }
    }

    pub async fn subscribe(&self, pattern: &str, filter: Option<WhereClause>) -> broadcast::Receiver<Event> {
        let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
        self.subscriptions.write().await.push(Subscription {
            pattern: pattern.to_string(),
            filter,
            tx,
        });
        rx
    }

    pub async fn publish(&self, event: &Event) {
        let subs = self.subscriptions.read().await;
        for sub in subs.iter() {
            if !matches_pattern(&sub.pattern, &event.stream) { continue; }
            if let Some(ref clause) = sub.filter {
                if !eval_clause(&event.payload, clause) { continue; }
            }
            let _ = sub.tx.send(event.clone());
        }
    }
}

fn eval_clause(payload: &serde_json::Value, clause: &WhereClause) -> bool {
    match clause.logic {
        Logic::And => clause.conditions.iter().all(|c| {
            payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
        }),
        Logic::Or => clause.conditions.iter().any(|c| {
            payload.get(&c.field).map(|v| eval_op(v, &c.op, &c.value)).unwrap_or(false)
        }),
    }
}

fn eval_op(field_val: &serde_json::Value, op: &str, value: &str) -> bool {
    match op {
        "=" | "==" => field_val.as_str().map(|s| s == value)
            .unwrap_or_else(|| field_val.to_string().trim_matches('"') == value),
        "!=" | "<>" => field_val.as_str().map(|s| s != value)
            .unwrap_or_else(|| field_val.to_string().trim_matches('"') != value),
        ">" => {
            let a = field_val.as_f64().or_else(|| field_val.as_str()?.parse().ok());
            let b = value.parse::<f64>().ok();
            matches!((a, b), (Some(a), Some(b)) if a > b)
        }
        ">=" => {
            let a = field_val.as_f64().or_else(|| field_val.as_str()?.parse().ok());
            let b = value.parse::<f64>().ok();
            matches!((a, b), (Some(a), Some(b)) if a >= b)
        }
        "<" => {
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
    if pattern == stream { return true; }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return stream.starts_with(prefix);
    }
    false
}

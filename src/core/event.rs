use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Set,
    Delete,
    Patch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id:      u64,
    pub ts:      u64,
    pub stream:  String,
    pub op:      Op,
    pub payload: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_id:  Option<u64>,
}

impl Event {
    pub fn new(id: u64, stream: impl Into<String>, op: Op, payload: serde_json::Value) -> Self {
        Self { id, ts: now_micros(), stream: stream.into(), op, payload, txn_id: None }
    }

    pub fn with_txn(mut self, txn_id: u64) -> Self {
        self.txn_id = Some(txn_id);
        self
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_micros() as u64
}

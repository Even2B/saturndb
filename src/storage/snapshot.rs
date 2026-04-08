use crate::core::event::{Event, Op};
use crate::storage::index::{Index, QueueJob};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const SNAPSHOT_PATH: &str = "saturn-snapshot.json";

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub last_event_id: u64,
    pub streams:       HashMap<String, serde_json::Value>,
    pub deleted:       Vec<String>,
    pub expiry:        HashMap<String, u64>,
    #[serde(default)]
    pub owners:        HashMap<String, String>,
    #[serde(default)]
    pub geo:           HashMap<String, [f64; 2]>,
    #[serde(default)]
    pub queues:        HashMap<String, Vec<QueueJob>>,
}

pub async fn save(index: &Index, last_event_id: u64) -> Result<()> {
    let snap = Snapshot {
        last_event_id,
        streams: index.snapshot_streams(),
        deleted: index.snapshot_deleted(),
        expiry:  index.snapshot_expiry(),
        owners:  index.snapshot_owners(),
        geo:     index.snapshot_geo().into_iter().map(|(k, (lat, lng))| (k, [lat, lng])).collect(),
        queues:  index.snapshot_queues(),
    };
    let json = serde_json::to_string(&snap)?;
    tokio::fs::write(SNAPSHOT_PATH, json).await?;
    println!("snapshot saved — last_event_id={last_event_id}");
    Ok(())
}

pub async fn load(index: &Index) -> Result<u64> {
    let raw = match tokio::fs::read_to_string(SNAPSHOT_PATH).await {
        Ok(r)  => r,
        Err(_) => return Ok(0), // no snapshot yet
    };

    let snap: Snapshot = serde_json::from_str(&raw)?;
    let last_id = snap.last_event_id;

    for (stream, value) in snap.streams {
        let event = Event::new(last_id, &stream, Op::Set, value);
        index.apply(event);
    }

    for stream in snap.deleted {
        let event = Event::new(last_id, &stream, Op::Delete, serde_json::Value::Null);
        index.apply(event);
    }

    for (stream, exp) in snap.expiry {
        index.restore_expiry(&stream, exp);
    }

    for (stream, user_id) in snap.owners {
        index.restore_owner(&stream, user_id);
    }

    for (stream, [lat, lng]) in snap.geo {
        index.restore_geo(&stream, lat, lng);
    }

    for (queue, jobs) in snap.queues {
        index.restore_queue(&queue, jobs);
    }

    println!("snapshot loaded — last_event_id={last_id}");
    Ok(last_id)
}

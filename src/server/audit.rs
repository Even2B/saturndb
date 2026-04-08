use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

/// Append-only audit log. Each entry is one JSON line.
/// Format: {"ts":<unix_secs>,"op":"EMIT","user":"uid-or-token","stream":"users:1","result":"OK"}
#[derive(Clone)]
pub struct AuditLog {
    tx: mpsc::Sender<String>,
}

impl AuditLog {
    pub async fn open(path: &str) -> anyhow::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .create(true).append(true).open(path).await?;
        let (tx, rx) = mpsc::channel::<String>(8192);
        tokio::spawn(audit_actor(file, rx));
        Ok(Self { tx })
    }

    /// Log an auditable event. Fire-and-forget — never blocks the caller.
    pub fn log(&self, op: &str, user: &str, stream: &str, ok: bool) {
        let ts     = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let result = if ok { "OK" } else { "ERR" };
        let line   = format!(
            "{{\"ts\":{ts},\"op\":\"{op}\",\"user\":\"{user}\",\"stream\":\"{stream}\",\"result\":\"{result}\"}}\n"
        );
        let _ = self.tx.try_send(line);
    }
}

async fn audit_actor(mut file: tokio::fs::File, mut rx: mpsc::Receiver<String>) {
    while let Some(line) = rx.recv().await {
        if let Err(e) = file.write_all(line.as_bytes()).await {
            eprintln!("[audit] write error: {e}");
        }
    }
}

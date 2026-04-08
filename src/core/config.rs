use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;

const CONFIG_PATH: &str = "saturn.toml";

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server:  ServerConfig,
    #[serde(default)]
    pub auth:    AuthConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub cluster: ClusterConfig,
}

// ── Server ────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host:            String,
    pub port:            u16,
    pub ws_port:         u16,
    pub tls:             bool,
    pub max_connections: usize,
    /// Seconds of inactivity before a connection is closed. 0 = disabled.
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    /// HTTP port for /health and /metrics. 0 = disabled.
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    /// Drop a connection after this many consecutive AUTH failures. 0 = disabled.
    #[serde(default = "default_max_auth_failures")]
    pub max_auth_failures: u32,
    /// Max new connections per second from a single IP (token bucket). 0 = disabled.
    #[serde(default = "default_max_conn_per_ip_per_sec")]
    pub max_conn_per_ip_per_sec: f64,
    /// Burst capacity for per-IP connection rate limit.
    #[serde(default = "default_max_conn_burst_per_ip")]
    pub max_conn_burst_per_ip: f64,
}

fn default_idle_timeout()            -> u64 { 300 }
fn default_metrics_port()            -> u16 { 9090 }
fn default_max_auth_failures()       -> u32 { 5 }
fn default_max_conn_per_ip_per_sec() -> f64 { 10.0 }
fn default_max_conn_burst_per_ip()   -> f64 { 20.0 }

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host:                    "127.0.0.1".to_string(),
            port:                    7379,
            ws_port:                 7380,
            tls:                     false,
            max_connections:         1000,
            idle_timeout_secs:       default_idle_timeout(),
            metrics_port:            default_metrics_port(),
            max_auth_failures:       default_max_auth_failures(),
            max_conn_per_ip_per_sec: default_max_conn_per_ip_per_sec(),
            max_conn_burst_per_ip:   default_max_conn_burst_per_ip(),
        }
    }
}

// ── Auth ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    pub admin_token: String,
    #[serde(default)]
    pub tokens: HashMap<String, TokenConfig>,
    #[serde(default)]
    pub jwt: JwtConfig,
}

/// JWT verification config. Set `secret` to enable.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct JwtConfig {
    /// HMAC secret for HS256. Empty = JWT auth disabled.
    #[serde(default)]
    pub secret: String,
    /// Role granted to JWT users. Default: "writer".
    #[serde(default = "default_jwt_role")]
    pub role: String,
    /// Namespace templates. `{sub}` is replaced with the JWT subject (user_id).
    /// Empty list = unrestricted (not recommended for production).
    #[serde(default)]
    pub namespaces: Vec<String>,
}

fn default_jwt_role() -> String { "writer".to_string() }

#[derive(Debug, Deserialize, Clone)]
pub struct TokenConfig {
    pub role:        String,
    #[serde(default)]
    pub namespaces:  Vec<String>,
    pub namespace:   Option<String>,
    pub can_watch:   Option<bool>,
    pub can_since:   Option<bool>,
    pub expires_at:  Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { admin_token: "saturn-admin-secret".to_string(), tokens: HashMap::new(), jwt: JwtConfig::default() }
    }
}

// ── Storage ───────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub log_path:          String,
    pub snapshot_interval: u64,
    /// "sync" (default) — wait for WAL write before responding (durable)
    /// "async"          — respond immediately, WAL writes in background (faster, ~10ms crash window)
    #[serde(default = "default_wal_mode")]
    pub wal_mode: String,
    /// Maximum number of events kept in memory per stream.
    /// Older events are dropped from the in-memory index (WAL on disk is untouched).
    /// None = unlimited (default). Set e.g. 1000 to cap memory usage.
    pub max_history_per_stream: Option<usize>,
    /// Max events returned by a single SINCE command. Prevents a client from
    /// replaying millions of events and tanking the server. Default: 50_000.
    #[serde(default = "default_max_since")]
    pub max_since_events: usize,
    /// Token-bucket rate limit: sustained writes per second per connection.
    /// Default: 500. Set to 0 to disable.
    #[serde(default = "default_rate_per_sec")]
    pub rate_limit_per_sec: f64,
    /// Token-bucket burst capacity per connection. Default: 1000.
    #[serde(default = "default_rate_burst")]
    pub rate_limit_burst: f64,
    /// Path to the reactive rules persistence file.
    #[serde(default = "default_rules_path")]
    pub rules_path: String,
    /// Path to the append-only audit log. Empty = disabled.
    #[serde(default)]
    pub audit_log: String,
}

fn default_wal_mode()     -> String { "sync".to_string() }
fn default_max_since()    -> usize  { 50_000 }
fn default_rate_per_sec() -> f64    { 500.0 }
fn default_rate_burst()   -> f64    { 1_000.0 }
fn default_rules_path()   -> String { "saturn-rules.json".to_string() }

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            log_path:               "saturn.log".to_string(),
            snapshot_interval:      1000,
            wal_mode:               "sync".to_string(),
            max_history_per_stream: None,
            max_since_events:       default_max_since(),
            rate_limit_per_sec:     default_rate_per_sec(),
            rate_limit_burst:       default_rate_burst(),
            rules_path:             default_rules_path(),
            audit_log:              String::new(),
        }
    }
}

// ── Cluster ───────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    /// Enable clustering. When false, node runs standalone.
    pub enabled: bool,

    /// Unique identifier for this node. Must be unique across the cluster.
    pub node_id: String,

    /// Address this node listens on for peer-to-peer connections (host:port).
    pub peer_addr: String,

    /// Addresses of all other nodes' peer_addr (including self is fine — it will be skipped).
    #[serde(default)]
    pub peers: Vec<String>,

    /// How many nodes store each stream (1 = no replication, 2 = one replica, …).
    pub replication_factor: usize,

    /// How many virtual nodes per real node on the consistent hash ring.
    pub vnodes_per_node: usize,

    /// Shared secret all nodes must present in the peer handshake.
    /// Empty string = no auth (insecure, for dev only).
    #[serde(default)]
    pub cluster_secret: String,

    /// Encrypt peer-to-peer traffic with TLS. Uses the same self-signed cert
    /// as the client listener (saturn-cert.pem / saturn-key.pem).
    /// Cert is not verified — cluster_secret handles peer authentication.
    #[serde(default)]
    pub peer_tls: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled:            false,
            node_id:            "node-1".to_string(),
            peer_addr:          "0.0.0.0:7381".to_string(),
            peers:              vec![],
            replication_factor: 1,
            vnodes_per_node:    150,
            cluster_secret:     String::new(),
            peer_tls:           false,
        }
    }
}

// ── Load ──────────────────────────────────────────────────────────────────────

impl Config {
    pub fn load() -> Result<Self> {
        let raw = std::fs::read_to_string(CONFIG_PATH).unwrap_or_default();
        Ok(toml::from_str(&raw)?)
    }
}

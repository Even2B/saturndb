use crate::core::config::{AuthConfig, JwtConfig};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Admin,
    Writer,
    Reader,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub role:       Role,
    pub namespaces: Vec<String>, // empty = all streams
    pub can_watch:  bool,
    pub can_since:  bool,
    pub expires_at: Option<u64>, // unix secs
    pub blocked:    bool,
    /// Set for JWT-authenticated users. Used for stream ownership checks.
    pub user_id:    Option<String>,
}

impl Token {
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(exp) => now_secs() >= exp,
            None      => false,
        }
    }

    fn matches_namespace(&self, stream: &str) -> bool {
        if self.namespaces.is_empty() { return true; }
        self.namespaces.iter().any(|ns| {
            if ns.ends_with('*') {
                stream.starts_with(&ns[..ns.len() - 1])
            } else {
                stream.starts_with(ns.as_str())
            }
        })
    }
}

#[derive(Clone)]
pub struct AuthStore {
    tokens:  Arc<DashMap<String, Token>>,
    jwt_cfg: Option<JwtConfig>,
}

impl AuthStore {
    pub fn from_config(cfg: &AuthConfig) -> Self {
        let jwt_cfg = if cfg.jwt.secret.is_empty() { None } else { Some(cfg.jwt.clone()) };
        let store = Self { tokens: Arc::new(DashMap::new()), jwt_cfg };

        store.tokens.insert(cfg.admin_token.clone(), Token {
            role:       Role::Admin,
            namespaces: vec![],
            can_watch:  true,
            can_since:  true,
            expires_at: None,
            blocked:    false,
            user_id:    None,
        });

        for (raw, tc) in &cfg.tokens {
            let role = match tc.role.to_lowercase().as_str() {
                "writer" => Role::Writer,
                "reader" => Role::Reader,
                _        => Role::Reader,
            };

            // merge namespace + namespaces into one list
            let mut namespaces = tc.namespaces.clone();
            if let Some(ns) = &tc.namespace {
                if !namespaces.contains(ns) {
                    namespaces.push(ns.clone());
                }
            }

            let expires_at = tc.expires_at.as_deref().and_then(parse_date_secs);

            store.tokens.insert(raw.clone(), Token {
                role,
                namespaces,
                can_watch:  tc.can_watch.unwrap_or(true),
                can_since:  tc.can_since.unwrap_or(true),
                expires_at,
                blocked:    false,
                user_id:    None,
            });
        }

        store
    }

    pub fn verify(&self, raw: &str) -> Option<Token> {
        let token = self.tokens.get(raw)?.clone();
        if token.is_expired() || token.blocked { return None; }
        Some(token)
    }

    /// Try to verify `raw` as a JWT. Returns an ephemeral Token if valid.
    /// Only active when `[auth.jwt] secret` is configured.
    pub fn verify_jwt(&self, raw: &str) -> Option<Token> {
        use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
        #[derive(serde::Deserialize)]
        struct Claims { sub: String, exp: Option<u64> }

        let cfg = self.jwt_cfg.as_ref()?;
        let key = DecodingKey::from_secret(cfg.secret.as_bytes());
        let mut val = Validation::new(Algorithm::HS256);
        val.validate_aud = false; // Supabase uses custom audience

        let data = decode::<Claims>(raw, &key, &val).ok()?;
        let sub  = data.claims.sub.clone();

        let role = match cfg.role.to_lowercase().as_str() {
            "writer" => Role::Writer,
            "reader" => Role::Reader,
            _        => Role::Writer,
        };
        let namespaces = cfg.namespaces.iter()
            .map(|ns| ns.replace("{sub}", &sub))
            .collect();

        Some(Token {
            role,
            namespaces,
            can_watch:  true,
            can_since:  true,
            expires_at: data.claims.exp,
            blocked:    false,
            user_id:    Some(sub),
        })
    }

    pub fn block(&self, raw: &str) -> bool {
        match self.tokens.get_mut(raw) {
            Some(mut t) => { t.blocked = true; true }
            None        => false,
        }
    }

    pub fn unblock(&self, raw: &str) -> bool {
        match self.tokens.get_mut(raw) {
            Some(mut t) => { t.blocked = false; true }
            None        => false,
        }
    }

    pub fn can_write(&self, token: &Token, stream: &str) -> bool {
        match token.role {
            Role::Admin  => true,
            Role::Writer => token.matches_namespace(stream),
            Role::Reader => false,
        }
    }

    pub fn can_read(&self, token: &Token, stream: &str) -> bool {
        match token.role {
            Role::Admin => true,
            _           => token.matches_namespace(stream),
        }
    }

    pub fn can_watch(&self, token: &Token, stream: &str) -> bool {
        token.can_watch && self.can_read(token, stream)
    }

    pub fn can_since(&self, token: &Token, stream: &str) -> bool {
        token.can_since && self.can_read(token, stream)
    }

    // runtime token management
    pub fn create(&self, raw: String, token: Token) {
        self.tokens.insert(raw, token);
    }

    pub fn revoke(&self, raw: &str) -> bool {
        self.tokens.remove(raw).is_some()
    }

    pub fn list(&self) -> Vec<(String, String, Vec<String>)> {
        self.tokens.iter().map(|e| {
            let role = match e.role {
                Role::Admin  => "admin",
                Role::Writer => "writer",
                Role::Reader => "reader",
            };
            (e.key().clone(), role.to_string(), e.namespaces.clone())
        }).collect()
    }
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

pub fn parse_date_secs_pub(s: &str) -> Option<u64> { parse_date_secs(s) }

fn parse_date_secs(s: &str) -> Option<u64> {
    // parse "2026-12-31" → unix timestamp
    let parts: Vec<u64> = s.split('-').filter_map(|p| p.parse().ok()).collect();
    if parts.len() != 3 { return None; }
    // rough calculation: days since epoch
    let (y, m, d) = (parts[0], parts[1], parts[2]);
    let days = days_since_epoch(y, m, d);
    Some(days * 86400)
}

pub fn days_since_epoch(y: u64, m: u64, d: u64) -> u64 {
    // days from 1970-01-01 to y-m-d (approximate, good enough for expiry)
    let months = [0u64, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let leap = |yr: u64| yr % 4 == 0 && (yr % 100 != 0 || yr % 400 == 0);
    let mut days = (y - 1970) * 365 + (1970..y).filter(|&yr| leap(yr)).count() as u64;
    for i in 1..m {
        days += months[i as usize];
        if i == 2 && leap(y) { days += 1; }
    }
    days + d - 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::{AuthConfig, TokenConfig};
    use std::collections::HashMap;

    fn make_store() -> AuthStore {
        let mut tokens = HashMap::new();
        tokens.insert("writer-tok".to_string(), TokenConfig {
            role:       "writer".to_string(),
            namespace:  None,
            namespaces: vec!["users".to_string(), "orders".to_string()],
            can_watch:  Some(true),
            can_since:  Some(true),
            expires_at: None,
        });
        tokens.insert("reader-tok".to_string(), TokenConfig {
            role:       "reader".to_string(),
            namespace:  None,
            namespaces: vec!["users".to_string()],
            can_watch:  Some(false),
            can_since:  Some(false),
            expires_at: None,
        });
        tokens.insert("expired-tok".to_string(), TokenConfig {
            role:       "reader".to_string(),
            namespace:  None,
            namespaces: vec![],
            can_watch:  Some(true),
            can_since:  Some(true),
            expires_at: Some("2000-01-01".to_string()), // already expired
        });
        AuthStore::from_config(&AuthConfig {
            admin_token: "admin-tok".to_string(),
            tokens,
            jwt: crate::core::config::JwtConfig::default(),
        })
    }

    #[test]
    fn admin_can_do_everything() {
        let store = make_store();
        let t = store.verify("admin-tok").unwrap();
        assert!(store.can_write(&t, "users:1"));
        assert!(store.can_write(&t, "anything:1"));
        assert!(store.can_read(&t, "anything:1"));
        assert!(store.can_watch(&t, "anything:1"));
    }

    #[test]
    fn writer_can_write_own_namespace() {
        let store = make_store();
        let t = store.verify("writer-tok").unwrap();
        assert!(store.can_write(&t, "users:1"));
        assert!(store.can_write(&t, "orders:99"));
    }

    #[test]
    fn writer_cannot_write_foreign_namespace() {
        let store = make_store();
        let t = store.verify("writer-tok").unwrap();
        assert!(!store.can_write(&t, "payments:1"));
    }

    #[test]
    fn reader_cannot_write() {
        let store = make_store();
        let t = store.verify("reader-tok").unwrap();
        assert!(!store.can_write(&t, "users:1"));
    }

    #[test]
    fn reader_can_read_own_namespace() {
        let store = make_store();
        let t = store.verify("reader-tok").unwrap();
        assert!(store.can_read(&t, "users:1"));
    }

    #[test]
    fn reader_cannot_read_foreign_namespace() {
        let store = make_store();
        let t = store.verify("reader-tok").unwrap();
        assert!(!store.can_read(&t, "orders:1"));
    }

    #[test]
    fn reader_with_can_watch_false() {
        let store = make_store();
        let t = store.verify("reader-tok").unwrap();
        assert!(!store.can_watch(&t, "users:1"));
        assert!(!store.can_since(&t, "users:1"));
    }

    #[test]
    fn expired_token_rejected() {
        let store = make_store();
        assert!(store.verify("expired-tok").is_none());
    }

    #[test]
    fn invalid_token_rejected() {
        let store = make_store();
        assert!(store.verify("not-a-real-token").is_none());
    }

    #[test]
    fn dynamic_create_and_revoke() {
        let store = make_store();
        store.create("new-tok".to_string(), Token {
            role:       Role::Reader,
            namespaces: vec!["logs".to_string()],
            can_watch:  true,
            can_since:  true,
            expires_at: None,
            blocked:    false,
            user_id:    None,
        });
        assert!(store.verify("new-tok").is_some());
        assert!(store.revoke("new-tok"));
        assert!(store.verify("new-tok").is_none());
    }

    #[test]
    fn revoke_nonexistent_returns_false() {
        let store = make_store();
        assert!(!store.revoke("ghost-token"));
    }

    #[test]
    fn block_prevents_auth() {
        let store = make_store();
        assert!(store.verify("writer-tok").is_some());
        assert!(store.block("writer-tok"));
        assert!(store.verify("writer-tok").is_none()); // blocked → rejected
    }

    #[test]
    fn unblock_restores_auth() {
        let store = make_store();
        store.block("writer-tok");
        assert!(store.verify("writer-tok").is_none());
        store.unblock("writer-tok");
        assert!(store.verify("writer-tok").is_some()); // unblocked → works again
    }

    #[test]
    fn block_nonexistent_returns_false() {
        let store = make_store();
        assert!(!store.block("ghost-token"));
    }
}

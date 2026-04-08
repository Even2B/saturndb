use crate::core::event::Event;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

// ── Rule types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ActionType {
    #[default]
    Emit,
    Enqueue,
}

/// Optional WHERE condition on a trigger rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleCondition {
    pub field: String,
    pub op:    String,
    pub value: String,
}

impl RuleCondition {
    /// Returns true if the event payload satisfies this condition.
    pub fn matches(&self, event: &Event) -> bool {
        let field_val = match &event.payload {
            serde_json::Value::Object(map) => match map.get(&self.field) {
                Some(v) => v.clone(),
                None    => return false,
            },
            _ => return false,
        };
        eval_condition(&field_val, &self.op, &self.value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub pattern:   String,
    /// Emit (default) or Enqueue.
    #[serde(default)]
    pub action:    ActionType,
    /// Target stream for Emit action (was the only action before).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target:    Option<String>,
    /// Queue name for Enqueue action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue:     Option<String>,
    pub template:  String,
    /// Optional WHERE condition — if set, only fire when payload matches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<RuleCondition>,
}

/// What a fired rule produces.
pub enum RuleFired {
    Emit    { stream: String, payload: serde_json::Value },
    Enqueue { queue:  String, payload: serde_json::Value },
}

/// When a stream matching `pattern` is written, auto-create a token named
/// `token_tmpl` with the given `role` and `namespaces`. Template variables:
/// `$stream`, `$stream_id` (part after last `:`), `$ts`, `$id`, `$<field>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRule {
    pub pattern:    String,
    pub token_tmpl: String,
    pub role:       String,
    pub namespaces: Vec<String>,
}

#[derive(Serialize, Deserialize, Default)]
struct RulesFile {
    #[serde(default)]
    emit_rules:  Vec<Rule>,
    #[serde(default)]
    token_rules: Vec<TokenRule>,
}

// ── Engine ────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct RulesEngine {
    rules:       Arc<RwLock<Vec<Rule>>>,
    token_rules: Arc<RwLock<Vec<TokenRule>>>,
    path:        String,
}

impl RulesEngine {
    pub fn new(path: String) -> Self {
        Self {
            rules:       Arc::new(RwLock::new(Vec::new())),
            token_rules: Arc::new(RwLock::new(Vec::new())),
            path,
        }
    }

    /// Load rules from disk. Supports both new `{emit_rules, token_rules}` format
    /// and legacy `[...]` (emit rules only). Missing file is not an error.
    pub async fn load(&self) -> usize {
        if self.path.is_empty() { return 0; }
        let text = match tokio::fs::read_to_string(&self.path).await {
            Ok(t)  => t,
            Err(_) => return 0,
        };

        // Migrate legacy Rule format: `{pattern, target, template}` → new Rule with action=Emit.
        // serde handles this automatically because `action` defaults to Emit and
        // `target` is now Option<String> (Some("...") deserializes fine from old "target":"...").

        let file: RulesFile = if let Ok(f) = serde_json::from_str::<RulesFile>(&text) {
            f
        } else if let Ok(v) = serde_json::from_str::<Vec<Rule>>(&text) {
            RulesFile { emit_rules: v, token_rules: vec![] }
        } else {
            return 0;
        };
        let n = file.emit_rules.len() + file.token_rules.len();
        *self.rules.write().unwrap()       = file.emit_rules;
        *self.token_rules.write().unwrap() = file.token_rules;
        n
    }

    fn persist_bg(&self) {
        if self.path.is_empty() { return; }
        let path  = self.path.clone();
        let file  = RulesFile {
            emit_rules:  self.rules.read().unwrap().clone(),
            token_rules: self.token_rules.read().unwrap().clone(),
        };
        tokio::spawn(async move {
            if let Ok(json) = serde_json::to_string_pretty(&file) {
                if let Err(e) = tokio::fs::write(&path, json).await {
                    eprintln!("[rules] persist error: {e}");
                }
            }
        });
    }

    /// Add or replace a rule (deduped by pattern + action + target/queue).
    pub fn add(&self, rule: Rule) {
        let mut rules = self.rules.write().unwrap();
        rules.retain(|r| !same_rule(r, &rule));
        rules.push(rule);
        drop(rules);
        self.persist_bg();
    }

    /// Remove a rule by pattern + key (target stream for Emit, queue name for Enqueue).
    /// Returns true if something was removed.
    pub fn remove(&self, pattern: &str, key: &str) -> bool {
        let mut rules = self.rules.write().unwrap();
        let before = rules.len();
        rules.retain(|r| {
            if r.pattern != pattern { return true; }
            match r.action {
                ActionType::Emit    => r.target.as_deref() != Some(key),
                ActionType::Enqueue => r.queue.as_deref()  != Some(key),
            }
        });
        let removed = rules.len() < before;
        drop(rules);
        if removed { self.persist_bg(); }
        removed
    }

    pub fn list(&self) -> Vec<Rule> {
        self.rules.read().unwrap().clone()
    }

    /// Check all rules against `event`. Returns what each matched rule wants to do.
    pub fn fire(&self, event: &Event) -> Vec<RuleFired> {
        let rules = self.rules.read().unwrap();
        let mut out = Vec::new();
        for rule in rules.iter() {
            if !matches_pattern(&rule.pattern, &event.stream) { continue; }
            // Check optional WHERE condition.
            if let Some(cond) = &rule.condition {
                if !cond.matches(event) { continue; }
            }
            let body_str = interpolate(&rule.template, event);
            let payload  = serde_json::from_str::<serde_json::Value>(&body_str)
                .unwrap_or(serde_json::Value::String(body_str));
            match rule.action {
                ActionType::Emit => {
                    if let Some(target) = &rule.target {
                        let stream = interpolate(target, event);
                        out.push(RuleFired::Emit { stream, payload });
                    }
                }
                ActionType::Enqueue => {
                    if let Some(queue) = &rule.queue {
                        let queue = interpolate(queue, event);
                        out.push(RuleFired::Enqueue { queue, payload });
                    }
                }
            }
        }
        out
    }

    /// Add or replace a token rule (deduped by pattern+token_tmpl).
    pub fn add_token_rule(&self, rule: TokenRule) {
        let mut rules = self.token_rules.write().unwrap();
        rules.retain(|r| !(r.pattern == rule.pattern && r.token_tmpl == rule.token_tmpl));
        rules.push(rule);
        drop(rules);
        self.persist_bg();
    }

    /// Remove a token rule. Returns true if something was removed.
    pub fn remove_token_rule(&self, pattern: &str, token_tmpl: &str) -> bool {
        let mut rules = self.token_rules.write().unwrap();
        let before = rules.len();
        rules.retain(|r| !(r.pattern == pattern && r.token_tmpl == token_tmpl));
        let removed = rules.len() < before;
        drop(rules);
        if removed { self.persist_bg(); }
        removed
    }

    pub fn list_token_rules(&self) -> Vec<TokenRule> {
        self.token_rules.read().unwrap().clone()
    }

    /// Fire token rules against `event`. Returns (token_raw, role, namespaces) tuples
    /// for each matched rule — caller creates the tokens in the auth store.
    pub fn fire_token_rules(&self, event: &Event) -> Vec<(String, String, Vec<String>)> {
        let rules = self.token_rules.read().unwrap();
        let mut out = Vec::new();
        for rule in rules.iter() {
            if !matches_pattern(&rule.pattern, &event.stream) { continue; }
            let token_raw  = interpolate(&rule.token_tmpl, event);
            let namespaces = rule.namespaces.iter()
                .map(|ns| interpolate(ns, event))
                .collect();
            out.push((token_raw, rule.role.clone(), namespaces));
        }
        out
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn same_rule(a: &Rule, b: &Rule) -> bool {
    if a.pattern != b.pattern || a.action != b.action { return false; }
    match a.action {
        ActionType::Emit    => a.target == b.target,
        ActionType::Enqueue => a.queue  == b.queue,
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

fn interpolate(template: &str, event: &Event) -> String {
    let mut s = template.to_string();
    let stream_id = event.stream.rsplit_once(':').map(|(_, id)| id).unwrap_or(&event.stream);
    s = s.replace("$stream_id", stream_id);
    s = s.replace("$stream", &event.stream);
    s = s.replace("$ts",     &event.ts.to_string());
    s = s.replace("$id",     &event.id.to_string());
    if let serde_json::Value::Object(map) = &event.payload {
        for (key, val) in map {
            let repl = match val {
                serde_json::Value::String(v) => v.clone(),
                other                        => other.to_string(),
            };
            s = s.replace(&format!("${key}"), &repl);
        }
    }
    s
}

fn eval_condition(field_val: &serde_json::Value, op: &str, value: &str) -> bool {
    match op {
        "=" | "==" => {
            field_val.as_str().map(|s| s == value)
                .unwrap_or_else(|| field_val.to_string().trim_matches('"') == value)
        }
        "!=" | "<>" => {
            field_val.as_str().map(|s| s != value)
                .unwrap_or_else(|| field_val.to_string().trim_matches('"') != value)
        }
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

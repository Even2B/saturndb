// Consistent hash ring using FNV-1a (deterministic, no deps).
// 150 virtual nodes per real node gives <5% std-dev load imbalance.

const VNODES: usize = 150;

fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 14695981039346656037;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

#[derive(Clone)]
pub struct Ring {
    /// Sorted by hash. Each entry: (hash, index into `nodes`).
    vnodes: Vec<(u64, usize)>,
    nodes:  Vec<String>,
}

impl Ring {
    pub fn new(nodes: Vec<String>) -> Self {
        let mut vnodes: Vec<(u64, usize)> = Vec::with_capacity(nodes.len() * VNODES);
        for (i, id) in nodes.iter().enumerate() {
            for v in 0..VNODES {
                let key  = format!("{id}#{v}");
                vnodes.push((fnv1a(&key), i));
            }
        }
        vnodes.sort_unstable_by_key(|(h, _)| *h);
        vnodes.dedup_by_key(|(h, _)| *h);
        Self { vnodes, nodes }
    }

    /// Primary node for a stream (O(log n) binary search).
    pub fn primary(&self, stream: &str) -> Option<&str> {
        self.walk(stream, 1).into_iter().next().map(|s| s.as_str())
    }

    /// Primary + (factor-1) distinct replica nodes.
    pub fn replicas(&self, stream: &str, factor: usize) -> Vec<&str> {
        self.walk(stream, factor).iter().map(|s| s.as_str()).collect()
    }

    pub fn is_local(&self, stream: &str, local_id: &str) -> bool {
        self.primary(stream).map(|id| id == local_id).unwrap_or(true)
    }

    /// Walk the ring clockwise from the stream's hash, collecting up to `n`
    /// distinct real nodes.
    fn walk(&self, stream: &str, n: usize) -> Vec<&String> {
        if self.vnodes.is_empty() { return vec![]; }
        let hash = fnv1a(stream);
        let start = self.vnodes.partition_point(|(h, _)| *h < hash);

        let mut seen:   Vec<bool>     = vec![false; self.nodes.len()];
        let mut result: Vec<&String>  = Vec::with_capacity(n);
        let total = self.vnodes.len();

        for i in 0..total {
            let (_, node_idx) = self.vnodes[(start + i) % total];
            if !seen[node_idx] {
                seen[node_idx] = true;
                result.push(&self.nodes[node_idx]);
                if result.len() == n { break; }
            }
        }
        result
    }

    pub fn nodes(&self) -> &[String] { &self.nodes }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_always_primary() {
        let ring = Ring::new(vec!["node-1".into()]);
        assert_eq!(ring.primary("users:1"), Some("node-1"));
        assert_eq!(ring.primary("orders:99"), Some("node-1"));
    }

    #[test]
    fn three_nodes_distribute_streams() {
        let ring = Ring::new(vec!["node-1".into(), "node-2".into(), "node-3".into()]);
        let mut counts = std::collections::HashMap::new();
        for i in 0..1000 {
            let s = format!("users:{i}:data");
            *counts.entry(ring.primary(&s).unwrap().to_string()).or_insert(0usize) += 1;
        }
        // All three nodes must own at least some streams (basic routing correctness).
        assert_eq!(counts.len(), 3, "all nodes should own at least 1 stream: {counts:?}");
        // No single node should dominate the ring (own >90%).
        for (node, count) in &counts {
            assert!(*count < 900, "node {node} dominates ring: {count}/1000");
        }
    }

    #[test]
    fn replicas_returns_distinct_nodes() {
        let ring = Ring::new(vec!["a".into(), "b".into(), "c".into()]);
        let replicas = ring.replicas("users:1", 3);
        assert_eq!(replicas.len(), 3);
        let unique: std::collections::HashSet<_> = replicas.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn is_local_correct() {
        let ring = Ring::new(vec!["node-1".into(), "node-2".into()]);
        let primary = ring.primary("test:stream").unwrap().to_string();
        let other   = if primary == "node-1" { "node-2" } else { "node-1" };
        assert!( ring.is_local("test:stream", &primary));
        assert!(!ring.is_local("test:stream", other));
    }
}

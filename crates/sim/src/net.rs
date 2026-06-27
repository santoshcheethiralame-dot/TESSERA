use std::collections::BTreeSet;

use crate::time::{millis, Duration};
use crate::NodeId;

#[derive(Clone)]
pub struct NetworkConfig {
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub drop_prob: f64,
    pub duplicate_prob: f64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            min_latency: millis(1),
            max_latency: millis(10),
            drop_prob: 0.0,
            duplicate_prob: 0.0,
        }
    }
}

#[derive(Clone, Default)]
pub struct Partitions {
    blocked: BTreeSet<(NodeId, NodeId)>,
}

impl Partitions {
    pub fn reachable(&self, from: NodeId, to: NodeId) -> bool {
        !self.blocked.contains(&(from, to))
    }

    pub fn cut(&mut self, a: NodeId, b: NodeId) {
        self.blocked.insert((a, b));
        self.blocked.insert((b, a));
    }

    pub fn heal_all(&mut self) {
        self.blocked.clear();
    }
}

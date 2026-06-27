use sim::NodeId;

pub fn shard_for(key: &[u8], num_shards: usize) -> usize {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in key {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % num_shards as u64) as usize
}

fn placement(num_shards: usize, replicas: usize) -> Vec<Vec<NodeId>> {
    (0..num_shards)
        .map(|shard| (0..replicas).map(|r| shard * replicas + r).collect())
        .collect()
}

pub struct Coordinator {
    shards: Vec<Vec<NodeId>>,
}

impl Coordinator {
    pub fn new(num_shards: usize, replicas: usize) -> Self {
        Coordinator {
            shards: placement(num_shards, replicas),
        }
    }

    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    pub fn replicas_of(&self, shard: usize) -> &[NodeId] {
        &self.shards[shard]
    }

    pub fn server_ids(&self) -> Vec<NodeId> {
        self.shards.iter().flatten().copied().collect()
    }

    pub fn router(&self) -> Router {
        Router::new(self.shards.clone())
    }
}

pub struct Router {
    shards: Vec<Vec<NodeId>>,
    leader: Vec<Option<NodeId>>,
    cursor: Vec<usize>,
}

impl Router {
    fn new(shards: Vec<Vec<NodeId>>) -> Self {
        let n = shards.len();
        Router {
            shards,
            leader: vec![None; n],
            cursor: vec![0; n],
        }
    }

    pub fn shard_for(&self, key: &[u8]) -> usize {
        shard_for(key, self.shards.len())
    }

    pub fn target(&self, shard: usize) -> NodeId {
        match self.leader[shard] {
            Some(node) => node,
            None => self.shards[shard][self.cursor[shard]],
        }
    }

    pub fn note_leader(&mut self, shard: usize, node: NodeId) {
        if self.shards[shard].contains(&node) {
            self.leader[shard] = Some(node);
        }
    }

    pub fn redirect(&mut self, shard: usize, hint: Option<NodeId>) {
        match hint {
            Some(node) if self.shards[shard].contains(&node) => self.leader[shard] = Some(node),
            _ => self.rotate(shard),
        }
    }

    pub fn rotate(&mut self, shard: usize) {
        self.leader[shard] = None;
        self.cursor[shard] = (self.cursor[shard] + 1) % self.shards[shard].len();
    }
}

use cluster::{shard_for, Coordinator, Router};
use consensus::{
    decode_value, encode_delete, encode_get, encode_put, ClientResult, KvStore, Message, Raft,
};
use lincheck::{linearizable, Op, OpKind};
use sim::{millis, secs, Io, NodeId, Process, Rng, Simulator, TimerId};

const RETRY: TimerId = 0;
const KEYSPACE: u64 = 12;

enum PendingKind {
    Put(Vec<u8>),
    Delete,
    Get,
}

struct Pending {
    request_id: u64,
    shard: usize,
    invoke: u64,
    key: Vec<u8>,
    kind: PendingKind,
}

struct ShardedClient {
    router: Router,
    ops_remaining: usize,
    next_request_id: u64,
    pending: Option<Pending>,
    history: Vec<Op>,
    rng: Rng,
}

impl ShardedClient {
    fn new(router: Router, ops: usize, seed: u64) -> Self {
        ShardedClient {
            router,
            ops_remaining: ops,
            next_request_id: 1,
            pending: None,
            history: Vec::new(),
            rng: Rng::new(seed),
        }
    }

    fn history(&self) -> &[Op] {
        &self.history
    }

    fn issue(&mut self, io: &mut Io<Message>) {
        if self.ops_remaining == 0 {
            return;
        }
        let key = format!("k{}", self.rng.gen_range(0, KEYSPACE)).into_bytes();
        let shard = self.router.shard_for(&key);
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let kind = match self.rng.gen_range(0, 3) {
            0 => PendingKind::Put(format!("v{}", self.rng.gen_range(0, 1_000_000)).into_bytes()),
            1 => PendingKind::Get,
            _ => PendingKind::Delete,
        };
        self.pending = Some(Pending {
            request_id,
            shard,
            invoke: io.now().as_nanos(),
            key,
            kind,
        });
        self.send(io);
    }

    fn send(&mut self, io: &mut Io<Message>) {
        let Some(pending) = self.pending.as_ref() else {
            return;
        };
        let command = match &pending.kind {
            PendingKind::Put(value) => encode_put(&pending.key, value),
            PendingKind::Delete => encode_delete(&pending.key),
            PendingKind::Get => encode_get(&pending.key),
        };
        let target = self.router.target(pending.shard);
        let request_id = pending.request_id;
        io.send(
            target,
            Message::ClientRequest {
                request_id,
                command,
            },
        );
        io.set_timer(RETRY, millis(200));
    }
}

impl Process for ShardedClient {
    type Message = Message;

    fn on_start(&mut self, io: &mut Io<Message>) {
        self.issue(io);
    }

    fn on_timer(&mut self, _timer: TimerId, io: &mut Io<Message>) {
        if let Some(shard) = self.pending.as_ref().map(|p| p.shard) {
            self.router.rotate(shard);
            self.send(io);
        }
    }

    fn on_message(&mut self, from: NodeId, msg: Message, io: &mut Io<Message>) {
        let Message::ClientReply { request_id, result } = msg else {
            return;
        };
        let Some(pending) = self.pending.as_ref() else {
            return;
        };
        if pending.request_id != request_id {
            return;
        }
        let shard = pending.shard;
        match result {
            ClientResult::Ok(response) => {
                let kind = match &pending.kind {
                    PendingKind::Put(value) => OpKind::Put(value.clone()),
                    PendingKind::Delete => OpKind::Delete,
                    PendingKind::Get => OpKind::Get(decode_value(&response)),
                };
                self.history.push(Op {
                    key: pending.key.clone(),
                    kind,
                    invoke: pending.invoke,
                    response: io.now().as_nanos(),
                });
                self.router.note_leader(shard, from);
                self.pending = None;
                self.ops_remaining -= 1;
                io.cancel_timer(RETRY);
                self.issue(io);
            }
            ClientResult::NotLeader(hint) => {
                self.router.redirect(shard, hint);
                self.send(io);
            }
        }
    }
}

enum Node {
    Server(Raft<KvStore>),
    Client(ShardedClient),
}

impl Process for Node {
    type Message = Message;

    fn on_start(&mut self, io: &mut Io<Message>) {
        match self {
            Node::Server(server) => server.on_start(io),
            Node::Client(client) => client.on_start(io),
        }
    }

    fn on_message(&mut self, from: NodeId, msg: Message, io: &mut Io<Message>) {
        match self {
            Node::Server(server) => server.on_message(from, msg, io),
            Node::Client(client) => client.on_message(from, msg, io),
        }
    }

    fn on_timer(&mut self, timer: TimerId, io: &mut Io<Message>) {
        match self {
            Node::Server(server) => server.on_timer(timer, io),
            Node::Client(client) => client.on_timer(timer, io),
        }
    }
}

fn build(
    num_shards: usize,
    replicas: usize,
    clients: usize,
    ops: usize,
    seed: u64,
) -> (Simulator<Node>, Coordinator) {
    let coord = Coordinator::new(num_shards, replicas);
    let mut nodes: Vec<Node> = Vec::new();
    for shard in 0..num_shards {
        let members = coord.replicas_of(shard).to_vec();
        for &id in &members {
            nodes.push(Node::Server(Raft::new(id, &members, KvStore::new())));
        }
    }
    for client in 0..clients {
        nodes.push(Node::Client(ShardedClient::new(
            coord.router(),
            ops,
            seed ^ (0x100 + client as u64),
        )));
    }
    let sim = Simulator::new(seed, nodes);
    (sim, coord)
}

fn is_leader(sim: &Simulator<Node>, id: NodeId) -> bool {
    matches!(sim.process(id), Node::Server(server) if server.is_leader())
}

fn history(sim: &Simulator<Node>, base: usize, clients: usize) -> Vec<Op> {
    let mut out = Vec::new();
    for i in base..base + clients {
        if let Node::Client(client) = sim.process(i) {
            out.extend(client.history().iter().cloned());
        }
    }
    out
}

#[test]
fn each_shard_elects_one_leader() {
    let (mut sim, coord) = build(3, 3, 0, 0, 1);
    sim.run_for(secs(3));
    for shard in 0..coord.num_shards() {
        let leaders = coord
            .replicas_of(shard)
            .iter()
            .filter(|&&id| is_leader(&sim, id))
            .count();
        assert_eq!(leaders, 1, "shard {shard} should have exactly one leader");
    }
}

#[test]
fn routes_across_shards_and_stays_linearizable() {
    let (mut sim, coord) = build(3, 3, 2, 30, 2);
    sim.run_for(secs(30));

    let base = coord.server_ids().len();
    let h = history(&sim, base, 2);
    assert!(!h.is_empty());

    let shards_used: std::collections::BTreeSet<usize> = h
        .iter()
        .map(|op| shard_for(&op.key, coord.num_shards()))
        .collect();
    assert!(
        shards_used.len() > 1,
        "operations should span multiple shards"
    );

    assert!(linearizable(&h));
}

#[test]
fn linearizable_under_per_shard_partitions() {
    let (mut sim, coord) = build(3, 3, 3, 15, 3);
    let mut nemesis = Rng::new(3 ^ 0x0000_feed);
    for _ in 0..10 {
        for shard in 0..coord.num_shards() {
            let replicas = coord.replicas_of(shard);
            let victim = replicas[nemesis.gen_range(0, replicas.len() as u64) as usize];
            sim.partitions_mut().isolate(victim, replicas);
        }
        sim.run_for(millis(400));
        sim.partitions_mut().heal_all();
        sim.run_for(millis(400));
    }
    sim.run_for(secs(30));

    let base = coord.server_ids().len();
    let h = history(&sim, base, 3);
    assert!(linearizable(&h), "sharded history must be linearizable");
}

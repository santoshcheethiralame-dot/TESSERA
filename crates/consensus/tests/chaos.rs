use consensus::{
    decode_value, encode_delete, encode_get, encode_put, ClientResult, KvStore, Message, Raft,
};
use lincheck::{linearizable, Op, OpKind};
use sim::{millis, secs, Io, NetworkConfig, NodeId, Process, Rng, Simulator, TimerId};

const RETRY: TimerId = 0;

enum PendingKind {
    Put(Vec<u8>),
    Delete,
    Get,
}

struct Pending {
    request_id: u64,
    invoke: u64,
    key: Vec<u8>,
    kind: PendingKind,
}

struct Client {
    servers: Vec<NodeId>,
    ops_remaining: usize,
    next_request_id: u64,
    target: usize,
    pending: Option<Pending>,
    history: Vec<Op>,
    rng: Rng,
}

impl Client {
    fn new(servers: Vec<NodeId>, ops: usize, seed: u64) -> Self {
        Client {
            servers,
            ops_remaining: ops,
            next_request_id: 1,
            target: 0,
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
        let key = format!("k{}", self.rng.gen_range(0, 4)).into_bytes();
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let kind = match self.rng.gen_range(0, 3) {
            0 => PendingKind::Put(format!("v{}", self.rng.gen_range(0, 1_000_000)).into_bytes()),
            1 => PendingKind::Get,
            _ => PendingKind::Delete,
        };
        self.pending = Some(Pending {
            request_id,
            invoke: io.now().as_nanos(),
            key,
            kind,
        });
        self.send_pending(io);
    }

    fn send_pending(&mut self, io: &mut Io<Message>) {
        let Some(pending) = self.pending.as_ref() else {
            return;
        };
        let command = match &pending.kind {
            PendingKind::Put(value) => encode_put(&pending.key, value),
            PendingKind::Delete => encode_delete(&pending.key),
            PendingKind::Get => encode_get(&pending.key),
        };
        let request_id = pending.request_id;
        let server = self.servers[self.target];
        io.send(
            server,
            Message::ClientRequest {
                request_id,
                command,
            },
        );
        io.set_timer(RETRY, millis(200));
    }

    fn rotate(&mut self) {
        self.target = (self.target + 1) % self.servers.len();
    }
}

impl Process for Client {
    type Message = Message;

    fn on_start(&mut self, io: &mut Io<Message>) {
        self.issue(io);
    }

    fn on_timer(&mut self, _timer: TimerId, io: &mut Io<Message>) {
        self.rotate();
        self.send_pending(io);
    }

    fn on_message(&mut self, _from: NodeId, msg: Message, io: &mut Io<Message>) {
        let Message::ClientReply { request_id, result } = msg else {
            return;
        };
        let Some(pending) = self.pending.as_ref() else {
            return;
        };
        if pending.request_id != request_id {
            return;
        }
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
                self.pending = None;
                self.ops_remaining -= 1;
                io.cancel_timer(RETRY);
                self.issue(io);
            }
            ClientResult::NotLeader(hint) => {
                match hint.and_then(|h| self.servers.iter().position(|&s| s == h)) {
                    Some(idx) => self.target = idx,
                    None => self.rotate(),
                }
                self.send_pending(io);
            }
        }
    }
}

enum Node {
    Server(Box<Raft<KvStore>>),
    Client(Client),
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

fn build(servers: usize, clients: usize, ops_each: usize, seed: u64) -> Simulator<Node> {
    let server_ids: Vec<usize> = (0..servers).collect();
    let mut nodes: Vec<Node> = server_ids
        .iter()
        .map(|&id| Node::Server(Box::new(Raft::new(id, &server_ids, KvStore::new()))))
        .collect();
    for client in 0..clients {
        let client_seed = seed ^ (0x100 + client as u64);
        nodes.push(Node::Client(Client::new(
            server_ids.clone(),
            ops_each,
            client_seed,
        )));
    }
    Simulator::new(seed, nodes)
}

fn history(sim: &Simulator<Node>, servers: usize, clients: usize) -> Vec<Op> {
    let mut out = Vec::new();
    for i in servers..servers + clients {
        if let Node::Client(client) = sim.process(i) {
            out.extend(client.history().iter().cloned());
        }
    }
    out
}

#[test]
fn linearizable_without_faults() {
    let mut sim = build(3, 2, 20, 1);
    sim.run_for(secs(30));
    let h = history(&sim, 3, 2);
    assert!(!h.is_empty());
    assert!(linearizable(&h));
}

#[test]
fn linearizable_under_partitions_and_loss() {
    for seed in 1..=20u64 {
        let mut sim = build(5, 3, 12, seed);
        sim.set_network(NetworkConfig {
            min_latency: millis(1),
            max_latency: millis(20),
            drop_prob: 0.05,
            duplicate_prob: 0.02,
        });
        let mut nemesis = Rng::new(seed ^ 0x9e37_79b9);
        for _ in 0..12 {
            let pivot = nemesis.gen_range(1, 5) as usize;
            let left: Vec<usize> = (0..pivot).collect();
            let right: Vec<usize> = (pivot..5).collect();
            sim.partitions_mut().split(&[left, right]);
            sim.run_for(millis(400));
            sim.partitions_mut().heal_all();
            sim.run_for(millis(400));
        }
        sim.run_for(secs(60));
        let h = history(&sim, 5, 3);
        assert!(linearizable(&h), "non-linearizable history at seed {seed}");
    }
}

#[test]
fn checker_catches_an_injected_violation() {
    let mut sim = build(3, 2, 20, 7);
    sim.run_for(secs(30));
    let mut h = history(&sim, 3, 2);
    assert!(linearizable(&h));
    h.push(Op {
        key: b"k0".to_vec(),
        kind: OpKind::Get(Some(b"never-written".to_vec())),
        invoke: 0,
        response: 1,
    });
    assert!(!linearizable(&h));
}

#[test]
#[ignore = "deep fuzz: cargo test -- --ignored"]
fn stress_linearizable_many_seeds() {
    for seed in 0..1000u64 {
        let mut sim = build(5, 3, 10, seed);
        sim.set_network(NetworkConfig {
            min_latency: millis(1),
            max_latency: millis(15),
            drop_prob: 0.05,
            duplicate_prob: 0.03,
        });
        let mut nemesis = Rng::new(seed ^ 0x00c0_ffee);
        for _ in 0..10 {
            let pivot = nemesis.gen_range(1, 5) as usize;
            let left: Vec<usize> = (0..pivot).collect();
            let right: Vec<usize> = (pivot..5).collect();
            sim.partitions_mut().split(&[left, right]);
            sim.run_for(millis(300));
            sim.partitions_mut().heal_all();
            sim.run_for(millis(300));
        }
        sim.run_for(secs(40));
        let h = history(&sim, 5, 3);
        assert!(linearizable(&h), "non-linearizable at seed {seed}");
    }
}

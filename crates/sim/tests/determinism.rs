use sim::{millis, secs, Io, NetworkConfig, Process, Simulator};

const TARGET: u64 = 20;
const RETRANSMIT: u64 = 0;

#[derive(Clone)]
enum Msg {
    Ping(u64),
    Pong(u64),
}

#[derive(Clone, Copy, PartialEq)]
enum Role {
    Pinger,
    Ponger,
}

struct Node {
    role: Role,
    peer: usize,
    received: u64,
    outstanding: Option<u64>,
}

impl Node {
    fn pinger(peer: usize) -> Self {
        Node {
            role: Role::Pinger,
            peer,
            received: 0,
            outstanding: None,
        }
    }

    fn ponger(peer: usize) -> Self {
        Node {
            role: Role::Ponger,
            peer,
            received: 0,
            outstanding: None,
        }
    }

    fn send_ping(&mut self, io: &mut Io<Msg>, n: u64) {
        self.outstanding = Some(n);
        io.send(self.peer, Msg::Ping(n));
        io.set_timer(RETRANSMIT, millis(50));
    }
}

impl Process for Node {
    type Message = Msg;

    fn on_start(&mut self, io: &mut Io<Msg>) {
        if self.role == Role::Pinger {
            self.send_ping(io, 0);
        }
    }

    fn on_message(&mut self, from: usize, msg: Msg, io: &mut Io<Msg>) {
        match (self.role, msg) {
            (Role::Ponger, Msg::Ping(n)) => io.send(from, Msg::Pong(n)),
            (Role::Pinger, Msg::Pong(n)) if self.outstanding == Some(n) => {
                self.received += 1;
                self.outstanding = None;
                io.cancel_timer(RETRANSMIT);
                if self.received < TARGET {
                    self.send_ping(io, n + 1);
                }
            }
            _ => {}
        }
    }

    fn on_timer(&mut self, _timer: u64, io: &mut Io<Msg>) {
        if self.role == Role::Pinger {
            if let Some(n) = self.outstanding {
                io.send(self.peer, Msg::Ping(n));
                io.set_timer(RETRANSMIT, millis(50));
            }
        }
    }
}

fn lossy() -> NetworkConfig {
    NetworkConfig {
        min_latency: millis(1),
        max_latency: millis(10),
        drop_prob: 0.3,
        duplicate_prob: 0.05,
    }
}

fn build(seed: u64, net: NetworkConfig) -> Simulator<Node> {
    let mut sim = Simulator::new(seed, vec![Node::pinger(1), Node::ponger(0)]);
    sim.set_network(net);
    sim
}

#[test]
fn same_seed_is_bit_for_bit_identical() {
    let mut a = build(0xdead_beef, lossy());
    let mut b = build(0xdead_beef, lossy());
    a.run_for(secs(120));
    b.run_for(secs(120));
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.process(0).received, b.process(0).received);
    assert_eq!(a.stats().dropped, b.stats().dropped);
}

#[test]
fn different_seeds_diverge() {
    let mut a = build(1, lossy());
    let mut b = build(2, lossy());
    a.run_for(secs(120));
    b.run_for(secs(120));
    assert_ne!(a.digest(), b.digest());
}

#[test]
fn completes_despite_loss_and_duplication() {
    let mut sim = build(0xc0ffee, lossy());
    sim.run_for(secs(120));
    assert_eq!(sim.process(0).received, TARGET);
    assert!(sim.stats().dropped > 0);
    assert!(sim.stats().duplicated > 0);
}

#[test]
fn partition_halts_then_recovers() {
    let net = NetworkConfig {
        drop_prob: 0.0,
        ..NetworkConfig::default()
    };
    let mut sim = build(7, net);

    sim.partitions_mut().cut(0, 1);
    sim.run_for(secs(10));
    assert_eq!(sim.process(0).received, 0);
    assert!(sim.stats().dropped > 0);
    assert_eq!(sim.stats().delivered, 0);

    sim.partitions_mut().heal_all();
    sim.run_for(secs(120));
    assert_eq!(sim.process(0).received, TARGET);
}

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};

use crate::net::{NetworkConfig, Partitions};
use crate::rng::Rng;
use crate::time::{Duration, Time};
use crate::{NodeId, TimerId};

pub enum Action<M> {
    Send { to: NodeId, msg: M },
    SetTimer { id: TimerId, after: Duration },
    CancelTimer { id: TimerId },
}

pub struct Io<'a, M> {
    me: NodeId,
    now: Time,
    rng: &'a mut Rng,
    actions: Vec<Action<M>>,
}

impl<'a, M> Io<'a, M> {
    pub fn me(&self) -> NodeId {
        self.me
    }

    pub fn now(&self) -> Time {
        self.now
    }

    pub fn send(&mut self, to: NodeId, msg: M) {
        self.actions.push(Action::Send { to, msg });
    }

    pub fn set_timer(&mut self, id: TimerId, after: Duration) {
        self.actions.push(Action::SetTimer { id, after });
    }

    pub fn cancel_timer(&mut self, id: TimerId) {
        self.actions.push(Action::CancelTimer { id });
    }

    pub fn gen_range(&mut self, lo: u64, hi: u64) -> u64 {
        self.rng.gen_range(lo, hi)
    }

    pub fn gen_bool(&mut self, p: f64) -> bool {
        self.rng.gen_bool(p)
    }

    pub fn new(me: NodeId, now: Time, rng: &'a mut Rng) -> Self {
        Io {
            me,
            now,
            rng,
            actions: Vec::new(),
        }
    }

    pub fn into_actions(self) -> Vec<Action<M>> {
        self.actions
    }
}

pub trait Process {
    type Message: Clone;

    fn on_start(&mut self, _io: &mut Io<Self::Message>) {}

    fn on_message(&mut self, from: NodeId, msg: Self::Message, io: &mut Io<Self::Message>);

    fn on_timer(&mut self, _timer: TimerId, _io: &mut Io<Self::Message>) {}

    fn reboot(&mut self, _io: &mut Io<Self::Message>) {}
}

enum Wake<M> {
    Start {
        node: NodeId,
    },
    Deliver {
        to: NodeId,
        from: NodeId,
        msg: M,
    },
    Timer {
        node: NodeId,
        id: TimerId,
        token: u64,
    },
}

struct Entry<M> {
    at: Time,
    seq: u64,
    wake: Wake<M>,
}

impl<M> PartialEq for Entry<M> {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}

impl<M> Eq for Entry<M> {}

impl<M> PartialOrd for Entry<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<M> Ord for Entry<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at.cmp(&other.at).then(self.seq.cmp(&other.seq))
    }
}

enum Cb<M> {
    Start,
    Msg(NodeId, M),
    Timer(TimerId),
    Reboot,
}

#[derive(Clone, Default, Debug)]
pub struct Stats {
    pub delivered: u64,
    pub dropped: u64,
    pub duplicated: u64,
    pub timers_fired: u64,
    pub events: u64,
}

pub struct Simulator<P: Process> {
    clock: Time,
    seq: u64,
    rng: Rng,
    net: NetworkConfig,
    partitions: Partitions,
    processes: Vec<P>,
    node_rngs: Vec<Rng>,
    timers: Vec<BTreeMap<TimerId, u64>>,
    queue: BinaryHeap<Reverse<Entry<P::Message>>>,
    stats: Stats,
    digest: u64,
}

impl<P: Process> Simulator<P> {
    pub fn new(seed: u64, processes: Vec<P>) -> Self {
        let n = processes.len();
        let mut seeder = Rng::new(seed);
        let rng = seeder.fork(0x5151_5151_5151_5151);
        let node_rngs = (0..n).map(|i| seeder.fork(i as u64 + 1)).collect();
        let timers = (0..n).map(|_| BTreeMap::new()).collect();
        let mut sim = Simulator {
            clock: Time::ZERO,
            seq: 0,
            rng,
            net: NetworkConfig::default(),
            partitions: Partitions::default(),
            processes,
            node_rngs,
            timers,
            queue: BinaryHeap::new(),
            stats: Stats::default(),
            digest: 0xcbf2_9ce4_8422_2325,
        };
        for node in 0..n {
            let seq = sim.next_seq();
            sim.queue.push(Reverse(Entry {
                at: Time::ZERO,
                seq,
                wake: Wake::Start { node },
            }));
        }
        sim
    }

    pub fn set_network(&mut self, net: NetworkConfig) {
        self.net = net;
    }

    pub fn partitions_mut(&mut self) -> &mut Partitions {
        &mut self.partitions
    }

    pub fn inject(&mut self, to: NodeId, msg: P::Message) {
        let seq = self.next_seq();
        self.queue.push(Reverse(Entry {
            at: self.clock,
            seq,
            wake: Wake::Deliver {
                to,
                from: usize::MAX,
                msg,
            },
        }));
    }

    pub fn reboot(&mut self, node: NodeId) {
        let actions = self.callback(node, Cb::Reboot);
        self.apply(node, actions);
    }

    pub fn now(&self) -> Time {
        self.clock
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    pub fn digest(&self) -> u64 {
        self.digest
    }

    pub fn nodes(&self) -> usize {
        self.processes.len()
    }

    pub fn process(&self, id: NodeId) -> &P {
        &self.processes[id]
    }

    pub fn process_mut(&mut self, id: NodeId) -> &mut P {
        &mut self.processes[id]
    }

    pub fn run(&mut self) {
        self.run_until(Time(u64::MAX));
    }

    pub fn run_for(&mut self, span: Duration) {
        let deadline = self.clock + span;
        self.run_until(deadline);
    }

    pub fn run_until(&mut self, deadline: Time) {
        while let Some(Reverse(entry)) = self.queue.peek() {
            if entry.at > deadline {
                break;
            }
            let Reverse(entry) = self.queue.pop().unwrap();
            self.clock = entry.at;
            match entry.wake {
                Wake::Start { node } => {
                    self.mix(entry.at, node, 1);
                    let actions = self.callback(node, Cb::Start);
                    self.apply(node, actions);
                }
                Wake::Deliver { to, from, msg } => {
                    self.mix(entry.at, to, 2);
                    let actions = self.callback(to, Cb::Msg(from, msg));
                    self.apply(to, actions);
                }
                Wake::Timer { node, id, token } => {
                    if self.timers[node].get(&id) == Some(&token) {
                        self.timers[node].remove(&id);
                        self.stats.timers_fired += 1;
                        self.mix(entry.at, node, 3);
                        let actions = self.callback(node, Cb::Timer(id));
                        self.apply(node, actions);
                    }
                }
            }
        }
        self.clock = self.clock.max(deadline);
    }

    fn next_seq(&mut self) -> u64 {
        let s = self.seq;
        self.seq += 1;
        s
    }

    fn mix(&mut self, at: Time, node: NodeId, kind: u64) {
        self.stats.events += 1;
        let mut h = self.digest;
        for v in [at.0, node as u64, kind] {
            h ^= v;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.digest = h;
    }

    fn callback(&mut self, node: NodeId, cb: Cb<P::Message>) -> Vec<Action<P::Message>> {
        let now = self.clock;
        let mut io = Io {
            me: node,
            now,
            rng: &mut self.node_rngs[node],
            actions: Vec::new(),
        };
        match cb {
            Cb::Start => self.processes[node].on_start(&mut io),
            Cb::Msg(from, msg) => self.processes[node].on_message(from, msg, &mut io),
            Cb::Timer(id) => self.processes[node].on_timer(id, &mut io),
            Cb::Reboot => self.processes[node].reboot(&mut io),
        }
        io.actions
    }

    fn apply(&mut self, node: NodeId, actions: Vec<Action<P::Message>>) {
        for action in actions {
            match action {
                Action::Send { to, msg } => self.route(node, to, msg),
                Action::SetTimer { id, after } => {
                    let seq = self.next_seq();
                    self.timers[node].insert(id, seq);
                    let at = self.clock + after;
                    self.queue.push(Reverse(Entry {
                        at,
                        seq,
                        wake: Wake::Timer {
                            node,
                            id,
                            token: seq,
                        },
                    }));
                }
                Action::CancelTimer { id } => {
                    self.timers[node].remove(&id);
                }
            }
        }
    }

    fn route(&mut self, from: NodeId, to: NodeId, msg: P::Message) {
        if to >= self.processes.len() {
            return;
        }
        if !self.partitions.reachable(from, to) {
            self.stats.dropped += 1;
            return;
        }
        if self.rng.gen_bool(self.net.drop_prob) {
            self.stats.dropped += 1;
            return;
        }
        let span = self.sample_latency();
        let duplicate = self.rng.gen_bool(self.net.duplicate_prob);
        let seq = self.next_seq();
        let at = self.clock + span;
        self.stats.delivered += 1;
        if duplicate {
            let span2 = self.sample_latency();
            let seq2 = self.next_seq();
            let at2 = self.clock + span2;
            self.queue.push(Reverse(Entry {
                at,
                seq,
                wake: Wake::Deliver {
                    to,
                    from,
                    msg: msg.clone(),
                },
            }));
            self.queue.push(Reverse(Entry {
                at: at2,
                seq: seq2,
                wake: Wake::Deliver { to, from, msg },
            }));
            self.stats.duplicated += 1;
        } else {
            self.queue.push(Reverse(Entry {
                at,
                seq,
                wake: Wake::Deliver { to, from, msg },
            }));
        }
    }

    fn sample_latency(&mut self) -> Duration {
        let lo = self.net.min_latency.0;
        let hi = self.net.max_latency.0;
        if hi <= lo {
            Duration(lo)
        } else {
            Duration(self.rng.gen_range(lo, hi + 1))
        }
    }
}

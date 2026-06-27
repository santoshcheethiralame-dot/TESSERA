use sim::{millis, Io, NodeId, Process, TimerId};

type Term = u64;

const ELECTION_TIMER: TimerId = 0;
const HEARTBEAT_TIMER: TimerId = 1;
const HEARTBEAT_INTERVAL_MS: u64 = 50;
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_MAX_MS: u64 = 300;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone)]
pub enum Message {
    RequestVote {
        term: Term,
        candidate: NodeId,
        last_log_index: usize,
        last_log_term: Term,
    },
    RequestVoteReply {
        term: Term,
        granted: bool,
    },
    AppendEntries {
        term: Term,
        leader: NodeId,
    },
    AppendEntriesReply {
        term: Term,
    },
}

pub struct Raft {
    id: NodeId,
    peers: Vec<NodeId>,
    current_term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    role: Role,
    votes: usize,
    last_log_index: usize,
    last_log_term: Term,
}

impl Raft {
    pub fn new(id: NodeId, cluster: &[NodeId]) -> Self {
        let peers = cluster.iter().copied().filter(|&node| node != id).collect();
        Raft {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            role: Role::Follower,
            votes: 0,
            last_log_index: 0,
            last_log_term: 0,
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader_id
    }

    fn majority(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    fn reset_election_timer(&self, io: &mut Io<Message>) {
        let span = io.gen_range(ELECTION_MIN_MS, ELECTION_MAX_MS);
        io.set_timer(ELECTION_TIMER, millis(span));
    }

    fn step_down(&mut self, term: Term, io: &mut Io<Message>) {
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.votes = 0;
        self.reset_election_timer(io);
    }

    fn start_election(&mut self, io: &mut Io<Message>) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes = 1;
        self.reset_election_timer(io);
        for &peer in &self.peers {
            io.send(
                peer,
                Message::RequestVote {
                    term: self.current_term,
                    candidate: self.id,
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                },
            );
        }
        if self.votes >= self.majority() {
            self.become_leader(io);
        }
    }

    fn become_leader(&mut self, io: &mut Io<Message>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.broadcast_heartbeats(io);
    }

    fn broadcast_heartbeats(&mut self, io: &mut Io<Message>) {
        for &peer in &self.peers {
            io.send(
                peer,
                Message::AppendEntries {
                    term: self.current_term,
                    leader: self.id,
                },
            );
        }
        io.set_timer(HEARTBEAT_TIMER, millis(HEARTBEAT_INTERVAL_MS));
    }

    fn up_to_date(&self, last_log_index: usize, last_log_term: Term) -> bool {
        last_log_term > self.last_log_term
            || (last_log_term == self.last_log_term && last_log_index >= self.last_log_index)
    }
}

impl Process for Raft {
    type Message = Message;

    fn on_start(&mut self, io: &mut Io<Message>) {
        self.reset_election_timer(io);
    }

    fn on_timer(&mut self, timer: TimerId, io: &mut Io<Message>) {
        match timer {
            ELECTION_TIMER if self.role != Role::Leader => self.start_election(io),
            HEARTBEAT_TIMER if self.role == Role::Leader => self.broadcast_heartbeats(io),
            _ => {}
        }
    }

    fn on_message(&mut self, from: NodeId, msg: Message, io: &mut Io<Message>) {
        match msg {
            Message::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => {
                if term > self.current_term {
                    self.step_down(term, io);
                }
                let granted = term == self.current_term
                    && (self.voted_for.is_none() || self.voted_for == Some(candidate))
                    && self.up_to_date(last_log_index, last_log_term);
                if granted {
                    self.voted_for = Some(candidate);
                    self.reset_election_timer(io);
                }
                io.send(
                    from,
                    Message::RequestVoteReply {
                        term: self.current_term,
                        granted,
                    },
                );
            }
            Message::RequestVoteReply { term, granted } => {
                if term > self.current_term {
                    self.step_down(term, io);
                    return;
                }
                if self.role == Role::Candidate && term == self.current_term && granted {
                    self.votes += 1;
                    if self.votes >= self.majority() {
                        self.become_leader(io);
                    }
                }
            }
            Message::AppendEntries { term, leader } => {
                if term < self.current_term {
                    io.send(
                        from,
                        Message::AppendEntriesReply {
                            term: self.current_term,
                        },
                    );
                    return;
                }
                if term > self.current_term {
                    self.current_term = term;
                    self.voted_for = None;
                }
                self.role = Role::Follower;
                self.leader_id = Some(leader);
                self.votes = 0;
                self.reset_election_timer(io);
                io.send(
                    from,
                    Message::AppendEntriesReply {
                        term: self.current_term,
                    },
                );
            }
            Message::AppendEntriesReply { term } => {
                if term > self.current_term {
                    self.step_down(term, io);
                }
            }
        }
    }
}

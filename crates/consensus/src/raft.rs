use sim::{millis, Io, NodeId, Process, TimerId};

use crate::kv::StateMachine;

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
pub struct LogEntry {
    pub term: Term,
    pub command: Vec<u8>,
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
        prev_log_index: usize,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    },
    AppendEntriesReply {
        term: Term,
        success: bool,
        match_index: usize,
    },
    ClientRequest {
        command: Vec<u8>,
    },
}

pub struct Raft<SM: StateMachine> {
    id: NodeId,
    peers: Vec<NodeId>,
    cluster_size: usize,
    current_term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    role: Role,
    votes: usize,
    log: Vec<LogEntry>,
    commit_index: usize,
    last_applied: usize,
    next_index: Vec<usize>,
    match_index: Vec<usize>,
    sm: SM,
}

impl<SM: StateMachine> Raft<SM> {
    pub fn new(id: NodeId, cluster: &[NodeId], sm: SM) -> Self {
        let n = cluster.len();
        Raft {
            id,
            peers: cluster.iter().copied().filter(|&node| node != id).collect(),
            cluster_size: n,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            role: Role::Follower,
            votes: 0,
            log: vec![LogEntry {
                term: 0,
                command: Vec::new(),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: vec![1; n],
            match_index: vec![0; n],
            sm,
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

    pub fn state_machine(&self) -> &SM {
        &self.sm
    }

    fn majority(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    fn last_log_index(&self) -> usize {
        self.log.len() - 1
    }

    fn last_log_term(&self) -> Term {
        self.log[self.log.len() - 1].term
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
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
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
        let next = self.last_log_index() + 1;
        for &peer in &self.peers {
            self.next_index[peer] = next;
            self.match_index[peer] = 0;
        }
        self.broadcast_append(io);
    }

    fn broadcast_append(&self, io: &mut Io<Message>) {
        for &peer in &self.peers {
            self.send_append(peer, io);
        }
        io.set_timer(HEARTBEAT_TIMER, millis(HEARTBEAT_INTERVAL_MS));
    }

    fn send_append(&self, peer: NodeId, io: &mut Io<Message>) {
        let next = self.next_index[peer];
        let prev_log_index = next - 1;
        let prev_log_term = self.log[prev_log_index].term;
        let entries = self.log[next..].to_vec();
        io.send(
            peer,
            Message::AppendEntries {
                term: self.current_term,
                leader: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        );
    }

    fn up_to_date(&self, last_log_index: usize, last_log_term: Term) -> bool {
        last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index())
    }

    fn maybe_commit(&mut self) {
        let last = self.last_log_index();
        let mut new_commit = self.commit_index;
        for n in (self.commit_index + 1)..=last {
            if self.log[n].term != self.current_term {
                continue;
            }
            let replicas = 1 + self
                .peers
                .iter()
                .filter(|&&peer| self.match_index[peer] >= n)
                .count();
            if replicas >= self.majority() {
                new_commit = n;
            }
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed();
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let command = self.log[self.last_applied].command.clone();
            self.sm.apply(&command);
        }
    }
}

impl<SM: StateMachine> Process for Raft<SM> {
    type Message = Message;

    fn on_start(&mut self, io: &mut Io<Message>) {
        self.reset_election_timer(io);
    }

    fn on_timer(&mut self, timer: TimerId, io: &mut Io<Message>) {
        match timer {
            ELECTION_TIMER if self.role != Role::Leader => self.start_election(io),
            HEARTBEAT_TIMER if self.role == Role::Leader => self.broadcast_append(io),
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
            Message::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                if term < self.current_term {
                    io.send(
                        from,
                        Message::AppendEntriesReply {
                            term: self.current_term,
                            success: false,
                            match_index: 0,
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

                if prev_log_index > self.last_log_index()
                    || self.log[prev_log_index].term != prev_log_term
                {
                    io.send(
                        from,
                        Message::AppendEntriesReply {
                            term: self.current_term,
                            success: false,
                            match_index: 0,
                        },
                    );
                    return;
                }

                let mut index = prev_log_index;
                for entry in entries {
                    index += 1;
                    if index <= self.last_log_index() {
                        if self.log[index].term != entry.term {
                            self.log.truncate(index);
                            self.log.push(entry);
                        }
                    } else {
                        self.log.push(entry);
                    }
                }

                if leader_commit > self.commit_index {
                    self.commit_index = leader_commit.min(index);
                    self.apply_committed();
                }
                io.send(
                    from,
                    Message::AppendEntriesReply {
                        term: self.current_term,
                        success: true,
                        match_index: index,
                    },
                );
            }
            Message::AppendEntriesReply {
                term,
                success,
                match_index,
            } => {
                if term > self.current_term {
                    self.step_down(term, io);
                    return;
                }
                if self.role != Role::Leader || term != self.current_term {
                    return;
                }
                if success {
                    self.match_index[from] = match_index;
                    self.next_index[from] = match_index + 1;
                    self.maybe_commit();
                } else if self.next_index[from] > 1 {
                    self.next_index[from] -= 1;
                    self.send_append(from, io);
                }
            }
            Message::ClientRequest { command } => {
                if self.role == Role::Leader {
                    self.log.push(LogEntry {
                        term: self.current_term,
                        command,
                    });
                    self.broadcast_append(io);
                    self.maybe_commit();
                }
            }
        }
    }
}

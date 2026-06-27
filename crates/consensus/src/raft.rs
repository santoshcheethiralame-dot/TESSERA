use std::collections::{BTreeMap, BTreeSet};

use sim::{millis, Io, NodeId, Process, TimerId};

use crate::kv::StateMachine;

type Term = u64;

const ELECTION_TIMER: TimerId = 0;
const HEARTBEAT_TIMER: TimerId = 1;
const HEARTBEAT_INTERVAL_MS: u64 = 50;
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_MAX_MS: u64 = 300;
const SNAPSHOT_THRESHOLD: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone)]
pub struct LogEntry {
    pub term: Term,
    pub client: NodeId,
    pub request_id: u64,
    pub command: Vec<u8>,
}

fn sentinel(term: Term) -> LogEntry {
    LogEntry {
        term,
        client: 0,
        request_id: 0,
        command: Vec::new(),
    }
}

struct Log {
    start: usize,
    entries: Vec<LogEntry>,
}

impl Log {
    fn new() -> Self {
        Log {
            start: 0,
            entries: vec![sentinel(0)],
        }
    }

    fn last_index(&self) -> usize {
        self.start + self.entries.len() - 1
    }

    fn last_term(&self) -> Term {
        self.entries.last().unwrap().term
    }

    fn term(&self, index: usize) -> Term {
        self.entries[index - self.start].term
    }

    fn get(&self, index: usize) -> &LogEntry {
        &self.entries[index - self.start]
    }

    fn has(&self, index: usize) -> bool {
        index >= self.start && index <= self.last_index()
    }

    fn slice_from(&self, index: usize) -> Vec<LogEntry> {
        self.entries[index - self.start..].to_vec()
    }

    fn truncate(&mut self, index: usize) {
        self.entries.truncate(index - self.start);
    }

    fn push(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    fn entry_count(&self) -> usize {
        self.entries.len()
    }

    fn compact(&mut self, up_to: usize) {
        let offset = up_to - self.start;
        self.entries.drain(0..offset);
        self.start = up_to;
    }

    fn install(&mut self, last_index: usize, last_term: Term) {
        self.start = last_index;
        self.entries = vec![sentinel(last_term)];
    }
}

#[derive(Clone)]
pub enum ClientResult {
    Ok(Vec<u8>),
    NotLeader(Option<NodeId>),
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
    InstallSnapshot {
        term: Term,
        leader: NodeId,
        last_index: usize,
        last_term: Term,
        data: Vec<u8>,
    },
    InstallSnapshotReply {
        term: Term,
        match_index: usize,
    },
    ClientRequest {
        request_id: u64,
        command: Vec<u8>,
    },
    ClientReply {
        request_id: u64,
        result: ClientResult,
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
    votes: BTreeSet<NodeId>,
    log: Log,
    snapshot: Vec<u8>,
    commit_index: usize,
    last_applied: usize,
    next_index: Vec<usize>,
    match_index: Vec<usize>,
    sessions: BTreeMap<NodeId, (u64, Vec<u8>)>,
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
            votes: BTreeSet::new(),
            log: Log::new(),
            snapshot: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: vec![1; n],
            match_index: vec![0; n],
            sessions: BTreeMap::new(),
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

    pub fn snapshot_index(&self) -> usize {
        self.log.start
    }

    pub fn log_entry_count(&self) -> usize {
        self.log.entry_count()
    }

    fn majority(&self) -> usize {
        self.cluster_size / 2 + 1
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
        self.votes.clear();
        self.reset_election_timer(io);
    }

    fn start_election(&mut self, io: &mut Io<Message>) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer(io);
        for &peer in &self.peers {
            io.send(
                peer,
                Message::RequestVote {
                    term: self.current_term,
                    candidate: self.id,
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                },
            );
        }
        if self.votes.len() >= self.majority() {
            self.become_leader(io);
        }
    }

    fn become_leader(&mut self, io: &mut Io<Message>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let next = self.log.last_index() + 1;
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
        if next <= self.log.start {
            io.send(
                peer,
                Message::InstallSnapshot {
                    term: self.current_term,
                    leader: self.id,
                    last_index: self.log.start,
                    last_term: self.log.term(self.log.start),
                    data: self.snapshot.clone(),
                },
            );
            return;
        }
        let prev_log_index = next - 1;
        let prev_log_term = self.log.term(prev_log_index);
        let entries = self.log.slice_from(next);
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
        last_log_term > self.log.last_term()
            || (last_log_term == self.log.last_term() && last_log_index >= self.log.last_index())
    }

    fn maybe_commit(&mut self, io: &mut Io<Message>) {
        let last = self.log.last_index();
        let mut new_commit = self.commit_index;
        for n in (self.commit_index + 1)..=last {
            if self.log.term(n) != self.current_term {
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
            self.apply_committed(io);
        }
    }

    fn apply_committed(&mut self, io: &mut Io<Message>) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = self.log.get(self.last_applied).clone();
            let response = self.apply_entry(&entry);
            if self.role == Role::Leader && entry.request_id != 0 {
                io.send(
                    entry.client,
                    Message::ClientReply {
                        request_id: entry.request_id,
                        result: ClientResult::Ok(response),
                    },
                );
            }
        }
        self.maybe_snapshot();
    }

    fn maybe_snapshot(&mut self) {
        if self.log.entry_count() > SNAPSHOT_THRESHOLD && self.last_applied > self.log.start {
            self.snapshot = self.sm.snapshot();
            self.log.compact(self.last_applied);
        }
    }

    fn apply_entry(&mut self, entry: &LogEntry) -> Vec<u8> {
        if entry.request_id != 0 {
            if let Some((last_id, last_response)) = self.sessions.get(&entry.client) {
                if entry.request_id <= *last_id {
                    return last_response.clone();
                }
            }
        }
        let response = self.sm.apply(&entry.command);
        if entry.request_id != 0 {
            self.sessions
                .insert(entry.client, (entry.request_id, response.clone()));
        }
        response
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
                    self.votes.insert(from);
                    if self.votes.len() >= self.majority() {
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
                self.votes.clear();
                self.reset_election_timer(io);

                let mut prev_log_index = prev_log_index;
                let mut prev_log_term = prev_log_term;
                let mut entries = entries;
                if prev_log_index < self.log.start {
                    let sent_through = prev_log_index + entries.len();
                    let skip = self.log.start - prev_log_index;
                    if skip < entries.len() {
                        entries.drain(0..skip);
                        prev_log_index = self.log.start;
                        prev_log_term = self.log.term(self.log.start);
                    } else {
                        io.send(
                            from,
                            Message::AppendEntriesReply {
                                term: self.current_term,
                                success: true,
                                match_index: sent_through,
                            },
                        );
                        return;
                    }
                }

                if prev_log_index > self.log.last_index()
                    || self.log.term(prev_log_index) != prev_log_term
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
                    if self.log.has(index) {
                        if self.log.term(index) != entry.term {
                            self.log.truncate(index);
                            self.log.push(entry);
                        }
                    } else {
                        self.log.push(entry);
                    }
                }

                if leader_commit > self.commit_index {
                    self.commit_index = leader_commit.min(index);
                    self.apply_committed(io);
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
                    if match_index > self.match_index[from] {
                        self.match_index[from] = match_index;
                    }
                    self.next_index[from] = self.match_index[from] + 1;
                    self.maybe_commit(io);
                } else if self.next_index[from] > 1 {
                    self.next_index[from] -= 1;
                    self.send_append(from, io);
                }
            }
            Message::InstallSnapshot {
                term,
                leader,
                last_index,
                last_term,
                data,
            } => {
                if term < self.current_term {
                    io.send(
                        from,
                        Message::InstallSnapshotReply {
                            term: self.current_term,
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
                self.votes.clear();
                self.reset_election_timer(io);

                if last_index > self.log.start {
                    self.sm.restore(&data);
                    self.snapshot = data;
                    self.log.install(last_index, last_term);
                    self.commit_index = self.commit_index.max(last_index);
                    self.last_applied = last_index;
                }
                io.send(
                    from,
                    Message::InstallSnapshotReply {
                        term: self.current_term,
                        match_index: self.log.last_index(),
                    },
                );
            }
            Message::InstallSnapshotReply { term, match_index } => {
                if term > self.current_term {
                    self.step_down(term, io);
                    return;
                }
                if self.role != Role::Leader || term != self.current_term {
                    return;
                }
                if match_index > self.match_index[from] {
                    self.match_index[from] = match_index;
                }
                self.next_index[from] = self.match_index[from] + 1;
                self.maybe_commit(io);
            }
            Message::ClientRequest {
                request_id,
                command,
            } => {
                if self.role == Role::Leader {
                    self.log.push(LogEntry {
                        term: self.current_term,
                        client: from,
                        request_id,
                        command,
                    });
                    self.broadcast_append(io);
                    self.maybe_commit(io);
                } else {
                    io.send(
                        from,
                        Message::ClientReply {
                            request_id,
                            result: ClientResult::NotLeader(self.leader_id),
                        },
                    );
                }
            }
            Message::ClientReply { .. } => {}
        }
    }
}

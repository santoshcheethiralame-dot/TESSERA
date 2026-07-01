use sim::NodeId;

use crate::raft::{ClientResult, Config, Durable, LogEntry, Message};

pub fn encode_message(msg: &Message) -> Vec<u8> {
    let mut b = Vec::new();
    match msg {
        Message::RequestVote {
            term,
            candidate,
            last_log_index,
            last_log_term,
        } => {
            b.push(0);
            put_u64(&mut b, *term);
            put_usize(&mut b, *candidate);
            put_usize(&mut b, *last_log_index);
            put_u64(&mut b, *last_log_term);
        }
        Message::RequestVoteReply { term, granted } => {
            b.push(1);
            put_u64(&mut b, *term);
            b.push(*granted as u8);
        }
        Message::AppendEntries {
            term,
            leader,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            b.push(2);
            put_u64(&mut b, *term);
            put_usize(&mut b, *leader);
            put_usize(&mut b, *prev_log_index);
            put_u64(&mut b, *prev_log_term);
            put_usize(&mut b, entries.len());
            for entry in entries {
                put_entry(&mut b, entry);
            }
            put_usize(&mut b, *leader_commit);
        }
        Message::AppendEntriesReply {
            term,
            success,
            match_index,
        } => {
            b.push(3);
            put_u64(&mut b, *term);
            b.push(*success as u8);
            put_usize(&mut b, *match_index);
        }
        Message::InstallSnapshot {
            term,
            leader,
            last_index,
            last_term,
            config,
            data,
        } => {
            b.push(4);
            put_u64(&mut b, *term);
            put_usize(&mut b, *leader);
            put_usize(&mut b, *last_index);
            put_u64(&mut b, *last_term);
            put_config(&mut b, config);
            put_bytes(&mut b, data);
        }
        Message::InstallSnapshotReply { term, match_index } => {
            b.push(5);
            put_u64(&mut b, *term);
            put_usize(&mut b, *match_index);
        }
        Message::ClientRequest {
            request_id,
            command,
        } => {
            b.push(6);
            put_u64(&mut b, *request_id);
            put_bytes(&mut b, command);
        }
        Message::ClientReply { request_id, result } => {
            b.push(7);
            put_u64(&mut b, *request_id);
            put_result(&mut b, result);
        }
        Message::ChangeConfig { members } => {
            b.push(8);
            put_ids(&mut b, members);
        }
    }
    b
}

pub fn decode_message(bytes: &[u8]) -> Option<Message> {
    let mut r = Reader { b: bytes, pos: 0 };
    let msg = match r.u8()? {
        0 => Message::RequestVote {
            term: r.u64()?,
            candidate: r.usize()?,
            last_log_index: r.usize()?,
            last_log_term: r.u64()?,
        },
        1 => Message::RequestVoteReply {
            term: r.u64()?,
            granted: r.bool()?,
        },
        2 => {
            let term = r.u64()?;
            let leader = r.usize()?;
            let prev_log_index = r.usize()?;
            let prev_log_term = r.u64()?;
            let count = r.usize()?;
            let mut entries = Vec::new();
            for _ in 0..count {
                entries.push(r.entry()?);
            }
            let leader_commit = r.usize()?;
            Message::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            }
        }
        3 => Message::AppendEntriesReply {
            term: r.u64()?,
            success: r.bool()?,
            match_index: r.usize()?,
        },
        4 => Message::InstallSnapshot {
            term: r.u64()?,
            leader: r.usize()?,
            last_index: r.usize()?,
            last_term: r.u64()?,
            config: r.config()?,
            data: r.bytes()?,
        },
        5 => Message::InstallSnapshotReply {
            term: r.u64()?,
            match_index: r.usize()?,
        },
        6 => Message::ClientRequest {
            request_id: r.u64()?,
            command: r.bytes()?,
        },
        7 => Message::ClientReply {
            request_id: r.u64()?,
            result: r.result()?,
        },
        8 => Message::ChangeConfig { members: r.ids()? },
        _ => return None,
    };
    Some(msg)
}

pub(crate) fn encode_durable(d: &Durable) -> Vec<u8> {
    let mut b = Vec::new();
    put_u64(&mut b, d.term);
    match d.voted_for {
        Some(node) => {
            b.push(1);
            put_usize(&mut b, node);
        }
        None => b.push(0),
    }
    put_usize(&mut b, d.log_start);
    put_usize(&mut b, d.log.len());
    for entry in &d.log {
        put_entry(&mut b, entry);
    }
    put_bytes(&mut b, &d.snapshot);
    put_config(&mut b, &d.snapshot_config);
    b
}

pub(crate) fn decode_durable(bytes: &[u8]) -> Option<Durable> {
    let mut r = Reader { b: bytes, pos: 0 };
    let term = r.u64()?;
    let voted_for = if r.u8()? == 1 { Some(r.usize()?) } else { None };
    let log_start = r.usize()?;
    let count = r.usize()?;
    let mut log = Vec::new();
    for _ in 0..count {
        log.push(r.entry()?);
    }
    let snapshot = r.bytes()?;
    let snapshot_config = r.config()?;
    Some(Durable {
        term,
        voted_for,
        log_start,
        log,
        snapshot,
        snapshot_config,
    })
}

fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn put_usize(b: &mut Vec<u8>, v: usize) {
    put_u64(b, v as u64);
}

fn put_bytes(b: &mut Vec<u8>, s: &[u8]) {
    put_usize(b, s.len());
    b.extend_from_slice(s);
}

fn put_ids(b: &mut Vec<u8>, ids: &[NodeId]) {
    put_usize(b, ids.len());
    for &id in ids {
        put_usize(b, id);
    }
}

fn put_config(b: &mut Vec<u8>, c: &Config) {
    put_ids(b, &c.old);
    match &c.new {
        Some(new) => {
            b.push(1);
            put_ids(b, new);
        }
        None => b.push(0),
    }
}

fn put_entry(b: &mut Vec<u8>, e: &LogEntry) {
    put_u64(b, e.term);
    put_usize(b, e.client);
    put_u64(b, e.request_id);
    put_bytes(b, &e.command);
    match &e.config {
        Some(c) => {
            b.push(1);
            put_config(b, c);
        }
        None => b.push(0),
    }
}

fn put_result(b: &mut Vec<u8>, r: &ClientResult) {
    match r {
        ClientResult::Ok(value) => {
            b.push(0);
            put_bytes(b, value);
        }
        ClientResult::NotLeader(hint) => {
            b.push(1);
            match hint {
                Some(node) => {
                    b.push(1);
                    put_usize(b, *node);
                }
                None => b.push(0),
            }
        }
    }
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn u8(&mut self) -> Option<u8> {
        let v = *self.b.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }

    fn u64(&mut self) -> Option<u64> {
        let end = self.pos + 8;
        let v = u64::from_le_bytes(self.b.get(self.pos..end)?.try_into().ok()?);
        self.pos = end;
        Some(v)
    }

    fn usize(&mut self) -> Option<usize> {
        Some(self.u64()? as usize)
    }

    fn bool(&mut self) -> Option<bool> {
        Some(self.u8()? != 0)
    }

    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.usize()?;
        let end = self.pos + len;
        let v = self.b.get(self.pos..end)?.to_vec();
        self.pos = end;
        Some(v)
    }

    fn ids(&mut self) -> Option<Vec<NodeId>> {
        let count = self.usize()?;
        let mut v = Vec::new();
        for _ in 0..count {
            v.push(self.usize()?);
        }
        Some(v)
    }

    fn config(&mut self) -> Option<Config> {
        let old = self.ids()?;
        let new = if self.u8()? == 1 {
            Some(self.ids()?)
        } else {
            None
        };
        Some(Config { old, new })
    }

    fn entry(&mut self) -> Option<LogEntry> {
        let term = self.u64()?;
        let client = self.usize()?;
        let request_id = self.u64()?;
        let command = self.bytes()?;
        let config = if self.u8()? == 1 {
            Some(self.config()?)
        } else {
            None
        };
        Some(LogEntry {
            term,
            client,
            request_id,
            command,
            config,
        })
    }

    fn result(&mut self) -> Option<ClientResult> {
        match self.u8()? {
            0 => Some(ClientResult::Ok(self.bytes()?)),
            1 => {
                let hint = if self.u8()? == 1 {
                    Some(self.usize()?)
                } else {
                    None
                };
                Some(ClientResult::NotLeader(hint))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: Message) {
        let bytes = encode_message(&msg);
        let back = decode_message(&bytes).expect("decode");
        assert_eq!(encode_message(&back), bytes);
        assert!(decode_message(&bytes[..bytes.len() - 1]).is_none() || bytes.len() == 1);
    }

    #[test]
    fn roundtrips_every_variant() {
        roundtrip(Message::RequestVote {
            term: 5,
            candidate: 2,
            last_log_index: 10,
            last_log_term: 4,
        });
        roundtrip(Message::RequestVoteReply {
            term: 5,
            granted: true,
        });
        roundtrip(Message::AppendEntries {
            term: 7,
            leader: 1,
            prev_log_index: 3,
            prev_log_term: 2,
            entries: vec![
                LogEntry {
                    term: 7,
                    client: 9,
                    request_id: 42,
                    command: b"put".to_vec(),
                    config: None,
                },
                LogEntry {
                    term: 7,
                    client: 0,
                    request_id: 0,
                    command: Vec::new(),
                    config: Some(Config {
                        old: vec![0, 1, 2],
                        new: Some(vec![0, 1, 2, 3]),
                    }),
                },
            ],
            leader_commit: 2,
        });
        roundtrip(Message::AppendEntriesReply {
            term: 7,
            success: false,
            match_index: 0,
        });
        roundtrip(Message::InstallSnapshot {
            term: 8,
            leader: 4,
            last_index: 100,
            last_term: 6,
            config: Config {
                old: vec![0, 1, 2],
                new: None,
            },
            data: vec![1, 2, 3, 4, 5],
        });
        roundtrip(Message::InstallSnapshotReply {
            term: 8,
            match_index: 100,
        });
        roundtrip(Message::ClientRequest {
            request_id: 1,
            command: b"hello".to_vec(),
        });
        roundtrip(Message::ClientReply {
            request_id: 1,
            result: ClientResult::Ok(b"world".to_vec()),
        });
        roundtrip(Message::ClientReply {
            request_id: 2,
            result: ClientResult::NotLeader(Some(3)),
        });
        roundtrip(Message::ClientReply {
            request_id: 3,
            result: ClientResult::NotLeader(None),
        });
        roundtrip(Message::ChangeConfig {
            members: vec![0, 1, 2, 3, 4],
        });
    }
}

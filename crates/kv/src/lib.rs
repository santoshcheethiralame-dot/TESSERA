use consensus::{decode_command, encode_value, KvCommand, StateMachine};
use storage::{Db, Disk};

pub struct LsmKv<D: Disk> {
    db: Db<D>,
}

impl<D: Disk> LsmKv<D> {
    pub fn new(db: Db<D>) -> Self {
        LsmKv { db }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).ok().flatten()
    }
}

impl<D: Disk> StateMachine for LsmKv<D> {
    fn apply(&mut self, command: &[u8]) -> Vec<u8> {
        match decode_command(command) {
            Some(KvCommand::Put(key, value)) => {
                let _ = self.db.put(&key, &value);
                Vec::new()
            }
            Some(KvCommand::Delete(key)) => {
                let _ = self.db.delete(&key);
                Vec::new()
            }
            Some(KvCommand::Get(key)) => encode_value(self.db.get(&key).ok().flatten().as_deref()),
            None => Vec::new(),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let pairs = self.db.scan().unwrap_or_default();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
        for (key, value) in pairs {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&key);
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(&value);
        }
        buf
    }

    fn restore(&mut self, snapshot: &[u8]) {
        for (key, _) in self.db.scan().unwrap_or_default() {
            let _ = self.db.delete(&key);
        }
        if snapshot.len() < 4 {
            return;
        }
        let count = u32::from_le_bytes(snapshot[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        for _ in 0..count {
            let Some(key) = take(snapshot, &mut pos) else {
                break;
            };
            let Some(value) = take(snapshot, &mut pos) else {
                break;
            };
            let _ = self.db.put(&key, &value);
        }
    }
}

fn take(bytes: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let len = u32::from_le_bytes(bytes.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
    *pos += 4;
    let out = bytes.get(*pos..*pos + len)?.to_vec();
    *pos += len;
    Some(out)
}

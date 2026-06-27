use std::collections::BTreeMap;

pub trait StateMachine {
    fn apply(&mut self, command: &[u8]) -> Vec<u8>;
}

pub enum KvCommand {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
}

#[derive(Default)]
pub struct KvStore {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore::default()
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).cloned()
    }
}

impl StateMachine for KvStore {
    fn apply(&mut self, command: &[u8]) -> Vec<u8> {
        match decode_command(command) {
            Some(KvCommand::Put(key, value)) => {
                self.map.insert(key, value);
                Vec::new()
            }
            Some(KvCommand::Delete(key)) => {
                self.map.remove(&key);
                Vec::new()
            }
            Some(KvCommand::Get(key)) => encode_value(self.map.get(&key).map(|v| v.as_slice())),
            None => Vec::new(),
        }
    }
}

pub fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = vec![1];
    put_bytes(&mut buf, key);
    put_bytes(&mut buf, value);
    buf
}

pub fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut buf = vec![0];
    put_bytes(&mut buf, key);
    buf
}

pub fn encode_get(key: &[u8]) -> Vec<u8> {
    let mut buf = vec![2];
    put_bytes(&mut buf, key);
    buf
}

pub fn decode_command(bytes: &[u8]) -> Option<KvCommand> {
    let tag = *bytes.first()?;
    let mut pos = 1;
    let key = take(bytes, &mut pos)?;
    match tag {
        1 => Some(KvCommand::Put(key, take(bytes, &mut pos)?)),
        0 => Some(KvCommand::Delete(key)),
        2 => Some(KvCommand::Get(key)),
        _ => None,
    }
}

pub fn encode_value(value: Option<&[u8]>) -> Vec<u8> {
    match value {
        Some(v) => {
            let mut buf = vec![1];
            buf.extend_from_slice(v);
            buf
        }
        None => vec![0],
    }
}

pub fn decode_value(bytes: &[u8]) -> Option<Vec<u8>> {
    match bytes.first() {
        Some(1) => Some(bytes[1..].to_vec()),
        _ => None,
    }
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn take(bytes: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let len = u32::from_le_bytes(bytes.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
    *pos += 4;
    let out = bytes.get(*pos..*pos + len)?.to_vec();
    *pos += len;
    Some(out)
}

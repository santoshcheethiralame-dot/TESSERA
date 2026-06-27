use crate::crc::crc32;

#[derive(Clone, Debug, PartialEq)]
pub enum Record {
    Put {
        key: Vec<u8>,
        seq: u64,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
        seq: u64,
    },
}

pub fn encode(record: &Record) -> Vec<u8> {
    let mut payload = Vec::new();
    match record {
        Record::Put { key, seq, value } => {
            payload.push(1);
            put_bytes(&mut payload, key);
            payload.extend_from_slice(&seq.to_le_bytes());
            put_bytes(&mut payload, value);
        }
        Record::Delete { key, seq } => {
            payload.push(0);
            put_bytes(&mut payload, key);
            payload.extend_from_slice(&seq.to_le_bytes());
        }
    }
    let mut frame = Vec::with_capacity(payload.len() + 8);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

pub fn replay(bytes: &[u8]) -> Vec<Record> {
    let mut records = Vec::new();
    let mut pos = 0;
    while pos + 8 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
        let start = pos + 8;
        let end = start + len;
        if end > bytes.len() {
            break;
        }
        let payload = &bytes[start..end];
        if crc32(payload) != crc {
            break;
        }
        match decode(payload) {
            Some(record) => records.push(record),
            None => break,
        }
        pos = end;
    }
    records
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn decode(payload: &[u8]) -> Option<Record> {
    let mut pos = 0;
    let tag = *payload.first()?;
    pos += 1;
    let key = take_bytes(payload, &mut pos)?;
    let seq = take_u64(payload, &mut pos)?;
    match tag {
        1 => {
            let value = take_bytes(payload, &mut pos)?;
            Some(Record::Put { key, seq, value })
        }
        0 => Some(Record::Delete { key, seq }),
        _ => None,
    }
}

fn take_bytes(payload: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let len = take_u32(payload, pos)? as usize;
    let end = *pos + len;
    let out = payload.get(*pos..end)?.to_vec();
    *pos = end;
    Some(out)
}

fn take_u32(payload: &[u8], pos: &mut usize) -> Option<u32> {
    let end = *pos + 4;
    let value = u32::from_le_bytes(payload.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(value)
}

fn take_u64(payload: &[u8], pos: &mut usize) -> Option<u64> {
    let end = *pos + 8;
    let value = u64::from_le_bytes(payload.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(value)
}

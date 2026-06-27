use std::cmp::Ordering;
use std::io;

use crate::bloom::Bloom;
use crate::disk::Disk;
use crate::memtable::{Lookup, Op};

const BLOCK_SIZE: usize = 4096;
const FOOTER_LEN: usize = 48;
const MAGIC: u64 = 0x7373_7461_626c_6531;

pub struct SsTable {
    name: String,
    index: Vec<IndexEntry>,
    bloom: Bloom,
    max_seq: u64,
}

struct IndexEntry {
    last_key: Vec<u8>,
    last_seq: u64,
    offset: u64,
    len: u32,
}

pub fn write<D: Disk>(
    disk: &D,
    name: &str,
    entries: &[(Vec<u8>, u64, Op)],
    max_seq: u64,
) -> io::Result<()> {
    disk.create(name)?;
    let mut bloom = Bloom::new(entries.len(), 10);
    let mut index: Vec<IndexEntry> = Vec::new();
    let mut block = Vec::new();
    let mut offset: u64 = 0;
    for (key, seq, op) in entries {
        bloom.add(key);
        encode_entry(&mut block, key, *seq, op);
        if block.len() >= BLOCK_SIZE {
            disk.append(name, &block)?;
            index.push(IndexEntry {
                last_key: key.clone(),
                last_seq: *seq,
                offset,
                len: block.len() as u32,
            });
            offset += block.len() as u64;
            block.clear();
        }
    }
    if !block.is_empty() {
        let (last_key, last_seq, _) = entries.last().unwrap();
        disk.append(name, &block)?;
        index.push(IndexEntry {
            last_key: last_key.clone(),
            last_seq: *last_seq,
            offset,
            len: block.len() as u32,
        });
        offset += block.len() as u64;
    }

    let bloom_bytes = bloom.encode();
    let bloom_offset = offset;
    disk.append(name, &bloom_bytes)?;
    offset += bloom_bytes.len() as u64;

    let index_bytes = encode_index(&index);
    let index_offset = offset;
    disk.append(name, &index_bytes)?;

    let footer = encode_footer(
        index_offset,
        index_bytes.len() as u64,
        bloom_offset,
        bloom_bytes.len() as u64,
        max_seq,
    );
    disk.append(name, &footer)?;
    disk.sync(name)?;
    Ok(())
}

impl SsTable {
    pub fn open<D: Disk>(disk: &D, name: &str) -> io::Result<SsTable> {
        let size = disk.size(name)?;
        if size < FOOTER_LEN as u64 {
            return Err(corrupt(name));
        }
        let footer = disk.read_at(name, size - FOOTER_LEN as u64, FOOTER_LEN)?;
        let index_offset = read_u64(&footer, 0);
        let index_len = read_u64(&footer, 8);
        let bloom_offset = read_u64(&footer, 16);
        let bloom_len = read_u64(&footer, 24);
        let max_seq = read_u64(&footer, 32);
        let magic = read_u64(&footer, 40);
        if magic != MAGIC {
            return Err(corrupt(name));
        }
        let bloom_bytes = disk.read_at(name, bloom_offset, bloom_len as usize)?;
        let bloom = Bloom::decode(&bloom_bytes);
        let index_bytes = disk.read_at(name, index_offset, index_len as usize)?;
        let index = decode_index(&index_bytes);
        Ok(SsTable {
            name: name.to_string(),
            index,
            bloom,
            max_seq,
        })
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    pub fn get<D: Disk>(&self, disk: &D, key: &[u8], snapshot: u64) -> io::Result<Lookup> {
        if !self.bloom.contains(key) {
            return Ok(Lookup::Absent);
        }
        let block_idx = self
            .index
            .partition_point(|e| cmp(&e.last_key, e.last_seq, key, snapshot) == Ordering::Less);
        let Some(entry) = self.index.get(block_idx) else {
            return Ok(Lookup::Absent);
        };
        let block = disk.read_at(&self.name, entry.offset, entry.len as usize)?;
        scan_block(&block, key, snapshot)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scan<D: Disk>(&self, disk: &D) -> io::Result<Vec<(Vec<u8>, u64, Op)>> {
        let mut out = Vec::new();
        for entry in &self.index {
            let block = disk.read_at(&self.name, entry.offset, entry.len as usize)?;
            let mut pos = 0;
            while pos < block.len() {
                let (user, seq, op, next) = decode_entry(&block, pos)?;
                out.push((user, seq, op));
                pos = next;
            }
        }
        Ok(out)
    }
}

fn cmp(a_user: &[u8], a_seq: u64, b_user: &[u8], b_seq: u64) -> Ordering {
    a_user.cmp(b_user).then(b_seq.cmp(&a_seq))
}

fn scan_block(block: &[u8], key: &[u8], snapshot: u64) -> io::Result<Lookup> {
    let mut pos = 0;
    while pos < block.len() {
        let (user, seq, op, next) = decode_entry(block, pos)?;
        if cmp(&user, seq, key, snapshot) != Ordering::Less {
            if user.as_slice() == key {
                return Ok(match op {
                    Op::Put(value) => Lookup::Found(value),
                    Op::Delete => Lookup::Deleted,
                });
            }
            return Ok(Lookup::Absent);
        }
        pos = next;
    }
    Ok(Lookup::Absent)
}

fn encode_entry(buf: &mut Vec<u8>, key: &[u8], seq: u64, op: &Op) {
    put_u32(buf, key.len() as u32);
    buf.extend_from_slice(key);
    buf.extend_from_slice(&seq.to_le_bytes());
    match op {
        Op::Put(value) => {
            buf.push(1);
            put_u32(buf, value.len() as u32);
            buf.extend_from_slice(value);
        }
        Op::Delete => buf.push(0),
    }
}

fn decode_entry(block: &[u8], pos: usize) -> io::Result<(Vec<u8>, u64, Op, usize)> {
    let mut cur = pos;
    let key = take_bytes(block, &mut cur)?;
    let seq = take_u64(block, &mut cur)?;
    let tag = *block.get(cur).ok_or_else(malformed)?;
    cur += 1;
    let op = match tag {
        1 => Op::Put(take_bytes(block, &mut cur)?),
        0 => Op::Delete,
        _ => return Err(malformed()),
    };
    Ok((key, seq, op, cur))
}

fn encode_index(index: &[IndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u32(&mut buf, index.len() as u32);
    for entry in index {
        put_u32(&mut buf, entry.last_key.len() as u32);
        buf.extend_from_slice(&entry.last_key);
        buf.extend_from_slice(&entry.last_seq.to_le_bytes());
        buf.extend_from_slice(&entry.offset.to_le_bytes());
        buf.extend_from_slice(&entry.len.to_le_bytes());
    }
    buf
}

fn decode_index(data: &[u8]) -> Vec<IndexEntry> {
    let mut pos = 0;
    let count = take_u32(data, &mut pos).unwrap_or(0) as usize;
    let mut index = Vec::with_capacity(count);
    for _ in 0..count {
        let last_key = take_bytes(data, &mut pos).unwrap();
        let last_seq = take_u64(data, &mut pos).unwrap();
        let offset = take_u64(data, &mut pos).unwrap();
        let len = take_u32(data, &mut pos).unwrap();
        index.push(IndexEntry {
            last_key,
            last_seq,
            offset,
            len,
        });
    }
    index
}

fn encode_footer(
    index_offset: u64,
    index_len: u64,
    bloom_offset: u64,
    bloom_len: u64,
    max_seq: u64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_LEN);
    for value in [
        index_offset,
        index_len,
        bloom_offset,
        bloom_len,
        max_seq,
        MAGIC,
    ] {
        buf.extend_from_slice(&value.to_le_bytes());
    }
    buf
}

fn put_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(data: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(data[at..at + 8].try_into().unwrap())
}

fn take_bytes(data: &[u8], pos: &mut usize) -> io::Result<Vec<u8>> {
    let len = take_u32(data, pos)? as usize;
    let end = *pos + len;
    let out = data.get(*pos..end).ok_or_else(malformed)?.to_vec();
    *pos = end;
    Ok(out)
}

fn take_u32(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    let end = *pos + 4;
    let value = u32::from_le_bytes(
        data.get(*pos..end)
            .ok_or_else(malformed)?
            .try_into()
            .unwrap(),
    );
    *pos = end;
    Ok(value)
}

fn take_u64(data: &[u8], pos: &mut usize) -> io::Result<u64> {
    let end = *pos + 8;
    let value = u64::from_le_bytes(
        data.get(*pos..end)
            .ok_or_else(malformed)?
            .try_into()
            .unwrap(),
    );
    *pos = end;
    Ok(value)
}

fn corrupt(name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("corrupt sstable: {name}"),
    )
}

fn malformed() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "malformed sstable entry")
}

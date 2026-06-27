use std::io;

use crate::disk::Disk;
use crate::memtable::{Memtable, Op};
use crate::wal::{self, Record};

const WAL: &str = "wal";

pub struct Db<D: Disk> {
    disk: D,
    memtable: Memtable,
    next_seq: u64,
}

impl<D: Disk> Db<D> {
    pub fn open(disk: D) -> io::Result<Self> {
        let mut memtable = Memtable::new();
        let mut next_seq = 1;
        if disk.exists(WAL) {
            let size = disk.size(WAL)? as usize;
            let bytes = disk.read_at(WAL, 0, size)?;
            for record in wal::replay(&bytes) {
                let seq = apply(&mut memtable, record);
                next_seq = next_seq.max(seq + 1);
            }
        } else {
            disk.create(WAL)?;
        }
        Ok(Db {
            disk,
            memtable,
            next_seq,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let seq = self.take_seq();
        let record = Record::Put {
            key: key.to_vec(),
            seq,
            value: value.to_vec(),
        };
        self.disk.append(WAL, &wal::encode(&record))?;
        self.memtable
            .insert(key.to_vec(), seq, Op::Put(value.to_vec()));
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let seq = self.take_seq();
        let record = Record::Delete {
            key: key.to_vec(),
            seq,
        };
        self.disk.append(WAL, &wal::encode(&record))?;
        self.memtable.insert(key.to_vec(), seq, Op::Delete);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.memtable.get(key, u64::MAX).map(|value| value.to_vec())
    }

    pub fn sync(&self) -> io::Result<()> {
        self.disk.sync(WAL)
    }

    fn take_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }
}

fn apply(memtable: &mut Memtable, record: Record) -> u64 {
    match record {
        Record::Put { key, seq, value } => {
            memtable.insert(key, seq, Op::Put(value));
            seq
        }
        Record::Delete { key, seq } => {
            memtable.insert(key, seq, Op::Delete);
            seq
        }
    }
}

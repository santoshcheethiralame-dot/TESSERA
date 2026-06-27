use std::io;

use crate::disk::Disk;
use crate::memtable::{Lookup, Memtable, Op};
use crate::sstable::{self, SsTable};
use crate::wal::{self, Record};

const WAL: &str = "wal";
const SST_PREFIX: &str = "sst-";
const DEFAULT_FLUSH_BYTES: usize = 1 << 20;
const DEFAULT_COMPACTION_TABLES: usize = 4;

pub struct Db<D: Disk> {
    disk: D,
    memtable: Memtable,
    tables: Vec<SsTable>,
    next_seq: u64,
    next_sst: u64,
    flush_bytes: usize,
    compaction_tables: usize,
}

impl<D: Disk> Db<D> {
    pub fn open(disk: D) -> io::Result<Self> {
        let mut names: Vec<String> = disk
            .list()
            .into_iter()
            .filter(|name| name.starts_with(SST_PREFIX) && !name.ends_with(".tmp"))
            .collect();
        names.sort();

        let mut tables = Vec::new();
        let mut next_sst = 1;
        let mut next_seq = 1;
        for name in &names {
            let table = SsTable::open(&disk, name)?;
            next_seq = next_seq.max(table.max_seq() + 1);
            if let Some(number) = sst_number(name) {
                next_sst = next_sst.max(number + 1);
            }
            tables.push(table);
        }
        tables.reverse();

        let mut memtable = Memtable::new();
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
            tables,
            next_seq,
            next_sst,
            flush_bytes: DEFAULT_FLUSH_BYTES,
            compaction_tables: DEFAULT_COMPACTION_TABLES,
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
        self.maybe_flush()
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let seq = self.take_seq();
        let record = Record::Delete {
            key: key.to_vec(),
            seq,
        };
        self.disk.append(WAL, &wal::encode(&record))?;
        self.memtable.insert(key.to_vec(), seq, Op::Delete);
        self.maybe_flush()
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self.memtable.get(key, u64::MAX) {
            Lookup::Found(value) => return Ok(Some(value)),
            Lookup::Deleted => return Ok(None),
            Lookup::Absent => {}
        }
        for table in &self.tables {
            match table.get(&self.disk, key, u64::MAX)? {
                Lookup::Found(value) => return Ok(Some(value)),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
        }
        Ok(None)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let frozen = std::mem::take(&mut self.memtable);
        let number = self.next_sst;
        self.next_sst += 1;
        let name = format!("{SST_PREFIX}{number:06}");
        let tmp = format!("{name}.tmp");
        let entries = frozen.into_entries();
        sstable::write(&self.disk, &tmp, &entries, self.next_seq - 1)?;
        self.disk.rename(&tmp, &name)?;
        let table = SsTable::open(&self.disk, &name)?;
        self.tables.insert(0, table);
        self.disk.create(WAL)?;
        self.disk.sync(WAL)?;
        self.maybe_compact()
    }

    pub fn compact(&mut self) -> io::Result<()> {
        if self.tables.len() < 2 {
            return Ok(());
        }
        let mut entries = Vec::new();
        for table in &self.tables {
            entries.extend(table.scan(&self.disk)?);
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

        let mut merged = Vec::new();
        let mut last: Option<Vec<u8>> = None;
        for (user, seq, op) in entries {
            if last.as_deref() == Some(user.as_slice()) {
                continue;
            }
            last = Some(user.clone());
            if matches!(op, Op::Put(_)) {
                merged.push((user, seq, op));
            }
        }

        let obsolete: Vec<String> = self.tables.iter().map(|t| t.name().to_string()).collect();
        let number = self.next_sst;
        self.next_sst += 1;
        let name = format!("{SST_PREFIX}{number:06}");
        let tmp = format!("{name}.tmp");
        sstable::write(&self.disk, &tmp, &merged, self.next_seq - 1)?;
        self.disk.rename(&tmp, &name)?;
        let table = SsTable::open(&self.disk, &name)?;
        self.tables = vec![table];
        for old in obsolete {
            self.disk.remove(&old)?;
        }
        Ok(())
    }

    pub fn sync(&self) -> io::Result<()> {
        self.disk.sync(WAL)
    }

    pub fn set_flush_threshold(&mut self, bytes: usize) {
        self.flush_bytes = bytes;
    }

    pub fn set_compaction_threshold(&mut self, tables: usize) {
        self.compaction_tables = tables.max(2);
    }

    fn take_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn maybe_flush(&mut self) -> io::Result<()> {
        if self.memtable.bytes() >= self.flush_bytes {
            self.flush()?;
        }
        Ok(())
    }

    fn maybe_compact(&mut self) -> io::Result<()> {
        if self.tables.len() >= self.compaction_tables {
            self.compact()?;
        }
        Ok(())
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

fn sst_number(name: &str) -> Option<u64> {
    name.strip_prefix(SST_PREFIX)?.parse().ok()
}

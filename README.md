# tessera

A distributed, sharded, replicated key-value store on a from-scratch storage engine.

Storage is an LSM-tree: memtable, SSTables, write-ahead log, leveled compaction, MVCC. Raft replicates each shard. The keyspace is split across many Raft groups behind a coordinator, with a routing client on top. The CockroachDB/TiKV shape, scaled down.

## More than a coursework Raft KV

Deterministic simulation testing. Time, network, disk, scheduling, and RNG are injectable. The whole cluster runs single-threaded inside a seeded world, so failure interleavings can be fuzzed by the million and any bug replays exactly from its seed. The way TigerBeetle and FoundationDB find the bugs that matter, built in from the start.

Linearizability checking of client histories under partitions, crashes, reordering, and clock skew.

The parts tutorials skip: snapshots and log compaction, linearizable reads via ReadIndex and leader leases, membership changes via joint consensus.

Measured, not claimed: p50/p99, throughput across read/write mixes, recovery after a leader kill, behavior under partition, against etcd and single-node RocksDB.

## Design

Consensus and storage are pure state machines. They consume events (a message, a fired timer, a finished disk write) and return intents (send a message, set a timer, start I/O). They never read the clock or touch the network or disk themselves. In production a real driver runs them; in tests the simulator does. Same code either way.

The rules that keep it deterministic: a single seeded RNG, an event queue ordered by `(time, sequence)`, and no `HashMap` on any path that affects behavior.

## Status

- [x] Simulation kernel: seeded RNG, virtual clock, event scheduler, network faults, partitions
- [x] LSM storage engine: WAL, MVCC memtable, SSTables, bloom filters, compaction, crash recovery
- [x] Raft group and KV state machine (election, log replication, LSM-backed apply)
- [x] Linearizability checker and chaos fuzzing (Wing-Gong checker; seed-reproducible; caught a real split-brain bug)
- [x] Snapshots, leases, joint-consensus membership
- [x] Sharding, coordinator, routing client
- [ ] Benchmarks vs etcd and RocksDB

## Build

```
cargo test
```

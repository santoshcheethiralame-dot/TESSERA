# Benchmarks

Reproduce: `cargo run --release -p bench`.

Two regimes, measured honestly:

- **Storage engine** — real numbers on **real disk with real `fsync`** (`RealDisk`, the production implementation of the `Disk` trait).
- **Distributed Raft** — measured in the **deterministic simulator** in virtual time. The consensus code is real; the network and clock are simulated (1–10 ms one-way latency). These isolate algorithmic behavior (election, recovery, availability), not physical hardware limits.

Environment: developer laptop, Windows 11, x86_64, Rust 1.96 release. Numbers vary run-to-run; these are representative.

## Storage engine (real disk)

From-scratch LSM — WAL + fsync, memtable, block SSTables + bloom filters, size-tiered compaction. 100-byte values.

| Workload | p50 | p99 | Throughput |
|----------|----:|----:|-----------:|
| Durable put (fsync per op) | 853 µs | 1.56 ms | ~1,140 ops/s |
| Batched put (group commit, one fsync) | — | — | ~44,000 ops/s |
| Random get (over 200k keys) | 15 µs | 50 µs | ~58,000 ops/s |

- **Durable writes are fsync-bound**: ~0.85 ms p50 is one `fsync` on this disk; throughput ≈ 1/fsync. This is the strict path — every write flushed before it's acknowledged.
- **Group commit amortizes fsync** across a batch → ~40× the write throughput. This is exactly why real databases batch the WAL.
- **Reads are fast** (~15 µs): a hit comes from the memtable or a cached SSTable block, with the bloom filter skipping SSTables that can't hold the key.

## Distributed Raft (deterministic simulator, 5 nodes, virtual time)

| Metric | Value |
|--------|------|
| Leader election | ~214 ms |
| Recovery after leader kill | ~194 ms |
| 3\|2 partition | majority stays available; minority cannot elect (no split-brain) |
| Replicated write, serial commit | ~10 ms/op (~96 ops/s) |

- **Election / recovery** are bounded by the randomized election timeout (150–300 ms). Recovery after killing the leader (~194 ms) is one timeout plus a vote+append round — exactly as expected.
- **Partition**: the majority side keeps committing; the minority spins as candidates and never elects. The safety property, measured.
- **Serial replicated-write latency** ≈ one network round-trip (the sim is configured at 1–10 ms one-way, so ~10 ms RTT). The ~96 ops/s figure is **serial** — one commit at a time — and therefore latency-bound, not peak throughput. Pipelining in-flight requests and batching log entries (standard, not yet implemented here) would multiply it.

## Honest scope — what isn't here yet

- **Real networked deployment + etcd comparison.** The distributed numbers come from the simulator. The architecture is built for it (the state machines are driver-agnostic — same code, two worlds), so a production TCP transport driver would let this same Raft run across real nodes/containers for a head-to-head against etcd. That transport plus a multi-node/container harness is the remaining step.
- **vs single-node RocksDB.** A direct comparison needs RocksDB available; the figures above are the from-scratch engine standalone.
- **Pipelined/batched replication throughput**, which would lift the distributed write number well above the serial figure.

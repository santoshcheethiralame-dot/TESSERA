# Benchmarks

Reproduce: `cargo run --release -p bench`.

Three regimes, measured honestly:

- **Storage engine** — real numbers on **real disk with real `fsync`** (`RealDisk`, the production implementation of the `Disk` trait).
- **Distributed Raft (simulator)** — measured in the **deterministic simulator** in virtual time. The consensus code is real; the network and clock are simulated (1–10 ms one-way latency). These isolate algorithmic behavior (election, recovery, availability), not physical hardware limits.
- **Distributed Raft (real TCP)** — the **same Raft and LSM code**, driven by the production runtime (`server` crate): real threads, real sockets, real OS timers, a 5-node cluster on localhost with a TCP client. This is the "same state machine, two worlds" claim made literal — the simulator and the network share one unchanged state machine.

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
| Recovery after leader kill | ~200 ms |
| 3\|2 partition | majority stays available; minority cannot elect (no split-brain) |
| Replicated write, serial commit | ~10 ms/op (~99 ops/s) |
| Replicated write, concurrent (pipelined) | ~83,000 ops/s |

- **Election / recovery** are bounded by the randomized election timeout (150–300 ms). Recovery after killing the leader (~200 ms) is one timeout plus a vote+append round — exactly as expected.
- **Partition**: the majority side keeps committing; the minority spins as candidates and never elects. The safety property, measured.
- **Serial vs concurrent is the whole story.** The ~99 ops/s figure is one commit at a time — pure latency (~10 ms RTT), not a throughput ceiling. Fire 2,000 writes at the leader at once and they commit in ~24 ms (~83k ops/s) — an ~840× jump — because the leader **pipelines**: a burst coalesces behind a single in-flight `AppendEntries` per follower, the next batch ships the instant an ack lands, and one RPC carries many entries. This is the standard Raft flow-control (one outstanding append per peer + batching), and it's why the serial number is the wrong one to quote for throughput.

## Distributed Raft over real TCP (5 nodes, localhost, real sockets)

The `server` crate runs the unchanged `Raft<LsmKv<RealDisk>>` under a real driver — an acceptor thread, per-connection reader threads, per-peer writer threads, and an event loop with real OS timers — speaking a hand-rolled length-prefixed wire codec, with Raft state fsync'd to disk before each reply. A blocking TCP client (`Client`) routes to the leader, follows `NotLeader` redirects, and retries. Five nodes on localhost, 100-byte values, one in-flight request at a time.

| Workload | p50 | p99 | Throughput |
|----------|----:|----:|-----------:|
| Replicated put (commit through Raft, fsync-durable) | ~445 µs | ~2 ms | ~1,750 writes/s |
| Linearizable get (leader lease read) | ~80 µs | ~390 µs | ~9,700 ops/s |
| Recovery after leader kill (client-observed) | — | — | ~2.1 s |

- **Durable writes are fsync-bound.** Each write persists the Raft log to disk before it's acknowledged, so serial write latency (~445 µs p50) is dominated by one `fsync` — the same durability barrier the storage table shows, now on the replicated path. That's the honest cost of not losing acknowledged writes; concurrency hides it (see the pipelined ~83k/s in the simulator), and dropping fsync would trade safety for speed.
- **Serial over localhost is still ~18× the simulator's serial number** (~1,750 vs ~99 writes/s) — the sim deliberately models 1–10 ms RTT while localhost is microseconds. Same code path, and the driver decides the physics.
- **Lease reads are local** to the leader (no replication round-trip, no fsync), so reads (~80 µs p50) run several times faster than writes.
- **Recovery is reported as the client sees it.** The protocol re-elects in ~194 ms (measured in the simulator); the ~2.1 s here is the *client's* failover policy — the simple round-robin `Client` retries against a now-stale leader hint and waits on read timeouts before rotating. It measures client behavior, not Raft latency. A smarter client (parallel probing, shorter adaptive timeouts) would close most of the gap.

## Honest scope — what isn't here yet

- **Multi-machine / container deployment + etcd comparison.** The TCP transport above is real, but run on one host (localhost, 5 processes-as-threads). The `tessera-server` binary takes node id, listen address, peers, and a data dir, so the same code runs across real machines or containers; a multi-host harness and an etcd head-to-head are the remaining step.
- **vs single-node RocksDB.** A direct comparison needs RocksDB available; the figures above are the from-scratch engine standalone.
- **Batching commands into a single log entry.** Replication already pipelines (one in-flight append per peer, many entries per RPC); packing multiple client commands into one *entry* would shave a little more per-entry overhead under extreme load, but the round-trip cost is already amortized.

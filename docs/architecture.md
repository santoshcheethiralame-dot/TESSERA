# Architecture

The shape is CockroachDB/TiKV in miniature: a routing client over a sharded keyspace, each shard an independent Raft group, each node persisting through an LSM engine. Every layer is written from scratch on the Rust standard library.

```
                    put / get / delete
        client ──────────────────────────► router
                                     key → shard → cached leader
                          ┌────────────────┼────────────────┐
                          ▼                ▼                ▼
                      shard 0          shard 1          shard 2
                    ┌───────────┐    ┌───────────┐    ┌───────────┐
                    │ L   F   F │    │ F   L   F │    │ F   F   L │   one raft group per shard
                    └───────────┘    └───────────┘    └───────────┘
                          │ committed entries
                          ▼
                    LSM engine per node
                    WAL → memtable → SSTables (+ bloom filters, compaction, MVCC)
```

## Crates

| Crate | What it is |
|---|---|
| `sim` | The deterministic world: virtual clock, seeded RNG, event queue, network faults, partitions, crash-restart |
| `storage` | The LSM engine: write-ahead log, MVCC memtable, block SSTables with bloom filters, size-tiered compaction, plus the `Disk` trait with in-memory and real (`fsync`) implementations |
| `consensus` | Raft: election, log replication, snapshots, leader leases, joint-consensus membership, durable state, the wire codec |
| `kv` | The state machine that applies committed commands to the LSM engine |
| `lincheck` | A Wing–Gong linearizability checker over client histories |
| `cluster` | Sharding: hash partitioning, the coordinator, the routing client |
| `server` | The production driver: TCP transport, real timers, on-disk Raft state, a blocking client, the server binary, and the browser console |
| `bench` | The benchmark harness behind [BENCHMARKS.md](../BENCHMARKS.md) |

## One idea, everywhere

Consensus and storage are pure state machines. They implement a small trait:

```rust
pub trait Process {
    type Message: Clone;

    fn on_start(&mut self, io: &mut Io<Self::Message>);
    fn on_message(&mut self, from: NodeId, msg: Self::Message, io: &mut Io<Self::Message>);
    fn on_timer(&mut self, timer: TimerId, io: &mut Io<Self::Message>);
    fn reboot(&mut self, io: &mut Io<Self::Message>);
}
```

A handler never touches the clock, the network, or a socket. It reads `io.now()`, draws randomness from `io.gen_range(..)`, and emits intents — `io.send(..)`, `io.set_timer(..)` — that the caller executes after the handler returns.

Two drivers pump the same state machines:

- the **simulator** (`sim`) executes intents against a virtual clock and a modeled network, single-threaded, from a seed;
- the **server runtime** (`server`) executes them against real sockets and OS timers, with threads for I/O and one event loop for logic.

The state machine cannot tell which world it is in. That is what makes the fuzzing in [simulation.md](simulation.md) possible, and it is why the code that survives a thousand simulated failure schedules is byte-for-byte the code listening on a TCP port.

## Life of a write

1. The client sends `ClientRequest { request_id, command }` to its cached leader; a follower answers `NotLeader(hint)` and the client redirects.
2. The leader appends the command to its log and replicates. Replication is pipelined: at most one `AppendEntries` is in flight per follower, so a burst of writes coalesces behind it and ships as one batch when the ack lands.
3. Each follower appends, **fsyncs its Raft state to disk**, and only then replies. Persist-before-reply is what makes "committed" mean something across crashes.
4. When a majority has acknowledged, the leader commits, applies the command to the LSM engine, and replies to the client.
5. Client sessions make retries exactly-once: each `(client, request_id)` is applied at most once, and a duplicate gets the cached response.

## Life of a read

If the leader holds a fresh lease — granted by a majority of heartbeat acks, with the lease shorter than the election timeout so a deposed leader's lease dies before a successor can exist — it serves the read locally, linearizably, with no network round trip. Otherwise the read goes through the log like a write.

## Durability

Raft's term, vote, log, and snapshot are serialized at every handler boundary and handed to a `RaftStore`. The production store writes to a temp file, `fsync`s, and atomically renames over `raft.state`, skipping the write entirely when nothing changed. A restarted node reloads that file, comes back as a follower, and relearns everything volatile from the next heartbeat. The crash tests in `consensus/tests/crash.rs` kill nodes mid-flight and check that committed writes survive and histories stay linearizable.

## The wire

Messages travel as length-prefixed frames: a 4-byte little-endian length, then a hand-rolled tagged encoding (one byte per variant, little-endian fields). The decoder is panic-free — every read is bounds-checked and a truncated or malformed frame decodes to `None`, so a bad peer cannot crash a node. Connections open with a 9-byte handshake (`role, id`) so the server knows whether it is talking to a peer or a client.

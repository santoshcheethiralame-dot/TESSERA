# Deterministic simulation

Distributed systems are hard to test because their bugs live in *orderings*: a message delayed past a timeout, a duplicate arriving mid-election, a crash between an append and an ack. Threads, sockets, and wall clocks produce a different ordering every run, so the ordering that kills you is one you will never see twice.

The fix is to take every source of nondeterminism away from the operating system and hand it to a scheduler you control.

## The world

The whole cluster — servers, clients, the network between them — runs single-threaded inside `sim::Simulator`. There is no wall clock and there are no sockets. There is only an event queue.

- **Time is virtual.** The clock is a `u64` of nanoseconds. It advances only when an event fires; a two-hour test of election timeouts runs in milliseconds of real time.
- **Every event is ordered.** The queue is a binary heap keyed by `(virtual time, sequence number)`. Two events at the same instant still have a total order, so execution is fully deterministic.
- **All randomness is seeded.** One SplitMix64 generator seeds the world; it forks one stream for the network and one per node. Node 3's randomness does not depend on how many packets node 2 dropped.
- **The network is a model.** Latency is drawn from a configured range; messages drop and duplicate with configured probabilities; partitions are a reachability matrix that tests mutate mid-run. Crash-restart is an event too — `sim.reboot(node)` wipes a process's volatile state and lets it recover from its persisted state, exactly like a power cut between two handler invocations.

Determinism is a discipline, not just a design: no `HashMap` anywhere behavior-relevant (its iteration order is randomized per process), no ambient clock reads, no thread spawns. The simulator folds every event into a running digest, and a test asserts that two runs of the same seed produce bit-identical digests — the discipline is itself under test.

## What runs inside it

The state machines from `consensus` and `kv` implement the `Process` trait described in [architecture.md](architecture.md): events in, intents out. The simulator pops an event, invokes the handler, collects the intents, and schedules their consequences — a send becomes a delivery event after a sampled latency (or a drop, or two deliveries), a timer becomes a timer event.

Simulated clients are processes too. They issue puts, gets, and deletes with retries and leader redirects, and they record an *operation history*: for every operation, the invocation time, the response time, and the result.

## Checking the histories

A separate crate, `lincheck`, implements a Wing–Gong linearizability checker. Given a history of concurrent operations, it searches for a sequential order — consistent with real-time (if A completed before B began, A comes first) — that a single correct key/value store could have produced. Histories decompose per key, which keeps the search tractable.

If no such order exists, the system lied to a client: a read saw a value it could not have seen, or an acknowledged write vanished. That is the property the whole project is accountable to.

## The harnesses

- `consensus/tests/chaos.rs` — clients hammer a five-node cluster while a nemesis splits the network at random pivots, with message loss and duplication on top. Every completed history must be linearizable. The ignored `stress_linearizable_many_seeds` test runs the same storm across 1,000 seeds.
- `consensus/tests/crash.rs` — the same setup with a nemesis that reboots a random server every few hundred milliseconds, plus a targeted test that a committed write survives crashing the exact leader that committed it.
- `cluster/tests/sharding.rs` — per-key linearizability across many Raft groups under per-shard partitions.

When a seed fails, that seed is the whole bug report: rerun it and the identical interleaving replays, event for event. [Seed 99](seed-99.md) is what that looks like in practice.

## Running it

```
cargo test                                           # the full deterministic suite
cargo test -p consensus --test chaos -- --ignored    # the 1,000-seed linearizability fuzz
cargo test -p consensus --test crash                 # crash-restart chaos
```

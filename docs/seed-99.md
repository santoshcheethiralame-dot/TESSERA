# Seed 99: anatomy of a split brain

Every claim this project makes about deterministic simulation testing comes down to one afternoon, one seed, and one integer that should have been a set.

## The setup

The chaos harness (`consensus/tests/chaos.rs`) puts a five-node Raft cluster and three simulated clients into the seeded world described in [simulation.md](simulation.md). Clients issue random puts, gets, and deletes with retries and leader redirects. A nemesis splits the network at random pivots, heals it, and splits it again; on top of that, every message has a 5% chance of being dropped and a 3% chance of being duplicated. Each client records its operation history, and after the storm the Wing–Gong checker asks one question: could a single correct key/value store have produced this history?

The deep fuzz runs that storm across 1,000 seeds. Seeds 0 through 98 passed.

## The alarm

Seed 99 failed the checker: the history was not linearizable. Concretely, a write that had been acknowledged to a client was not visible where it had to be — the cluster had confirmed an operation and then lost it.

This is the worst class of bug a replicated store can have, and the most useless kind of failure report in a conventional system: it happened once, under a partition, with packet loss, at a timing you did not choose. In production the evidence would be a support ticket and a log file that proves nothing.

Here it was a number. Running seed 99 again replayed the identical interleaving — every delivery, every timeout, every duplicate, in the same order, every time.

## The trace

Replaying with the cluster's state visible showed the impossible directly: **two nodes acting as leader in the same term**. Raft's Election Safety property — at most one leader per term — is the foundation everything else stands on. With two leaders in one term, both can commit entries independently, and one of their logs must eventually be overwritten. That is how an acknowledged write dies.

The election math is supposed to make this impossible: a candidate needs votes from a majority, and each node votes at most once per term. Working backward through the trace, the second "leader" had won its election with replies from only two distinct voters in a five-node cluster.

## The bug

The candidate counted votes with an integer tally, incremented on every granted `RequestVoteReply` it received.

The simulated network duplicated one of those replies — the same grant, delivered twice, exactly what the 3% duplication knob exists to produce. The tally counted it twice. Two distinct voters plus itself read as a majority of five, and the candidate crowned itself. Every individual node had followed the voting rules; the counter had still manufactured a quorum out of a network artifact.

The fix is to count voters, not votes:

```rust
Message::RequestVoteReply { term, granted } => {
    // ...
    if self.role == Role::Candidate && term == self.current_term && granted {
        self.votes.insert(from);
        if self.has_majority(&self.votes) {
            self.become_leader(io);
        }
    }
}
```

`votes` is a `BTreeSet<NodeId>`. A duplicated reply inserts the same node twice and changes nothing. Majority is computed over distinct granters — and later, when joint-consensus membership arrived, over distinct granters in *both* the old and new configurations.

## Why this bug survives ordinary testing

For the tally to matter, a very specific conjunction has to occur: a reply to a *candidate* must be duplicated, *during* an election, in a cluster where that one extra count is the difference between losing and winning, and the resulting second leader must commit something before stepping down. Unit tests never duplicate messages. Integration tests on a real network duplicate them rarely and never on schedule. A cluster in production would eventually roll these dice — once, unreproducibly, at night.

The simulator rolled them on purpose, thousands of times, and handed back the losing roll as a replayable artifact.

## Epilogue

The fix landed with the fuzz as its permanent regression test. The same 1,000-seed storm has since been rerun over every change that touches the hot path — leader leases, joint consensus, crash-restart persistence, pipelined replication — and it has to pass before anything merges into the story this repository tells about itself.

One integer became a set, and the whole project's thesis got its proof: the bugs that matter live in orderings, and orderings are only testable when you own them.

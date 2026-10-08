---
agent_spec: "docs/agents/api/lattice.json"
---

# Formal Coverage

Orleans.Lattice checks its most concurrency-sensitive protocols formally, not
only with prose and chaos tests. Epic #4430 extended that coverage from the
atomic-commit protocol to five more areas. This page indexes them: what each
area checks, how much, which production defects the checking found, and, just
as prominently, what each area does **not** cover.

## How every area is checked

Each area follows the pattern the
[atomic-commit specification](verified-atomic-commit.md) set:

- **TLA+ modules checked by TLC on every pull request.** They live under
  [`spec/`](../../spec/README.md), one directory per area.
- **A mutation for every property and every action.** Each is a deliberate
  defect that must make TLC report a violation, so no property holds
  vacuously and no protocol step goes unchecked.
- **A refinement note per module.** It maps every variable, action and property
  to the production code that plays its role, and to detector tests that were
  shown to go red when that code is perturbed.
- **Pure cores and Coyote models.** The decisions the specifications check run
  in production through pure cores, and Coyote models drive those cores under
  systematic interleaving, each with a guard test that must find a violation.

The Formal test gates discover every module from disk and check all of this on
every build. The acceptance census (#4442) adds two more gates:

- every behaviour-asserting row of every refinement note must read `Yes`, with no
  `Partial`, `None`, gap or assumption-only verdict;
- no note may cite an open issue as the place a gap is tracked.

## The five areas

The figures were derived from the module manifests and refinement notes when the
epic was accepted. Each area's spec `README.md` holds the figures the build
checks.

| Area | Modules | Mutations | Behaviour rows | Detector tests | Page |
|------|---------|-----------|----------------|----------------|------|
| WAL durability lifecycle | 2 | 60 | 56 | 138 | [Verified WAL](verified-wal.md#the-tla-specification-and-the-scope-of-the-assurance) |
| Shard ownership | 4 | 105 | 100 | 166 | [Verified Shard Ownership](verified-shard-ownership.md) |
| Cross-cluster atomic visibility | 1 | 59 | 39 | 116 | [Verified Atomic-Commit: the replicated half](verified-atomic-commit.md#scope-the-single-cluster-protocol-and-the-replicated-half-checked-separately) |
| Replication convergence | 4 | 82 | 64 | 190 | [Replication: formal verification](../lattice.replication/architecture.md#formal-verification) |
| Backup and restore | 5 | 71 | 78 | 154 | [Verified backup and restore](../lattice.backup/verified-backup.md) |

**Behaviour rows** are the rows of a refinement note's action and property tables
that assert a production behaviour. **Detector tests** are the distinct tests those
rows cite.

## What each area covers, and what it does not

Coverage of one area must not be read as coverage of another. Each area's page,
and each module's refinement note under its section on deliberate abstraction
gaps, states its full scope.

- **WAL durability lifecycle** ([`spec/wal/`](../../spec/wal/README.md)).
  - **Covers:** a leaf's lifecycle under crash-anywhere recovery, across one WAL
    partition shared by two leaves, including snapshot and row loss and a purge;
    and a WAL shard move with a durable fence, a shard crash and the loss of its
    coordinator.
  - **Does not cover:** more than two partitions; splits, resharding and saga
    state; replication consumers, beyond their effect on the trim floor.
- **Shard ownership** ([`spec/shard-ownership/`](../../spec/shard-ownership/README.md)).
  - **Covers:** who serves a key across an adaptive split, an online reshard, an
    online resize and its undo, with stale routers and an atomic write bound to
    one physical copy; the transaction registry's retention, late forwarded
    prepares and leaf reactivation; CRDT-mode keys across the same moves; and an
    atomic write bound across a shadow-cutover restore.
  - **Does not cover:** other alias moves, shard consolidation, and any
    cross-cluster replication of ownership moves.
- **Cross-cluster atomic visibility** ([`spec/atomic-commit/`](../../spec/atomic-commit/README.md),
  module `AtomicCommitCrossCluster`).
  - **Covers:** the receiver side of an atomic write replicated to a peer: the
    per-source-shard terminal tally, the cross-tree receiver barrier, every way
    production loses a record to a peer, and a receiver's bootstrap and
    cross-tree import.
  - **Does not cover:** the single-cluster protocol, which the `AtomicCommit`
    module covers, separately and from before this epic. Keys a peer's key
    filter excludes are outside the all-or-nothing guarantee.
- **Replication convergence** ([`spec/replication/`](../../spec/replication/README.md)).
  - **Covers:** plain replication over a lossy, duplicating, reordering transport:
    the cycle-break, duplicate suppression, the bootstrap handoff, causal
    dependencies and their low watermark, and an in-place re-bootstrap after
    the source reaped a delete, including the bootstrap drop floor.
  - **Does not cover:** atomic-write sagas carried over replication (the
    cross-cluster area covers them), anti-entropy repair, and more than one WAL
    partition per cluster.
- **Backup and restore** ([`spec/backup/`](../../spec/backup/README.md)).
  - **Covers:** capture, incremental chains, provenance, a coordinated restore
    across regions, and a local cutover and its revert, all against in-flight
    atomic writes.
  - **Does not cover:** in-place and cold restores, reader atomicity across a
    backup set's members during the alias swap, and a replication batch delayed
    across a whole restore cutover.

Across every area, the instances are bounded: each module checks a small number
of copies, shards, keys and faults exhaustively, and says which. A clean run is
a statement about the design over that instance. The integration and
[chaos tests](chaos-tests.md) remain the evidence for the deployed system.

Three other verified pages predate this epic and are outside its census:
[Verified Atomic-Commit](verified-atomic-commit.md) for the single-cluster
protocol, [Verified Atomic Action](verified-atomic-action.md) and
[Verified Distributed Lock](verified-lock.md).

## Defects the coverage found and fixed

Each defect below was found by building or reviewing an area's specification,
reproduced against production, and fixed; each fix is pinned by detectors, and
the mutation that reproduced production stays as a regression check. Each
area's list is the authoritative one:

| Area | Defects | Full list |
|------|---------|-----------|
| WAL durability lifecycle | #4450, #4451, #4456, #4467, #4523, #4525, #4621, #4622, #4634, #4641, #4654, #4669, #4699, #4700 | [Verified WAL](verified-wal.md#the-tla-specification-and-the-scope-of-the-assurance) |
| Shard ownership | #4452, #4453, #4454, #4455, #4473, #4474, #4475, #4503, #4522, #4545, #4564, #4611, #4613, #4618, #4619, #4689 | [`spec/shard-ownership/`](../../spec/shard-ownership/README.md#defects-this-area-found) |
| Cross-cluster atomic visibility | #4480, #4481, #4482, #4508, #4511, #4526, #4533, #4534, #4591, #4627, #4664, #4683, #4684, #4685 | [Verified Atomic-Commit](verified-atomic-commit.md#scope-the-single-cluster-protocol-and-the-replicated-half-checked-separately) |
| Replication convergence | #4463, #4464, #4465, #4504, #4537, #4549, #4585, #4586, #4587, #4603, #4604, #4614, #4615, #4673, #4707 | [`spec/replication/`](../../spec/replication/README.md#production-defects-this-module-found) |
| Backup and restore | #4485, #4490, #4589, #4593, #4686 | [`spec/backup/`](../../spec/backup/README.md#defects-the-modules-found) |

## Related

- [`spec/README.md`](../../spec/README.md) - the specification index, the module
  layout and the CI decision.
- [Chaos Tests](chaos-tests.md) - the integration suite that drives a live
  cluster under concurrent load and faults.

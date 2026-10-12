---
agent_spec: "docs/agents/api/lattice.json"
---

# Formal Coverage

Orleans.Lattice checks its most concurrency-sensitive protocols formally, not
only with prose and chaos tests. Epic #4430 extended that coverage from the
atomic-commit protocol to five more areas. The B+ tree topology census now also
checks cascading splits through a fan-out-three instance reaching its six-level
bound, and durable leaf-reclaim recovery through successor back-link repair.
This page indexes what each area checks, how much, which production defects the
checking found, and, just as prominently, what each area does **not** cover.

## How every area is checked

Each area follows the pattern the
[atomic-commit specification](verified-atomic-commit.md) set:

- **TLA+ modules checked by TLC in CI.** They live under
  [`spec/`](../../spec/README.md), one directory per area. Routine pull requests
  retain a base smoke check; exhaustive bases, variants and mutants run when the
  diff touches a TLC input, and on integration/release pushes and nightly runs.
- **A mutation for every property and every action.** Each is a deliberate
  defect that must make TLC report a violation, so no property holds
  vacuously and no protocol step goes unchecked.
- **A refinement note per module.** It maps every variable, action and property
  to the production code that plays its role, and to detector tests that were
  shown to go red when that code is perturbed.
- **Production detectors, with pure-core and Coyote tiers where applicable.**
  The established concurrency cores also run under systematic Coyote
  interleaving with negative controls. The view, anti-entropy and lifecycle
  additions check bounded TLA+ protocol abstractions mapped to production grain
  and integration tests; they do not claim a new pure-core or Coyote proof.

The structural Formal gates discover every module from disk and check its
manifest, mutation coverage and refinement mappings independently of whether
CI selects exhaustive TLC. The acceptance census (#4442) adds two more gates:

- every behaviour-asserting row of every refinement note must read `Yes`, with no
  `Partial`, `None`, gap or assumption-only verdict;
- no note may cite an open issue as the place a gap is tracked.

## Areas

Figures are derived from module manifests and refinement notes. Each area's spec
`README.md` holds the figures the build checks; the B+ tree topology row reflects
its four checked modules.

| Area | Modules | Mutations | Behaviour rows | Detector tests | Page |
|------|---------|-----------|----------------|----------------|------|
| WAL durability lifecycle | 2 | 60 | 56 | 138 | [Verified WAL](verified-wal.md#the-tla-specification-and-the-scope-of-the-assurance) |
| Shard ownership | 4 | 105 | 100 | 166 | [Verified Shard Ownership](verified-shard-ownership.md) |
| Cross-cluster atomic visibility | 1 | 59 | 39 | 116 | [Verified Atomic-Commit: the replicated half](verified-atomic-commit.md#scope-the-single-cluster-protocol-and-the-replicated-half-checked-separately) |
| Replication convergence | 4 | 82 | 64 | 190 | [Replication: formal verification](../lattice.replication/architecture.md#formal-verification) |
| Backup and restore | 5 | 71 | 78 | 154 | [Verified backup and restore](../lattice.backup/verified-backup.md) |
| B+ tree topology | 4 | 47 | 73 | 45 | [B+ tree topology](../../spec/bplustree/README.md) |
| Materialised-view tail and lifecycle | 1 | 22 | 27 | 30 | [Materialised views](../../spec/materialised-views/README.md) |
| Tree lifecycle composition | 2 | 46 | 66 | 48 | [Resize, reshard and shadow cutover](../../spec/tree-lifecycle/README.md) |
| Anti-entropy remediation | 1 | 32 | 39 | 59 | [Digest-driven repair](../../spec/anti-entropy/README.md) |
| Shared alias routing and revert | 2 | 25 | 40 | 22 | [Aliasing](../../spec/aliasing/README.md) |

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
- **B+ tree topology** ([`spec/bplustree/`](../../spec/bplustree/README.md)).
  - **Covers:** a donor-to-sibling split and parent-link crash window; right-edge
    parent splits from three to six levels in the checked instance; interleaved
    writes through a bounded leaf split and fold; sibling-chain and parent-route
    publication; and durable retirement, route removal, successor back-link
    repair, clearing, and recovery for a reclaimed leaf. Range ownership during
    adaptive shard split, online reshard and resize is already covered by the
    `ShardOwnership` models above.
  - **Does not cover:** an unbounded-height proof (the level-indexed model is
    checked only through six levels), simultaneous cascading split propagation,
    the orphan audit/repair algorithm itself, or the full reshard/resize protocol
    in this module. See the refinement notes.
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

- **Materialised-view tail and lifecycle** ([`spec/materialised-views/`](../../spec/materialised-views/README.md)).
  - **Covers:** physical WAL binding, checkpoint and published read-position
    entitlement, cursor and retention trims, fall-off and lag rebuild, finite
    crash replay, shadow-generation publication, reclaim and decommission,
    and source identity cutover followed by rebind.
  - **Does not cover:** atomic/cross-tree staging, aggregation membership,
    history shaping, per-partition terminal recovery, or reader-handle reclaim
    grace. Source swaps are environment steps; lifecycle internals are checked
    by the lifecycle and alias companions. Replay can transiently regress a
    value: this model checks prefix convergence, not monotonic reads across a
    replay. Its seven invariants and five temporal properties are not a claim
    of read-atomic safety for the separate staging protocol.

- **Tree lifecycle composition** ([`spec/tree-lifecycle/`](../../spec/tree-lifecycle/README.md)).
  - **Covers:** sparse resize copy/fence/swap/undo; grow and shrink reshard;
    local shadow restore/revert overtaking a physically bound migration; alias
    reservations, deletion exclusion and completion-before-release recovery;
    publish-before-read resize versus split/fold or legacy bootstrap admission;
    once-per-operation undo clearance when replication becomes enabled; and
    refusal of old undo after a later restore or administrative alias cutover.
  - **Does not cover:** arbitrary-await crash safety, a cutover racing every
    compensation await of a still-undoable resize, snapshot transport, or the
    data-plane rows and saga internals already modelled by the ownership, WAL
    and backup companions.
    An undo request can overtake already-admitted work; its admission interlock
    prevents new work, not retroactive cancellation.

- **Anti-entropy remediation** ([`spec/anti-entropy/`](../../spec/anti-entropy/README.md)).
  - **Covers:** digest comparison and Merkle localisation; opt-in, traffic-window
    and breaker gates; retained-WAL targeted replay and snapshot fallback;
    origin suppression, receiver dedup and LWW joins; dropped Sets and Deletes, foreign-origin
    rows, trim/cursor blind spots, finite transport faults and activation restart.
    Topology changes invalidate localisation through a physical generation.
  - **Does not cover:** full topology data movement, bounded-cache eviction,
    unbounded clocks or transport faults, or repairing lost deletes after their
    tombstones have been reaped. Retained snapshot tombstones repair trimmed-WAL
    and below-cursor lost deletes; reaping is outside the model. The Alias variant
    covers one physical swap, not rollback restores. See the refinement note.

- **Shared alias routing and revert** ([`spec/aliasing/`](../../spec/aliasing/README.md)).
  - **Covers:** two logical aliases sharing physical copies; intent-before-fence
    and fence-before-publication ordering; atomic alias/map publication;
    logical-alias-specific fence ownership; finite crash/retry and stale durable
    administrative recovery after ABA; restore revert's early refusal and
    expected-physical fence; deletion admission; and split/fold physical binding
    across repeated alias publications.
  - **Does not cover:** unbounded redirect chains, physical purge of shared
    copies, authorization, or an epoch-fenced restore revert. A revert admission
    read is not a lock: a superseding move after it can leave lineage/reservation
    changed on later refusal, and a return to the same shadow passes the physical
    check. No completion guarantee is claimed for permanently superseded work.
    Physical WAL consumers, atomic visibility and data movement retain the
    view, ownership, anti-entropy and lifecycle companion models' guarantees.

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
| B+ tree topology | #4795, #4815, #4818 | [`spec/bplustree/`](../../spec/bplustree/README.md#defects-this-specification-found) |
| Materialised-view tail and lifecycle | #4853, #4856, #4857 | [`spec/materialised-views/`](../../spec/materialised-views/README.md#defects-found) |
| Tree lifecycle composition | #4858 | [`spec/tree-lifecycle/`](../../spec/tree-lifecycle/README.md#defect-found) |
| Anti-entropy remediation | #4859, #4862, #4863 | [`spec/anti-entropy/`](../../spec/anti-entropy/README.md) |
| Shared alias routing and revert | #4860 | [`spec/aliasing/`](../../spec/aliasing/README.md#defect-found) |

## CI partition and non-vacuity

The new view, anti-entropy and lifecycle JVM cases use dedicated
deterministic-only TLC shards with measured durations in the matrix planner.
Bases and variants leave the catch-all. Anti-entropy is split into three
measured groups; its generated mutation-specific config headers prevent
same-target controls from sharing the current byte-keyed cache. Structural
discovery and refinement cases remain
outside the TLC shards and run even when policy skips exhaustive model checking.
The #4852 regression prevents a shard category from silently moving those
structural checks into skipped work.

## Related

- [`spec/README.md`](../../spec/README.md) - the specification index, the module
  layout and the CI decision.
- [Chaos Tests](chaos-tests.md) - the integration suite that drives a live
  cluster under concurrent load and faults.

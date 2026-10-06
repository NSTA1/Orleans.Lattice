# Verified Shard Ownership

Every read and write in Orleans.Lattice is routed to the one shard that owns its
key, and that owner moves: an adaptive [shard split](shard-splitting.md) moves a
slot to a new shard, an [online reshard](online-reshard.md) drives splits until
the tree has the shards it needs, and an online resize copies the tree to a new
physical copy and moves the logical tree's alias onto it
([tree sizing](tree-sizing.md)), with an undo that moves it back. Routing
activations cache where keys live, and an [atomic write](atomic-writes.md)'s saga
stays bound to the physical copy it prepared on. Ownership is what keeps all of
that consistent: at every moment each key has exactly one place that answers for
it, and no move loses or resurrects a value.

This document describes the formal coverage of that ownership protocol: four TLA+
modules checked by TLC, the pure cores production is routed through, and the
Coyote models that check those cores. It is an assurance document; the runtime
behaviour it protects is documented in the pages linked above.

## Scope

The coverage is of the **single-cluster** ownership protocol over a bounded
instance: two physical copies, two shards per copy, two keys, one split, one
reshard, one resize with its undo, one saga, one later write. What it covers,
and what it does not, is stated in the modules' refinement notes; the points a
reader is most likely to over-read are these.

- **Two modules, not one.** `ShardOwnership` checks who serves a key across
  split, reshard, resize and undo, with stale routers and the saga's binding,
  under a transaction registry that always reports the saga's decision.
  `ShardOwnershipRetention` checks what the registry's retention (declining to
  report a decision, retiring its row), a late forwarded prepare and a leaf
  reactivation do to a saga bound across a split and a resize. A behaviour that
  needs a retention event together with a stale writer, a saga re-bind, a
  reshard, a refused flip, an undo before a flip, a write
  stamp that disagrees with real time or a migrated row is checked by **neither**
  module's CI gate. Their composition is clean in this instance: every action
  of both, `ShardOwnership`'s write stamps and migrated rows, and all their
  properties. It costs about nineteen minutes of TLC, which is why it is not
  a gate
  ([the seam](../../spec/shard-ownership/README.md#two-modules-and-the-seam-between-them)).
  A third module, `ShardOwnershipCrdt`, checks that the same moves join a
  CRDT-mode key's copies rather than overwrite them.
- **A shadow-cutover restore has its own module.** `ShardOwnershipCutover`
  checks an atomic write bound to the previous copy across a shadow-cutover
  restore and its revert. That move is not a resize, so the copy the write leaves
  mirrors nowhere. The write re-binds, and it discards the prepares it left on
  the previous copy before it decides (#4689, fixed).
- **Other alias moves are not modelled.** An explicit alias change and schema
  remediation move the alias too, and neither is covered here.
- **Shard consolidation is not modelled.** The reshard here only grows.
- **Cross-cluster replication is not modelled.** Unlike the
  [atomic-commit protocol](verified-atomic-commit.md#the-replicated-half), whose
  replicated half has its own module, shard ownership's has no formal artefact.
- **Where production has an open defect, the modules model the intended
  design.** Each such place has an issue and a standing mutation that
  reproduces production. A clean model run is a statement about the design once
  those fixes land, not about every released build.

## What is checked

| Property | Plain-language guarantee | Module |
|----------|--------------------------|--------|
| `UniqueOwner` | Any routing pair any router may hold, however stale, is either refused for a key or reaches that key's one owner. | `ShardOwnership` |
| `NoKeyLost` | The current owner holds every value acknowledged to a writer. | both |
| `NoResurrection` | No read returns a value older than one already acknowledged, through any routing pair. | both |
| `SagaBatchOnOneCopy` | A committed atomic write's prepared batch sits only on the copy it is bound to and the copy that copy mirrors into. | `ShardOwnership` |
| `AtomicOnOwner` | A fresh reader sees an atomic write's batch on every key or on none. | both |
| `OwnerMonotonic` | The value a fresh reader gets never moves backwards, except across an undo, which discards the resized copy's writes by contract ([consistency](consistency.md)). | both |
| `SplitCompletes`, `ReshardCompletes`, `ResizeCompletes`, `SagaCompletes` | Each operation that started finishes, under fair scheduling of the steps its coordinator takes on its own. | both, `ReshardCompletes` in `ShardOwnership` |
| `RoutingConverges` | Eventually the registry's own routing pair serves every key: no fence or redirect outlives the operation that set it. | `ShardOwnership` |
| `NoStrandedBucket` | A decided atomic write's prepared bucket on a copy that can still become the tree is eventually consumed. | `ShardOwnershipRetention` |
| `ReadableOnceComplete` | Once an atomic write has completed, no read of its keys is held back by a leftover shadow marker. | `ShardOwnershipRetention` |
| `NoLiveBucketAfterForget` | No copy that can still become the tree keeps a prepared bucket of an atomic write the registry has forgotten. | `ShardOwnershipRetention` |
| `NoLostContribution` | A CRDT-mode key keeps every acknowledged contribution across a leaf split, a shard split and an online resize. | `ShardOwnershipCrdt` |
| `AtomicAcrossCutover` | No reader, fresh or stale, is served an atomic write's batch on one key and not the other across a shadow-cutover restore or its revert. | `ShardOwnershipCutover` |
| `CommittedBatchOnBoundCopy` | A committed atomic write holds prepared buckets only on the copy it is bound to: every copy it re-bound away from was discarded. | `ShardOwnershipCutover` |
| `SagaSettles` | An atomic write settles however a restore and its revert interleave with it. | `ShardOwnershipCutover` |

Every property has a mutation that breaks it, run as a two-arm experiment: the
property holds on the unmutated module and fails on the mutant. Every action in
each module is perturbed by at least one mutation. All four run in CI through
`TlcModelCheckTests`.

## Defects the coverage found

Writing the modules against production found defects that tests had not, each
filed and kept as a standing mutation: a split in flight across a resize
(#4452, fixed), an undo that let both copies serve (#4453, fixed), a mid-dispatch
re-bind that ignores the bound copy's mirror (#4454, fixed), an online snapshot that
drops prepared buckets (#4455, fixed), a split sweep that treats an undeterminable
registry answer as in flight (#4473, fixed), a saga that never completes once the
copy it is bound to is discarded by an undo (#4474, fixed) or, as an old copy,
purged (#4475, fixed), saga values installed at a stamp that overwrites a later
write (#4522, fixed), a
migration import dropped over a destination row the saga already resolved
(#4564, fixed), a shadow marker a leaf split strands on a sibling that never sees the
terminal (#4545, fixed), a late forward of an atomic write the registry has forgotten
left stranded (#4619, fixed), CRDT copies overwritten rather than joined by the
terminal's backstop, a split or a resize (#4611, #4613, #4618, all fixed), and,
found by the review of this coverage, a router that cached the old copy reading
empty and losing writes once that copy is purged (#4503, fixed). The cutover
module found one more: an atomic write that re-binds across a shadow-cutover
restore left its prepares on the previous copy, and a revert served them torn
(#4689, fixed). The modules' [README](../../spec/shard-ownership/README.md#defects-this-area-found)
maps each to its mutation.

## The cores production is routed through

Three ownership decisions that were inline in grains are extracted into pure,
dependency-free cores in the product assembly, and the grains call them, so the
artefact the models check is the artefact that runs:

| Core | Decision | Called from |
|------|----------|-------------|
| `ResizeFence` | Whether a fenced old copy admits a saga bound to it, and whether a failed alias flip lifts the fence | `ShardRootGrain`, `TreeResizeGrain` |
| `SagaCopyBinding` | Whether the routing tier dispatches a bound saga's prepare, and whether the saga stays bound, re-binds or commits across an alias move | `LatticeGrain`, `AtomicWriteGrain` |
| `RoutingPairPublishGate` | Whether a routing lookup may publish the pair it read, given invalidations since it began | `LatticeGrain` |

Each core has unit tests, and each has a Coyote model (`ResizeFenceModel`,
`SagaCopyBindingModel`, `RoutingPairPublishModel`) with a fixed-design test and
companion guard tests that remove one rule and must find the violation it
prevents.

## Related

- [Verified Atomic-Commit](verified-atomic-commit.md) - the protocol whose
  sagas these modules bind to physical copies.
- [`spec/shard-ownership/`](../../spec/shard-ownership/README.md) - the modules,
  their refinement notes and mutation catalogues.

# TLA+ specification of shard ownership

This directory holds the formal coverage of key ownership in Orleans.Lattice
across an adaptive shard split, an online reshard, and an online resize with its
fence, alias flip, undo and purge. Routing caches can be stale, an atomic-write
saga is bound to one physical copy, ordinary writes and reads run alongside,
and the transaction registry retires what it knows. It is the deliverable of
#4434 under epic #4430.

It is one area indexed in [`spec/README.md`](../README.md), which describes the
layout every module follows, how to run TLC and why TLC runs in CI. This README
covers only what is particular to shard ownership. The area is specified by
**two modules**, and the seam between them is part of what this README
documents. A third, `ShardOwnershipCrdt`, checks the same ownership moves for a
CRDT-mode key, whose copies must be joined rather than overwritten (see
[`RefinementCrdt.md`](RefinementCrdt.md)).

## Files

| File | What it is |
|------|-----------|
| [`ShardOwnership.tla`](ShardOwnership.tla) | Ownership: who serves a key across split, reshard, resize and undo, with stale routers and the saga's binding and re-binding. |
| [`ShardOwnership.cfg`](ShardOwnership.cfg) | Its TLC model: the bounded instance and the invariants and properties it checks. |
| [`ShardOwnership.manifest.json`](ShardOwnership.manifest.json) | Its manifest (see [Counts](#counts)). |
| [`mutations/`](mutations/) | Its mutation catalogue. See [`mutations/README.md`](mutations/README.md), which covers both catalogues. |
| [`Refinement.md`](Refinement.md) | Its refinement note. |
| [`ShardOwnershipRetention.tla`](ShardOwnershipRetention.tla) | Retention: what the registry's mask and retirement, a late forwarded prepare and a leaf reactivation do to a saga bound across a split and a resize. |
| [`ShardOwnershipRetention.cfg`](ShardOwnershipRetention.cfg) | Its TLC model. |
| [`ShardOwnershipRetention.manifest.json`](ShardOwnershipRetention.manifest.json) | Its manifest. |
| [`mutations-retention/`](mutations-retention/) | Its mutation catalogue. |
| [`RefinementRetention.md`](RefinementRetention.md) | Its refinement note. |
| [`ShardOwnershipCrdt.tla`](ShardOwnershipCrdt.tla) | CRDT ownership: that a leaf split, a shard split and an online resize join a CRDT-mode key's copies, so no acknowledged contribution is lost. |
| [`ShardOwnershipCrdt.cfg`](ShardOwnershipCrdt.cfg) | Its TLC model. |
| [`ShardOwnershipCrdt.manifest.json`](ShardOwnershipCrdt.manifest.json) | Its manifest. |
| [`mutations-crdt/`](mutations-crdt/) | Its mutation catalogue. |
| [`RefinementCrdt.md`](RefinementCrdt.md) | Its refinement note. |
| `README.md` | This file. |

## Two modules and the seam between them

### Why two

The area was first written as one module. Once the registry's retention, the
saga's abort path and leaf reactivation were added to it, it reached 693,228
distinct states, and checking its liveness properties took over eight minutes on
two TLC workers. `TlcModelCheckTests` gives each TLC run five minutes and runs a
control arm per mutation, so one module of that size would have cost CI the
better part of an hour and still breached the per-run limit. It was split along
the line where its concerns stop interacting, not shrunk.

### What each owns

| | `ShardOwnership` | `ShardOwnershipRetention` |
|---|---|---|
| Adaptive split (sweep, freeze, commit) | yes | yes |
| Online reshard | yes | no |
| Resize: snapshot, fence, flip, retire, purge | yes | yes |
| A refused flip | yes | no |
| Undo before the flip | yes | no |
| Undo after the flip | yes | yes |
| Stale routers that write | yes | no (writes use the current pair) |
| Stale routers that read | yes | yes |
| Saga bind, prepare, decide, abort, broadcast, complete | yes | yes |
| Saga re-binds | yes | no (a saga that cannot commit aborts, fairly) |
| Registry declines to report the decision (Indeterminate) | no | yes |
| Registry retires the saga's row | no | yes |
| A delayed shadow-forwarded prepare | no | yes |
| A leaf reactivation (terminal memory and shadow markers lost) | no | yes |
| `UniqueOwner`, `SagaBatchOnOneCopy`, `ReshardCompletes`, `RoutingConverges` | yes | no |
| `NoKeyLost`, `NoResurrection`, `AtomicOnOwner`, `OwnerMonotonic` | yes | yes |
| `SplitCompletes`, `ResizeCompletes`, `SagaCompletes` | yes | yes |
| `NoStrandedBucket` | no | yes |

The retention module keeps the ownership machinery its concerns act through, a
saga bound across one split and one resize with its undo, so every
counterexample found when they were one module is still reachable in a single
module. The mutation catalogues show it: each mutation was re-proved red, with
a clean control, in the module it now lives in.

### What composing them costs, and what it shows

The behaviours the split puts in neither module on its own are a retention
event **together with** something only `ShardOwnership` has: a stale router
writing, a saga re-binding, a reshard-driven split, a refused flip, or an undo
before the flip. They were checked once, by composing the modules, rather than
argued. The review (#4435, finding F7) built the compositions from
`ShardOwnershipRetention.tla` and re-checking them here reproduced its results.
Each composition was run under the retention cfg (`TypeOK`, `NoKeyLost`,
`NoResurrection`, `AtomicOnOwner`, `OwnerMonotonic`, `SplitCompletes`,
`ResizeCompletes`, `SagaCompletes`, `NoStrandedBucket`) on two TLC workers,
liveness checked at the end:

| Composition | Added to the retention module | Result |
|---|---|---|
| (a) | stale writers: `SagaPrepare(k, p)` and `LaterWrite(p)` over every published pair | clean, 82,155 distinct states, depth 24: **identical to the module alone**, so stale writers add no reachable behaviour in this instance |
| (b) | (a), plus `SagaRebindOnRefusal` and `SagaRebindBeforeDecision` (weakly fair) and `UndoBeforeFlip` | clean, 132,491 distinct states, depth 25, 3 min 11 s |
| (c) | (b), plus the reshard (`ReshardStart`, `ReshardFinish` fair, the split's `rs = "migrating"` arm, the resize's reshard interlock) and `ResizeFlipRefused` | clean, 497,105 distinct states, depth 29, 9 min 54 s |

Composition (c) is everything `ShardOwnership` has that the retention module
lacks. It was also checked against the four properties only `ShardOwnership`
states (`UniqueOwner`, `SagaBatchOnOneCopy`, `ReshardCompletes`,
`RoutingConverges`), all thirteen in one run, and is clean: the state count and
time above are that run's. The review measured 498,905 states and 8 min 35 s for
(c) under the retention cfg alone, on the bucket before the purge's timing
assumption was removed (#4503); the difference is that change.

So in this instance nothing is lost by the split. The composition is not a CI
gate because (c) exceeds the five-minute per-run limit on two workers by a wide
margin, and every mutation would pay it twice. It is the measured cost of
composing the two modules, and the reason they are separate; anyone changing
either module's shared machinery should re-run it. The compositions are
mechanical: replace the retention module's current-pair writers with the
published-pair forms, then add the named actions and their fairness from
`ShardOwnership.tla` with the retention variables added to their `UNCHANGED`
tuples.
### Budget

Each module's full configuration, liveness included, runs on two TLC workers
inside the per-run limit, with headroom; the figures are under
[Last checked](#last-checked). Three choices in the retention module are
budget-driven and argued where they are made: `Reactivate` is offered on the
split destination only, `NoStrandedBucket` is one leads-to rather than one per
bucket, and `RegistryMask` is enabled only once there is a decision to mask.
Each loses no behaviour the instance can distinguish.

## What is modelled

- **The split** (`SplitBegin`, `SplitSweep`, `SplitFreeze`, `SplitCommit`,
  `SplitAbandon`): an adaptive split of `k2`'s slot from `s1` to `s2` on the
  copy the tree resolves to, with its shadow-write window, retroactive sweep,
  Reject freeze and final drain before the map moves. It may run on the resized
  copy once the resize completed, while the old copy still mirrors into it: the
  mirror chases a refusal to the slot's current owner, a mirrored terminal
  reaches the split closure, and an undo abandons the split (#4478).
- **The reshard** (`ReshardStart`, `ReshardFinish`; ownership module only): a
  reshard that drives the split, interlocked with resize in both directions.
- **The resize** (`ResizeBegin`, `SnapCopy`, `ResizeFence`, `ResizeFlip`,
  `ResizeFlipRefused`, `ResizeRetire`, `ResizePurge`): an online resize from the
  old copy `T` to the resized copy `R`, index-for-index, fenced before a
  single-write flip, soft-deleted, then purged.
- **The undo** (`UndoBeforeFlip`, `UndoArm`, `UndoSwap`, `UndoClear`): before
  the flip it discards `R`; after it, it arms `R` to redirect, swaps back, and
  lifts `T`'s fence, in that order (#4453).
- **Routing** (`published`): any pair the registry ever published may be held by
  some router at any time.
- **The saga** (`SagaStart` through `SagaComplete`, `SagaAbort`): an atomic
  write of `k1` and `k2` bound to one physical copy, with its prepares, its
  re-binds, its decision and its terminal broadcast.
- **Retention** (retention module only): `RegistryMask`, `RegistryForget`,
  `DeliverLate` and `Reactivate`, and a leaf split of the split destination's
  leaf (`LeafSplit`) that moves a shadow marker to a fresh sibling (#4545).
- **A later write** (`LaterWrite`) of `k2`, which gives `NoResurrection` a newer
  value to protect.
- **Stamps and migrated rows** (ownership module only): a row holds a version of
  a write, whose real-time rank and HLC stamp are kept apart, and a flag for a
  row last written by a cross-shard migration. The base stamps every version in
  real-time order; mutations use the separation to reproduce the stamp and
  import defects (#4522, #4564).

- **CRDT keys** (CRDT module only): a staged CRDT mutation in a saga, a
  non-atomic CRDT write, a leaf split that strands the saga's bucket, and one
  shard split or online resize whose forward, drains and terminal must join the
  key's copies.

Each module modelled the **intended** design while production had an open
defect, with a mutation that reproduced production. Every such defect is now
fixed, and each mutation stays as a regression check; the refinement notes list
them with their issues.

## Properties checked

| Property | Kind | Module | Meaning |
|----------|------|--------|---------|
| `TypeOK` | invariant | all | State stays well-typed. |
| `UniqueOwner` | invariant | ownership | Every routing pair any router may hold is refused for a key or reaches that key's one owner. |
| `NoKeyLost` | invariant | both | The owner holds every value acknowledged to a writer (or, under retention, the gate declines to answer). |
| `NoResurrection` | invariant | both | No read through any pair returns a value older than one already acknowledged. |
| `SagaBatchOnOneCopy` | invariant | ownership | A committed saga holds buckets only on its bound copy and the copy that copy mirrors into. |
| `AtomicOnOwner` | invariant | both | Until the later write, a fresh reader sees the batch on both keys or on neither. |
| `OwnerMonotonic` | invariant over a ghost history | both | A fresh reader's value never moves backwards, except across the undo's swap, which discards the resized copy's writes by contract. |
| `SplitCompletes` | liveness | both | A split that opened its window finishes. |
| `ReshardCompletes` | liveness | ownership | A reshard that started reaches its target. |
| `ResizeCompletes` | liveness | both | A resize that started is purged or undone. |
| `SagaCompletes` | liveness | both | A saga that started completes. |
| `RoutingConverges` | liveness | ownership | Eventually the registry's own pair serves every key. |
| `ReadableOnceComplete` | invariant | retention | Once a saga has completed, and while its row is neither retired nor masked, no read at the owner is gated (#4545). |
| `NoLiveBucketAfterForget` | invariant | retention | No copy that can still become the tree keeps a bucket of a saga whose registry row is retired (#4619). |
| `NoLostContribution` | invariant | crdt | The owner's copy of a CRDT key holds every contribution acknowledged to a writer. |
| `NoStrandedBucket` | liveness | retention | A decided saga's bucket on a copy that can still become the tree is eventually consumed, unless the registry retired the row first. |

Every liveness property can fail on a protocol defect under the fairness the
spec asserts, shown by a mutation that leaves that fairness intact. Many of
those mutations are a step that records nothing (`SplitCompletesSweepStalls`),
but some are real protocol defects: `SplitCompletesUndoWithoutAbandon` is a
split of the resized copy an undo retargets, never abandoned, with every
fairness condition kept; `SagaCompletesDiscardedCopyRefusesTerminal`,
`SagaCompletesPurgedCopyRefusesTerminal` and
`NoStrandedBucketTerminalNotMirrored` are a broadcast production did not
finish before its fix.

### Classification

Per #2321's taxonomy, every property above is **reachable**: each has a mutation
that perturbs an action the specification already has and makes it fire, and
none needs an action added. None is unreached, bounded-out or inexpressible in
that sense.

The gaps the refinement notes list are classified there. A second saga
contending for a key, a second split, and a split of the resized copy while the
resize is still in flight are **bounded out** by the instance. The abandon of a
split an alias move retargets (`AbandonRetargetedSplitAsync`) is modelled for
the move an undo of a resize makes (`SplitAbandon`, #4478); the other alias
cutovers are not modelled at all.

## The bounded instance

Both configurations fix two physical copies (`T`, the original; `R`, the resize
destination), two shards per copy, and two keys: `k1`, whose slot never moves,
and `k2`, whose slot an adaptive split moves from `s1` to `s2`. One split, one
resize, one saga writing `k1` and `k2`, and one later write of `k2`; the
ownership module adds one reshard and one refused flip, the retention module one
delayed forward, one mask toggle at a time, one retirement and one reactivation.

## Defects this area found

Each was found by TLC against the intended design, filed, and kept as a
standing mutation. The refinement notes record which are fixed.

| Issue | Defect | Mutation |
|-------|--------|----------|
| #4452 | A split in flight across a resize: the resize neither captures nor fences the split target, and an undo restores a pre-split map (fixed, #4466; relaxed for a split by #4478, which made the mirror and the terminal follow it) | `UniqueOwnerSplitDuringResize`, `NoKeyLostResizeDuringSplit`, `NoKeyLostRetainedSplitDuringResize`, `NoKeyLostSplitInSoftDeleteWindow` |
| #4453 | The undo cleared the old copy's fence before the swap and armed the resized copy after it (fixed, #4457) | `UniqueOwnerUndoClearsBeforeSwap` |
| #4454 | The mid-dispatch re-bind ignores the bound copy's mirror (fixed, #4521) | `SagaBatchOnOneCopyRebindIgnoresMirror` |
| #4455 | The online snapshot does not copy prepared buckets (fixed, #4506) | `OwnerMonotonicSnapshotSkipsBuckets`, `OwnerMonotonicRetainedSnapshotDropsBuckets` |
| #4473 | The split's sweep treats Indeterminate as InFlight (fixed, #4561) | `OwnerMonotonicSweepIndeterminateLeavesMarker` |
| #4474 | A saga bound to the copy an undo discarded never completes: the discarded copy refuses its terminals and the broadcast does not follow the refusal. Following it to the old copy, the naive fix, lands part of the batch there (fixed, #4516) | `SagaCompletesDiscardedCopyRefusesTerminal`, `AtomicOnOwnerDiscardedCopyTerminalRedirects` |
| #4475 | A saga bound to a purged old copy never completes (fixed, #4531 and #4581) | `SagaCompletesPurgedCopyRefusesTerminal` |
| #4522 | A saga value installed at a stamp other than its own prepare stamp overwrites a later acknowledged write: the backstop's and the drain's fresh stamps, the resize mirror's re-minted prepare, and the snapshot's fresh-stamp resolution (found while confirming #4475's design; fixed, #4566, #4610 and #4629) | `NoKeyLostFreshStampBackstop`, `NoKeyLostRetainedFreshStampBackstop`, `NoKeyLostFreshStampDrainOverMigratedRow`, `NoKeyLostResizeMirrorUnmarkedPrepare`, `NoKeyLostSnapshotResolvesAtFreshStamp` |
| #4564 | A cross-shard migration import is dropped over a non-migrated destination row, so a later write the split carries is lost (found while confirming #4522's design; fixed, #4600) | `NoKeyLostMigrationImportDropped` |
| #4545 | A shadow marker installed after its terminal is copied by a leaf split to a sibling that never sees the terminal, gating the key after the saga completed (found by 10238ade's CI triage; fixed, #4608 and #4644) | `ReadableOnceCompleteDeadMarkerTransferred`, `ReadableOnceCompleteMarkerWithoutSelfCheck`, `ReadableOnceCompleteWitnessNotCarried`, `ReadableOnceCompleteWitnessNotDurable` |
| #4503 | A router that cached the old copy reads empty and loses writes once that copy is purged (found by review #4435, which showed the purge's timing assumption false; fixed, #4528) | `NoResurrectionPurgedCopyServesEmpty`, `NoKeyLostPurgedCopyAcceptsWrites`, `NoResurrectionRetainedPurgedCopyServesEmpty` |
| #4611 | A CRDT prepare applied by the terminal's backstop is installed last-writer-wins, losing contributions the row gained after staging (found while extending this area to CRDT keys; fixed, #4617) | `NoLostContributionBackstopInstallsLastWriterWins` |
| #4613 | A split's import of a CRDT row is dropped over the destination's own fold, and CRDT writes were not forwarded during the split (fixed, #4626) | `NoLostContributionSplitImportDropsOverOwnRow` |
| #4618 | A resize neither mirrors a CRDT write nor joins the rows it merges into the resized copy (fixed, #4665) | `NoLostContributionResizeWriteNotMirrored`, `NoLostContributionResizeDrainLastWriterWins` |
| #4619 | A late forward of a saga whose registry row was retired is bucketed on a leaf that lost its memory of the terminal, stranded for good (found while answering #4619's question; fixed, #4638) | `NoLiveBucketAfterForgetLateForwardBucketed`, `NoLiveBucketAfterForgetForwardRecreatesRow`, `AtomicOnOwnerParticipantRowBestEffort` |

#4445 (a late forwarded orphan read past the terminal) was fixed elsewhere
(#4461); its mutation is `NoResurrectionLatePrepareActivationMemory`, and the
module showed the stale read needs no reactivation at all.

## How to run TLC

From this directory, with the toolchain described in
[how to run TLC](../README.md#how-to-run-tlc):

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC -workers 2 -lncheck final -config ShardOwnership.cfg ShardOwnership.tla
java -cp /path/to/tla2tools.jar tlc2.TLC -workers 2 -lncheck final -config ShardOwnershipRetention.cfg ShardOwnershipRetention.tla
java -cp /path/to/tla2tools.jar tlc2.TLC -workers 2 -config ShardOwnershipCrdt.cfg ShardOwnershipCrdt.tla
```

`-lncheck final` defers the liveness check to the end of the search, which is
how the CI gate runs it. A clean run ends with
`Model checking completed. No error has been found.` and reports the
distinct-state count in [Counts](#counts).

## Last checked

The modules were checked with tla2tools v1.7.4 on a Temurin 17 JDK, liveness
checked at the end, deadlock checking on, on a 16-core workstation, with two
TLC workers for `ShardOwnership` and four for the other two (wall clock
includes JVM start-up):

```
ShardOwnership:          100,666 distinct states, depth 26, clean, 1 min 32 s
ShardOwnershipRetention: 142,980 distinct states, depth 24, clean, 1 min 18 s
ShardOwnershipCrdt:        1,573 distinct states, depth 11, clean, 2 s
```

The current specifications are model-checked in CI by
`TlcModelCheckTests.The_base_specification_holds`.

## Counts

The modules' current totals. `SpecModuleDiscoveryTests` checks this table
against the manifests, and the other Formal gates check the manifests against
the specifications, the cfgs, the mutation catalogues, the refinement notes and
TLC's own state counts.

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `ShardOwnership` | 7 | 5 | 28 | 44 | 38 | 100,666 |
| `ShardOwnershipRetention` | 7 | 4 | 27 | 36 | 36 | 142,980 |
| `ShardOwnershipCrdt` | 2 | 0 | 10 | 9 | 10 | 1,573 |

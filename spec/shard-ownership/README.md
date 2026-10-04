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
documents.

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

### What is lost by not composing them

A behaviour that needs a retention event **together with** something only
`ShardOwnership` has is checked by neither module:

- a **stale router writing** while the registry masks or has retired the row;
- a saga **re-binding** (`SagaRebindOnRefusal`, `SagaRebindBeforeDecision`) and
  then meeting a mask, a retirement, a late forward or a reactivation;
- a **reshard-driven** split under retention (the retention module's split is
  adaptive; the two differ only in the interlock that admits them);
- a **refused flip** or an **undo before the flip** under retention.

Two arguments narrow, but do not close, that gap. Retention acts on a saga's
buckets and the registry's answer about them, and each of the excluded
behaviours changes where buckets land or which copy serves, which
`ShardOwnership` checks exhaustively under a registry that always answers.
And the retention module's readers may hold any pair the registry ever
published, so a stale *read* under retention is covered. What is not argued is
that a stale *write* landing a bucket somewhere unusual cannot then be stranded
or reverted by a retention event; that is the composition this split gives up.
The review issue for this area (#4435) should audit exactly this seam.

### Budget

Each module's full configuration, liveness included, runs on two TLC workers
inside the per-run limit, with headroom; the figures are under
[Last checked](#last-checked). Three choices in the retention module are
budget-driven and argued where they are made: `Reactivate` is offered on the
split destination only, `NoStrandedBucket` is one leads-to rather than one per
bucket, and `RegistryMask` is enabled only once there is a decision to mask.
Each loses no behaviour the instance can distinguish.

## What is modelled

- **The split** (`SplitBegin`, `SplitSweep`, `SplitFreeze`, `SplitCommit`): an
  adaptive split of `k2`'s slot from `s1` to `s2` on the copy the tree resolves
  to, with its shadow-write window, retroactive sweep, Reject freeze and final
  drain before the map moves.
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
  `DeliverLate` and `Reactivate`.
- **A later write** (`LaterWrite`) of `k2`, which gives `NoResurrection` a newer
  value to protect.

Both modules model the **intended** design where production has an open defect,
and keep a mutation that reproduces production as it stands. The refinement
notes list each one with its issue.

## Properties checked

| Property | Kind | Module | Meaning |
|----------|------|--------|---------|
| `TypeOK` | invariant | both | State stays well-typed. |
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
| `NoStrandedBucket` | liveness | retention | A decided saga's bucket on a copy that can still become the tree is eventually consumed, unless the registry retired the row first. |

Every liveness property can fail on a protocol defect under the fairness the
spec asserts, shown by a mutation that leaves that fairness intact (for
example `SplitCompletesSweepStalls`, `NoStrandedBucketTerminalNotMirrored`).

### Classification

Per #2321's taxonomy, every property above is **reachable**: each has a mutation
that perturbs an action the specification already has and makes it fire, and
none needs an action added. None is unreached, bounded-out or inexpressible in
that sense.

The gaps the refinement notes list are classified there. The one that is
**blindly inexpressible** is worth naming here: production can abandon a split
when an alias move retargets it (`AbandonRetargetedSplitAsync`), and neither
module has that action, because their interlock makes it unreachable. The
mutations that break the interlock therefore let a stranded split count as
finished, standing in for the abandon rather than adding it; each says so in its
header. A second saga contending for a key, a second split and a split of the
resized copy during the resize are **bounded out** by the instance.

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
| #4452 | A split in flight across a resize: the resize neither captures nor fences the split target, and an undo restores a pre-split map (fixed, #4466) | `UniqueOwnerSplitDuringResize`, `NoKeyLostResizeDuringSplit`, `NoKeyLostSplitInSoftDeleteWindow` |
| #4453 | The undo cleared the old copy's fence before the swap and armed the resized copy after it (fixed, #4457) | `UniqueOwnerUndoClearsBeforeSwap` |
| #4454 | The mid-dispatch re-bind ignores the bound copy's mirror | `SagaBatchOnOneCopyRebindIgnoresMirror` |
| #4455 | The online snapshot does not copy prepared buckets | `OwnerMonotonicSnapshotSkipsBuckets`, `OwnerMonotonicRetainedSnapshotDropsBuckets` |
| #4473 | The split's sweep treats Indeterminate as InFlight | `OwnerMonotonicSweepIndeterminateLeavesMarker` |
| #4474 | Terminals for a saga bound to the copy an undo discarded are re-sent to the old copy | `AtomicOnOwnerDiscardedCopyTerminalRedirects` |
| #4475 | A saga bound to a purged old copy never completes | `SagaCompletesPurgedCopyRefusesTerminal` |
| #4503 | A router that cached the old copy reads empty and loses writes once that copy is purged (found by review #4435, which showed the purge's timing assumption false) | `NoResurrectionPurgedCopyServesEmpty`, `NoKeyLostPurgedCopyAcceptsWrites`, `NoResurrectionRetainedPurgedCopyServesEmpty` |

#4445 (a late forwarded orphan read past the terminal) was fixed elsewhere
(#4461); its mutation is `NoResurrectionLatePrepareActivationMemory`, and the
module showed the stale read needs no reactivation at all.

## How to run TLC

From this directory, with the toolchain described in
[how to run TLC](../README.md#how-to-run-tlc):

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC -workers 2 -lncheck final -config ShardOwnership.cfg ShardOwnership.tla
java -cp /path/to/tla2tools.jar tlc2.TLC -workers 2 -lncheck final -config ShardOwnershipRetention.cfg ShardOwnershipRetention.tla
```

`-lncheck final` defers the liveness check to the end of the search, which is
how the CI gate runs it. A clean run ends with
`Model checking completed. No error has been found.` and reports the
distinct-state count in [Counts](#counts).

## Last checked

Both modules were checked with tla2tools v1.7.4 on a Temurin 17 JDK, two TLC
workers, liveness checked at the end, deadlock checking on, on a 16-core
workstation (wall clock includes JVM start-up):

```
ShardOwnership:          69,088 distinct states, depth 26, clean, 1 min 18 s
ShardOwnershipRetention: 82,155 distinct states, depth 24, clean, 1 min 36 s
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
| `ShardOwnership` | 7 | 5 | 27 | 35 | 37 | 69,088 |
| `ShardOwnershipRetention` | 5 | 4 | 25 | 26 | 32 | 82,155 |

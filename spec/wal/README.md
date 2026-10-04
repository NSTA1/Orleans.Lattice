# TLA+ specification of the WAL durability lifecycle

This directory holds the formal specification of the Orleans.Lattice
write-ahead log (WAL) under crash-anywhere recovery. It is part of epic #4430 and
the deliverable of issue #4432. It follows the pattern the atomic-commit module
set: a TLA+ design checked by TLC in CI, every property and every action paired
with a mutation that makes a property fire, a refinement note mapping each
construct to production and to detector tests, pure cores in the product
assembly, and a Coyote model that executes those cores.

## Modules

| Module | What it specifies |
|--------|-------------------|
| [`WalDurability.tla`](WalDurability.tla) | The leaf side, end to end: append, out-of-order flush and acknowledgement, a shared stream read through per-leaf READ positions, checkpoint persists that can fail, snapshot captures that can fail, durable materialiser pins, the GC trim floor and its block pins, and leaf and shard crashes at any step with recovery from a snapshot or cold. |
| [`WalMove.tla`](WalMove.tla) | A shard move: fence, quiesce, copy and switch, concurrent with appends, out-of-order flushes, a consumer, the GC and a shard crash. |

They are two modules because a move touches none of the leaf state. Keeping the
move out of `WalDurability` keeps that module small enough to check its liveness
properties exhaustively within the CI fixture's per-run ceiling.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `WalDurability` | 8 | 4 | 14 | 20 | 25 | 116,530 |
| `WalMove` | 5 | 1 | 9 | 9 | 13 | 497 |

`Actions` counts the disjuncts of `Next`, including `WalMove`'s non-behavioural
`Stutter`. `Behaviour rows` counts the action rows of the module's refinement note,
excluding non-behavioural actions, plus its property rows. `Distinct states` is
TLC's count for the module's own cfg. `WalDurability` searches to depth 27 and
`WalMove` to depth 15, with tla2tools v1.7.4.

## Files

| File | What it is |
|------|-----------|
| `WalDurability.tla` / `.cfg` / `.manifest.json` | The leaf-lifecycle module, its TLC model and its manifest. |
| [`mutations/`](mutations/) | One or more deliberate defects per property and per action of `WalDurability`. |
| [`Refinement.md`](Refinement.md) | `WalDurability` mapped to production: variables, actions, properties, detectors, classification and gaps. |
| `WalMove.tla` / `.cfg` / `.manifest.json` | The move module, its TLC model and its manifest. |
| [`move-mutations/`](move-mutations/) | `WalMove`'s mutation catalogue. |
| [`MoveRefinement.md`](MoveRefinement.md) | `WalMove` mapped to production. |

## Properties checked

`WalDurability`:

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `AckedWriteDurable` | Invariant | Every acknowledged write is recoverable by its owner from its durable snapshot and the readable WAL. |
| `TrimCoveredBySnapshot` | Invariant | The GC never trims an acknowledged write its owner's durable snapshot does not hold. |
| `ReadPositionHonest` | Invariant | An active leaf's read position never passes an acknowledged write it owns and does not hold. |
| `ShippingNeverSkips` | Invariant | No reader passes an offset still in flight. |
| `OffsetContiguity` | Invariant | No acknowledged offset is reissued. |
| `RecoveryNeverFallsOffLog` | Invariant | No leaf ever latches `LeafProjectionStaleException`. |
| `PersistedBeliefHonest` | Invariant | A leaf's belief about its persisted checkpoint is what storage holds, or the anchor it started from. |
| `SnapshotCoverageMonotonic` | Action | Durable snapshot coverage never regresses. |
| `PublishedPinWithinPersistedBelief` | Action | A newly published pin never exceeds the persisted checkpoint. |
| `EveryAckedWriteMaterialised` | Liveness | Every acknowledged write is eventually held by its owner's projection. |
| `ReclamationEventuallyAdvances` | Liveness | The WAL is eventually fully reclaimed. |

`WalMove`:

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `MovedStreamKeepsAckedWrites` | Invariant | A move never loses an acknowledged write. |
| `CopyTakenQuiesced` | Invariant | The copy is taken only from a quiesced stream. |
| `ReaderNeverPassesHole` | Invariant | The consumer never passes an append still in flight. |
| `AllocatorNeverReissues` | Invariant | A recovered allocator never reissues an acknowledged offset. |
| `StreamEventuallyComplete` | Liveness | A move's fence is always eventually lowered. |

## Defects this specification found

Building the model against production surfaced four durability defects. Each was
reproduced against the real `BPlusLeafGrain` before it was filed (except #4467,
found in source by the fix session and reproduced only by the model so far). The
model specifies the INTENDED design for each, and a standing mutation reproduces
current production behaviour so the property keeps firing on it:

| Issue | Defect | Standing mutation |
|-------|--------|-------------------|
| #4450 | A snapshot that fails to load falls through to a cold replay of a WAL trimmed under its coverage; the leaf comes up silently missing acknowledged writes. **Fixed by #4470**: the replay now fails closed. | `ReadPositionHonestLoadFailureColdReplays`, now an ordinary regression mutation |
| #4451 | A capture during a cold rebuild claims the persisted checkpoint as coverage for a partly rebuilt projection, licensing the GC to trim rows that exist nowhere else. | `TrimCoveredBySnapshotColdCaptureOverclaims` |
| #4456 | A never-written leaf releases its block pin at its persisted checkpoint above its snapshot's coverage, and its next activation latches stale. | `RecoveryNeverFallsOffLogNeverWrittenReleaseUnbounded` |
| #4467 | A cold rebuild that faults part-way re-arms warm from the persisted checkpoint over a partial projection. | `ReadPositionHonestFaultedColdReplayResumesWarm` |

The model also showed that one proposed fix for #4450 - cold-starting whenever the
WAL prefix probes intact - is unsafe. The leaf's pin was resolved against the
snapshot that failed to load and cannot be lowered, so the GC may trim under it
while the cold rebuild runs. `ReadPositionHonestLoadFailureColdStartsOverIntactWal`
keeps that standing.

When a fix lands, its gap row in `Refinement.md` is removed and its detector is
re-proven red against the reproducing mutation.

## What the assurance covers, and what it does not

It covers the leaf lifecycle's durability logic on one partition shared by two
leaves, with one fault per behaviour, and the move protocol with one move and one
crash. It does NOT cover:

- multi-partition checkpoint arrays;
- splits, resharding and saga state;
- retention TTLs, which trim past the floor by design;
- interleavings inside a grain turn's awaits;
- replication consumers, beyond their effect on the trim floor;
- any composition of two faults.

The full list is under the abstraction gaps of each refinement note. A property that
holds here is evidence about the modelled design, not about the gaps.

## How to run TLC

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalDurability.cfg WalDurability.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalMove.cfg WalMove.tla
```

Pass `-metadir` with a directory outside the repository, or delete the `states/`
directory TLC leaves beside the module. `WalDurability` takes about a minute and a
half on four workers, because it checks two liveness properties over the full
state graph; `WalMove` takes seconds.

## The Coyote companion

`WalDurabilityLifecycleModel` (`test/lattice/BPlusTree/Coyote/`) runs the same
lifecycle, with crash-anywhere interleavings, against the production cores:

- `WalOffsetAllocationCore`;
- `WalShippingWatermark`;
- `LeafDurablePinCore`;
- `WalGcTrimCore` with `WalGcOffsetAdmission`;
- `WalFallOffCore`.

Its guard tests each remove one fix, and each must be caught by the assertion that
fix protects, identified by its tag in the bug report.

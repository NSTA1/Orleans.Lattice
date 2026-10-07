# TLA+ specification of the WAL durability lifecycle

This directory holds the formal specification of the Orleans.Lattice
write-ahead log (WAL) under crash-anywhere recovery. It is part of epic #4430, the
deliverable of issue #4432 and its review remediation, issue #4433. It follows the
pattern the atomic-commit module set: a TLA+ design checked by TLC in CI, every
property and every action paired with a mutation that makes a property fire, a
refinement note mapping each construct to production and to detector tests, pure
cores in the product assembly, and a Coyote model that executes those cores.

## Modules

| Module | What it specifies |
|--------|-------------------|
| [`WalDurability.tla`](WalDurability.tla) | The leaf side, end to end: append, out-of-order flush and acknowledgement, a shared stream read through per-leaf READ positions, checkpoint persists that can fail, snapshot captures that can fail, durable materialiser pins, the GC trim floor and its block pins, and leaf and shard crashes at any step with recovery from a snapshot or cold. |
| [`WalMove.tla`](WalMove.tla) | A shard move: a durable fence under a lease and the source activation's fence, quiesce, copy and switch, concurrent with appends, out-of-order flushes, a consumer, the GC, a shard crash and the loss of the move's coordinator. |

They are two modules because a move touches none of the leaf state. Keeping the
move out of `WalDurability` keeps that module small enough to check its liveness
properties exhaustively within the CI fixture's per-run ceiling.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `WalDurability` | 11 | 4 | 25 | 42 | 38 | 111,154 |
| `WalMove` | 5 | 2 | 13 | 18 | 18 | 1,617 |

`Actions` counts the disjuncts of `Next`, including `WalMove`'s non-behavioural
`Stutter`. `Behaviour rows` counts the action rows of the module's refinement note,
excluding non-behavioural actions, plus its property rows. `Distinct states` is
TLC's count for the module's own cfg. `WalDurability` searches to depth 27 and
`WalMove` to depth 16, with tla2tools v1.7.4.

`WalDurability` also has one variant configuration, `WalDurability.TwoFaults.cfg`
(see "Variant configurations" in [`../README.md`](../README.md)). It checks every
invariant and both action properties with a budget of two faults instead of one:
680,122 distinct states to depth 32. The liveness properties stay at one fault,
because at two the full configuration takes about ten minutes, past the TLC budget.

Its second variant, `WalDurability.SnapshotLoss.cfg`, lets the environment destroy a
leaf's durable snapshot (`SnapshotVanish`, issue #4634) or its state row
(`LeafRowVanish`, issue #4654), and the operator purge the tree (`PurgeClear`), at two
faults so a snapshot and a row can both vanish. Destroyed data is outside the durability
properties by construction, so it checks the properties that say the loss is never
silent - `ReadPositionHonest` above all, carved out only for the writes a purge
deleted (issue #4700) - with the other safety invariants that still apply: 3,277,793
distinct states. A purge marks a leaf before clearing it, and recovery re-creates only
a marked leaf (`MarkPurge`, `PurgeClear`, `Recover`); the lifecycle resumes once
recovery has reset every marker.

`WalMove` has one too, `WalMove.TwoMoves.cfg`: two moves of the same stream, each
with its own coordinator, so one can take over the other's lapsed fence while the
other is still copying. It checks every property, safety and liveness: 16,225
distinct states to depth 21, about fifteen seconds on two workers.

## Files

| File | What it is |
|------|-----------|
| `WalDurability.tla` / `.cfg` / `.manifest.json` | The leaf-lifecycle module, its TLC model and its manifest. |
| `WalDurability.TwoFaults.cfg` | The same module checked against its safety properties at two faults. |
| [`mutations/`](mutations/) | One or more deliberate defects per property and per action of `WalDurability`. |
| [`Refinement.md`](Refinement.md) | `WalDurability` mapped to production: variables, actions, properties, detectors, classification and gaps. |
| `WalMove.tla` / `.cfg` / `.manifest.json` | The move module, its TLC model and its manifest. |
| `WalMove.TwoMoves.cfg` | The move module with two moves contending for the stream, checked with every property. |
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
| `ReleaseBackedBySnapshot` | Invariant | Every trim entitlement the pin store has published is backed by the leaf's durable snapshot coverage. |
| `SnapshotCoverageMonotonic` | Action | Durable snapshot coverage never regresses. |
| `PublishedPinWithinPersistedBelief` | Action | A newly published pin never exceeds the persisted checkpoint. |
| `EveryAckedWriteMaterialised` | Liveness | Every acknowledged write is eventually held by its owner's projection. |
| `ReclamationEventuallyAdvances` | Liveness | Everything appended is eventually reclaimed from storage, and no append or abandoned call is left outstanding. |

`WalMove`:

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `MovedStreamKeepsAckedWrites` | Invariant | A move never loses an acknowledged write. |
| `CopyTakenQuiesced` | Invariant | While the move holds its fence, a copied stream has nothing in flight. |
| `ReaderNeverPassesHole` | Invariant | The consumer never passes an append still in flight. |
| `AllocatorNeverReissues` | Invariant | A recovered allocator never reissues an acknowledged offset. |
| `StreamEventuallyComplete` | Liveness | A move's fence is always eventually lowered. |
| `FenceEventuallyReleased` | Liveness | A durable fence is never held for ever, even once its coordinator is lost. |

## Defects this specification found

Building the model against production surfaced four durability defects. Each was
reproduced against the real `BPlusLeafGrain` before it was filed (except #4467,
found in source by the fix session and reproduced only by the model). The model
specifies the INTENDED design for each, and a mutation reproduced production's
behaviour so the property kept firing on it until the fix landed. All four are
fixed, and those mutations are ordinary regression checks:

| Issue | Defect | Mutation |
|-------|--------|----------|
| #4450 | A snapshot that fails to load falls through to a cold replay of a WAL trimmed under its coverage; the leaf comes up silently missing acknowledged writes. **Fixed by #4470**: the replay now fails closed. | `ReadPositionHonestLoadFailureColdReplays` |
| #4451 | A capture during a cold rebuild claims the persisted checkpoint as coverage for a partly rebuilt projection, licensing the GC to trim rows that exist nowhere else. **Fixed by #4489**: an unanchored capture claims only what has been re-read. | `TrimCoveredBySnapshotColdCaptureOverclaims` |
| #4456 | A never-written leaf releases its block pin at its persisted checkpoint above its snapshot's coverage, and its next activation latches stale. **Fixed by #4497**: the release is bounded by coverage. | `RecoveryNeverFallsOffLogNeverWrittenReleaseUnbounded` |
| #4467 | A cold rebuild that faults part-way re-arms warm from the persisted checkpoint over a partial projection. **Fixed by #4489**: the retry stays cold. | `ReadPositionHonestFaultedColdReplayResumesWarm` |

The independent review (#4433) found two more, both hidden by a bound or an
abstraction the first version had. Both are fixed, and their mutations are ordinary
regression checks:

| Issue | Defect | Mutation |
|-------|--------|-------------------|
| #4523 | A never-written leaf that holds no snapshot released at its persisted checkpoint; a later cold rebuild captures below that release, the GC trims past it, and the next activation latches stale. Needs two faults, which the one-fault configuration hid. **Fixed by #4535**: the release fires only under durable coverage. | `ReleaseBackedBySnapshotNoSnapshotReleasesCheckpoint` (no fault needed) and `RecoveryNeverFallsOffLogNoSnapshotReleaseTwoFaults` (the two-fault composition) |
| #4525 | A move's fence lives only in the source activation's memory and the flip re-checks nothing about the source, so a source re-activated after the copy acknowledges writes the flip discards. The first model reset the move on a shard crash and could not see it. **Fixed by #4557**: a durable fence every new activation re-derives, a flip that requires it still held, and a drain that waits out abandoned provider work. | `MovedStreamKeepsAckedWritesFenceInMemoryOnly` |

The model also showed that one proposed fix for #4450 - cold-starting whenever the
WAL prefix probes intact - is unsafe. The leaf's pin was resolved against the
snapshot that failed to load and cannot be lowered, so the GC may trim under it
while the cold rebuild runs. `ReadPositionHonestLoadFailureColdStartsOverIntactWal`
keeps that standing.

The models found six more while the review's follow-ups were closed out, each
reproduced before its fix and each fixed:

| Issue | Defect | Check |
|-------|--------|-------|
| #4621 | A flush abandoned at its deadline can land after readers have passed its offset, below their cursors. **Fixed by #4659**: the watermark is held below every abandoned call until it settles, and a provider trim watermark tells a hole from a trim. | `LogPrefixAppliedWatermarkIgnoresAbandoned`, `ShippingNeverSkipsTrailingHoleExposed` |
| #4634 | A cold start over a vanished snapshot replays a WAL trimmed under it. **Fixed by #4653**: the leaf row records held coverage and the activation fails closed. | `ReadPositionHonestVanishedSnapshotStartsCold`, `ReadPositionHonestVanishedSnapshotTailZeroShortcut` |
| #4654 | A leaf whose state row vanishes comes up empty. **Fixed by #4671**: a rowless leaf serves data only under a create intent. | `ReadPositionHonestRowlessActivationStartsCold`, `ReadPositionHonestSelfHealPassesIntent` |
| #4622 | A retention TTL yields only to a `Zero` pin, so it trims a write made after an empty release. **Fixed by #4646**: the TTL ceiling is capped at the lowest frontier of a partition's uncovered pins. | `WalPartitionReleaseCoyoteTests.A_ttl_that_yields_only_to_zero_pins_trims_a_write_made_after_an_empty_release` |
| #4669 | A cold leaf's checkpoint flush tail releases a partition its replay has not read. **Fixed by #4677**: no empty release before the replay barrier latches. | `WalPartitionReleaseCoyoteTests.An_empty_release_published_before_the_partition_is_replayed_trims_an_unread_write` |
| #4641 | A write stamped below an empty release's frontier is trimmed by every stamp-based GC arm. **Fixed by #4679**: a durable override hold the GC reads as a block until coverage lands. | `WalPartitionReleaseCoyoteTests.Skipping_the_override_hold_lets_the_gc_trim_an_override_stamped_write_issue_4641` |

The last three rows of that table depend on HLC stamps and on several partitions,
which `WalDurability.tla` abstracts away; `WalPartitionReleaseModel`, a Coyote
model over the production pin and trim cores, checks them (see the refinement note's
abstraction gaps).

When a fix lands, its gap row is removed and its detectors are re-proven red against
the reproducing mutation.

## What the assurance covers, and what it does not

It covers the leaf lifecycle's durability logic on one partition shared by two
leaves - with two faults per behaviour for safety and one for liveness - and the
move protocol with one shard crash and one coordinator crash, for one move and, in the
`TwoMoves` variant, for two moves contending for the stream (a move may take over
another's lapsed fence). It does NOT
cover:

- multi-partition checkpoint arrays, HLC stamps and retention TTLs in TLC; these are
  checked by `WalPartitionReleaseModel` instead;
- splits, resharding and saga state;
- interleavings inside a grain turn's awaits;
- replication consumers, beyond their effect on the trim floor;
- liveness under two faults, or any property under three.

The full list is under the abstraction gaps of each refinement note. A property that
holds here is evidence about the modelled design, not about the gaps.

## How to run TLC

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalDurability.cfg WalDurability.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalDurability.TwoFaults.cfg WalDurability.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalMove.cfg WalMove.tla
```

Pass `-metadir` with a directory outside the repository, or delete the `states/`
directory TLC leaves beside the module. On two workers `WalDurability` takes about
two minutes, because it checks two liveness properties over the full state graph;
its `TwoFaults` variant takes about forty seconds; `WalMove` takes seconds, and its
`TwoMoves` variant about fifteen.

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
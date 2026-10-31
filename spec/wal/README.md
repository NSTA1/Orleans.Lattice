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
| [`WalPinRetirement.tla`](WalPinRetirement.tla) | The GC retiring orphaned materialiser pins - the bulk orphan sweep and the starvation drive, each taking a token per pin-store key, reading the leaf's durable row and removing through a fence as separate steps, two sweepers racing - against the leaf's fold, purge, discard and re-creation under the same grain id. See [below](#walpinretirement). |

They are separate modules because a move touches none of the leaf state, and pin
retirement only the leaf's lifecycle, not its replay. Keeping each out of
`WalDurability` keeps that module small enough to check its liveness properties
exhaustively within the TLC budget.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `WalDurability` | 11 | 4 | 26 | 43 | 39 | 226,592 |
| `WalMove` | 5 | 2 | 13 | 18 | 18 | 1,617 |
| `WalPinRetirement` | 3 | 3 | 25 | 31 | 30 | 284,019 |

`Actions` counts the disjuncts of `Next`, including `WalMove`'s non-behavioural
`Stutter`. `Behaviour rows` counts the action rows of the module's refinement note,
excluding non-behavioural actions, plus its property rows. `Distinct states` is
TLC's count for the module's own cfg. `WalDurability` searches to depth 35 and
`WalMove` to depth 16, with tla2tools v1.7.4.

`WalDurability` has a two-fault variant configuration, `WalDurability.TwoFaults.cfg`
(see "Variant configurations" in [`../README.md`](../README.md)). It checks every
invariant and both action properties with a budget of two faults instead of one:
1,310,600 distinct states to depth 42. The liveness properties stay at one fault,
because at two the full configuration takes about ten minutes, past the TLC budget.

Its second variant, `WalDurability.SnapshotLoss.cfg`, lets the environment destroy a
leaf's durable snapshot (`SnapshotVanish`, issue #4634) or its state row
(`LeafRowVanish`, issue #4654), and the operator purge the tree (`PurgeClear`), at two
faults so a snapshot and a row can both vanish. Destroyed data is outside the durability
properties by construction, so it checks the properties that say the loss is never
silent - `ReadPositionHonest` above all, carved out only for the writes a purge
deleted (issue #4700) - with the other safety invariants that still apply: 6,402,166
distinct states to depth 42. A purge marks a leaf before clearing it, and recovery re-creates only
a marked leaf (`MarkPurge`, `PurgeClear`, `Recover`); the lifecycle resumes once
recovery has reset every marker.

The `WalDurability.WarmLeaf.cfg` variant checks every property without faults:
17,041 distinct states to depth 28. Replay requires a scheduled request, consumed
by each bounded read slice; foreground writes cannot arm it. Weak fairness belongs
to the timer/GC scheduling trigger and then the requested read, assuming eventual
admission and responsive storage. The #3314 mutation
`ReclamationEventuallyAdvancesWarmLeafNeedsCheckpoint` reproduces the circular
birth-time registration gate: a warm foreground-only leaf needs a checkpoint
before its timer can schedule the read that earns that checkpoint. The production
regression uses the actual timer, loads accepted snapshot coverage and performs
real GC without deactivation or checkpoint hints.

`WalMove` has one too, `WalMove.TwoMoves.cfg`: two moves of the same stream, each
with its own coordinator, so one can take over the other's lapsed fence while the
other is still copying. It checks every property, safety and liveness: 16,225
distinct states to depth 21, about fifteen seconds on two workers.

## Files

| File | What it is |
|------|-----------|
| `WalDurability.tla` / `.cfg` / `.manifest.json` | The leaf-lifecycle module, its TLC model and its manifest. |
| `WalDurability.WarmLeaf.cfg` | Permanently active leaves, with no faults and all safety and liveness properties. |
| `WalDurability.TwoFaults.cfg` | The same module checked against its safety properties at two faults. |
| `WalDurability.SnapshotLoss.cfg` | Snapshot and state-row loss and an operator purge, at two faults, checked against the safety properties that still apply. |
| [`mutations/`](mutations/) | One or more deliberate defects per property and per action of `WalDurability`. |
| [`Refinement.md`](Refinement.md) | `WalDurability` mapped to production: variables, actions, properties, detectors, classification and gaps. |
| `WalMove.tla` / `.cfg` / `.manifest.json` | The move module, its TLC model and its manifest. |
| `WalMove.TwoMoves.cfg` | The move module with two moves contending for the stream, checked with every property. |
| [`move-mutations/`](move-mutations/) | One or more deliberate defects per property and per action of `WalMove`. |
| [`MoveRefinement.md`](MoveRefinement.md) | `WalMove` mapped to production. |
| `WalPinRetirement.tla` / `.cfg` / `.manifest.json` | The pin-retirement module, its TLC model and its manifest. |
| `WalPinRetirement.OneFault.cfg` | The pin-retirement module with one environment fault, checked against every safety property. |
| `WalPinRetirement.TwoSweepers.cfg` | The pin-retirement module with a second orphan sweeper racing the first and the drive, checked against every safety property. |
| [`pin-retirement-mutations/`](pin-retirement-mutations/) | One or more deliberate defects per property and per action of `WalPinRetirement`. |
| [`PinRetirementRefinement.md`](PinRetirementRefinement.md) | `WalPinRetirement` mapped to production. |
| [`perturbations/`](perturbations/README.md) | Production perturbations for `WalPinRetirement`'s detectors. |

## Properties checked

`WalDurability`:

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `AckedWriteDurable` | Invariant | Every acknowledged write is recoverable by its owner from its durable snapshot and the readable WAL. |
| `TrimCoveredBySnapshot` | Invariant | The GC never trims an acknowledged write its owner's durable snapshot does not hold. |
| `ReadPositionHonest` | Invariant | An active leaf's read position never passes an acknowledged write it owns and does not hold. |
| `LogPrefixApplied` | Invariant | No entry becomes readable below a reader's position: every readable entry a leaf owns at or below its read position is in its projection, acknowledged or not. |
| `ClearRecorded` | Invariant | Every leaf a purge cleared keeps its own purge marker until recovery re-creates it. |
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
four minutes, because it checks two liveness properties over the full state graph;
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

## WalPinRetirement

`WalPinRetirement` models the decision `spec/UNMODELLED.md` used to record as a gap:
when the GC may remove a leaf's durable materialiser pin. Removing a pin lets the
trim floor advance, so removing one a live - or re-creatable - leaf still relies on
licenses a trim over writes it has not checkpointed. The module takes
`WalDurability`'s floor, block pin and trim as given and models what sits beside
them: the leaf's own retirement at its terminal step (a fold's, before its row
clear; a purge's, after it, issue #4700), the deleted-tree hold's retirement of a
discarded copy, a leaf cleared by a purge or a fold populated again under the same grain id, and
the two GC repairs - the bulk orphan sweep, from each silo's loop or the tree's
`WalOrphanPinSweepGrain`, and the starvation drive's retirement after a NotDriven
verdict. Each repair takes a token from every pin-store key, reads the pins, reads
the leaf's durable row once and decides each pin as separate steps, so the leaf can
change under it. A completed purge's tree-level tail - the WAL trim, the pin-store
clear and the registry removal that frees the id - is modelled too, re-driven until
its record lands and acting only on the registry lineage it recorded.

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `NoTrimPastLiveNeed` | Invariant | The WAL is never trimmed past a write a live, or recoverable, leaf still needs to replay. |
| `LivePublisherPinned` | Invariant | A seeded incarnation whose row is bound always holds a pin somewhere in the store. |
| `RetireOnlyHusk` | Action | A GC retirement removes a pin only if its publisher's row was cleared after publishing it and no later incarnation has merged into it. |
| `UnreadableNeverRetired` | Action | A leaf read that faulted never retires a pin. |
| `OrphanEventuallyRetired` | Liveness | A gone leaf's pins are eventually all retired, unless its grain id is populated again first. |

The base checks every property with one sweeper, the drive and no environment
fault (an interrupted self-retirement is not one; it is what leaves orphans). `WalPinRetirement.OneFault.cfg` adds one fault - a routing change that
duplicates the pin across keys, an unreadable leaf read or an abandoned run - and
`WalPinRetirement.TwoSweepers.cfg` races a second sweeper; both check every safety
property, and liveness stays on the base (with two sweepers it takes about eight
minutes). The variants' state counts are in the manifest.

### Defects on this path

| Issue | Defect | Mutations |
|-------|--------|-----------|
| #5358 | The GC removed an orphaned pin by consumer id, unconditionally, after a husk read taken in a separate step. A purge-cleared leaf re-created in between - a recovery re-binding it, a reused tree id re-seeding a deterministic root leaf, a bulk load re-using a leaf id - seeds its block pin under the same consumer id, often into the surviving entry where the monotone merge changes nothing; the removal then erased it and the trim could pass the re-created leaf's un-checkpointed writes. Both the sweep and the drive's NotDriven retirement had the window. **Fixed in this change**: every key's token is taken before its pins are read, the pin grain stamps every merge, `RemoveIfUnmergedSinceAsync` refuses a removal the consumer was merged past, and the drive retires through the same fenced sequence with a fresh durable read. | `NoTrimPastLiveNeedUnfencedRemoval`, `RetireOnlyHuskUnfencedRemoval`, `NoTrimPastLiveNeedDriveRemovesOnProbe` |
| #5416 | Found by the independent review (#5351) rather than by the model: a completed tree purge removed the tree's registry entry - which frees the id for reuse - before it cleared the tree's whole pin store, so a write re-registering the id in between re-seeded the shard's deterministic root leaf under the purged leaf's consumer id, and the clear then erased that block pin, unfenced. **Fixed in the review's remediation**: the pin store is cleared before the registry entry is removed. The model's purge tail is one step, so this ordering is pinned by `TreeDeletionGrainTests` instead (see the refinement note's abstraction gaps). | None |
| #5417 | Found by the independent review (#5351): a completed purge's tail - WAL trim, pin-store clear, registry removal - was re-driven whenever its registry removal was still recorded as owed, and the record is cleared only by a write after the removal. A removal that landed with its record unwritten (the write, or the loop stop before it, threw) was re-driven against an id a write or a create had registered again: the new tree's acknowledged writes were trimmed, its pins cleared and its registration removed. **Fixed**: the purge records the lineage of the registry entry it removes with its completion, and a re-drive acts only while the entry still carries it; otherwise it only records the removal. | `NoTrimPastLiveNeedPurgeRedriveIgnoresLineage`, `NoTrimPastLiveNeedRedriveReReadsLineage` |
| #5458 | Found by the independent review's confirmation pass (#5351), with `Recreate` widened to keep the registry entry's lineage: a purged shard admitted a write whenever the registry named the id live, and while a purge's tail is owed that is the purged tree's own entry. The write re-seeded the root leaf under the purged lineage, so the tail's lineage check took the entry for the purge's own and unpinned and unregistered the live leaf; the acknowledged write was lost on a cold replay. **Fixed**: `ShardRootGrain.PrepareForPurgedCopyAsync` asks the deletion record (`HoldsDeletionAsync`), which holds the id until the purge has removed its registry row, and refuses the write until then. | `LivePublisherPinnedRecreateWhilePurgeHoldsId` |
| #5459 | Found by the same pass: a resized tree's logical purge re-drove its completion - the logical registry entry's removal - against a logical id a create had registered again, the #5417 class on the logical path. **Fixed**: the completion records the lineage of the entry it removes, in its own write before the first removal, and a re-drive that finds the entry gone or carrying another lineage removes nothing. The model has no data-less logical id, so `TreeDeletionGrainTests` pin it (see the refinement note's abstraction gaps). | None |

### What it covers, and what it does not

It covers the retirement decision - the husk read, the consumer-id gate (#4238), the
fence, fail-closed reads - and its interleaving with the leaf's fold, purge,
discard, recovery and re-creation, a routing change, and two sweepers. It does NOT
cover several leaves or partitions per leaf, the pin's HLC frontier and override
holds, the in-memory cursor registry, late delivery of a lost silo's removal
message or a duplicate activation's, the order of the steps inside a purge's
tree-level tail (the pin-store clear before the registry frees the id, issue #5416,
pinned by tests), a resized tree's logical purge completion (pinned by tests), or
the sweep's floor-holder census and budgets; the refinement note's
abstraction gaps list them. Coverage of this module is not coverage of the trim
itself, which is `WalDurability`'s.

### Coyote companion

`WalPinRetirementModel` (`test/lattice/BPlusTree/Coyote/`) executes the production
core, `WalOrphanPinRetirementCore`, against a re-created leaf under every
interleaving. Its guard tests remove the fence, and take the token after the read;
Coyote re-finds the retired pin of a re-created leaf in each.

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalPinRetirement.cfg WalPinRetirement.tla
```

On six workers the base took about a minute and a half, `OneFault` about a minute and
`TwoSweepers` about a minute and a half.
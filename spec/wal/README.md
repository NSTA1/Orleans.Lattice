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
| [`WalDurability.tla`](WalDurability.tla) | The leaf side, end to end: append, out-of-order flush and acknowledgement, a shared stream read through per-leaf READ positions, checkpoint persists that can fail, snapshot captures that can fail, durable materialiser pins, the GC trim floor and its block pins, and leaf and shard crashes at any step with recovery from a snapshot or cold; on the WAL side, abandoned flushes still in motion, a lost grain activation whose calls keep running, and the allocator's re-seed past them (#5799). |
| [`WalMove.tla`](WalMove.tla) | A shard move: a durable fence under a lease and the source activation's fence, quiesce, copy and switch, concurrent with appends, out-of-order flushes, a consumer, the GC, a shard crash and the loss of the move's coordinator. |
| [`WalPinRetirement.tla`](WalPinRetirement.tla) | The GC retiring orphaned materialiser pins - the bulk orphan sweep and the starvation drive, each taking a token per pin-store key, reading the leaf's durable row and removing through a fence as separate steps, two sweepers racing - against the leaf's fold, purge, discard and re-creation under the same grain id. See [below](#walpinretirement). |
| [`WalAppendEpoch.tla`](WalAppendEpoch.tla) | The append-epoch fence of the fenced live checkpoint advance (epic #5605): one leaf's activations claiming an epoch at a WAL partition in three steps - the epoch row's conditional write, the durable entry's max-merge and the in-memory raise that returns the claim head - the bump on a refusal, and the shard refusing a superseded or retired activation's append, against a WAL shard restart, another leaf's retirement and the leaf's three-step clear. The first module promoted from the design draft below. See [below](#walappendepoch). |
| [`WalHandOffFence.tla`](WalHandOffFence.tla) | The hand-off fence of the fenced live checkpoint advance (epic #5605): a foreign author's writes to a range the owner leaf owns - opened under the owner's durable marker, stamped at admission, and closed by a fence that re-claims the initiator's generation and records the allocator head - with point and batch writes, another shard's range-delete walk and rule 3's re-stamp, a WAL shard restart, and a hand-off in flight across the fence gate's opening. The second module promoted from the design draft. See [below](#walhandofffence). |
| [`WalRoutingFloor.tla`](WalRoutingFloor.tla) | The routing floor of the fenced live checkpoint advance (epic #5605): a tree's purge and re-creation keeping every write the purged incarnation admitted below the close the re-created incarnation's first initialisation records, and every live shard root's routing generation above its own floor - the per-initiator floors in the row and at the WAL, the claim where a floor is checked, the seam retry that never re-stamps (#5705), and the re-seed catch-up after a purge that finished on 10.2.0 (#5716). The third module promoted from the design draft. See [below](#walroutingfloor). |
| [`WalBulkSwitch.tla`](WalBulkSwitch.tla) | The bulk switch of the fenced live checkpoint advance: a bulk append's leaf born from the old rightmost leaf's range - the birth hint stored with the switch's open token, the run's hint and the fence that folds its completed switches, the drain before the narrow, and the relocation of the rows the range held (copy, deliver, drop) - against a crash of the shard root, a drain past its deadline and a foreign record of the range the old leaf replays, with the hint capped at its read frontier. See [below](#walbulkswitch). |
| [`WalLiveAdvanceCore.tla`](WalLiveAdvanceCore.tla) | The core of the same live advance: when a leaf's activation is armed to advance its read position over its own applied appends without reading the WAL - its raised claim, its replay to the claim head and to the last close's heads, no open or uninitialised marker, every reported unstamped append read, every own append applied - and the shared snapshot and GC trim that follow, with the other promoted modules' guarantees as environment constraints. The sink of their assume-guarantee graph. See [below](#walliveadvancecore). |
| [`WalLiveAdvanceComposite.tla`](WalLiveAdvanceComposite.tla) | The interface check of the five promoted modules: every producer guarantee, copied from its module, as a step constraint, and every assumption the hand-off, routing-floor and core modules make of it as an edge invariant, over the contract's vocabulary at minimal constants. A step the guarantees allow and an assumption forbids is an interface mismatch. See [below](#walliveadvancecomposite). |

They are separate modules because a move touches none of the leaf state, pin
retirement only the leaf's lifecycle, not its replay, and the append-epoch fence
only which activation may append, not what it reads, the hand-off fence only
which foreign records the owner's marker bounds, and the routing floor only the purge's
rows and generations, not a leaf's epochs, and the live-advance core only the advance
over a leaf's own appends, taking the fences as given. Keeping each out of
`WalDurability` keeps that module small enough to check its liveness properties
exhaustively within the TLC budget.

## Design drafts

`design/` holds a draft that is not a module: `design/WalLiveAdvance.tla`, its
variant configurations and its `design/mutations/` model the fenced redesign of the
live checkpoint advance (issues #5572, #5593, epic #5605), described in
[`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md). Module discovery reads
only top-level `.tla` files, so no TLC case runs the draft. It has been promoted as five
focused modules with an acyclic assume-guarantee graph,
[`LiveAdvanceInterface.md`](LiveAdvanceInterface.md): [`WalAppendEpoch`](#walappendepoch),
[`WalHandOffFence`](#walhandofffence), [`WalRoutingFloor`](#walroutingfloor),
[`WalBulkSwitch`](#walbulkswitch) and [`WalLiveAdvanceCore`](#walliveadvancecore). Those
modules, with the composite interface check that composes them (#5774), are the CI
authority for the live advance. The draft and its configurations stay as local evidence,
and its mutations as the record each module took its own from.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `WalDurability` | 15 | 6 | 30 | 52 | 48 | 420,229 |
| `WalMove` | 5 | 2 | 13 | 18 | 18 | 1,617 |
| `WalPinRetirement` | 3 | 3 | 25 | 31 | 30 | 284,019 |
| `WalAppendEpoch` | 5 | 0 | 17 | 23 | 20 | 150,706 |
| `WalHandOffFence` | 6 | 1 | 37 | 48 | 42 | 8,826 |
| `WalRoutingFloor` | 3 | 1 | 14 | 21 | 16 | 28,956 |
| `WalBulkSwitch` | 6 | 1 | 21 | 28 | 26 | 1,047,630 |
| `WalLiveAdvanceCore` | 18 | 2 | 58 | 134 | 76 | 2,021,047 |
| `WalLiveAdvanceComposite` | 16 | 0 | 19 | 36 | 18 | 402,394 |

`Actions` counts the disjuncts of `Next`, including the non-behavioural `Stutter`
of `WalMove`, `WalAppendEpoch`, `WalHandOffFence`, `WalRoutingFloor`, `WalBulkSwitch` and `WalLiveAdvanceCore`. `Behaviour rows` counts the action rows of the
module's refinement note, excluding non-behavioural actions, plus its property rows. `Distinct states` is
TLC's count for the module's own cfg. `WalDurability` searches to depth 35 and
`WalMove` to depth 16, with tla2tools v1.7.4. `WalAppendEpoch` searches to depth
25, and its `WideEpochs` variant finds 495,810 distinct states to depth 26.
`WalHandOffFence` searches to depth 19, and its `WideWindows` variant finds 47,000
distinct states to depth 23; its `StaleDonor`, `StaleDonorPreArm` and `StaleDonorRearmRace`
variants (#5841, #5840 S4) find 574,540, 2,651,431 and 64,825. `WalRoutingFloor` searches to depth 15, and its
`WideGenerations` variant finds 24,084 distinct states to depth 15.
`WalLiveAdvanceCore` searches to depth 25; its variants find 3,052,664 (`Restart`,
depth 26), 3,373,761 (`GateFlip`, depth 25), 3,693,715 (`Recreate`, depth 26),
4,638,713 (`RecreateSurvivor`, depth 26), 1,622,161 (`RecreateSurvivorMixed`, depth 25)
and 1,114,185 (`Bulk`, depth 25) distinct states. Its claimless-advance variants (#5837)
find 4,680,662 (`Free`), 2,386,428 (`FreeKeyMove`), 3,562,343 (`FreeSplitHint`),
2,146,154 (`FreeSplitBirth`), 428,890 (`FreeSplitLink`), 892,259 (`FreeLatch`), 707,726
(`FreeOrphan`), 3,855,129 (`FreeRestart`), 477,300 (`FreeRestartKeyMove`), 1,715,417
(`FreeRecreate`), 4,908,025 (`FreeRecreateSurvivor`), 3,309,965 (`FreeBulk`), 1,904,304
(`FreeGateFlip`), 4,217,142 (`FreeFwdExclusion`) and 821,800 (`FreeTagEvict`).
`WalBulkSwitch` searches to depth 29, and its `Skip` variant finds 197,156 distinct
states to depth 30.
`WalLiveAdvanceComposite`'s actions are non-behavioural interface steps, which the five
modules map to production, except the three WAL shard checks its edges rest on.

`WalDurability` has a two-fault variant configuration, `WalDurability.TwoFaults.cfg`
(see "Variant configurations" in [`../README.md`](../README.md)). It checks every
invariant and both action properties with a budget of two faults instead of one:
3,237,411 distinct states to depth 42. The liveness properties stay at one fault,
because at two the full configuration is far past the TLC budget.

Its second variant, `WalDurability.SnapshotLoss.cfg`, lets the environment destroy a
leaf's durable snapshot (`SnapshotVanish`, issue #4634) or its state row
(`LeafRowVanish`, issue #4654), and the operator purge the tree (`PurgeClear`), at two
faults so a snapshot and a row can both vanish. It turns abandoned flushes and
activation loss off to stay within the TLC budget (see the refinement note's
classification). Destroyed data is outside the durability
properties by construction, so it checks the properties that say the loss is never
silent - `ReadPositionHonest` above all, carved out only for the writes a purge
deleted (issue #4700) - with the other safety invariants that still apply: 5,070,839
distinct states to depth 42. A purge marks a leaf before clearing it, and recovery re-creates only
a marked leaf (`MarkPurge`, `PurgeClear`, `Recover`); the lifecycle resumes once
recovery has reset every marker.

Its `WalDurability.CrossSilo.cfg` variant lets a lost activation's successor be on
another silo, which cannot see its predecessor's abandoned calls still in motion
(issue #5799), and whose predecessor's late batches land through that silo's own
provider instance, held off the successor's batches by the TAIL commit fence (issue
#5810) and by the successor's re-stamp of `TAIL` on recovery (issue #5825): every
invariant and all four action properties, 1,954,533 distinct states to depth 35. The base keeps such moves off, because with them its liveness check would
pass the TLC budget.

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
| `WalDurability.CrossSilo.cfg` | A lost WAL shard activation whose successor is on another silo, checked against every invariant and the action properties. |
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
| `WalAppendEpoch.tla` / `.cfg` / `.manifest.json` | The append-epoch fence module, its TLC model and its manifest. |
| `WalAppendEpoch.WideEpochs.cfg` | The append-epoch fence module with epochs up to 4, checked against every invariant. |
| `LiveAdvanceInterface.md` | The assume-guarantee contract between the five live-advance modules (`WalAppendEpoch` and the four that follow): the graph, the shared vocabulary, each guarantee and what the core assumes. |
| [`append-epoch-mutations/`](append-epoch-mutations/) | One or more deliberate defects per property and per action of `WalAppendEpoch`. |
| [`AppendEpochRefinement.md`](AppendEpochRefinement.md) | `WalAppendEpoch` mapped to production, and the interface the other promoted modules assume. |
| `WalHandOffFence.tla` / `.cfg` / `.manifest.json` | The hand-off fence module, its TLC model and its manifest. |
| `WalHandOffFence.WideWindows.cfg` | The hand-off fence module with four offsets, generations up to 4 and three windows, checked against every invariant and the action property. |
| `WalBulkSwitch.Skip.cfg` | The bulk switch module on a never-written shard (#5812, B2): no root when the load begins, an unstamped seed, and a batch write that may seed a root mid-load, checked against every invariant. |
| [`handoff-fence-mutations/`](handoff-fence-mutations/) | One or more deliberate defects per property and per action of `WalHandOffFence`. |
| `WalRoutingFloor.tla` / `.cfg` / `.manifest.json` | The routing-floor module, its TLC model and its manifest. |
| `WalRoutingFloor.WideGenerations.cfg` | The routing-floor module with the purged incarnation's generations up to 3, checked against every invariant and the action property. |
| [`routing-floor-mutations/`](routing-floor-mutations/) | One or more deliberate defects per property and per action of `WalRoutingFloor`. |
| [`RoutingFloorRefinement.md`](RoutingFloorRefinement.md) | `WalRoutingFloor` mapped to production, and its interface. |
| `WalLiveAdvanceCore.tla` / `.cfg` / `.manifest.json` | The live-advance core module, its TLC model and its manifest. |
| `WalLiveAdvanceCore.Restart.cfg` | The live-advance core with a WAL shard restart, checked against every invariant. |
| `WalLiveAdvanceCore.GateFlip.cfg` | The live-advance core with own unstamped appends by any running activation, checked against every invariant. |
| `WalLiveAdvanceCore.Recreate.cfg` | The live-advance core with the tree purged and re-created, a straddler and the first initialisation, for purges that find the born activation claimed, checked against every invariant. |
| `WalLiveAdvanceCore.RecreateSurvivor.cfg` | The same purge for purges that find the born activation unclaimed, which may then claim as the re-created incarnation's, without the 10.2.0 silo, checked against every invariant. |
| `WalLiveAdvanceCore.RecreateSurvivorMixed.cfg` | The unclaimed-survivor purge with the 10.2.0 silo and no hand-off window, checked against every invariant. |
| `WalLiveAdvanceCore.Bulk.cfg` | The live-advance core with a bulk-born leaf, checked against every invariant and the liveness property. |
| [`live-advance-core-mutations/`](live-advance-core-mutations/) | One or more deliberate defects per property and per action of `WalLiveAdvanceCore`, and one weakening of each assumption it takes. |
| [`LiveAdvanceCoreRefinement.md`](LiveAdvanceCoreRefinement.md) | `WalLiveAdvanceCore` mapped to production, and its interface: each assumption with the guarantee and module that discharge it. |
| [`HandOffFenceRefinement.md`](HandOffFenceRefinement.md) | `WalHandOffFence` mapped to production, and its interface: E's entry check assumed, `HandOffBound`, `AbsentMarkerHolds` and `CloseHeadMonotone` guaranteed. |
| `WalBulkSwitch.tla` / `.cfg` / `.manifest.json` | The bulk switch module, its TLC model and its manifest. |
| [`bulk-switch-mutations/`](bulk-switch-mutations/) | One or more deliberate defects per property and per action of `WalBulkSwitch`. |
| [`BulkSwitchRefinement.md`](BulkSwitchRefinement.md) | `WalBulkSwitch` mapped to production, and its interface. |
| `WalLiveAdvanceComposite.tla` / `.cfg` / `.manifest.json` | The interface check of the five promoted modules, its TLC model and its manifest. |
| [`composite-mutations/`](composite-mutations/) | For each edge invariant, a witness that a step of its kind is reachable, and a weakening of each guarantee it relies on. |
| [`LiveAdvanceCompositeRefinement.md`](LiveAdvanceCompositeRefinement.md) | `WalLiveAdvanceComposite`'s edges, each copied guarantee and assumption with its source, its scope cuts and its open findings. |

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
| `AckedOccupies` | Invariant | An acknowledged write keeps its offset while it is stored: no late-landing abandoned call or reissue displaces it. |
| `AllocatorAboveOrphans` | Invariant | The allocator never hands out an offset at or below an abandoned call still in motion that the shard's activation can see. |
| `OffsetWrittenOnce` | Invariant | No offset is written twice: two batches with different start offsets never both hold one. |
| `GuardCoversFence` | Invariant | While an instance's view of `TAIL` is current and no re-seed is pending, its allocator is above `TAIL`. |
| `RecoveryNeverFallsOffLog` | Invariant | No leaf ever latches `LeafProjectionStaleException`. |
| `PersistedBeliefHonest` | Invariant | A leaf's belief about its persisted checkpoint is what storage holds, or the anchor it started from. |
| `ReleaseBackedBySnapshot` | Invariant | Every trim entitlement the pin store has published is backed by the leaf's durable snapshot coverage. |
| `SnapshotCoverageMonotonic` | Action | Durable snapshot coverage never regresses. |
| `PublishedPinWithinPersistedBelief` | Action | A newly published pin never exceeds the persisted checkpoint. |
| `TailMonotone` | Action | The store's `TAIL` row never regresses. |
| `RecoverNeverInsideCommitted` | Action | A re-seeded allocator never hands out an offset a committed batch holds. |
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

Extending the model to the allocator's re-seed (#5803) and reviewing that extension
found two more, in the Azure Table provider across silos. The model reproduced each
before its fix, and both are fixed:

| Issue | Defect | Mutation |
|-------|--------|----------|
| #5841 | Found by the independent review of #5840 (#5833's pre-arm): a stale duplicate of a split's donor, still holding the span from before the split, claimed the donor's epoch after the split - its claim wrote the epoch row + 1 over the split's bump, and under pre-arm the split bumped only when its own activation had claimed - so its epoch-only writes of moved keys (the PendingTx backstop, the span-forward receive) were fenced by nothing the receiver reads, and an acknowledged write could be trimmed. Also present under today's eager arm for an idle duplicate. **Fixed** by a Dekker pair: a claim writes the row, then re-reads the leaf state and is stale if it changed; the split persists its narrow, then re-reads the row and bumps it whenever it exists. | `HandOffBoundStaleDonorArmsAfterSplit5841` |
| #5825 | The phase-2 worker commits in arrival order, so across a silo move a predecessor's lower batch can commit after its successor recovered and readers passed the offset as a settled hole: the entry appears below them. The #5811 fence does not refuse it, because the successor only read `TAIL`. **Fixed by #5827**: recovery re-stamps `TAIL` under the ETag it read before the activation serves a read, which puts the predecessor's view out of date. | `LogPrefixAppliedRecoveryLeavesTailUnstamped` |
| #5810 | A predecessor activation's late batch on another silo, starting at a different offset, lands over a successor's acknowledged batch, so every read yields the shared offsets twice; its blind `TAIL` upsert regresses `TAIL`, so the next recovery reissues committed offsets. **Fixed by #5811**: a phase-2 commit is conditional on the `TAIL` ETag its provider instance last observed, under a fence generation that refuses every claim made before a refused write. | `OffsetWrittenOnceStaleWriterUnfenced`, `TailMonotoneStaleWriterBlindUpsert`, `RecoverNeverInsideCommittedUnfencedBlindTailUnstamped` |

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

## The live advance's gap register

The five live-advance modules each keep a register of what they do not model, in their
refinement notes' `## Deliberate abstraction gaps`. This is every entry, by module, so a
reader can see the whole cut in one place; the note holds the reasoning and the
production symbols.

- **`WalAppendEpoch`** ([gaps](AppendEpochRefinement.md#deliberate-abstraction-gaps)):
  the fence gate and the unstamped fallback; one leaf, one partition, two activations;
  the leaf's gate at the partition; the clear's kept row has no detector; retirements
  other than the leaf's clear; stamped batches and the entry row's retries; self-fenced
  rejections.
- **`WalHandOffFence`** ([gaps](HandOffFenceRefinement.md#deliberate-abstraction-gaps)):
  one initiator, one owner, one partition; the owner's read and its replay; a close with
  the gate view closed; unstamped writes; rule 3's intent identity; stamped batches,
  seam retries and the deadline.
- **`WalRoutingFloor`** ([gaps](RoutingFloorRefinement.md#deliberate-abstraction-gaps)):
  the range-delete fence is not a separate initiator; the mixed-version residue is
  excluded; one partition, one write per shard root at a time; the purge is one step;
  the marker-write guard is not modelled; a refused write above its floor is not
  re-stamped there.
- **`WalBulkSwitch`** ([gaps](BulkSwitchRefinement.md#deliberate-abstraction-gaps)):
  one key, one appended leaf, one chunk; the bulk entry is held by every reader; the
  close's fence is the hand-off module's; the gate, older silos and bulk loads; shadow
  markers move with the relocation, outside the model (#5755); the persisted drop.
- **`WalLiveAdvanceCore`** ([gaps](LiveAdvanceCoreRefinement.md#deliberate-abstraction-gaps)):
  the liveness property excludes a current activation that has fallen off the log
  (`CurrentFellOffLog`, answered in production by the stale latch of #3477, whose
latch-disabled red proof ran in CI); production
  holds the advance in more cases than `Armed`; the marker read is atomic with the
  advance; refusals that allocate nothing; one partition, one leaf, two activations;
  re-claims; the duplicate's own snapshot and the purge's trim; the unreachable leaf's
  tick re-check.

The epic also recorded one gap in the WAL durability model it builds on:
`WalDurability` ([gaps](Refinement.md#deliberate-abstraction-gaps)) has no in-process
resync and no reactivation that keeps an abandoned call running, and cannot tell which
write occupies an offset, so an allocator re-seeded beneath a call still in motion
(#5799) was outside it until #5803 extended it.

Two cuts are budget bounds rather than abstractions, and the core's
[property classification](LiveAdvanceCoreRefinement.md#property-classification) names
them: the liveness property is checked on the base and `Bulk` only (`Restart` holds it
but takes over seven minutes, and `GateFlip` and `Recreate` are outside its premise), and
CI checks a purge's unclaimed survivor with a hand-off window and, separately, with a
10.2.0 silo. The combination of both was checked locally and is clean at 8,913,193 states
(232 seconds), but is kept out of CI only for the 5M-state case budget.

The composite interface check ([gaps and scope cuts](LiveAdvanceCompositeRefinement.md#scope-cuts))
has four scope cuts of its own: one WAL shard restart; no purge after a bulk birth (no
module combines the two either); no step for the bulk switch's own close, which no B
guarantee states, and which is shown harmless: C's `Bulk` configuration without the close's
`phase` guard is clean at 1,114,208 states; and two offsets with one hand-off window, which its witness mutations
show reach every edge.

## How to run TLC

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalDurability.cfg WalDurability.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalDurability.TwoFaults.cfg WalDurability.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config WalMove.cfg WalMove.tla
```

Pass `-metadir` with a directory outside the repository, or delete the `states/`
directory TLC leaves beside the module. On two workers `WalDurability` takes about
twelve minutes, because it checks two liveness properties over the full state graph;
its `TwoFaults` variant takes about two and a half minutes, `SnapshotLoss` about five and `CrossSilo`
about two; `WalMove` takes seconds, and its
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
pinned by tests), a resized tree's logical purge completion (pinned by tests), how
long a write to an id created again waits while a purge's tail is owed, how long a
live leaf's pin stays at the block value before its first WAL replay, or
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

## WalAppendEpoch

`WalAppendEpoch` models the epoch fence of the fenced live checkpoint advance (epic
#5605, [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md) section 1): which
activation of a leaf may append at a WAL partition. Before its first append there an
activation claims an epoch in three steps - it persists the epoch it read plus one to
the leaf's epoch row as a strict conditional write, the WAL shard max-merges it into
the leaf's durable entry, and the shard raises its in-memory entry under the lock that
assigns offsets and returns the allocator head as the claim head. A refusal while the
row still holds the activation's own write bumps above the refusal and claims again; a
moved row stops it. The shard admits a stamped append only from a loaded entry and an
epoch not below it, and never checks the partition's retired floor there (#5635). The
environment restarts the WAL shard, retires another leaf of the partition, forces bumps
from a second partition and clears the leaf in production's three steps: read the
epoch row, retire the entry under a floor of at least the higher of the entry and the
row (#5641), then delete the row conditionally on the ETag read.

It is the first of five modules promoted from the design draft, and the root of their
assume-guarantee graph: it assumes nothing, and its four guarantees are what the
hand-off, routing-floor, bulk-switch and core modules assume
([the interface](AppendEpochRefinement.md#interface)).

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `StaleEpochNeverAppends` | Invariant | No append is allocated from an epoch below the highest raised one, or at or below an epoch the leaf's clear retired, across a WAL shard restart and the purge straddle. |
| `ClaimHeadCoversPriorAppends` | Invariant | When a claim is raised, every offset already allocated lies below its claim head. |
| `RaisedEpochUnique` | Invariant | No two activations that can still append hold the same raised epoch, within an incarnation or across the leaf's clear. |
| `OnlySupersededRefused` | Invariant | Only a superseded activation is ever refused, at a claim or at an append. |

The base checks every property with every environment step on, at epochs up to 3 (a
few seconds on two workers); `WalAppendEpoch.WideEpochs.cfg` re-checks them at epochs
up to 4 (about ten seconds). Thirteen of the design draft's mutations moved here,
re-targeted at these properties; the refinement note lists them, and the two
redundancy checks this module keeps, which stay clean and so are not in its mutation
directory.

### What it covers, and what it does not

It covers the claim's three steps and its bump, stale and unloaded rejections and the
re-presented claim, a WAL shard restart, another leaf's retirement, bumps forced on a
second partition, and the leaf's clear with its conditional row delete. It does NOT
cover the fence gate and the unstamped fallback, markers, foreign authors, routing
generations, bulk loads, replay and read positions (the other promoted modules'),
more than one leaf or partition beyond those two environment steps, multi-entry
stamped batches, the WAL GC's and the sweep's retirement scheduling, or the leaf's gate
that serialises its own clear with its claims (the model is weaker there). The
refinement note's abstraction gaps list them; in particular, no test yet drives the
clear's conflicting row delete, which the model shows is load-bearing.

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalAppendEpoch.cfg WalAppendEpoch.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalAppendEpoch.WideEpochs.cfg WalAppendEpoch.tla
```

## WalHandOffFence

`WalHandOffFence` models the hand-off fence of the fenced live checkpoint advance
(epic #5605, [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md) section 2):
how a foreign author - a split donor, a reclaim neighbour, a migrating or folding shard
root, the tree's range-delete fence or a bulk switch - is bounded by the owner leaf's
durable hand-off marker. The initiator opens the owner's marker before its first
admission, stamps each write with its generation in the same step that admits it, and
closes by fence: it stops admitting, bumps and re-claims its generation, which raises
its entry at the WAL shard and reads the allocator head in one step, and records that
head as the close, max-merged with the heads earlier closes recorded. Another shard's range-delete walk carries the same tree generation,
so one re-claim fences it; a rejected walk re-stamps only while its own intent holds
the owner (rule 3), and otherwise goes on unstamped. A hand-off begun before the fence
gate opened has no marker; it opens one when it resumes, and the owner's marker state
is initialised only once no such hand-off is in flight.

It is module H of the five promoted from the design draft. It assumes only
`WalAppendEpoch`'s entry check, as an environment constraint, and its three guarantees
are what the bulk-switch and core modules assume
([the interface](HandOffFenceRefinement.md#interface)).

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `HandOffBound` | Invariant | Whenever the owner's marker reads closed, every stamped foreign append lies below the heads its closes recorded. |
| `AbsentMarkerHolds` | Invariant | The owner's marker state is initialised only while no hand-off begun without a marker is in flight, so an absent marker holds the owner. |
| `CloseHeadMonotone` | Action property | The heads the owner's marker requires never decrease: every close is max-merged with the heads earlier closes recorded. |

The base checks every property with every environment step on (about a second on four
workers); `WalHandOffFence.WideWindows.cfg` re-checks them with four offsets,
generations up to 4 and three windows (a few seconds). Twelve of the design draft's
mutations moved here, re-targeted at these properties, eight more perturb the
actions the draft had none for, and one makes a close overwrite the recorded heads; the refinement note lists them, and the redundancy
check this module keeps.

### What it covers, and what it does not

It covers the marker's open and its fenced close, stamping at admission and the
admission mark, point and batch writes, the WAL shard's acceptance and rejection on the
loaded generation entry, its restart, another shard's range-delete walk with its
re-stamp and unstamped fallback and another intent's close, and a hand-off in flight
across the gate's opening with the marker initialisation that follows. It does NOT
cover the owner's own appends, its read of the marker or its replay to the heads (the
core module's), routing floors and purges, more than one initiator, owner or partition,
a close with the initiator's gate view closed, multi-entry batches, seam retries, or a
range delete's deadline reminder; the refinement note's abstraction gaps list them.

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalHandOffFence.cfg WalHandOffFence.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalHandOffFence.WideWindows.cfg WalHandOffFence.tla
```

## WalRoutingFloor

`WalRoutingFloor` models the routing floor of the fenced live checkpoint advance (epic
#5605, [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md) section 2): how a
tree's purge and re-creation keep a write the purged incarnation admitted - a
straddler - out of the range a re-created leaf advances over without reading. Each
shard root has a routing generation, a durable entry at the leaf's WAL partition and
the shard's loaded copy, a floor in the tree's routing-floor row and the shard's
in-memory copy of that floor. The WAL admits a stamped append on the loaded entry
alone; an append that meets an unloaded entry re-presents a claim, and the claim is
where the floor is checked. The purge raises every initiator's floor before any
delete (#5668), the re-created incarnation's first initialisation seeds the floor
plus one, claims it and records a close, a refused write's seam retry never
re-stamps it (#5705), and after a purge that finished on 10.2.0 the re-seed's
catch-up retires only a marker of another incarnation and raises only its own
initiator's floor (#5716).

It is module R of the five promoted modules: it assumes only `WalAppendEpoch`'s
entry check, as an environment constraint, and the core module assumes its
`StraddlerBelowInit` and `InitHeadCoversPriorAllocations`
([the interface](RoutingFloorRefinement.md#interface)).

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `StraddlerBelowInit` | Invariant | Whenever the leaf's marker reads initialised after the re-creation, every offset allocated to a shard root of the purged incarnation lies below the close the re-created incarnation's first initialisation recorded. |
| `LiveGenerationAboveFloor` | Invariant | Every live shard root's routing generation is above its own routing floor. |
| `InitHeadCoversPriorAllocations` | Action property | The re-created incarnation's first initialisation records a head above every offset allocated before it, whoever allocated it. |

The base checks every property with both purge kinds, a second shard root and a WAL
shard restart on (about a second); `WalRoutingFloor.WideGenerations.cfg` re-checks
them with the purged incarnation's generations up to 3. Nine of the design draft's
mutations moved here, re-targeted at these properties; the refinement note lists
them and the seven the module adds.

### What it covers, and what it does not

It covers the purge's floors-first retirement, the claim's floor check, the seam
retry's deleted outcome, the first initialisations of L's shard root and of another
shard root, the 10.2.0 purge with the re-seed catch-up and a duplicate shard-root
activation, and a WAL shard restart. It does NOT cover the range-delete fence as a
separate initiator, the mixed-version residue (a cross-shard straddler spanning a
10.2.0 purge, which `StraddlerBelowInitLegacyPurgeCrossShard` shows is real),
leaf epochs, hand-off windows, replay and read positions (the other promoted
modules'), or the purge's steps as separate actions.

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalRoutingFloor.cfg WalRoutingFloor.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalRoutingFloor.WideGenerations.cfg WalRoutingFloor.tla
```

## WalBulkSwitch

`WalBulkSwitch` models the bulk switch of the fenced live checkpoint advance (epic
#5605, [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md) section 3): how a
bulk append's leaf is born from the old rightmost leaf's range. The operation stores
its birth hint and opens its switch token in one write before its first appended leaf
is born, reusing a stored hint on a retry, the heads of a fence that folds the run's
completed switches, or the run's hint, and reading the heads otherwise. The graft
narrows the old leaf, which first drains the commits it admitted, relocates the rows
the range held into the appended leaf and only then drops them, and the switch ends by
making the leaf's read position the hint capped at the old leaf's read frontier when it
narrowed (#5752), only when every row was relocated. A drain
past its deadline takes the 10.2.0 path and applies no hint. Completed switches are
closed in batches by one fence that removes their hints.

The appended leaf's seed of its bulk entry is a WAL record that claims no append epoch
(#5812): under the open token it carries the bulk-seed stamp class, admitted only on a
positive match with the routing generation every close re-claims, and unreported; a
seed still in flight when the operation is abandoned is refused once its switch has
closed. On a never-written shard (`NeverWritten = 1`, the `Skip` variant) the load opens
no token and stores no hint, seeds unstamped and reported, and checks in its publish
that no batch write seeded a root meanwhile, falling back to the switched path if one
did.

It is module B of the promoted modules' assume-guarantee graph: it assumes nothing, and
its four guarantees are what the core module assumes of a bulk-born leaf and of a bulk
operation's seed ([the interface](BulkSwitchRefinement.md#interface)).

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `BulkHintBound` | Invariant | The appended leaf's read position is at or below every record of its range it does not hold, from its birth through its replay. |
| `ContestedKeyAgrees` | Invariant | The appended leaf resolves a contested key to the value every WAL reader resolves it to: the later-stamped write wins. |
| `HintOnlyWhileOpen` | Invariant | A stored birth hint belongs to an open switch. |
| `SeedCovered` | Invariant | Every seed record is admitted under its switch's open token and below its close heads, or reported in the unstamped watermark. |
| `ReplacedRootRelocated` | Invariant | A load published over a root a batch write seeded while it ran has relocated that root, never overwritten it (#5819). |

The base checks every property with every environment step on, in about fifteen
seconds at two workers; the `Skip` variant checks them on a never-written shard.
Three of the design draft's mutations moved here, re-targeted at these properties; the
refinement note lists them.

### What it covers, and what it does not

It covers the hint's sources (stored, folded fence, run, fresh), the drain before the
narrow and its deadline, the relocation's copy, delivery and drop, a crash of the shard
root before or during the graft, the batched close, an abandoned switch's close, and
the appended leaf's replay; the seed's admission, its refusal after a close, an
abandoned attempt and its retry, and a bulk load into a never-written shard with a
batch write seeding a root mid-load. It does NOT cover several keys or appended leaves,
the re-delivery skip, the gate closed or an older silo, the close's fence beyond the
generation's re-claim (the hand-off module's), or arming; the refinement note's
abstraction gaps list them.

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalBulkSwitch.cfg WalBulkSwitch.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalBulkSwitch.Skip.cfg WalBulkSwitch.tla
```

## WalLiveAdvanceCore

`WalLiveAdvanceCore` models the core of the fenced live checkpoint advance (epic #5605,
[`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md), section 1's "Arming" list
and section 4): when an activation of a leaf may advance its read position over the
appends it issued and applied itself, without reading the WAL, and what that does to the
shared per-leaf snapshot and the GC trim. An activation is armed only when its claim is
raised, the leaf's marker state is initialised, it has replayed to its claim head, no
marker is open and it has replayed to the heads the marker's closes recorded, it has read past every
unstamped offset reported to it, and every own append it issued is applied. Otherwise it
advances by replay.

It is the last of the five modules promoted from the design draft, and the sink of their
assume-guarantee graph ([`LiveAdvanceInterface.md`](LiveAdvanceInterface.md)). It assumes
every guarantee of the other four and models each only as an environment constraint in the
form stated there: only the current activation's stamped appends are allocated, at or above
its claim head, and a purge retires every raised epoch (`WalAppendEpoch`); foreign appends
only while the marker is open or not initialised, below the heads a close records once it
is, which the marker max-merges so they never decrease, and an absent marker holds the leaf
(`WalHandOffFence`); a straddler of a purged incarnation only before the re-created
incarnation's first initialisation, which records a head above every earlier allocation
(`WalRoutingFloor`); and a bulk-born leaf holds every record at or below its birth read
position (`WalBulkSwitch`). It models the unstamped watermark itself: a 10.2.0 silo's
append is always admitted and raises it, every stamped response reports it, a WAL shard
restart resets it to the recovered head minus one, and an own unstamped append reports
itself ([the interface](LiveAdvanceCoreRefinement.md#interface)).

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `ReadPositionHonest` | Invariant | No running activation's read position passes a durable entry it does not hold, acknowledged or not, whoever appended it. |
| `SnapshotHonest` | Invariant | The shared snapshot never claims coverage of a durable entry it lacks. |
| `NoTrimPastAcked` | Invariant | The GC never trims an acknowledged append the snapshot lacks. |
| `OldNeverRejected` | Invariant | An unstamped append is never rejected. |
| `NewbornPinEventuallyUsable` | Liveness | Under fairness of the leaf's own steps, with replay fair only toward a bound the arming list names, the shared snapshot eventually covers every own append, so the newborn's pin becomes usable without a replay from -1. |

The base checks every property with the duplicate, one hand-off window and a 10.2.0 silo's
unstamped append, the liveness property under `LiveSpec`'s single fairness condition
(about a minute and a half on two workers). Each variant adds one environment:
`WalLiveAdvanceCore.Restart.cfg` a WAL shard restart, `WalLiveAdvanceCore.GateFlip.cfg` own
unstamped appends, the purge with a straddler and the first initialisation in three
slices (`WalLiveAdvanceCore.Recreate.cfg`, `.RecreateSurvivor.cfg` and
`.RecreateSurvivorMixed.cfg`, by whether the purge finds the born activation claimed), and
`WalLiveAdvanceCore.Bulk.cfg` a bulk-born leaf (each between a minute and three; `Bulk`
also re-checks the liveness property). The design model's larger combined configurations stay as
local evidence in the design note. Seven of the design draft's mutations moved here; the
refinement note lists them, the assumption mutations that weaken each assumption in turn,
and the one redundancy check this module keeps, which stays clean and so is not in its
mutation directory. The refinement note's property classification gives the fairness,
the one exclusion (a current activation that has fallen off the log) and the measured
cost of the liveness check.

### What it covers, and what it does not

It covers the arming conditions, the live advance, replay, the snapshot capture and the GC
trim, the applied-prefix tracker, the unstamped watermark with its restart reset and the
self-report, a duplicate activation, and each other module's guarantee as the environment
it is stated to be. It does NOT cover the other modules' mechanisms (claims, markers,
routing generations, the bulk switch), the cases in which production holds the advance
beyond the design's arming list (a pending saga prepare, a replay in flight, a stale latch,
an overlap that cannot be excluded, an unfenced storage provider), the split between
reading the marker and stamping the target, refusals that allocate nothing, re-claims,
more than one partition or leaf, or combined environments. The refinement note's
abstraction gaps list them.

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalLiveAdvanceCore.cfg WalLiveAdvanceCore.tla
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalLiveAdvanceCore.Recreate.cfg WalLiveAdvanceCore.tla
```

## WalLiveAdvanceComposite

`WalLiveAdvanceComposite` checks the interface of the five modules promoted from the design
draft (epic #5605): that every guarantee in
[`LiveAdvanceInterface.md`](LiveAdvanceInterface.md)'s graph meets the assumption that
relies on it.

Each module checks its own guarantees, and models the others only as an environment in the
form they guarantee. A guarantee weaker than the assumption that relies on it is therefore
invisible to every one of them. This module is where it shows.

Its state is the contract's vocabulary only:
- one partition's allocator and authors;
- the claims of L's activations, of a foreign author `F`, and of the purged incarnation's
  shard root `S`;
- L's marker;
- a purge of either kind, with the re-seed catch-up;
- the first initialisation;
- a bulk birth.

Each step is generated freely within the vocabulary, and kept only if every producer
guarantee still holds after it. The guarantees are copied from their modules, and so is each
consumer's environment guard. An edge invariant holds unless a kept step of its kind fails
that guard.

`LiveAdvanceCompositeTests` pins every copy to its source, after the identifier renames it
lists, so a module and its copy cannot drift apart.

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed. |
| `EntryCheckMeetsHandOffAdmission` | Invariant | E -> H: a foreign stamped append E's entry check admits is one H's admission admits. |
| `ClaimHeadMeetsHandOffClose` | Invariant | E -> H: a close's re-claim head lies above every stamped foreign append. |
| `EntryCheckMeetsRoutingAdmission` | Invariant | E -> R: a straddler E's entry check admits is one R's admission admits. |
| `ClaimWriteMeetsRoutingClaim` | Invariant | E -> R: a re-presented claim E's claim write admits is one R's claim admits. |
| `ClaimHeadMeetsRoutingInit` | Invariant | E -> R: the first initialisation re-claims at the allocator head. |
| `EpochMeetsCoreClaim` | Invariant | E -> C: a claim E allows is one C's environment allows. |
| `EpochMeetsCoreOwnAlloc` | Invariant | E -> C: an own append E admits comes from C's current activation. |
| `HandOffMeetsCoreForeignAlloc` | Invariant | H -> C: a foreign append H allows is one C's environment allows. |
| `HandOffMeetsCoreClose` | Invariant | H -> C: a close H allows records heads C's environment allows. |
| `RoutingMeetsCoreStraddler` | Invariant | R -> C: a straddler R allows is one C's environment allows. |
| `RoutingMeetsCoreInit` | Invariant | R -> C: a first initialisation R allows records heads C's environment allows. |
| `BulkMeetsCoreCreate` | Invariant | B -> C: a birth B allows is one C's environment allows. |
| `ColdLoadMeetsCoreStraddler` | Invariant | R -> C: a straddler admitted through a read-only cold-loaded entry is one C allows; the cold load never lowers a re-claimed generation (#5839). |
| `BulkCloseMeetsCoreClose` | Invariant | B -> C: a bulk switch's close covers every prior allocation, as C's lazy-arm close assumes (#5839). |
| `DeferredCloseMeetsCoreResolve` | Invariant | H -> C: a lazy close H allows defers L's head at the raised generation, and its resolution records the allocator head, as C assumes (#5839). |

The base checks every edge in seconds. Each edge has a witness mutation, which
shows that a step of its kind is reachable. Each guarantee an edge relies on also has a
mutation that weakens it, which must turn the edge red.

[`LiveAdvanceCompositeRefinement.md`](LiveAdvanceCompositeRefinement.md) tables these
mutations. It also records:
- the redundancy check;
- the scope cuts: one restart, a purge never after a bulk birth, the bulk switch's own close, two offsets and one window;
- the module configurations that cover what the cuts drop.

### What it covers, and what it does not

It covers every assumption edge of the graph, at the vocabulary's level. It does NOT:
- re-check any module's own guarantees;
- model any module's mechanism beyond what its guarantees state;
- relate the vocabulary to production.

The five modules' notes do that.

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 2 -config WalLiveAdvanceComposite.cfg WalLiveAdvanceComposite.tla
```

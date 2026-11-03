# Refinement note: the WAL bulk switch to production

This note maps [`WalBulkSwitch.tla`](WalBulkSwitch.tla) - a bulk append's leaf born
from the old rightmost leaf's range, with a birth read position no record it lacks lies
below, and with every key the range already held resolved as every WAL reader resolves
it - to the Orleans.Lattice code that plays each role. It is module B of the five
modules promoted from the design model
[`design/WalLiveAdvance.tla`](design/WalLiveAdvance.tla) for epic #5605, and the design
authority is [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md), section 3
("The bulk birth hint") and the hand-off table's bulk row. Like the other notes in this
directory it is a documented mapping, not a refinement proof: the gates check its names,
detectors and coverage, and its claims were checked by reading production.

## Model scope in one paragraph

One WAL partition of four offsets and one key K in the appended range, the contention
worst case: every record writes K. The old rightmost leaf O holds K's range before the
switch; the appended leaf N is born for it. The bulk operation stores its birth hint and
opens its switch token in one write (Begin), choosing the stored hint of an earlier
attempt, else the heads of a fence that folds the run's completed switch, else the run's
hint, else the heads now. N is created holding the bulk entry and reading from the log's
start. The graft narrows O, which first drains the commits it admitted, and hands back a
copy of O's rows in the range with its read frontier, or no frontier when an earlier
attempt already narrowed it; the copy is delivered into N and only then dropped from O;
a drain past its deadline narrows nothing and the graft takes the 10.2.0 path. The switch
ends by making N's read position the hint capped at that frontier (#5752), only when every
row was relocated and a frontier was reported, and stays
open until a batched close removes its hint. The environment: O's write before the
switch, a commit O admits and stores in two steps, one foreign record of the range, the
run's earlier completed switch and its hint, one crash of the shard root before or
during the graft, O's replay of the records of its range, and N's replay. Epochs, claims, routing floors, arming and the live
advance belong to the other modules (see the interface).

## Interface

This module is B in the assume-guarantee graph of the five modules that replace the
design model ([`LiveAdvanceInterface.md`](LiveAdvanceInterface.md)).

- **The vocabulary.** The interface's L, the owner leaf whose live advance is in
  question, is N here. N's read position `rpN` is L's read position at birth: Finish sets
  it from the hint, and N's replay advances it. The partition's allocator is `next`, and
  `author` records each offset's author: `p` and `c` are writes of L's range by O, which
  the interface counts as appends by another author of the range, and `f` is
  `Foreign(i)`, a stamped append of a key in L's range by a hand-off initiator. N's own
  appends after its birth, and the relocation's re-appends, are `Own` appends above the
  hint whose rows N holds, so they are not modelled. L's marker is the shard marker's
  `bulk:{operation}` token (`tokOpen`); `prevOpen` is a completed switch of the same run.
- **Assumptions.** None. Neither guarantee reads the switch's marker: they bound N's birth
  read position and its resolution of K, which hold whatever the close does, so `Close`
  models only its effect on the token and the hint row. The switch's fence is the core
  module's to take, through H's `HandOffBound` and `AbsentMarkerHolds`, because the bulk
  switch's shard root is a `Foreign(i)` initiator in the vocabulary; exercising that edge
  is the composite check's job.
- **The hint's cap (#5752).** The narrow reports O's read frontier, read in the same turn
  that narrows it, and `Finish` caps N's read position at it, per partition
  (`ShardRootGrain.CapAtReadFrontier`). Every record of the range at or below the frontier
  is one O has replayed, and so holds by its narrow and hands back; every record above it
  N replays. A lower read position only lengthens N's replay. A resumed narrow of a leaf
  an earlier attempt already narrowed reports no frontier, because O's replay keeps
  running after the narrow and may since have passed a record of the range without
  holding it, and with no frontier no hint is applied.
  `BulkHintBoundForeignUnheldBeforeNarrow`, the hint applied uncapped, shows the cap is
  load-bearing: a foreign record O had not replayed when it narrowed is otherwise applied
  by no leaf; `BulkHintBoundResumedNarrowReportsFrontier` shows the resumed narrow's
  missing frontier is too.
  **Units.** A read position (`rpO`, `rpN`, `fO`) is inclusive: the last offset applied, as
  a leaf's checkpoint is (it replays offsets above it). The model's hint and heads are
  exclusive: the next offset to allocate, as `GetHeadOffsetAsync` returns, so `Finish` sets
  `rpN` to `Min2(hint - 1, fO)`. Production stores the hint already converted, as the heads
  minus one (`BPlusLeafGrain.HeadsAsCheckpoints`, #5760), and applies
  `min(hint, frontier)`: in checkpoint terms `min(head - 1, O's checkpoint)`, which is the
  model's `Min2(hint - 1, fO)`.- **Guarantees.** Two, each a checked invariant of the base:
  - `BulkHintBound`: N's read position is at or below every record of its range it does
    not hold, from its birth through its replay. Rows relocated into it at birth are
    held, by the offset of the record that wrote them.
  - `ContestedKeyAgrees`: N resolves K, up to its read position, to the value every WAL
    reader resolves it to there: last writer wins by version, the later-stamped write
    wins.
- **Module-local invariant.** `HintOnlyWhileOpen`: a stored hint belongs to an open
  switch, since the hint and the token are stored in one write and removed in one write.
  It is not an interface guarantee, and nothing outside B assumes it.
- **The graph.** E <- H, E <- R, and {E, H, R, B} <- C. B assumes nothing, and adds no
  edge.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next`, `author` | The WAL allocator, and the author of each record of K | `WalShardGrain._nextOffset`; the record's author is the leaf or initiator whose append allocated it. |
| `rpO` | O's read position, inclusive: the last offset it has replayed, as a leaf checkpoint | The old leaf's WAL read position, which its replay (`BPlusLeafGrain.AwaitReplayBarrierAsync`) advances. |
| `rowsO` | The rows O holds, by the record that wrote them | O's `BPlusLeafGrain` row cache; rows outside its declared span after the narrow are invisible to range-routed reads. |
| `narrowed` | O's high bound is narrowed below K | `BPlusLeafGrain.NarrowHighKeyAsync`, which narrows the leaf's high bound and links N in one persist. |
| `cst`, `cCounted` | O's admitted commit: in flight or stored, and counted among the mutations a narrow drains | The leaf's mutations counted by admission epoch, drained by `BPlusLeafGrain.DrainMutationsAdmittedBeforeAsync`; `BPlusLeafGrain.EnterLeafRoutingMutation` scopes the routing mutations. |
| `phase` | The current bulk operation's step | `ShardRootGrain.BeginBulkSwitchAsync`, the leaf creation, the persisted `PendingBulkGraft`, `ShardRootGrain.CompleteBulkGraftAsync` and `ShardRootGrain.FinishBulkSwitchAsync`. |
| `hint` | The operation's stored birth hint, exclusive: the next offset N must read | `ShardHandOffState.BulkBirthHints`, keyed by the operation id, in the shard's hand-off row; production stores it inclusive, as the heads minus one (`BPlusLeafGrain.HeadsAsCheckpoints`, #5760). |
| `tokOpen` | The operation's switch token is open | The token `ShardRootGrain.BulkHandOffToken` in the shard marker's open set (`HandOffMarkerCore.Open`, `HandOffMarkerCore.Close`). |
| `prevOpen`, `prevHint` | A completed switch of the same run, and its stored hint | Another `bulk:` token and its entry in `ShardHandOffState.BulkBirthHints`, counted by `ShardRootGrain.IsCompletedBulkSwitch`. |
| `runHint` | The activation's run hint | `ShardRootGrain._bulkRunHint`, in memory only. |
| `bulkAt` | The head when the bulk entry was stamped | Modelling device for the bulk entry's version: `HybridLogicalClock.Tick` stamps it on the shard root's clock when its chunk is appended. |
| `copied` | The copy of O's rows the narrow handed back | The list `BPlusLeafGrain.NarrowHighKeyAsync` returns, built by `BPlusLeafGrain.CopyRowsAtOrAbove`, held in the shard root's memory. |
| `relocOk`, `fO` | The narrow moved every row of the range and reported a frontier, and that frontier: O's read position, inclusive, when it narrowed | The read frontier `ShardRootGrain.NarrowAndRelocateAsync` returns from `BPlusLeafGrain.NarrowHighKeyAsync`; null when the narrow timed out, an older silo hosts the leaf, or an earlier attempt already narrowed it. |
| `nUp`, `heldN`, `rpN` | N exists, the rows it holds, and its read position, inclusive: the last offset it has applied, as a leaf checkpoint | The appended `BPlusLeafGrain`; its read position is set by `BPlusLeafGrain.SetCheckpointOffsetHintsAsync`. |
| `crashed` | Environment budget | Modelling device. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `PreWrite` | Environment: before the bulk operation begins, O takes a write of K and stores it | A point write stored on the rightmost leaf (`BPlusLeafGrain.SetAsync`). | Yes: `BulkAppendExistingRowsIntegrationTests.BulkAppend_below_rows_the_shard_holds_relocates_them_and_refuses_nothing` and `BulkAppendExistingRowsIntegrationTests.BulkAppend_after_point_writes_above_the_maximum_keeps_every_key_readable_last_writer_wins`. |
| `CommitAlloc` | Environment: O admits a write in its span, counts it, and its record is allocated before it is stored | The leaf's span check before the WAL append, under its admission count (`BPlusLeafGrain.EnterLeafRoutingMutation`). | Yes: `BPlusLeafGrainTests.NarrowHighKey_waits_for_a_write_admitted_before_it_and_hands_its_row_back` and `BPlusLeafGrainTests.NarrowHighKey_waits_for_a_replayed_prepared_commit_admitted_before_it`. |
| `CommitStore` | Environment: the admitted commit is stored on O | The leaf stores the row after the WAL append returns. | Yes: `BPlusLeafGrainTests.NarrowHighKey_waits_for_a_write_admitted_before_it_and_hands_its_row_back`. |
| `ForeignAlloc` | Environment: a foreign record of the range, reached only by replay: O's before the narrow, N's after it | Another initiator's stamped append of a key in the range. | Yes: `BulkAppendExistingRowsIntegrationTests.BulkLoad_leaves_start_from_the_birth_hint_and_every_key_survives_reactivation` (a record above the hint is reached by replay). |
| `ReplayO` | O replays the next record; while it declares the range it holds a record another author wrote, and after the narrow its replay passes the range's records without holding them | The old leaf's WAL replay (`BPlusLeafGrain.AwaitReplayBarrierAsync`), span-filtered once it has narrowed. | Yes: `BPlusLeafGrainTests.Every_data_entry_point_waits_for_the_replay_barrier` and `BulkAppendExistingRowsIntegrationTests.BulkAppend_leaves_replay_a_record_of_their_range_the_old_leaf_never_applied`. |
| `Begin` | Store the birth hint and open the switch token in one write, before the first appended leaf; fold the run's completed switches by one fence on the cadence or in a fresh activation; reuse a stored, fence or run hint, else read the heads | `ShardRootGrain.BeginBulkSwitchAsync`, with `ShardRootGrain.FenceBulkSwitchesAsync`, `ShardRootGrain.CaptureBulkBirthHeadsAsync` and `ShardRootGrain.BulkSwitchesPerFence`; the heads are stored as checkpoints, one below them (`BPlusLeafGrain.HeadsAsCheckpoints`, #5760). | Yes: `ShardRootGrainBulkLoadGraftTests.A_bulk_append_stores_its_hint_and_opens_its_marker_before_its_first_appended_leaf_is_born`, `ShardRootGrainBulkLoadGraftTests.A_resumed_append_reuses_the_stored_hint_rather_than_capturing_a_later_one`, `ShardRootGrainBulkLoadGraftTests.The_next_chunk_of_a_run_reuses_the_runs_hint_and_reads_no_heads`, `ShardRootGrainBulkLoadGraftTests.The_chunk_after_a_full_run_closes_it_with_one_fence_folded_into_its_open_and_takes_the_fences_heads` and `ShardRootGrainBulkLoadGraftTests.A_resumed_chunk_keeps_its_stored_hint_when_it_folds_a_fence`. |
| `Create` | N is created holding the bulk entry and reading from the log's start; the graft intent is persisted | The appended leaf's `BPlusLeafGrain.InitializeSiblingAsync` and `BPlusLeafGrain.MergeEntriesAsync`, then `ShardRootState.PendingBulkGraft`. | Yes: `ShardRootGrainBulkLoadGraftTests.The_hint_applied_is_the_one_stored_before_the_first_appended_leaf_was_born` and `ShardRootGrainBulkLoadGraftTests.A_redelivered_append_for_the_pending_operation_completes_the_graft_and_stops`. |
| `Narrow` | Drain the counted commits (only when it would narrow), narrow and link in one persist, and hand back a copy of the rows at or above the bound with O's read frontier read in the same turn; a resumed narrow of an already-narrowed leaf drains nothing and reports no frontier | `BPlusLeafGrain.NarrowHighKeyAsync`, `BPlusLeafGrain.DrainMutationsAdmittedBeforeAsync`, `BPlusLeafGrain.CopyRowsAtOrAbove`, called by `ShardRootGrain.NarrowAndRelocateAsync`. | Yes: `BPlusLeafGrainTests.NarrowHighKey_waits_for_a_write_admitted_before_it_and_hands_its_row_back`, `BPlusLeafGrainTests.NarrowHighKey_on_a_leaf_already_narrowed_does_not_wait_and_hands_back_its_stranded_rows`, `BPlusLeafGrainTests.NarrowHighKey_hands_back_a_copy_of_every_row_at_or_above_the_bound_and_keeps_them_until_dropped` and `BPlusLeafGrainTests.NarrowHighKey_links_the_successor_in_the_same_persist_so_a_later_write_above_the_bound_forwards`. |
| `NarrowTimeout` | The drain misses its deadline: nothing is narrowed, and the graft links without narrowing and applies no hint | `BPlusLeafGrain.NarrowHighKeyAsync` returns null; `ShardRootGrain.NarrowAndRelocateAsync` returns a null frontier, so no hint is applied. | Yes: `BPlusLeafGrainTests.NarrowHighKey_whose_drain_misses_its_deadline_narrows_nothing_and_returns_null` and `ShardRootGrainBulkLoadGraftTests.A_graft_whose_old_leaf_could_not_drain_in_time_succeeds_links_without_narrowing_and_applies_no_hint`. |
| `Deliver` | Merge the copy into N with its stamps | `ShardRootGrain.RelocateIntoGraftedLeavesAsync` through `BPlusLeafGrain.MergeEntriesAsync`. | Yes: `ShardRootGrainBulkLoadGraftTests.The_graft_relocates_the_rows_the_narrowed_leaf_hands_back_into_the_appended_leaf_declaring_each` and `BulkAppendExistingRowsIntegrationTests.BulkAppend_below_rows_the_shard_holds_relocates_them_and_refuses_nothing`. |
| `Drop` | Only once delivered, O drops exactly the rows it handed back | `BPlusLeafGrain.DropRelocatedRowsAsync`, after the delivery in `ShardRootGrain.NarrowAndRelocateAsync`. | Yes: `BPlusLeafGrainTests.DropRelocatedRows_drops_exactly_the_rows_handed_back_and_keeps_rows_in_span` and `ShardRootGrainBulkLoadGraftTests.A_relocation_that_fails_keeps_the_rows_on_the_old_leaf_and_the_resumed_graft_delivers_them`. |
| `Finish` | N's read position becomes the stored hint capped at the narrow's frontier (#5752), only when every row was relocated and a frontier was reported; the switch stays open for the run's fence | `ShardRootGrain.FinishBulkSwitchAsync` with `ShardRootGrain.CapAtReadFrontier` and `BPlusLeafGrain.SetCheckpointOffsetHintsAsync`. **Unit conversion:** production's `min(hint, frontier)`, in checkpoint terms `min(head - 1, O's checkpoint)` (#5760), is the model's `Min2(hint - 1, fO)`. | Yes: `ShardRootGrainBulkLoadGraftTests.A_bulk_append_applies_its_hint_capped_per_partition_at_the_old_leafs_replay_frontier`, `ShardRootGrainBulkLoadGraftTests.A_graft_whose_old_leaf_reports_no_frontier_applies_no_hint`, `ShardRootGrainBulkLoadGraftTests.A_bulk_append_applies_its_hint_to_every_appended_leaf_and_the_runs_close_closes_its_marker_by_fence`, `ShardRootGrainBulkLoadGraftTests.A_graft_whose_old_leaf_could_not_be_narrowed_does_not_apply_the_hint` and `BulkAppendExistingRowsIntegrationTests.BulkAppend_appended_leaves_start_from_the_birth_hint_and_the_runs_switches_close_by_fence`. |
| `Close` | One fence closes every completed switch, an abandoned one included but not a pending graft's, and removes their hints in the same write; ends the run | `ShardRootGrain.CloseBulkSwitchesAsync`, run by the idle close (`ShardRootGrain.BulkSwitchIdleClose`) or a bulk load's end, with `ShardRootGrain.IsCompletedBulkSwitch`. | Yes: `ShardRootGrainBulkLoadGraftTests.The_idle_close_closes_every_completed_switch_with_one_fence_and_ends_the_run`, `ShardRootGrainBulkLoadGraftTests.The_next_bulk_operation_closes_a_switch_an_abandoned_append_left_open_without_applying_its_hint` and `ShardRootGrainBulkLoadGraftTests.The_next_bulk_operation_leaves_the_switch_of_a_graft_still_pending_open`. |
| `Crash` | The shard root dies once: before the graft intent the client retries from Begin; after it the graft resumes from the narrow | `ShardRootGrain.ResumePendingBulkGraftAsync` and `ShardRootGrain.CompleteBulkGraftAsync`; a retried operation re-enters `ShardRootGrain.BeginBulkSwitchAsync` under the same operation id. | Yes: `ShardRootGrainBulkLoadGraftTests.An_ordinary_operation_resumes_an_interrupted_graft_before_serving`, `ShardRootGrainBulkLoadGraftTests.A_relocation_that_fails_keeps_the_rows_on_the_old_leaf_and_the_resumed_graft_delivers_them` and `ShardRootGrainBulkLoadGraftTests.A_resumed_append_reuses_the_stored_hint_rather_than_capturing_a_later_one`. |
| `Replay` | Once its switch is finished, N applies the next record above its read position | The leaf's WAL replay from its checkpoint. | Yes: `BulkAppendExistingRowsIntegrationTests.BulkLoad_leaves_start_from_the_birth_hint_and_every_key_survives_reactivation`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor so TLC does not report a quiescent state as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `BulkHintBound` | An appended leaf's birth read position is at or below every record of its range it does not hold: rows the range already held are relocated into it, admitted commits are drained before the narrow, and the hint is applied only after a complete relocation. | Yes: `BulkAppendExistingRowsIntegrationTests.BulkAppend_appended_leaves_start_from_the_birth_hint_and_the_runs_switches_close_by_fence`, `BulkAppendExistingRowsIntegrationTests.BulkLoad_leaves_start_from_the_birth_hint_and_every_key_survives_reactivation`, `BPlusLeafGrainTests.NarrowHighKey_waits_for_a_write_admitted_before_it_and_hands_its_row_back` and `ShardRootGrainBulkLoadGraftTests.A_graft_whose_old_leaf_could_not_drain_in_time_succeeds_links_without_narrowing_and_applies_no_hint`. |
| `ContestedKeyAgrees` | A bulk append over a key the range already holds returns the later-stamped value on the origin, as on every replica and view. | Yes: `BulkAppendExistingRowsIntegrationTests.BulkAppend_after_point_writes_above_the_maximum_keeps_every_key_readable_last_writer_wins` and `BulkAppendExistingRowsIntegrationTests.BulkAppend_over_deleted_keys_resolves_each_by_its_stamp`. |
| `HintOnlyWhileOpen` | A stored birth hint belongs to an open switch: stored with its open token and removed with its close. | Yes: `ShardRootGrainBulkLoadGraftTests.A_bulk_append_stores_its_hint_and_opens_its_marker_before_its_first_appended_leaf_is_born` and `ShardRootGrainBulkLoadGraftTests.The_idle_close_closes_every_completed_switch_with_one_fence_and_ends_the_run`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

All four checked properties are state safety, reached and checked on the base with every
environment step on, so nothing is bounded out. Liveness is not checked here: that a
switch eventually closes and its leaves arm is the core module's concern. Every
experiment is an anchored action perturbation except `ContestedKeyAgreesStrandedValueKept`,
which edits the owner's resolution; none splices in an action. A clean witness run
confirms the steps the properties depend on are reached: a relocated row held below the
read position, the owner resolving K to a commit stamped after the bulk entry, a foreign
record above the read position not yet replayed, the 10.2.0 path replaying from the log's
start, a read position capped below the hint at the narrow's frontier, a foreign record above the frontier held by N's replay, a graft resumed after its narrow applying no hint, a hint reused from the run, a graft resumed after a crash relocating again, a retry
after an abandoned switch's close reading the heads afresh, the bulk value winning after
a replay, and a drained commit relocated below the hint.

## Design mutations moved here

The design model's mutations the interface assigns to this module were moved, re-targeted
at its properties and re-anchored on its text.

| Design mutation | Here |
|-----------------|------|
| `ReadPositionHonestBulkWithoutHint` | `BulkHintBoundWithoutHint`; anchored on `Finish`, where production applies the read position. |
| `ReadPositionHonestBulkAppendDropsExistingRows` | `BulkHintBoundDropsExistingRows`; anchored on `Deliver`. |
| `ContestedKeyAgreesStrandedValueKept` | Same name; it edits the owner's resolution, so it names no perturbed action. |

## Deliberate abstraction gaps

- **One key, one appended leaf, one chunk.** Every record writes one key of the appended
  range, the operation appends one leaf, and the run's earlier chunk appears only as one
  completed switch with a hint read at the run's start. Keys that fill the old leaf's free
  space, below-edge runs and their re-delivery skip, several appended leaves and their
  ranges, and the descent that links them are not modelled.
  Production symbols: `ShardRootGrain.RelocateIntoGraftedLeavesAsync`, `ShardRootGrain.CompleteBulkGraftAsync`.
- **The bulk entry is held by every reader.** N and every WAL reader hold the bulk entry,
  whose version is fixed between the records allocated before Begin and those after; the
  appended leaf's own records, the relocation's re-appends and writes forwarded to it
  after the narrow are its own appends, above the hint, and are not modelled.
  Production symbols: `HybridLogicalClock.Tick`, `BPlusLeafGrain.MergeEntriesAsync`.
- **The close's fence is the hand-off module's.** `Close` removes the hint and closes the
  token; the routing generation's raise and re-claim, the drain of batch writes admitted
  before it, and the close heads it records are what H's `HandOffBound` covers, and the
  idle close's timer is not modelled.
  Production symbols: `ShardRootGrain.FenceBulkSwitchesAsync`, `ShardRootGrain.DrainBatchWritesAdmittedBeforeAsync`, `ShardRootGrain.RaiseAndReclaimRoutingGenerationAsync`, `ShardRootGrain.BulkSwitchIdleClose`.
- **The gate, older silos and bulk loads.** With the append-fence gate closed, or an older
  silo hosting the old leaf, the operation stores no hint and takes the 10.2.0 path; the
  model reaches that path only through a drain past its deadline. A bulk load, which
  replaces the root and relocates a batch write stored on it, is not modelled.
  Production symbols: `ShardRootGrain.BulkGraftSeamsAvailable`, `WalAppendFenceMethodProbe.IsDefiniteMethodUnknown`.
- **Shadow markers are not carried by the relocation.** The model has no sagas or reshard migration, so it does not check that a relocated row keeps its read gate. A split and a span forward install shadow markers on the destination for every saga still isolating a moved key (prepared, not yet terminal, or a migrated row) before they merge the rows; the bulk graft's relocation does not, so a prepared or migrated key relocated into the appended leaf can be read ungated there until its backstop lands. The remedy, the same transfer per destination before the merge, is #5755, and this entry stands until that change is in this tree.
  Production symbols: `BPlusLeafGrain.TransferShadowMarkersToSiblingAsync`, `ShardRootGrain.RelocateIntoGraftedLeavesAsync`, `BPlusLeafGrain.NarrowHighKeyAsync`.
- **The persisted drop.** The old leaf persists its drop, so a reloaded snapshot cannot
  bring the relocated rows back outside its span; the model has no snapshot of the old
  leaf, and its counts are not modelled.
  Production symbols: `BPlusLeafGrain.DropRelocatedRowsAsync`.

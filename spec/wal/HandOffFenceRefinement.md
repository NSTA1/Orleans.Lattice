# Refinement note: the WAL hand-off fence to production

This note maps [`WalHandOffFence.tla`](WalHandOffFence.tla) - a foreign author's
writes to a range the owner leaf L owns, bounded by L's durable hand-off marker - to
the Orleans.Lattice code that plays each role. It is module H of the five modules
promoted from the design model [`design/WalLiveAdvance.tla`](design/WalLiveAdvance.tla)
for epic #5605, under the contract in [`LiveAdvanceInterface.md`](LiveAdvanceInterface.md).
The design authority is [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md),
section 2 ("The hand-off fence: other writers"). Like the other notes in this
directory it is a documented mapping, not a refinement proof: the gates check its
names, detectors and coverage, and its claims were checked by reading production.

## Model scope in one paragraph

One owner leaf L's hand-off marker at one WAL partition of three offsets (four in the
`WideWindows` variant), with generations up to 3 (4). One foreign initiator f stands
for every author of a record L owns that is not an activation of L: a split donor, a
reclaim neighbour, an adaptive split's or a shard fold's donor shard root, the tree's
range-delete fence, and the shard root of a bulk switch. It opens L's marker, admits
point and batch writes stamped with the generation current at admission, and closes
by fence: it stops admitting, bumps and re-claims its generation, which raises its
entry at the WAL shard and reads the allocator head in one step, and records that
head as the marker's close. The WAL shard allocates a stamped append at or above the
entry and rejects one below it. Two hand-off windows run in turn (three in the
variant). Another shard's walk of a tree-level range delete g is stamped with the
same tree generation; a rejected walk re-stamps while L's marker holds its intent and
otherwise appends unstamped; another range delete's close re-claims the shared
generation without closing L's marker. The WAL shard restarts once. A hand-off begun
before the fence gate opened may be in flight at the start, with the gate closed and
L's marker state uninitialised; it opens its marker when it resumes under the gate,
and L's marker state is initialised once it is no longer in flight. L's own appends,
replay and read positions, routing floors and purges are other modules'.

## Interface

This module is H in the assume-guarantee graph of the five modules that replace the
design model.

- **Assumptions.** E's entry check, as an environment constraint and in exactly the
  interface's form. An append below the initiator's entry is never allocated
  (`ForeignAlloc`, `ForeignReject`, `GAlloc` and `GReject` read the entry `fFence`
  alone, and never a floor). A re-claim raises the entry in the same step that reads
  the allocator head it returns (`Close` writes `fFence` and `hoClosedAt` in one step).
  The entry is always loaded here: a WAL shard restart unloads it in production and
  the next append's re-presented claim reloads it from the durable row, which this
  module takes as one step (`Restart` keeps `fFence`). Nothing is retired here, so the
  clause that a claim at or below `floor(i)` is never raised is not exercised; it is
  the routing-floor module's. The module reads no state of E: the entry check is the
  shape of these actions, not an import.
- **Guarantees.** Three, each checked in the base and the variant: two invariants
  and an action property.
  - `HandOffBound`: whenever L's marker reads closed (its state is initialised and no
    window is open), every stamped foreign append - `Foreign(i)` in the interface's
    vocabulary, author `f` or `g` here - lies below the head its last close recorded.
    Checked at every state, this is the interface's form: a `Foreign(i)` offset is
    allocated only while the marker is open, or below the `closedAt` the close
    recorded, whatever its kind (point, batch, another shard's walk) or path (stamp
    at admission, a re-stamp, a restart in between).
  - `AbsentMarkerHolds`: L's marker state is initialised only while no hand-off begun
    without a marker is in flight, so an absent or uninitialised marker holds L.
  - `CloseHeadMonotone`: the heads L's marker requires, `closedAt`, never decrease.
    `Close` records any head up to the allocator's above every stamped foreign
    append - the weakest head E's re-claim allows, since the claim head covers every
    append before it - and max-merges it into `closedAt`, as production's
    `HandOffMarkerCore.RequiredHead` does. A later window whose own appends all lie
    below a lower head therefore cannot release an owner an earlier close holds.
- **The vocabulary.**

  | Interface | Here |
  |-----------|------|
  | L | The owner whose marker is `hoOpen` / `hoClosedAt` / `mInit`. |
  | The partition's allocator `next` | `next`. |
  | `Foreign(i)` | An offset whose `author` is `f` (a point or batch write) or `g` (another shard's range-delete walk), allocated stamped. |
  | `Unstamped` | An offset whose `author` is `u`: a walk that goes on unstamped once its intent has closed. |
  | `Own(a)`, `Straddler(i)` | Not modelled: E's and R's. |
  | `entry(i)` | `fFence`, the WAL shard's entry for the initiator. |
  | `floor(i)` | Not modelled: R's, and read only at the claim. |
  | L's marker: `initialised`, `open`, `closedAt` | `mInit`, `hoOpen`, `hoClosedAt`. |
  | `claimHead[a]`, `seenU[a]` | Not modelled: C's. |

- **Who assumes what.** C assumes all three for every foreign append and close, the bulk
  switch's included, and owns the arming that reads the marker and replays to its heads;
  B assumes nothing of H. The graph is E <- H, E <- R, and {E, H, R, B} <- C,
  where an arrow points from the module that assumes to the module that guarantees.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next`, `author` | The WAL shard's allocator, and the kind of author each offset went to | `WalShardGrain._nextOffset`, assigned under the shard's state lock. |
| `hoOpen`, `hoClosedAt` | L's marker: a window open over L's range, and the maximum of the heads its closes recorded | The durable marker rows: `ShardHandOffState` (row `_lattice_shard-handoff`, through `HandOffMarkerRows.UpdateShardAsync`) for shard-level hand-offs and `LeafHandOffState` for a split's; `ShardHandOffState.Open` and `ShardHandOffState.Closes`, read through `HandOffMarkerCore.IsHeldOpen` and `HandOffMarkerCore.RequiredHeads` (per partition, `HandOffMarkerCore.RequiredHead`), the maximum over every retained close and `ShardHandOffState.CompactedHeads`. |
| `windows` | Hand-off windows opened so far | Modelling device. |
| `fAdmit` | The initiator is admitting writes for L's range | The initiator's own phase: a split donor's span before its narrow, a migration's `ShardSplitInProgress` before its reject phase, an open `TreeRangeDeleteIntent`. |
| `fpend`, `fkind`, `fstamp` | A write admitted and not yet allocated or rejected: its kind and the generation it was admitted under | A stamped call in flight carrying `WalRoutingStamp.Generation` (and, for a leaf initiator, `WalRoutingStamp.AdmissionMark`). |
| `fEpoch` | The initiator's current generation | `ShardHandOffState.RoutingGeneration` for a shard root; `TreeRangeDeleteFenceState.Generation` for the range-delete fence; `LeafAppendEpochFence.Epoch` for a leaf donor. |
| `fFence` | The WAL shard's entry for the initiator | The durable row `WalAppendFenceRows.GenerationGrainType`, loaded into `WalShardGrain._generationEntries` (a leaf donor's is its epoch entry, E's). |
| `restarted` | Environment budget | Modelling device. |
| `gst`, `gstamp` | Another shard's range-delete walk and the tree generation it carries | The walk loop of `ShardRootGrain.DeleteRangeBoundedCoreAsync` and its `WalRoutingStamp.ForTreeRangeDelete` stamp. |
| `closing` | A close between its re-claim and its marker close | `TreeRangeDeleteIntent.Closing`; always FALSE here because the close is atomic (rule 1). |
| `gate` | The cluster's fence gate | `IWalAppendFenceGate.IsOpen` (`ClusterManifestWalAppendFenceGate`). |
| `hfly`, `resumed` | A hand-off begun with no marker is in flight, and has resumed | A shard root's `ShardRootState.SplitInProgress`, `ShardRootState.PendingBulkGraft` or `ShardRootState.ShadowForward` record begun with the gate closed, and its resume path. |
| `mInit` | L's marker state is initialised | `ShardHandOffState.Initialised` and `LeafHandOffState.Initialised`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `ForeignOpen` | Open L's marker before the first admission of a write L will own | `HandOffMarkerCore.Open` through `ShardRootGrain.OpenShardMarkerAsync` (a migration's `ShardRootGrain.OpenMigrationHandOffAsync`, a reclaim's `ShardRootGrain.OpenReclaimHandOffAsync`), `BPlusLeafGrain.OpenSplitHandOffAsync` for a split, and `TreeRangeDeleteFenceGrain.OpenIntentAsync` for a range delete. | Yes: `HandOffMarkerCoreTests.An_open_is_idempotent_and_holds_only_overlapping_owners`, `LeafHandOffFenceIntegrationTests.Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_donors_bump` and `ShardRoutingGenerationIntegrationTests.A_slot_migration_opens_its_marker_on_the_target_and_closes_it_by_the_donors_re_claim`. |
| `ForeignAdmit(k)` | Admit a point or batch write while admitting, stamped in the same step with the current generation, or an admission mark | `ShardRootGrain.AdmissionRoutingStamp`, taken with the phase check; a leaf's `BPlusLeafGrain.StampedRouting`, which captures `WalRoutingStamp.AdmissionMark` from `LeafAppendEpochFence.HandOffCloses`. | Yes: `ShardRoutingGenerationIntegrationTests.Client_writes_are_stamped_with_the_shard_routing_generation_never_appended_unstamped`, `BPlusLeafGrainTests.A_stamped_write_appends_with_the_leafs_own_admission_mark_not_its_callers` and `BPlusLeafGrainTests.A_backstop_append_carries_an_admission_mark_captured_before_its_span_check`. |
| `ForeignAlloc` | The WAL shard allocates a stamped append at or above the initiator's loaded entry | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsGenerationAdmitted`, under the lock that assigns `WalShardGrain._nextOffset`. | Yes: `WalAppendFenceCoreTests.A_routing_generation_is_admitted_only_from_a_loaded_current_entry` and `WalShardGrainTests.Routing_generations_are_fenced_and_durable`. |
| `ForeignReject` | A stamped append below the entry is rejected, allocating nothing, and retried at its seam | `WalShardGrain.AdmitStampLocked` returns `WalAppendFenceOutcome.StaleGeneration`; the caller sees `WalRoutingGenerationStaleException` and `ShardRootGrain.NextRoutingRetryAsync` re-admits; a leaf's own pre-close write is a self-fence (`LeafAppendEpochFence.OnAdmitted`). | Yes: `WalCommitLogWriterTests.A_stale_generation_point_append_is_reported_refused_without_allocation`, `WalCommitLogWriterTests.A_self_fenced_point_append_is_reported_refused_without_allocation` and `ShardRoutingGenerationIntegrationTests.A_write_admitted_under_a_re_claimed_generation_is_retried_from_the_shard_root_and_succeeds`. |
| `Close` | Stop admitting, bump and re-claim the generation (raising the entry and reading the head in one step), and close the marker with a head at or below it and above every stamped foreign append, max-merged with the heads earlier closes recorded | `ShardRootGrain.CloseShardMarkerAsync` through `ShardRootGrain.RaiseAndReclaimRoutingGenerationAsync` and `ShardRootGrain.ClaimRoutingGenerationEverywhereAsync`, then `HandOffMarkerCore.Close`, which appends the close and folds an evicted one into `ShardHandOffState.CompactedHeads` by a per-partition maximum; a split's `BPlusLeafGrain.CloseSplitHandOffAsync`; the range delete's `TreeRangeDeleteFenceGrain.CloseCoreAsync`. The re-claim waits out a stamped batch still assigning offsets (`WalShardGrain.GenerationBatchDrainLocked`). | Yes: `HandOffMarkerCoreTests.A_close_records_the_fence_heads_over_the_hand_off_range_and_never_reopens`, `ShardRoutingGenerationIntegrationTests.A_slot_migration_opens_its_marker_on_the_target_and_closes_it_by_the_donors_re_claim`, `WalShardGrainTests.A_generation_re_claim_never_splits_a_generation_stamped_batch`, `LeafAppendEpochFenceTests.A_hand_off_close_with_the_gate_view_closed_after_stamping_leaves_the_marker_open` and `HandOffMarkerCoreTests.A_later_close_with_lower_heads_never_lowers_the_required_heads`. |
| `Restart` | A WAL shard reactivation, or a move's cutover; the generation entry is a durable row and survives | The row `WalAppendFenceRows.GenerationGrainType`, reloaded by a re-presented `WalShardGrain.ClaimRoutingGenerationAsync`. **Environment:** once in the model. | Yes: `WalShardGrainTests.A_generation_entry_is_unloaded_until_re_presented_after_a_restart` and `WalShardGrainTests.Routing_generations_are_fenced_and_durable`. |
| `GAdmit` | Another shard's walk is stamped with the tree's range-delete generation only under an open intent | `TreeRangeDeleteFenceGrain.OpenIntentAsync` hands out the generation; `ShardRootGrain.DeleteRangeBoundedCoreAsync` stamps the walk with `WalRoutingStamp.ForTreeRangeDelete` only when it carries an intent, and appends a walk with none unstamped. | Yes: `TreeRangeDeleteFenceIntegrationTests.A_range_delete_is_fenced_at_the_tree_and_closes_every_shard_marker_with_its_heads` and `TreeRangeDeleteFenceIntegrationTests.An_unfenced_walk_through_the_existing_method_is_appended_unstamped`. |
| `GAlloc` | The walk's stamped append is allocated at or above the tree generation's entry | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsGenerationAdmitted` for the range-delete initiator. | Yes: `TreeRangeDeleteFenceGrainTests.The_first_intent_is_handed_a_generation_claimed_at_every_partition` and `TreeRangeDeleteFenceIntegrationTests.A_range_delete_is_fenced_at_the_tree_and_closes_every_shard_marker_with_its_heads`. |
| `GReject` | One re-claim of the tree generation rejects every shard's walk admitted before it | `WalAppendFenceOutcome.StaleGeneration` for the range-delete initiator, caught in `ShardRootGrain.DeleteRangeBoundedCoreAsync`. | Yes: `TreeRangeDeleteFenceGrainTests.One_close_fences_every_open_intent_and_an_open_intent_re_stamps_with_the_new_generation`. |
| `GRestamp` | A rejected walk re-stamps only while its own intent is open, asked of the fence grain in a turn that never interleaves with a close | `ShardRootGrain.DeleteRangeBoundedCoreAsync` asks `TreeRangeDeleteFenceGrain.GetOpenIntentGenerationAsync` by intent id; the close marks the intent closing before its first re-claim (`TreeRangeDeleteFenceGrain.CloseCoreAsync`). **Over-approximation:** reading L's marker also allows a re-stamp under a newer intent covering L. | Yes: `TreeRangeDeleteFenceIntegrationTests.A_walk_rejected_while_its_own_intent_is_open_re_stamps_with_the_current_generation` and `TreeRangeDeleteFenceGrainTests.A_close_marks_its_intent_closing_durably_before_its_first_re_claim`. |
| `GUnstamped` | A rejected walk whose own intent has closed appends unstamped, admitted and reported | `ShardRootGrain.DeleteRangeBoundedCoreAsync` drops its ticket and calls the unstamped leaf method, recording the source (`ShardRootGrain.RecordUnfencedRangeDeleteAppend`). | Yes: `TreeRangeDeleteFenceIntegrationTests.A_walk_whose_own_intent_closed_goes_on_unstamped_even_while_another_intent_is_open`. |
| `OtherIntentClose` | Another range delete's close re-claims the shared generation and closes only its own intent's markers | `TreeRangeDeleteFenceGrain.CloseCoreAsync` closes the markers under its own token (`TreeRangeDeleteFenceGrain.MarkerToken`) through `HandOffMarkerCore.Close`. | Yes: `TreeRangeDeleteFenceGrainTests.One_close_fences_every_open_intent_and_an_open_intent_re_stamps_with_the_new_generation` and `TreeRangeDeleteFenceIntegrationTests.A_walk_rejected_while_its_own_intent_is_open_re_stamps_with_the_current_generation`. |
| `OpenGate` | The last older-build silo leaves and the fence gate opens | `ClusterManifestWalAppendFenceGate.IsOpen`. **Environment.** | Yes: `ClusterManifestWalAppendFenceGateTests.Opens_when_every_active_silo_advertises_the_marker`. |
| `Resume` | A hand-off begun with no marker opens it on every resume path before its next admission (rule 1) | `ShardRootGrain.OpenMigrationHandOffAsync` from `ShardRootGrain.EnterRejectPhaseAsync`, `ShardRootGrain.CompleteSplitAsync` and a re-asserted `ShardRootGrain.BeginSplitAsync`. | Yes: `ShardRoutingGenerationIntegrationTests.A_migration_resumed_without_its_marker_opens_it_before_its_next_admission`. |
| `LegacyComplete` | A resumed hand-off, its marker closed and nothing pending, completes | `ShardRootGrain.CompleteSplitAsync` closes by fence and then clears the shard's `ShardSplitInProgress` record. | Yes: `ShardRoutingGenerationIntegrationTests.A_migration_resumed_without_its_marker_opens_it_before_its_next_admission` and `ShardRoutingGenerationIntegrationTests.A_slot_migration_opens_its_marker_on_the_target_and_closes_it_by_the_donors_re_claim`. |
| `MarkInit` | With no marker-less hand-off in flight, L's marker state is initialised (rule 2) | `ShardRootGrain.TryInitialiseHandOffMarkerAsync`, which refuses while a migration, bulk graft or shadow forward is in flight; a leaf takes its shard's flag only with no split in flight. | Yes: `ShardRoutingGenerationIntegrationTests.A_shard_root_with_a_migration_in_flight_leaves_its_marker_uninitialised`, `BPlusLeafGrainTests.A_leaf_with_a_split_in_flight_does_not_adopt_its_shards_flag` and `HandOffMarkerRowRoundTripTests.An_absent_marker_reads_as_uninitialised_with_nothing_open`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor so TLC does not report a quiescent state as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `HandOffBound` | Once a hand-off's marker closes, every record its initiator appended under the fence that the owner holds lies below the recorded heads, so an owner that replays to them before arming passes no foreign record it has not applied. | Yes: `ShardRoutingGenerationIntegrationTests.A_slot_migration_opens_its_marker_on_the_target_and_closes_it_by_the_donors_re_claim`, `TreeRangeDeleteFenceIntegrationTests.A_range_delete_is_fenced_at_the_tree_and_closes_every_shard_marker_with_its_heads`, `LeafHandOffFenceIntegrationTests.Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_donors_bump` and `BPlusLeafGrainTests.A_close_head_above_the_checkpoint_holds_the_advance_until_a_replay_reaches_it`. |
| `CloseHeadMonotone` | The heads an owner must replay to before it re-arms never decrease: a later close with lower heads, or the eviction of a retained close, leaves the maximum in force. | Yes: `HandOffMarkerCoreTests.A_later_close_with_lower_heads_never_lowers_the_required_heads` and `HandOffMarkerCoreTests.Required_heads_cover_only_overlapping_closes_and_every_evicted_one`. |
| `AbsentMarkerHolds` | An absent or uninitialised marker never reads as closed: its owner is held until the marker state is initialised, which happens only with no marker-less hand-off in flight. | Yes: `ShardRoutingGenerationIntegrationTests.A_shard_root_with_a_migration_in_flight_leaves_its_marker_uninitialised`, `BPlusLeafGrainTests.An_uninitialised_leaf_marker_holds_until_the_leaf_adopts_its_shards_flag` and `HandOffMarkerRowRoundTripTests.An_absent_marker_reads_as_uninitialised_with_nothing_open`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

`TypeOK` and the two invariants are state safety, and `CloseHeadMonotone` is an action
property; all four are checked on the base and re-checked in the `WideWindows` variant.
A close may choose a head below the allocator's only once a walk has gone on
unstamped, which takes a close to reject it, so a head below an earlier close's needs a
third close of L; the base's generations allow two. The property's mutation therefore
runs with the variant's three windows and four generations. Every environment step is on in the base,
and the initial state may or may not carry a marker-less hand-off, so nothing is
bounded out; the variant adds an offset, a generation and a third window. Liveness is
not checked here: that a close eventually lands, under fair storage and the fence
grain's reminder, is the core module's concern. Every experiment is an anchored
action perturbation; `HandOffBoundRestampDuringClose` also splices in the split
close's second step (`CloseMark`), as the design's mutation did. A clean witness run
confirms the steps the properties depend on are reached: a stamped foreign append and
a stamped walk, a point and a batch write rejected by a close, a rejection across the
WAL shard restart, a walk re-stamped and re-rejected, a walk appended unstamped, a
second window closed above the first, a resumed legacy hand-off's append under an
initialised marker, and the marker reading closed above stamped foreign appends.

## Design mutations moved here

The design model's mutations whose guard lives in this module were moved, re-targeted
at this module's properties and re-anchored on its text. The design model keeps its
copies until every module is promoted.

| Design mutation | Here |
|-----------------|------|
| `ReadPositionHonestNoOpenCheck` | `HandOffBoundNoOpenCheck`; anchored on `ForeignOpen`. The owner's read of the marker is the marker itself here, so the open L cannot see is an open the initiator never wrote; the arming that reads it is C's. |
| `ReadPositionHonestRearmWithoutReplay` | `HandOffBoundRearmWithoutReplay`; anchored on `Close`. The replay to the heads is C's; this module's half is that the close records the heads to replay to. |
| `ReadPositionHonestCloseHeadUnderstated` | `HandOffBoundCloseHeadUnderstated`; anchored on `Close`. |
| `ReadPositionHonestDrainWithoutFence` | `HandOffBoundDrainWithoutFence`; anchored on `Close`. |
| `ReadPositionHonestDrainPointOnly` | `HandOffBoundDrainPointOnly`; anchored on `ForeignAlloc` and `ForeignReject`. |
| `ReadPositionHonestStampAtAppend` | `HandOffBoundStampAtAppend`; anchored on `ForeignAlloc` and `ForeignReject`. |
| `ReadPositionHonestGenerationNotDurable` | `HandOffBoundGenerationNotDurable`; anchored on `Restart`. |
| `ReadPositionHonestRangeDeletePerShardClose` | `HandOffBoundRangeDeletePerShardClose`; anchored on `GAlloc` and `GReject`. |
| `ReadPositionHonestRangeDeleteWithoutIntent` | `HandOffBoundRangeDeleteWithoutIntent`; anchored on `GAdmit`. |
| `ReadPositionHonestRestampDuringClose` | `HandOffBoundRestampDuringClose`; anchored on `Close` and `GRestamp`. |
| `ReadPositionHonestRestampUnderAnyIntent` | `HandOffBoundRestampUnderAnyIntent`; anchored on `GRestamp`. |
| `ReadPositionHonestResumeWithoutMarker` | `HandOffBoundResumeWithoutMarker`; anchored on `Resume`. |

The module adds eight of its own, so that every action has a perturbation:
`HandOffBoundAdmitAfterClose` (`ForeignAdmit`), `HandOffBoundLazyAdmissionMark`
(`ForeignAdmit`, `ForeignAlloc`, `ForeignReject`), `HandOffBoundUnstampedFallbackStamped`
(`GUnstamped`), `HandOffBoundOtherIntentClosesMarker` (`OtherIntentClose`),
`HandOffBoundCompleteWithoutClose` (`LegacyComplete`),
`AbsentMarkerHoldsInitWithHandOffInFlight` (`MarkInit`),
`AbsentMarkerHoldsInitAtGateOpen` (`OpenGate`) and `TypeOkCloseUnbounded` (`Close`).

## Redundancy checks

The design model kept one guard of this module that its model implies, as a
redundancy check expected to stay clean. It is not a mutation, so it is not in the
mutation directory; it was run locally against this module.

| Check | Here | Result |
|-------|------|--------|
| `ReadPositionHonestNoMarkerInit` | Kept: `HandOffBound` reads the marker as closed whenever no window is open, ignoring whether its state is initialised. | Clean against `HandOffBound` in the base: rule 1 (a resume opens its marker before its next admission) already bounds every stamped append, so rule 2 is implied here, as in the design. It is kept because a resume path the stamp census misses would otherwise read as closed. |

## Deliberate abstraction gaps

- **One initiator, one owner, one partition.** The foreign author stands for every
  hand-off kind, one at a time, and the module checks a single partition. Production
  re-claims every partition the initiator has claimed in its activation and records
  a head per partition; a marker carries several tokens over several ranges, and an
  owner is held by the tokens that overlap its range.
  Production symbols: `HandOffMarkerCore.RequiredHeads`, `HandOffMarkerCore.IsHeldOpen`, `HandOffMarkerCore.Overlaps`.
- **The owner's read and its replay.** `HandOffBound` is stated over the marker
  itself. How the owner reads it (fresh, after its last own append), and its replay
  to the heads before it arms, are the core module's.
  Production symbols: `ShardRootGrain.GetLiveAdvanceViewAsync`, `ShardRootGrain.GetHandOffMarkerAsync`.
- **A close with the gate view closed.** The model's close always bumps. Production's
  close by an initiator whose view of the gate is closed leaves the marker open if it
  has stamped, and records unfenced heads only if it never stamped; a migration's
  close with the gate closed records the current heads over unstamped writes, which
  the watermark reports.
  Production symbols: `ShardRootGrain.CloseMigrationHandOffAsync`, `BPlusLeafGrain.CaptureAllocatorHeadsAsync`.
- **Unstamped writes.** A hand-off's writes before the gate opens are unstamped, and
  so not `Foreign(i)`: they are admitted and reported, the core module's concern, and
  are not modelled here. Only the unstamped fallback of a walk is, to show it stays
  outside the bound.
  Production symbols: `ShardRootGrain.RecordUnfencedRangeDeleteAppend`.
- **Rule 3's intent identity.** The re-stamp's guard reads L's marker, a safe
  over-approximation of "the walk's own intent is open"; production asks by intent id.
  Production symbols: `TreeRangeDeleteFenceGrain.GetOpenIntentGenerationAsync`.
- **Stamped batches, seam retries and the deadline.** A write allocates one offset in
  one step, and a rejected write is dropped rather than retried; production admits a
  multi-entry batch whole, retries a rejection at its seam under the current
  generation, and closes a range delete's intents past their deadline from a reminder.
  Production symbols: `WalFencedAppendDispatch.AppendBatchAsync`, `TreeRangeDeleteFenceGrain.ReceiveReminder`.

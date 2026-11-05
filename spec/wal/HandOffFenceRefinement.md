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

In plain terms, the module guarantees that once a hand-off's marker on L reads closed,
every record another author appended to L's range under the fence lies below the heads
the close recorded, so L can replay to those heads and miss nothing; that a missing or
uninitialised marker never reads as closed; and that the heads L must replay to never
go down.

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
- **Guarantees.** Five, each checked in the base and the variants: four invariants
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
  - `LiveDonorProgress` (#5840 S4): under pre-arm, the live donor is never
    superseded by the split's link-time fence, nor left self-fencing its own appends,
    unless a stale duplicate's claim raised D's row. It is vacuous outside the
    stale-donor variants.
  - `StaleDetectionHonoured` (#5840 S4, R6): a claim probe that finds the live donor
    stale marks it stale, even when the relinquish discards the claim.
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
| `eRow` | The split donor D's epoch (#5841, `WithStaleDonor`): its durable row and, at once, D's own entry at every partition; 0 while no activation of D has claimed | The leaf epoch row `WalAppendFenceRows.LeafEpochGrainType` and D's per-leaf entries in `WalShardGrain`. The split raises D's own entry at every partition to the bumped value (`WalShardGrain.ClaimAppendEpochAsync`), never the partition's shared epoch floor; the model's one step stands for the row write and the per-partition raises, which is sound because the heads the close records are read after each raise. |
| `liveArmed` | The splitting activation has claimed | `LeafAppendEpochFence.HasClaimedAnywhere`; always TRUE outside pre-arm, where the splitting activation has written stamped. |
| `sNarrowed` | D's narrowed leaf state is persisted | `BPlusLeafGrain`'s split persist of `LeafNodeState` with `UnlinkedSplitSiblingId`. |
| `dst`, `dEpoch` | A stale duplicate D' of the donor and the epoch its claim wrote | A second activation of D that loaded its state before the split. |
| `dpend`, `dstamp` | D''s epoch-only write of a moved key, admitted under its epoch | A `WalRoutingStamp.None` append: the PendingTx terminal backstop and the span-forward receive. |
| `bst`, `bRead` | The split's bump of D's row and the row value it read | The split's conditional row write. |
| `dRetried` | D' has used its one probed re-claim after a `StaleEpoch` at its own epoch | `LeafAppendEpochFence`'s per-close re-claim allowance. |
| `lpend`, `lstamp`, `lUsed` | The live donor's write of a moved key admitted before the narrow, and the epoch it was stamped with | A stamped append of the live donor of D (#5840 S4). |
| `relEpoch` | The epoch the live donor relinquished at its counted pre-arm close; 0 none | `LeafAppendEpochFence._relinquishedEpoch`, set by `LeafAppendEpochFence.RelinquishLocked` from `LeafAppendEpochFence.CountSplitClose`. |
| `lEpoch`, `lCached`, `lClaim` | The live donor's installed epoch, the row value its fence caches, and the epoch of a claim it has in flight | `LeafAppendEpochFence._epoch`, `LeafAppendEpochFence._row` and an in-flight `LeafAppendEpochFence.ClaimRowAsync` or `LeafAppendEpochFence.BumpAsync`. |
| `lAllow` | The live donor has used its one probed re-claim | The per-close allowance in `LeafAppendEpochFence.OnStale`. |
| `lBumped` | The live donor has taken its one floor-refusal bump | `LeafAppendEpochFence.BumpAsync`. |
| `lStaleLeaf` | Another activation has persisted D's state since the live donor loaded it, so its claim's probe finds its ETag stale | The ETag comparison in `LeafAppendEpochFence.ClaimRowAsync`'s probe (#5841, A1). |
| `lStamped`, `lDead`, `lStuck`, `dupRaised`, `lProbeFailed` | Ghosts: the live donor stamped after the split's bump, was superseded or marked stale, or is left self-fencing; a duplicate's claim raised D's row; a probe of the live donor found its state stale | None: proof bookkeeping for `LiveDonorProgress` and `StaleDetectionHonoured`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `ForeignOpen` | Open L's marker before the first admission of a write L will own | `HandOffMarkerCore.Open` through `ShardRootGrain.OpenShardMarkerAsync` (a migration's `ShardRootGrain.OpenMigrationHandOffAsync`, a reclaim's `ShardRootGrain.OpenReclaimHandOffAsync`), `BPlusLeafGrain.OpenSplitHandOffAsync` for a split, and `TreeRangeDeleteFenceGrain.OpenIntentAsync` for a range delete. | Yes: `HandOffMarkerCoreTests.An_open_is_idempotent_and_holds_only_overlapping_owners`, `LeafHandOffFenceIntegrationTests.Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_modes_fence` and `ShardRoutingGenerationIntegrationTests.A_slot_migration_opens_its_marker_on_the_target_and_closes_it_by_the_donors_re_claim`. |
| `ForeignAdmit(k)` | Admit a point or batch write while admitting, stamped in the same step with the current generation, or an admission mark | `ShardRootGrain.AdmissionRoutingStamp`, taken with the phase check; a leaf's `BPlusLeafGrain.StampedRouting`, which captures `WalRoutingStamp.AdmissionMark` from `LeafAppendEpochFence.HandOffCloses`. | Yes: `ShardRoutingGenerationIntegrationTests.Client_writes_are_stamped_with_the_shard_routing_generation_never_appended_unstamped`, `BPlusLeafGrainTests.A_stamped_write_appends_with_the_leafs_own_admission_mark_not_its_callers` and `BPlusLeafGrainTests.A_backstop_append_carries_an_admission_mark_captured_before_its_span_check`. |
| `ForeignAlloc` | The WAL shard allocates a stamped append at or above the initiator's loaded entry | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsGenerationAdmitted`, under the lock that assigns `WalShardGrain._nextOffset`. | Yes: `WalAppendFenceCoreTests.A_routing_generation_is_admitted_only_from_a_loaded_current_entry` and `WalShardGrainTests.Routing_generations_are_fenced_and_durable`. |
| `ForeignReject` | A stamped append below the entry is rejected, allocating nothing, and retried at its seam | `WalShardGrain.AdmitStampLocked` returns `WalAppendFenceOutcome.StaleGeneration`; the caller sees `WalRoutingGenerationStaleException` and `ShardRootGrain.NextRoutingRetryAsync` re-admits; a leaf's own pre-close write is a self-fence (`LeafAppendEpochFence.OnAdmitted`). | Yes: `WalCommitLogWriterTests.A_stale_generation_point_append_is_reported_refused_without_allocation`, `WalCommitLogWriterTests.A_self_fenced_point_append_is_reported_refused_without_allocation` and `ShardRoutingGenerationIntegrationTests.A_write_admitted_under_a_re_claimed_generation_is_retried_from_the_shard_root_and_succeeds`. |
| `Close` | Stop admitting, bump and re-claim the generation (raising the entry and reading the head in one step), and close the marker with a head at or below it and above every stamped foreign append, max-merged with the heads earlier closes recorded | `ShardRootGrain.CloseShardMarkerAsync` through `ShardRootGrain.RaiseAndReclaimRoutingGenerationAsync` and `ShardRootGrain.ClaimRoutingGenerationEverywhereAsync`, then `HandOffMarkerCore.Close`, which appends the close and folds an evicted one into `ShardHandOffState.CompactedHeads` by a per-partition maximum; a split's `BPlusLeafGrain.CloseSplitHandOffAsync`; the range delete's `TreeRangeDeleteFenceGrain.CloseCoreAsync`. The re-claim waits out a stamped batch still assigning offsets (`WalShardGrain.GenerationBatchDrainLocked`). | Yes: `HandOffMarkerCoreTests.A_close_records_the_fence_heads_over_the_hand_off_range_and_never_reopens`, `ShardRoutingGenerationIntegrationTests.A_slot_migration_opens_its_marker_on_the_target_and_closes_it_by_the_donors_re_claim`, `WalShardGrainTests.A_generation_re_claim_never_splits_a_generation_stamped_batch`, `LeafAppendEpochFenceTests.A_split_fence_with_the_gate_view_closed_after_stamping_leaves_the_marker_open` and `HandOffMarkerCoreTests.A_later_close_with_lower_heads_never_lowers_the_required_heads`. |
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
| `MarkInit` | With no marker-less hand-off in flight, L's marker state is initialised (rule 2) | `ShardRootGrain.TryInitialiseHandOffMarkerAsync`, which refuses while a migration, bulk graft or shadow forward is in flight; a leaf takes its shard's flag only with no split in flight. Since #5831 a shard root whose routing floor is 0 initialises in the same marker write that seeds its generation (`ShardRootGrain.EnsureRoutingGenerationSlowAsync`), rule 2 evaluated in the step that builds the write; with a floor above 0 the recreate fence still runs between the two writes. A marker write may be conditioned on the ETag the activation last read or wrote (`ShardRootGrain.TryUpdateCachedHandOffMarkerAsync`), falling back to a re-read on a conflict; the conditional write is the model's atomic update either way. | Yes: `ShardRoutingGenerationIntegrationTests.A_shard_root_with_a_migration_in_flight_leaves_its_marker_uninitialised`, `BPlusLeafGrainTests.A_leaf_with_a_split_in_flight_does_not_adopt_its_shards_flag`, `HandOffMarkerRowRoundTripTests.An_absent_marker_reads_as_uninitialised_with_nothing_open` and `WalAppendFenceFirstTouchCostIntegrationTests.On_a_purged_id_the_first_raise_fences_between_seeding_and_initialising_the_marker`. |
| `ClaimWrite` | A claim writes D's epoch row + 1 before anything else | `LeafAppendEpochFence.ClaimRowAsync`, its conditional row write. | Yes: `WalShardGrainTests.ClaimSkipsLeafStateCheck` and `WalShardGrainTests.ClaimReadsStateBeforeRowWrite`. |
| `ClaimCheck` | After its row write, the claim reads D's leaf state fresh; an ETag other than the one the activation loaded marks it stale, and a stale activation makes no claim at any partition (a5e2e2ef's A1 and A4) | `LeafAppendEpochFence.ClaimRowAsync`: after the row write, a fresh read of the leaf's state under the leaf's persist gate (A1); on a mismatch the fence marks itself stale and throws before any partition claim (A4). | Yes: `WalShardGrainTests.ClaimSkipsLeafStateCheck`, `WalShardGrainTests.ClaimReadsStateBeforeRowWrite`, `WalShardGrainTests.StaleClaimantAppends` and `WalShardGrainTests.A_claim_racing_its_own_in_flight_persist_stays_live`. |
| `DupAdmit` | An epoch-only write is admitted only by a claimed, non-stale activation, under its epoch | `BPlusLeafGrain.StampedRouting` with `WalRoutingStamp.None`, through `LeafAppendEpochFence.StampForAsync`. | Yes: `WalShardGrainTests.StaleDonorArmsAfterSplit5841` and `WalShardGrainTests.StaleClaimantAppends`. |
| `DupAlloc` | The WAL shard allocates an epoch-only append at or above D's entry | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsEpochAdmitted`. | Yes: `WalShardGrainTests.StaleDonorArmsAfterSplit5841` and `WalShardGrainTests.EpochOnlyWriteUnfenced`. |
| `DupReject` | An epoch-only append below D's entry is refused, allocating nothing | `WalShardGrain.AdmitStampLocked` returns `WalAppendFenceOutcome.StaleEpoch`, and the leaf's fence marks the duplicate stale. | Yes: `WalShardGrainTests.EpochOnlyWriteUnfenced` and `WalShardGrainTests.BumpOnlyIfSplittingActivationClaimed`. |
| `DupRetry` | D''s append refused `StaleEpoch` at its own current epoch re-runs the claim once - a fresh row read, row + 1 conditional on it, then the probe - which marks a stale activation stale (#5840 S4 (a)) | `LeafAppendEpochFence.OnStale` re-running `LeafAppendEpochFence.ClaimRowAsync`, at most once per counted split close. | Yes: `WalShardGrainTests.StaleEpochReclaimSkipsProbe` and `WalShardGrainTests.The_reclaim_allowance_is_one_per_close_and_never_accumulates`. |
| `LiveAdmit` | The live donor admits a write of a key its span still holds, stamped with its installed epoch | `BPlusLeafGrain.StampedRouting` through `LeafAppendEpochFence.StampForAsync`. | Yes: `WalShardGrainTests.RelinquishedAppendRestamped`. |
| `LiveAlloc` | The WAL shard allocates the live donor's stamped append at or above D's entry | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsEpochAdmitted`. | Yes: `WalShardGrainTests.RelinquishedAppendRestamped`. |
| `LiveRefuse` | Refused below D's entry at an epoch at or below the relinquished one, or below its current epoch (R3, the `<=` form), the append self-fences and is re-admitted at its seam, where the key has moved; it is never re-stamped and appended to D. A refusal above the relinquished epoch with no current epoch is foreign and supersedes it | `LeafAppendEpochFence.OnStale`: the self-fence when `stamp.Epoch <= _relinquishedEpoch` or below `_epoch`, `MarkStale` otherwise. | Yes: `WalShardGrainTests.RelinquishedAppendRestamped` and `WalShardGrainTests.A_stale_stamp_above_the_relinquished_epoch_still_supersedes`. |
| `LiveBump` | A floor refusal of the pending write, stamped under the live donor's current epoch above the relinquished one, bumps D's row + 1; the bump is in flight and the write is re-stamped at its epoch. A write stamped at or below the relinquished epoch never bumps: its refusal self-fences | `LeafAppendEpochFence.BumpAsync`. | Yes: `WalShardGrainTests.A_floor_refusal_of_a_write_stamped_before_the_relinquish_self_fences_and_never_bumps`, `WalShardGrainTests.A_floor_refusal_at_the_current_epoch_above_the_relinquished_one_still_bumps` and `WalShardGrainTests.RelinquishLateClaimReinstalls`; the guard's H mutation is `LiveDonorProgressRelinquishedStampRefusalBumps`. |
| `LiveClaimStart` | The live donor's claim writes D's row + 1 and is in flight | `LeafAppendEpochFence.ClaimRowAsync` (or `LeafAppendEpochFence.BumpAsync`) before it installs. | Yes: `WalShardGrainTests.RelinquishLateClaimReinstalls`. |
| `LiveClaimDone` | The claim completes and installs - unless the counted close relinquished it meanwhile, when it is discarded, installs nothing and max-merges its epoch into the relinquished epoch in the same `_sync` section (R2, R5, R6). Its probe runs on completion: a probe that finds the state stale marks the activation stale, discarded or not (R6) | `LeafAppendEpochFence.ClaimRowAsync` discarding a completion started under an older relinquish count; `LeafAppendEpochFence.BumpAsync` installing only if `_epoch == epoch`. | Yes: `WalShardGrainTests.RelinquishLateClaimReinstalls`, `WalShardGrainTests.A_claim_discarded_after_a_relinquish_that_overtook_its_row_read_is_still_recorded` and `WalShardGrainTests.A_discarded_claim_whose_leaf_state_moved_on_still_supersedes`. |
| `LiveArm` | After the relinquish, the live donor arms again with a fresh row read, never its cached row (R1) | `LeafAppendEpochFence.ClaimRowAsync` after `LeafAppendEpochFence.RelinquishLocked` cleared `_row`. | Yes: `WalShardGrainTests.RelinquishKeepsCachedRow` and `WalShardGrainTests.A_claim_row_conflict_never_spends_the_allowance`. |
| `LiveStamp` | After the split's bump, the live donor stamps; refused at its current epoch it takes its one probed re-claim, and a second refusal supersedes it | `LeafAppendEpochFence.OnStale` with the per-close allowance. | Yes: `WalShardGrainTests.A_pre_arm_donor_keeps_stamping_after_the_link_fence` and `WalShardGrainTests.The_reclaim_allowance_is_one_per_close_and_never_accumulates`. |
| `SplitPersist` | The split persists D's narrowed state before it reads D's epoch row, and under pre-arm the live donor's counted close relinquishes its claim locally: its installed epoch and any claim in flight are recorded as the relinquished epoch, its cached row is cleared, and its token is kept | `BPlusLeafGrain.CompleteSplitAsync`, the narrow's persist, which precedes `BPlusLeafGrain.CloseSplitHandOffAsync` in both modes (#5837, rule S: under pre-arm the donor fences and closes its sibling's window itself, and the shard root's link only raises); the relinquish is `LeafAppendEpochFence.CountSplitClose` with `LeafAppendEpochFence.RelinquishLocked`, run just before the donor's own fence, whose adopting install it does not discard. | Yes: `BPlusLeafGrainTests.SplitReadsRowBeforePersist`, `BPlusLeafGrainTests.PreArmDonorFencesAfterPersistBeforeClose` and `WalShardGrainTests.RelinquishKeepsCachedRow`. |
| `SplitRowRead` | After the narrow is durable, the split reads D's epoch row fresh | `LeafAppendEpochFence.FenceSplitAsync`, run by the donor's live activation in `BPlusLeafGrain.CloseSplitHandOffAsync` (eager), and in its `TryCloseWithDeferredBoundAsync` before the one marker write that closes the sibling's window with its deferred bound (pre-arm, #5837 rule S; an unsettled close keeps the donor's link obligation, so the split re-surfaces, L3). | Yes: `BPlusLeafGrainTests.SplitReadsRowBeforePersist`, `WalShardGrainTests.BumpOnlyIfSplittingActivationClaimed`, `BPlusLeafGrainTests.PreArmDonorFencesAfterPersistBeforeClose`, `BPlusLeafGrainTests.PreArmDonorFenceFailureLeavesSiblingOpenAndObligationPending` and `ShardRootGrainSplitLinkTests.A_split_link_never_closes_its_sibling_marker_and_witnesses_its_own_link_after_the_raise`. |
| `SplitRowWrite` | If D's durable row exists, whichever activation claimed, the split writes row + 1 conditionally on the value it read, re-reading on a conflict, raises D's entry at every partition to that value, and, in the eager arm, adopts the new row into the live activation (a5e2e2ef's A3) | `LeafSplitDuplicateFence.FenceAsync`, which `LeafAppendEpochFence.FenceSplitAsync` runs with the live activation's adoption. | Yes: `WalShardGrainTests.BumpOnlyIfSplittingActivationClaimed`, `WalShardGrainTests.SplitRowWriteDropsBump` and `WalShardGrainTests.SplitBumpUsesCachedEtag`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor so TLC does not report a quiescent state as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `HandOffBound` | Once a hand-off's marker closes, every record its initiator appended under the fence that the owner holds lies below the recorded heads, so an owner that replays to them before arming passes no foreign record it has not applied. | Yes: `ShardRoutingGenerationIntegrationTests.A_slot_migration_opens_its_marker_on_the_target_and_closes_it_by_the_donors_re_claim`, `TreeRangeDeleteFenceIntegrationTests.A_range_delete_is_fenced_at_the_tree_and_closes_every_shard_marker_with_its_heads`, `LeafHandOffFenceIntegrationTests.Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_modes_fence` and `BPlusLeafGrainTests.A_close_head_above_the_checkpoint_holds_the_advance_until_a_replay_reaches_it`. |
| `CloseHeadMonotone` | The heads an owner must replay to before it re-arms never decrease: a later close with lower heads, or the eviction of a retained close, leaves the maximum in force. | Yes: `HandOffMarkerCoreTests.A_later_close_with_lower_heads_never_lowers_the_required_heads` and `HandOffMarkerCoreTests.Required_heads_cover_only_overlapping_closes_and_every_evicted_one`. |
| `LiveDonorProgress` | Under pre-arm, the split's link-time fence never supersedes the live donor, nor leaves it self-fencing its own appends: only a stale duplicate's claim raising D's row can (#5840 S4, a5e2e2ef's R1-R4). | Yes: `WalShardGrainTests.A_pre_arm_donor_keeps_stamping_after_the_link_fence`, `WalShardGrainTests.RelinquishKeepsCachedRow` and `WalShardGrainTests.RelinquishLateClaimReinstalls`. |
| `StaleDetectionHonoured` | A claim probe that finds the live donor's state stale marks it stale, even when the relinquish discards the claim: discarding never swallows a real #5841 detection (R6). | Yes: `WalShardGrainTests.RelinquishLateClaimReinstalls`. |
| `AbsentMarkerHolds` | An absent or uninitialised marker never reads as closed: its owner is held until the marker state is initialised, which happens only with no marker-less hand-off in flight. | Yes: `ShardRoutingGenerationIntegrationTests.A_shard_root_with_a_migration_in_flight_leaves_its_marker_uninitialised`, `BPlusLeafGrainTests.An_uninitialised_leaf_marker_holds_until_the_leaf_adopts_its_shards_flag` and `HandOffMarkerRowRoundTripTests.An_absent_marker_reads_as_uninitialised_with_nothing_open`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

`TypeOK` and the four invariants are state safety, and `CloseHeadMonotone` is an action
property; all six are checked on the base and re-checked in the `WideWindows` variant.
The stale duplicate donor of #5841 is checked in two more variants, `StaleDonor`
(today's eager arm, where the splitting activation has always claimed: 574,540 states)
and `StaleDonorPreArm` (#5833's pre-arm, where it may not have, with the live donor's
relinquish, re-claim and floor bump of #5840 S4, and the WAL restart off to fit the case
budget, which `StaleDonor` keeps on: 2,651,431 states), each with the split's one hand-off
window; the base leaves it off and keeps its count. `StaleDonorRearmRace` adds a fourth
epoch with the batch, the tree walk, the legacy hand-off and the restart off (64,825
states), so the live donor's re-arm race - it re-arms between its counted close and the
link fence, and its one probed re-claim restores it - is reachable; three epochs run out
before the re-claim.
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
own copies, as the record each module took its own from.

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

The module adds nine of its own, so that every action and property has a perturbation:
`HandOffBoundAdmitAfterClose` (`ForeignAdmit`), `HandOffBoundLazyAdmissionMark`
(`ForeignAdmit`, `ForeignAlloc`, `ForeignReject`), `HandOffBoundUnstampedFallbackStamped`
(`GUnstamped`), `HandOffBoundOtherIntentClosesMarker` (`OtherIntentClose`),
`HandOffBoundCompleteWithoutClose` (`LegacyComplete`),
`CloseHeadMonotoneCloseOverwritesHeads` (`Close`, with three windows and generations up
to 4),
`AbsentMarkerHoldsInitWithHandOffInFlight` (`MarkInit`),
`AbsentMarkerHoldsInitAtGateOpen` (`OpenGate`) and `TypeOkCloseUnbounded` (`Close`).

#5841 adds eight, each run in `StaleDonor` or `StaleDonorPreArm`:
`HandOffBoundStaleDonorArmsAfterSplit5841` (the defect as filed: no staleness check, and
the bump only when the splitting activation claimed; `ClaimCheck`),
`HandOffBoundClaimSkipsLeafStateCheck` (`ClaimCheck`; red in both modes, the eager
residual), `HandOffBoundBumpOnlyIfSplittingActivationClaimed` (pre-arm only),
`HandOffBoundClaimReadsStateBeforeRowWrite` (`ClaimWrite`, `ClaimCheck`),
`HandOffBoundSplitReadsRowBeforePersist` (`SplitPersist`, `SplitRowRead`),
`HandOffBoundStaleClaimantAppends` (`DupAdmit`; a5e2e2ef's A4),
`HandOffBoundEpochOnlyWriteUnfenced` (`DupAlloc`, `DupReject`) and
`HandOffBoundSplitRowWriteDropsBump` (`SplitRowWrite`).

#5840 S4 (the live donor superseded by the link-time fence) adds fourteen, run in
`StaleDonorPreArm`:
`HandOffBoundStaleEpochReclaimSkipsProbe` (`DupRetry`: the re-claim skips the probe, so
a stale duplicate re-arms),
`HandOffBoundRelinquishedAppendRestamped` (`LiveRefuse`: a write refused at the
relinquished epoch is re-stamped and appended to D),
`LiveDonorProgressRelinquishKeepsCachedRow` (`LiveArm`; R1),
`LiveDonorProgressRelinquishLateClaimReinstalls` (`LiveClaimDone`; R2),
`LiveDonorProgressLiveDonorSupersededByLinkFence` (`LiveStamp`: S4 as raised, no
allowance), `HandOffBoundLiveMovedKeyWriteUnfenced` (`LiveAlloc`),
`HandOffBoundLiveAdmitsMovedKeyAfterNarrow` (`LiveAdmit`),
`HandOffBoundClaimInstalledWithoutRowWrite` (`LiveClaimStart`),
`LiveDonorProgressRelinquishRecordsOnlyInstalledEpoch` (`SplitPersist`, `LiveClaimDone`;
R7: a write re-stamped at an in-flight floor bump's epoch then supersedes the live donor),
`StaleDetectionHonouredDiscardedClaimSwallowsStaleProbe` (`LiveClaimDone`; R6),
`HandOffBoundLiveBumpWithoutRowWrite` (`LiveBump`),
`LiveDonorProgressRelinquishedStampRefusalBumps` (`LiveBump`, in `StaleDonorRearmRace`;
R8: a write stamped before the relinquish bumps on a floor refusal), and two witnesses that
`LiveDonorProgress` is checked on real steps: `LiveDonorProgressStampsAfterFenceWitness`
(the common order: count, link fence, fresh re-arm, stamp) and
`LiveDonorProgressReclaimAfterRearmRaceWitness` (in `StaleDonorRearmRace`: the donor
re-arms between its count and the link fence, and its one probed re-claim restores it).

## Redundancy checks

The design model kept one guard of this module that its model implies, as a
redundancy check expected to stay clean. It is not a mutation, so it is not in the
mutation directory; it was run locally against this module.

| Check | Here | Result |
|-------|------|--------|
| `HandOffBoundSplitRowWriteLosesToClaim` | #5841: the split's row write made unconditional, so a claim between its read and its write is overwritten at the same epoch. | Clean in `StaleDonorPreArm`: the narrow is persisted before the split's read, so that claim's state check sees it and the claimant is stale. The conditional write is kept, since the alternative fix (a split sequence on the row, checked by the claim, at one row write per split) needs it, and #5841's model of that alternative was red without it. |
| `ReadPositionHonestNoMarkerInit` | Kept: `HandOffBound` reads the marker as closed whenever no window is open, ignoring whether its state is initialised. | Clean against `HandOffBound` in the base: rule 1 (a resume opens its marker before its next admission) already bounds every stamped append, so rule 2 is implied here, as in the design. It is kept because a resume path the stamp census misses would otherwise read as closed. |

## Deliberate abstraction gaps

- **#5841: the Dekker pair's storage assumption.** The fix rests on each side's write
  completing before its own read, and on each read being a fresh storage read that
  returns the latest completed write of the other row: strong read-after-write across
  the leaf epoch-row store and the grain-state store (a5e2e2ef's A2). The model makes
  each read and write one step on one variable. A false stale verdict from the
  claimant's own in-flight persist (A1) only deactivates it, and is not modelled. The
  split's bump adopting the ETag it wrote into the live activation's row (A3) is a
  liveness obligation, not checked here: without it the live donor supersedes itself
  on its next row write. Its progress property belongs to #5839.
  Production symbols: `LeafAppendEpochFence.ClaimRowAsync`, `LeafStateCurrencyProbe.IsCurrentAsync`, `LeafAppendEpochFence.FenceSplitAsync`.

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

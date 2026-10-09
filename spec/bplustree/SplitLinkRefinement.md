# Refinement note: leaf split link to production

This note maps [`SplitLink.tla`](SplitLink.tla) - a leaf split completing on the donor,
the shard root recording and applying the child link, and a root crash between them -
to the Orleans.Lattice code that plays each role. Like the other refinement notes it
is a documented mapping, not a refinement proof; the gates check its names, detectors
and coverage, and none checks its claims.

The model exists because of issue #4795. Before the fix, `BPlusLeafGrain.CompleteSplitAsync`
cleared `SplitInFlight` and handed the split result back in memory; only the shard
root persisted the link intent, afterwards. A root-only crash in that window left a
populated, chained sibling no parent routed to. The model specifies the fixed
ordering, with a durable marker on the donor.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `rowsD`, `rowsS` | Rows held by the donor and by the new sibling | The donor leaf's and the new sibling leaf's persisted entries. |
| `acked` | Acknowledged writes | Writes whose `BPlusLeafGrain.SetAsync` completed. |
| `phase` | The donor's split progress | `LeafNodeState.SplitInFlight` and the split fields: `"idle"`, `"intent"` (split recorded), `"moving"` (sibling chained, rows transferring) and `"done"` (`SplitInFlight` cleared). |
| `born`, `chained` | The sibling exists, and the donor's next-sibling link names it | The sibling leaf's first persist and the donor's `NextSibling` update inside `BPlusLeafGrain.CompleteSplitAsync`. |
| `held` | The shard root holds the split result in memory | The `SplitResult` returned to `ShardRootGrain.LinkSplitLockedAsync`; lost when the root activation is lost. |
| `pending` | A durable link intent at the root | The `PendingChildLink` the root persists in `ShardRootGrain.RecordPendingChildLinksAsync`. |
| `linked` | The parent routes to the sibling | The parent's child list after the link is applied. |
| `marker` | The donor's durable unacknowledged-split marker | `LeafNodeState.UnlinkedSplitSiblingId` and `UnlinkedSplitKey`. |
| `crashes` | Environment budget | A modelling device only. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write(k)` | A write is acknowledged and lands on the donor, or on the sibling once linked | `BPlusLeafGrain.SetAsync` routed by the parent. | Yes: `BPlusLeafGrainTests.Recovery_applies_write_locally_when_key_below_split_key` and `BPlusLeafGrainTests.Recovery_forwards_write_to_sibling_when_key_at_or_above_split_key`. |
| `SplitIntent` | The donor records the split | `BPlusLeafGrain.SplitAsync` sets `SplitInFlight`. | Yes: `BPlusLeafGrainTests.SetAsync_returns_split_result_when_state_has_in_progress_split`. |
| `Birth` | The sibling is persisted | `BPlusLeafGrain.CompleteSplitAsync` creates the sibling. | Yes: `BPlusLeafGrainTests.Recovery_reuses_persisted_sibling_id` and `BPlusLeafGrainTests.Split_stamps_sibling_key_range_with_split_key_and_inherited_high`. |
| `Chain` | The donor's next-sibling link names the sibling | `BPlusLeafGrain.CompleteSplitAsync` narrows the donor. | Yes: `BPlusLeafGrainTests.Split_recovery_sets_new_sibling_PrevSibling_to_this_leaf` and `BPlusLeafGrainTests.Split_recovery_updates_old_next_PrevSibling_to_new_sibling`. |
| `MoveRow(k)` | Rows at or above the split key move to the sibling | `BPlusLeafGrain.CompleteSplitAsync` row transfer. | Yes: `BPlusLeafGrainTests.Recovery_trims_entries_from_original_leaf` and `BPlusLeafGrainTests.Recovery_preserves_tombstones_in_right_half`. |
| `FinishSplit` | The donor clears `SplitInFlight`, sets the marker and returns the result | `BPlusLeafGrain.CompleteSplitAsync` persists `UnlinkedSplitSiblingId` before returning, and names itself in `SplitResult.Donor`. | Yes: `BPlusLeafGrainTests.Completed_split_result_names_its_donor_for_the_acknowledgement` and `BPlusLeafGrainTests.Set_after_a_lost_split_result_resurfaces_the_unlinked_sibling`. |
| `RecordPending` | The root persists the link intent | `ShardRootGrain.RecordPendingChildLinksAsync`. | Yes: `ShardRootGrainSplitLinkTests.The_link_intent_is_persisted_before_the_parent_is_asked_to_accept`. |
| `Acknowledge` | The root retires the donor's marker once the intent is durable | `ShardRootGrain.AcknowledgeRecordedSplitsAsync` calls `IBPlusLeafGrain.AcknowledgeSplitLinkRecordedAsync`, best effort, after the intent is recorded. | Yes: `ShardRootGrainSplitLinkTests.A_recorded_leaf_split_acknowledges_the_link_to_its_donor`, `ShardRootGrainSplitLinkTests.A_failed_acknowledgement_does_not_fail_the_write` and `BPlusLeafGrainTests.Acknowledging_the_recorded_link_retires_the_marker_so_later_writes_do_not_resurface`. |
| `Resurface` | A donor recovery entry point hands a still-marked result back to the root | `BPlusLeafGrain.NeedsSplitRecovery` and `UnlinkedSplitResult`, used by the recovery guards. | Yes: `BPlusLeafGrainTests.Set_after_a_lost_split_result_resurfaces_the_unlinked_sibling` and `BPlusLeafGrainTests.Acknowledging_a_different_sibling_keeps_the_marker`. |
| `Link` | The root applies the link to the parent | `ShardRootGrain.LinkSplitLockedAsync`; re-applying a result is idempotent (`AcceptSplitCoreAsync`). | Yes: `ShardRootGrainSplitLinkTests.A_leaf_split_is_linked_under_the_parent_a_fresh_descent_finds`. |
| `Crash` | The root loses its in-memory result | Loss of the `ShardRootGrain` activation. | Yes: `ShardRootGrainSplitLinkTests.A_link_that_faults_stays_recorded_and_the_next_operation_redelivers_it` and `BPlusLeafGrainTests.Set_after_a_lost_split_result_resurfaces_the_unlinked_sibling`. |
| `Stutter` | Terminal self-loop so a finished run is not reported as a deadlock | Not applicable: a modelling device with no production step. | Not applicable |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `NoKeyLostOrDuplicated` | A split neither loses nor duplicates an acknowledged key. | Yes: `BPlusLeafGrainTests.Recovery_trims_entries_from_original_leaf` and `BPlusLeafGrainTests.Recovery_preserves_tombstones_in_right_half`. |
| `LinkObligationDurable` | A completed, unlinked split is always recorded durably, by the root's link intent or the donor's marker. | Yes: `BPlusLeafGrainTests.Set_after_a_lost_split_result_resurfaces_the_unlinked_sibling`. |
| `IntentOnlyForCompleteSplit` | The root records a link intent only for a completed split. | Yes: `BPlusLeafGrainTests.Split_flushes_source_state_before_returning_SplitResult`. |
| `LinkedClearsIntent` | A linked sibling carries no pending intent. | Yes: `ShardRootGrainSplitLinkTests.A_recorded_leaf_split_acknowledges_the_link_to_its_donor`. |
| `ChainedImpliesBorn` | The donor never chains to a sibling that does not exist. | Yes: `BPlusLeafGrainTests.Split_recovery_sets_new_sibling_PrevSibling_to_this_leaf`. |
| `BornImpliesIntent` | A sibling is born only for a recorded split. | Yes: `BPlusLeafGrainTests.Recovery_reuses_persisted_sibling_id`. |
| `SplitEventuallyLinked` | A completed split is eventually linked into the parent, whatever the root loses. | Yes: `BPlusLeafGrainTests.Set_after_a_lost_split_result_resurfaces_the_unlinked_sibling` and `ShardRootGrainSplitLinkTests.A_recorded_leaf_split_acknowledges_the_link_to_its_donor`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `SplitOnlyOfNonEmptyLeaf` | A guard that keeps the abstract split from being vacuous; production splits are triggered by a size threshold, which the model does not represent. |
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Defect mutations whose fixes have landed

| Mutation | Behaviour it reproduced | Issue | Detectors |
|----------|-------------------------|-------|-----------|
| `LinkObligationDurableNoMarker` | The donor completes a split with no durable record, so a root crash strands the sibling. | #4795 | `BPlusLeafGrainTests.Set_after_a_lost_split_result_resurfaces_the_unlinked_sibling`, proven red with the marker disabled. |

## Deliberate abstraction gaps

- **One split, one sibling, one key boundary.** Multiple concurrent splits, cascading
  splits and the parent's own splits are not modelled.
- **Root-level splits.** A split of the root leaf is covered by `PendingPromotion`, and
  its marker re-surfaces later; the model does not distinguish it.
- **Idempotent re-link.** The model lets `Link` run once; production tolerates a
  re-surfaced result being applied again.

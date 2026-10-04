# Refinement note: ShardOwnershipRetention to production

This note maps the TLA+ specification in
[`ShardOwnershipRetention.tla`](ShardOwnershipRetention.tla) to Orleans.Lattice
as it exists in code. The module is the companion of `ShardOwnership`
([`Refinement.md`](Refinement.md)); the decomposition and what is lost by not
composing the two are described in
[`README.md`](README.md#two-modules-and-the-seam-between-them).

It is a **documented mapping, not a machine-checked refinement proof**. The
Detector column names the production test that goes red if the behaviour a row
abstracts regresses, and every named detector was shown red against a
production perturbation of the seam it covers; the log is in the pull request
that added the module.

## What this module is for

`ShardOwnership` checks who serves a key under a registry that always reports
the saga's decision. This module checks what the transaction registry's
retention, and the per-activation memory of a leaf, do to a saga bound across
an adaptive split and an online resize with its undo:

- the registry declining to report a decision it holds (`RegistryMask`,
  production's `TxStatus.Indeterminate` for an aged-out row or an unreachable
  cross-tree coordinator), with no ordering against anything before the row's
  retirement;
- the row's retirement (`RegistryForget`), after which the registry reads the
  saga as InFlight;
- a delayed shadow-forwarded prepare reaching the split destination at any time
  (`DeliverLate`);
- a leaf reactivation that loses the activation's terminal memory and its
  shadow markers (`Reactivate`).

It keeps the ownership machinery those act through, unchanged from
`ShardOwnership`: the split with its sweep, freeze and commit; the resize with
its snapshot, fence, flip, retirement and purge; the undo after a flip; the
saga's binding, prepare, decision, abort, broadcast and completion; and a later
write. It drops what retention does not act through: the reshard, the refused
flip, the undo before a flip, the saga's re-binds, and stale writers. Readers
may still hold any pair the registry ever published.

## The base module models the intended design where production has an open defect

| Intended design in the base | Production today | Issue | Mutation reproducing production |
|---|---|---|---|
| A terminal a purged old copy refuses is delivered to the copy it mirrored into, following that copy's own layout; one the copy an undo discarded refuses counts as delivered (`SagaTerminal`, `TermTargets`) | The broadcast fails on a purged copy, and on a copy the undo discarded (the refusal is `InvalidOperationException`, which it does not follow), so the saga never completes | #4475, #4474 | in `ShardOwnership`: `SagaCompletesPurgedCopyRefusesTerminal`, `SagaCompletesDiscardedCopyRefusesTerminal` (`AtomicOnOwnerDiscardedCopyTerminalRedirects` stands against a fix that follows the refusal) |
| The terminal's committed-values backstop installs a key it finds no bucket for last-writer-wins at the saga's own stamp (`TermRow`) | The backstop is stamped above whatever the row holds, so it overwrites a later write of a moved key | #4522 | `NoKeyLostRetainedFreshStampBackstop` |
| A shadow marker gates the split destination's key only while the leaf holding it has not applied the saga's terminal and the row is older than the saga's prepare stamp; the sweep's replay installs none, and a leaf split transfers none, for a saga whose terminal the leaf has applied (`LeafGated`, `DeliverLate`, `LeafSplit`) | A marker installed after the terminal is copied by a leaf split to a sibling that never sees the terminal, which gates the key for as long as the registry reports the saga Committed | #4545 | `ReadableOnceCompleteDeadMarkerTransferred` (`ReadableOnceCompleteMarkerWithoutSelfCheck` stands against a fix without the self-check) |

The extent of the split/resize interlock (#4452) left this table when its fix
landed (#4466): production holds a split until no shard of the replaced copy
mirrors into the resized one, which is the base's `SplitBegin`.
`NoKeyLostSplitInSoftDeleteWindow` stays as the standing check on the rule the
fix first proposed, which stopped at the end of the resize.

The sweep's Indeterminate answer (#4473) left this table when its fix landed
(#4561): the pre-check and the post-sweep cleanup follow an Indeterminate
answer with the recorded decision, which is the base's `SplitSweep`.
`OwnerMonotonicSweepIndeterminateLeavesMarker` stays as the standing check.

The purged copy's routed reads (#4503) left this table when their fix landed
(#4528): the purge leaves a tombstone that refuses a router whose logical tree
resolves elsewhere, which is the base's `Gone`.
`NoResurrectionRetainedPurgedCopyServesEmpty` stays as the standing check.

The snapshot's prepared buckets (#4455) left this table when their fix landed
(#4506): the online snapshot sweeps each source shard's prepared buckets onto
the resized copy, which is the base's `SnapCopy`.
`OwnerMonotonicRetainedSnapshotDropsBuckets` stays as the standing check.

The late-prepare refusal (#4445) left this table when its fix landed (#4461):
production refuses a forwarded prepare on the registry's recorded decision,
resolving an Indeterminate answer to the verdict behind it, which is the base's
`DeliverLate`. `NoResurrectionLatePrepareActivationMemory` stays as the standing
check on the activation-memory refusal it replaced.

`TermTargets` follows the copy the terminal goes to rather than the bound copy.
This module found why that matters: after a purge the resized copy may split,
and a terminal sent to the purged copy's layout never reaches the split
destination, stranding the bucket the sweep replayed there (`NoStrandedBucket`).
That is the intended design #4475's fix has to meet.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `alias` | The physical copy the logical tree resolves to | `TreeRegistryEntry.PhysicalTreeId`, written with its map by `ILatticeRegistry.SwapAliasAsync` (#4357). |
| `rmap` | The logical row's routing map | `TreeRegistryEntry.ShardMap`, abstracted to the shard `k2`'s virtual slot routes to; moved by `ILatticeRegistry.ReassignSlotsAsync`. |
| `published` | Every (copy, map) pair a reader may hold | The `RoutingInfo` pairs `LatticeGrain` caches, published under `RoutingPairPublishGate.ShouldPublish`. Here only reads use stale pairs; writes go through the current pair. |
| `rmapR` | The resized copy's own map | The map `TreeSnapshotGrain.InitiateSnapshotStateAsync` registers the destination with. |
| `rmapOld` | The map an undo restores | `TreeResizeState.OldRegistryEntry`. |
| `row[c][s][k]` | A shard's committed projection value for a key | The leaf projection rows; values are write stamps, so last-writer-wins is `Max`. |
| `pend[c][s][k]` | The saga's prepared bucket, or a shadow marker | The leaf's per-transaction pending bucket (`BPlusLeafGrain.PendingTx`), or, as `"mark"`, a destination shadow marker with no bucket under it (`BPlusLeafGrain.MarkSagaShadowAsync`), which `ShadowedMigrationReadGuard.ResolveSaga` gates on. The base never installs a marker without a bucket. |
| `term[c][s]` | The activation's memory of the saga's terminal | `BPlusLeafGrain.IsRecentlyTerminal`, the per-activation `_recentlyTerminal` set: the orphan guard's input, and the late-prepare refusal's. |
| `sp`, `spCopy` | The adaptive split's phase and bound copy | `TreeShardSplitState.Phase` and `TreeShardSplitState.PhysicalTreeId`. |
| `rz`, `rzShards` | The resize's phase and the old copy's shard set | `TreeResizeState.Phase` and `TreeResizeState.ShardIndices` (from `RoutedShardIndices.Resolve`). |
| `fence` | The old copy's fenced shards | Shards in `ShadowForwardPhase.Rejecting` (`ShardRootGrain.EnterRejectingAsync`). |
| `redir` | The resized copy armed by an undo | `ShardRootState.RetainedRedirect` (`AliasCutoverShardMaps.ArmRedirectsAsync`). |
| `sg`, `bound`, `prepped`, `told`, `dec` | The saga's phase, binding, dispatched keys, visited shards and recorded decision | `AtomicWriteState.Phase`, `AtomicWriteState.BoundPhysicalTreeId`, `AtomicWriteState.NextIndex`, `AtomicWriteState.TouchedShards`, and `TxRegistryState.Decisions`. |
| `masked` | The registry declines to report the decision it holds | `TxRegistryGrain.GetStatusAsync` answering `TxStatus.Indeterminate`: an expired tombstone not yet pruned, or an unreachable cross-tree coordinator (`TxRegistryGrain.ResolveDelegatedAsync`). |
| `forgotten` | The saga's row has left the registry | `TxRegistryGrain.ForgetAsync` and the prune behind it; an absent row reads `TxStatus.InFlight`. |
| `late` | A delayed shadow-forwarded prepare en route to the split destination | A split forward still in flight after `ShardRootGrain.ForwardWithDeadlineAsync` gave up waiting on it, or a retried one. |
| `wDone` | A later plain write has happened | Modelling device: one write of `k2` after the decision. |
| `reacted` | Reactivation budget | Modelling device: one leaf reactivation. |
| `lk` | A shadow marker on the split destination's leaf holding `k2` | `BPlusLeafGrain`'s `_shadowedSagas` entry for the saga, installed by `BPlusLeafGrain.MarkSagaShadowAsync` and moved by a leaf split; activation memory. |
| `lt` | That leaf has applied the saga's terminal | `BPlusLeafGrain`'s `_recentlyTerminal` on the leaf holding `k2`: lost on a reactivation, and empty on the fresh sibling a leaf split creates. |
| `ackOn[c][k]` | What has been acknowledged that copy `c` must hold | Ghost variable with no production counterpart, as in `ShardOwnership`. |
| `vis[k]` | The highest value a fresh reader has been served | Ghost variable with no production counterpart: the history `OwnerMonotonic` is stated over, so a hidden read cannot launder a reversion. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `SplitBegin` | An adaptive split opens its shadow-write window | `TreeShardSplitGrain.SplitAsync` and `ShardRootGrain.BeginSplitAsync`. It refuses while `TreeResizeGrain.HoldsShardMigrationsAsync` reports a hold, which lasts until no shard of the replaced copy mirrors into the resized one (#4452). | Yes: `ResizeMigrationHoldDetectorIntegrationTests.A_split_is_refused_by_the_resize_hold_while_the_replaced_copy_still_mirrors` (real grains end to end, red when the hold stops holding), `TreeResizeGrainTests.HoldsShardMigrations_is_true_while_any_replaced_shard_still_mirrors_into_the_resized_copy`, and the caller test `TreeShardSplitGrainTests.SplitAsync_refuses_while_a_completed_resize_still_has_the_replaced_copy_mirroring`, which stubs the hold. The fence's two arms: `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map` (the alias already moved) and `TreeShardSplitGrainTests.Swap_while_a_cutover_has_carried_the_copy_map_but_not_swapped_the_alias_does_not_apply_the_diff` (a cutover carried the map but has not swapped). |
| `SplitSweep` | The retroactive sweep of prepares that predate the window | `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync` through `PreparedBucketSweep.RunAsync`, whose pre-check and post-sweep cleanup follow an Indeterminate answer with `ITxRegistryGrain.GetRecordedStatusAsync` (#4473). | Yes: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight`, `TreeShardSplitGrainTests.RetroactiveSweep_skips_replay_and_applies_commit_terminal_when_saga_already_committed`, `TreeShardSplitGrainTests.RetroactiveSweep_applies_the_recorded_commit_when_the_pre_check_is_masked` (the pre-check), `TreeShardSplitGrainTests.RetroactiveSweep_cleanup_applies_the_recorded_commit_of_a_saga_masked_during_the_sweep` (the cleanup) and `PreparedBucketSweepIndeterminateIntegrationTests.A_sweep_settles_a_prepare_whose_committed_decision_the_registry_masks`. |
| `SplitFreeze` | The source refuses the moved slot | `TreeShardSplitGrain.SwapAsync`: `MarkLeavesMovedAwayAsync`, then `EnterRejectPhaseAsync`. | Yes: `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map` and `TreeShardSplitGrainTests.Swap_calls_source_enter_reject_phase_exactly_once`. |
| `SplitCommit` | Final drain, then the map moves | `TreeShardSplitGrain.SwapAsync`'s final drain (`ForwardMovedSlotEntriesAtomicallyAsync`) and `ILatticeRegistry.ReassignSlotsAsync`. | Yes: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip`. |
| `ResizeBegin` | A resize captures its shard set | `TreeResizeGrain.InitiateResizeStateAsync` over `RoutedShardIndices.Resolve`. | Yes: `RoutedShardIndicesTests.Resolve_adds_a_shard_a_split_allocated_above_the_pinned_count`. |
| `SnapCopy` | The online snapshot copies the old copy | `TreeSnapshotGrain`'s online drain over `RoutedShardIndices.OrContiguous`, and `TreeSnapshotGrain.SweepPreparedBucketsAsync`, which carries prepared buckets through `PreparedBucketSweep.RunAsync` (#4455). | Yes: `TreeSnapshotGrainTests.BeginShadowForward_covers_a_shard_a_split_allocated_above_the_pinned_count`, `TreeSnapshotGrainTests.Online_shadow_begin_carries_an_in_flight_prepared_bucket_onto_the_destination_shard` and `TreeSnapshotGrainTests.Online_shadow_begin_applies_the_terminal_of_a_saga_decided_before_the_sweep`. |
| `ResizeFence(s)` | One old shard enters Rejecting before the flip | `TreeResizeGrain.SwapAliasAsync` calling `ShardRootGrain.EnterRejectingAsync` (#4362). | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`. |
| `ResizeFlip` | The alias and map move in one write | `ILatticeRegistry.SwapAliasAsync` from `TreeResizeGrain.SwapAliasAsync`. | Yes: `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row`. |
| `ResizeRetire` | Reject, then soft-delete the old copy | `TreeResizeGrain.RejectOldShardsAsync` and `TreeResizeGrain.CleanupOldTreeAsync`. | Yes: `TreeResizeGrainTests.Cleanup_soft_deletes_a_later_resizes_old_physical_tree`. |
| `ResizePurge` | The purge clears the old copy | `ShardRootGrain.PurgeAsync`. Pairs naming the old copy stay published, as in `ShardOwnership`; the purge leaves a tombstone that refuses a routed read whose logical tree resolves elsewhere (#4503). | Yes: `ShardRootGrainPurgeTests.PurgeAsync_clears_the_single_root_leaf_when_tree_is_flat`, `ShardRootGrainPurgeTests.PurgeAsync_leaves_only_a_purge_tombstone`, `ShardRootGrainPurgeTests.A_routed_call_whose_tree_resolves_elsewhere_is_refused_as_stale`, `PurgedCopyStaleRoutingIntegrationTests.A_stale_router_is_refused_by_a_purged_first_resize_copy` and `PurgedCopyStaleRoutingIntegrationTests.A_stale_router_is_refused_by_a_purged_later_resize_copy_which_is_not_resurrected`. |
| `UndoArm` | The resized copy is armed to redirect, before the swap | `AliasCutoverShardMaps.ArmRedirectsAsync` from `TreeResizeGrain.UndoResizeCoreAsync` (#4453). | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`. |
| `UndoSwap` | The alias and the old map move back | `ILatticeRegistry.SwapAliasAsync` from `TreeResizeGrain.UndoResizeCoreAsync`. | Yes: `TreeResizeGrainTests.UndoResize_recovers_old_tree_and_removes_alias`. |
| `UndoClear` | The old copy's fence lifts, after the swap | `ShardRootGrain.ClearShadowForwardAsync` on every old shard (#4453). | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`. |
| `SagaStart` | The saga binds to the copy the tree resolves to | `AtomicWriteGrain` binding `AtomicWriteState.BoundPhysicalTreeId`. | Yes: `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on`. |
| `SagaPrepare(k, p)` | One key's prepared write through the current pair | `LatticeGrain.SetManyAsyncCore` through `SagaCopyBinding.AdmitsDispatch`, admitted by the bound copy through a fence via `ResizeFence.AdmitsBoundSaga`, and mirrored by `ShardRootGrain.ForwardShadowAsync`. **Under-approximation, deliberate:** `Next` dispatches through the current pair only; stale-pair dispatch is `ShardOwnership`'s. | Yes: `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_to_the_fenced_copy_is_applied_and_forwarded` and `ShardRootGrainShadowForwardTests.SetManyAsync_forwards_full_batch_in_single_call_to_destination`. |
| `SagaDecide` | The commit decision is recorded | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` answering `Commit` or `StayBound`. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaAbort` | The abort decision is recorded; the broadcast compensates | `AtomicWriteGrain.BroadcastTerminalsAsync` with an abort. **Environment action:** unguarded through the execute phase; fair only once the bound copy can no longer commit, which stands for production's prepare retries ending in an abort where this module has no re-bind. | Yes: `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. |
| `SagaTerminal(s)` | One shard of the terminal broadcast | `AtomicWriteGrain.MarkOneShardAsync` into `ShardRootGrain.AppendTxTerminalAsync` and `MigrationTerminalCore.DecideBucketAction`. Delivery past a purge or an undo (#4475, #4474), and a backstop stamped at the saga's stamp (`TermRow`, #4522), are the intended design. | Partial: `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_addressed_to_the_fenced_copy_directly_is_applied_and_forwarded` and `BPlusLeafGrainTests.ApplyTxTerminalAsync_with_already_terminalled_txid_discards_orphan_pending_bucket`. Production fails the broadcast on a purged copy (#4475) and on a copy the undo discarded (#4474), and stamps the backstop above the row (#4522). |
| `SagaComplete` | The broadcast finished; the caller is acknowledged | `AtomicWriteGrain.CompleteSagaAsync`. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `RegistryMask` | The registry stops, or resumes, reporting the decision | `TxRegistryGrain.GetStatusAsync` answering `TxStatus.Indeterminate` for an expired tombstone or an unreachable cross-tree coordinator. **Environment action, over-approximating:** it may toggle at any point after the decision and before the row is retired, whatever the participants have seen; production's retention mask follows the fan-out and its snapshot pin is what clears it, and a dial failure has no ordering at all, so every Indeterminate answer production gives is one the model can give. Before the decision the registry reads InFlight whatever the mask, so the guard loses nothing. | Yes: `TxRegistryGrainTests.GetStatusAsync_reports_an_aged_out_decision_as_indeterminate_not_in_flight`. |
| `RegistryForget` | The saga's row leaves the registry | `TxRegistryGrain.ForgetAsync` from the completed saga (`AtomicWriteGrain`'s retention keepalive), and the prune behind it. **Environment action, not fair:** retirement may never happen. | Yes: `AtomicWriteGrainTests.ReceiveReminder_keepalive_on_a_completed_saga_arms_retention_and_forgets_the_decision`. |
| `DeliverLate` | A delayed shadow-forwarded prepare reaches the split destination | The split hot-path forward through `ShardRootGrain.ForwardShadowAsync`, refused at the leaf by `BPlusLeafGrain.IsLatePrepareForTerminalTransactionAsync` when the activation remembers the terminal or, for a prepare marked forwarded (`LatticeForwardedPrepareContext`), when the registry reports the saga decided, following an Indeterminate answer to the recorded verdict (#4445). It also installs a shadow marker on the leaf (`BPlusLeafGrain.MarkSagaShadowAsync`); installing none once the leaf has applied the saga's terminal is the intended design (#4545). **Environment action:** delivery is unfair and may happen at any time, which is what a forward outliving its deadline can do; it also stands for the split sweep's non-atomic replay. | Yes: `BPlusLeafGrainTests.Delayed_forwarded_prepare_that_outruns_the_terminal_is_refused_once_the_saga_has_decided`, `BPlusLeafGrainTests.Forwarded_prepare_after_a_reactivation_is_refused_when_the_registry_reports_the_saga_committed`, `BPlusLeafGrainTests.Forwarded_prepare_is_refused_on_the_recorded_verdict_behind_a_masked_decision` and `ShardRootGrainSplitShadowForwardTests.Hot_path_shadow_forward_trailing_the_terminal_installs_no_orphan_on_a_destination_leaf_that_remembers_it`. |
| `LeafSplit` | A leaf split moves the split destination's key to a fresh sibling leaf | `BPlusLeafGrain.TransferShadowMarkersToSiblingAsync` from the leaf split (`BPlusLeafGrain.CollectShadowMarkers` gathers the donor's markers and prepared buckets for the moved keys, then `MarkSagaShadowAsync` installs them on the sibling). Transferring none for a saga whose terminal the donor applied is the intended design (#4545). The leaf dimension is modelled only for the split destination's `k2`; elsewhere a shard is one leaf. **Environment action:** a leaf splits whenever it fills, any number of times; it is offered only while it can move a marker or precede the late forward, the only cases the module can observe. | Partial: `BPlusLeafGrainTests.Split_transfers_destination_side_shadow_markers_for_migrated_keys` and `BPlusLeafGrainTests.Split_unions_marker_and_pending_sources_without_duplicating_a_key`. The transfer copies a marker whose terminal the donor applied (#4545). |
| `LaterWrite(p)` | A client write of `k2` through the current pair | A routed write through `LatticeGrain`, mirrored by `ShardRootGrain.ForwardShadowAsync` while the shard forwards. | Yes: `ShardRootGrainShadowForwardTests.SetAsync_forwards_during_draining`. |
| `Reactivate(c, s)` | A leaf activation is replaced | A new `BPlusLeafGrain` activation: `_recentlyTerminal` and `_shadowedSagas` start empty, so the leaf loses its marker and its terminal memory together; prepared buckets are rebuilt by replay. **Environment action:** unfair and at most once. `Next` offers it on the split destination only, because nothing reaches any other shard after its terminal, so a reactivation there would only spend the budget. | Yes: `BPlusLeafGrainTests.Materialiser_replays_prepared_set_into_pending_tx` pins that replay rebuilds the buckets the model keeps across a reactivation. |
| `Stutter` | Quiescence | Not a protocol step: a stuttering successor once nothing is in flight. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `NoKeyLost` | The owner holds every acknowledged value, or the read gate declines to answer: a retired row never makes an acknowledged commit unreadable. | Partial: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip` and `TreeResizeGrainTests.HoldsShardMigrations_is_true_while_any_replaced_shard_still_mirrors_into_the_resized_copy`. A backstop stamped above the row overwrites a later write of a moved key (#4522). |
| `NoResurrection` | No served read returns a value older than one already acknowledged, including a late forwarded orphan outranking a newer row (#4445). | Yes: `BPlusLeafGrainTests.Delayed_forwarded_prepare_that_outruns_the_terminal_is_refused_once_the_saga_has_decided` and `ShardRootGrainSplitShadowForwardTests.Hot_path_shadow_forward_trailing_the_terminal_installs_no_orphan_on_a_destination_leaf_that_remembers_it` and `PurgedCopyStaleRoutingIntegrationTests.A_stale_router_is_refused_by_a_purged_first_resize_copy`. |
| `AtomicOnOwner` | A fresh reader that gets an answer for both keys sees the batch on both or on neither. | Yes: `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on`. |
| `OwnerMonotonic` | A fresh reader's value never moves backwards across a mask, a retirement, a late forward or a reactivation; a hidden read in between does not launder a reversion. | Yes: `TxRegistryGrainTests.GetStatusAsync_reports_an_aged_out_decision_as_indeterminate_not_in_flight` and `BPlusLeafGrainTests.Materialiser_replays_prepared_set_into_pending_tx` and `TreeShardSplitGrainTests.RetroactiveSweep_applies_the_recorded_commit_when_the_pre_check_is_masked`. |
| `SplitCompletes` | A split that opened its window finishes. | Yes: `TreeShardSplitGrainTests.ProcessNextPhase_drives_the_shadow_write_phase_through_the_full_split_pass`. |
| `ResizeCompletes` | A resize that started is purged or undone. | Yes: `TreeResizeGrainTests.Cleanup_soft_deletes_a_later_resizes_old_physical_tree`. |
| `SagaCompletes` | A saga that started completes. | Partial: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. A saga bound to a purged old copy never completes (#4475), nor does one bound to a resized copy an undo discards before its broadcast finishes (#4474). |
| `ReadableOnceComplete` | Once the saga has completed, and while its registry row is neither retired nor masked, no read at the owner is gated: every shadow marker was cleared by the saga's terminal or verifies against a row that already holds the saga's value. Stated as an invariant because production recovers once the decision ages out, which an eventual-readability property cannot tell from a fix. | Partial: `BPlusLeafGrainTests.Split_transfers_destination_side_shadow_markers_for_migrated_keys` and `BPlusLeafGrainTests.GetAsync_non_migrated_entry_under_shadow_marker_serves_entries_unchanged`. A marker installed after its terminal and copied by a leaf split gates the key after the saga completed (#4545). |
| `NoStrandedBucket` | A decided saga's prepared bucket on a copy that can still become the tree is eventually consumed by its terminal, unless the registry retired the row first. | Partial: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard` and `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. A terminal that cannot follow the resized copy's split strands its bucket until #4475's fix lands. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Over-approximation arguments

Each environment action is argued in its row. Two deliberate
under-approximations are stated so that they are not mistaken for coverage:

- **Writers use the current pair.** `SagaPrepare` and `LaterWrite` dispatch
  through the pair the registry names now, not through any pair it ever
  published. Stale writers are `ShardOwnership`'s territory, where the same
  actions range over `published`. Composing them with this module's retention
  events was checked once and adds no reachable state (82,155 distinct states,
  identical to this module alone).
- **No re-bind, reshard, refused flip or undo before a flip.** These are
  `ShardOwnership`'s. The fair abort (see `SagaAbort`) is what keeps a saga
  that would have re-bound from stalling here.

`NoStrandedBucket` is stated as one leads-to rather than one per bucket. With
one saga, no bucket appears after the decision until the row is retired,
because a forwarded prepare is refused while the registry reports the
decision. So "every bucket is eventually consumed" and "eventually none is
left" coincide in this instance; the single form is what keeps the liveness
check inside the harness's per-run budget.

## Deliberate abstraction gaps

- **Composition with `ShardOwnership`.** Not a CI gate. A behaviour that needs
  a stale writer, a re-bind, a reshard, a refused flip or an undo before a flip
  together with a retention event is checked by neither module's gate. The
  composition of everything `ShardOwnership` has with this module was checked
  once and is clean against all thirteen properties of both modules (497,105
  distinct states, depth 29, 9 min 54 s on two workers); it exceeds the per-run
  budget, which is why the modules are separate. See the README.
- **The post-sweep cleanup and the sweep's non-atomic window.** The sweep is one
  step. Its window (a decision landing between the pre-check and the replay) is
  covered by the late forward, which may arrive at any time; the cleanup that
  closes it is not modelled separately.
- **Cross-tree delegation.** Only its observable effect, an Indeterminate
  answer at any time before retirement, is modelled.
- **Reactivation elsewhere than the split destination.** Argued in the
  `Reactivate` row: no step reads the lost memory anywhere else here.
- **Leaves.** A shard is one leaf, except the split destination's `k2`, whose
  leaf can split any number of times (`LeafSplit`), so a shadow marker can
  move to a sibling that never saw the terminal (#4545). Leaf splits elsewhere,
  and a marker on a leaf other than the one that took the prepare, are not
  modelled; the self-verifying gate covers them because it reads only the
  leaf's own row and marker.
- **More than one of anything.** One split, one resize with its undo, one saga,
  one later write, one late forward, one reactivation, one mask toggle at a
  time.
- **Stamps that disagree with real time, and migrated rows.** This module's
  values are write stamps in commit order, so property H (a write acknowledged
  after a prepare on the same leaf is stamped above it) is built in, and it
  does not track migrated rows. Both are `ShardOwnership`'s: its versions
  separate a write's real-time rank from its stamp, and it reproduces the
  stamp and import defects (#4522, #4564) as standing mutations. What this
  module adds to them, a reactivation that loses the activation's memory
  before a fresh-stamp backstop, is `NoKeyLostRetainedFreshStampBackstop`.
- **Time.** Retention windows, deadlines and the purge's delay are not modelled.

## Territory owned by other open issues

| Issue | Claim it owns |
|-------|---------------|
| #4545 | A shadow marker stranded on a leaf that never sees the terminal (`DeliverLate`, `LeafSplit`, `ReadableOnceComplete`). |
| #4474 | A terminal the copy an undo discarded refuses (`SagaTerminal`, `SagaCompletes`). |
| #4475 | A terminal a purged old copy refuses, and following the resized copy's layout after it (`SagaTerminal`, `SagaCompletes`, `NoStrandedBucket`). |
| #4522 | The stamp of the terminal's committed-values backstop (`SagaTerminal`, `NoKeyLost`). |

When one of these lands, its rows move from `Partial` to `Yes` with the fix's
regression test named, after that test is shown red against the mutation that
reproduces the defect.

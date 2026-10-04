# Refinement note: ShardOwnership to production

This note maps the TLA+ specification in
[`ShardOwnership.tla`](ShardOwnership.tla) to Orleans.Lattice as it exists in
code: the adaptive shard split (`TreeShardSplitGrain`), the online reshard
(`TreeReshardGrain`), the online resize with its undo (`TreeResizeGrain`,
`TreeSnapshotGrain`), the stateless routing tier (`LatticeGrain`), an
atomic-write saga's binding to a physical copy (`AtomicWriteGrain`), and the
leaf's prepared buckets and terminal memory (`BPlusLeafGrain`).

It is a **documented mapping, not a machine-checked refinement proof**. When
the ownership protocol changes, this note shows which spec action and which
production seam must move together, and the Detector column names the
production test that goes red if the behaviour a row abstracts regresses.
Every named detector was shown red against a production perturbation of the
seam it covers; the log is in the pull request that added the module.

## This module and its companion

The area is specified by two modules, documented together in
[`README.md`](README.md#two-modules-and-the-seam-between-them):

- **`ShardOwnership`** (this note) owns who serves a key: the split, the
  reshard, the resize with its fence, flip, refused flip, undo and purge, stale
  routing pairs, and the saga's binding and re-binding to a physical copy. Its
  registry always reports the saga's decision.
- **`ShardOwnershipRetention`** ([`RefinementRetention.md`](RefinementRetention.md))
  owns what the transaction registry's retention does to a saga bound across a
  split and a resize: the registry declining to report the decision, the row's
  retirement, a delayed shadow-forwarded prepare, and leaf reactivation.

A property this note marks clean holds here under a registry that always
answers. It is not a claim about behaviour under retention; read the companion
note for that.

## The base module models the intended design where production has an open defect

The base specification is clean because, where production has an open defect
in this module's territory, it models the **intended** design. Each such place
has an open issue and a mutation in [`mutations/`](mutations/) that restores the
production behaviour and makes a property fire. Until the fix lands, the rows
that cover it read `Partial` and cite the issue.

| Intended design in the base | Production today | Issue | Mutation reproducing production |
|---|---|---|---|
| An adaptive split refuses a resize until the old copy is purged or the resize undone, and a resize refuses a split in flight (`SplitBegin`, `ResizeBegin`) | Only reshard and resize are interlocked | #4452 | `UniqueOwnerSplitDuringResize`, `NoKeyLostResizeDuringSplit` |
| A mid-dispatch re-bind stays bound while the bound copy mirrors into the resolved one, and the routing tier dispatches to it (`SagaRebindOnRefusal`, `SagaPrepare`) | It re-binds unconditionally | #4454 | `SagaBatchOnOneCopyRebindIgnoresMirror` |
| The online snapshot carries prepared buckets (`SnapCopy`) | It copies committed entries only | #4455 | `OwnerMonotonicSnapshotSkipsBuckets` |
| A terminal the copy an undo discarded refuses counts as delivered (`SagaTerminal`) | `AtomicWriteGrain.MarkOneShardAsync` follows the refusal and re-sends the terminal, with its backstop, to the old copy | #4474 | `AtomicOnOwnerDiscardedCopyTerminalRedirects` |
| A terminal a purged old copy refuses is delivered to the copy it mirrored into, following that copy's own layout (`SagaTerminal`, `TermTargets`) | `ShardRootGrain.AppendTxTerminalAsync` on a purged copy throws `InvalidOperationException`, which the broadcast does not follow, so the saga never completes | #4475 | `SagaCompletesPurgedCopyRefusesTerminal` |

The extent of the split interlock is itself pinned. The rule first proposed for
#4452 refused a split only while a resize was in flight; it leaves a split on
the resized copy possible while the old copy still mirrors into it and a saga
is still bound to the old copy. That loses an acknowledged write once the
registry retires the saga's row, so its standing mutation,
`NoKeyLostSplitInSoftDeleteWindow`, lives in the companion module, the one
that models retirement.

The undo's order (#4453) was in this table until its fix landed (#4457). The
base's `UndoArm`, `UndoSwap` and `UndoClear` are now production's order, and
`UniqueOwnerUndoClearsBeforeSwap` stays as the standing regression check for
the order it replaced.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `alias` | The physical copy the logical tree resolves to | `TreeRegistryEntry.PhysicalTreeId` on the logical tree's registry row, written with its map in one row write by `ILatticeRegistry.SwapAliasAsync` (#4357). |
| `rmap` | The logical row's routing map | `TreeRegistryEntry.ShardMap`, abstracted to the shard `k2`'s virtual slot routes to. Adaptive splits write it through `ILatticeRegistry.ReassignSlotsAsync`; every alias swap re-versions it above the row's previous `ShardMap.Version`. |
| `published` | Every (copy, map) pair any routing activation may hold | The `RoutingInfo` pairs `LatticeGrain` caches per activation, resolved together from one registry row by `LatticeGrain.GetRoutingSlowAsync` and published only under `RoutingPairPublishGate.ShouldPublish`. Activations cache for their lifetime, so the spec lets a router hold any pair the registry ever published. |
| `rmapR` | The resized copy's own map | The map `TreeSnapshotGrain.InitiateSnapshotStateAsync` registers the destination with, which `TreeResizeGrain.SwapAliasAsync` carries onto the logical row. |
| `rmapOld` | The map an undo restores | `TreeResizeState.OldRegistryEntry`, captured by `TreeResizeGrain.InitiateResizeStateAsync`. |
| `row[c][s][k]` | A shard's committed projection value for a key | The leaf projection rows of shard `s` of copy `c`. Values are write stamps in commit order, so last-writer-wins is `Max`. |
| `pend[c][s][k]` | The saga's prepared bucket | The leaf's per-transaction pending bucket (`BPlusLeafGrain.PendingTx`). `"old"` and `"new"` record whether the bucket was stamped before or after the later write, which decides whether it outranks that write's row. |
| `term[c][s]` | The activation's memory of the saga's terminal | `BPlusLeafGrain.IsRecentlyTerminal`, the per-activation `_recentlyTerminal` set: the orphan guard's input. This module never loses it; the companion models reactivation. |
| `sp`, `spCopy` | The adaptive split's phase and bound copy | `TreeShardSplitState.Phase` and `TreeShardSplitState.PhysicalTreeId`, with the source shard's `ShardRootState.SplitInProgress`. `"shadow"` is `BeginShadowWrite` before the retroactive sweep, `"swept"` is `Drain`/`Swap` before the freeze, `"frozen"` is the source in Reject before the map moves. |
| `rs` | The reshard coordinator's phase | `TreeReshardState.Phase`. |
| `rz`, `rzShards` | The resize's phase and the old copy's shard set | `TreeResizeState.Phase` and `TreeResizeState.ShardIndices` (from `RoutedShardIndices.Resolve`). `"snap"` is the snapshot running, `"copied"` the swap pending, `"swapped"` Reject, `"retired"` the soft-deleted old copy, `"undoing"` an undo between its first and last step. |
| `fence` | The old copy's fenced shards | Shards whose `ShardRootState.ShadowForward` phase is `ShadowForwardPhase.Rejecting` (`ShardRootGrain.EnterRejectingAsync`, `ShardRootGrain.ExitRejectingAsync`). |
| `redir` | The resized copy armed by an undo | `ShardRootState.RetainedRedirect` on the resized copy's shards (`AliasCutoverShardMaps.ArmRedirectsAsync`). |
| `refusals` | Budget for one refused flip | Modelling device only: it keeps the environment from refusing the flip forever, which production does not guarantee but which an operator resolves. |
| `sg`, `bound`, `prepped`, `told`, `dec` | The saga's phase, binding, dispatched keys, visited shards and decision | `AtomicWriteState.Phase`, `AtomicWriteState.BoundPhysicalTreeId`, `AtomicWriteState.NextIndex`, `AtomicWriteState.TouchedShards`, and `TxRegistryState.Decisions`. The registry here always reports the decision (`RegistryView`). |
| `wDone` | A later plain write has happened | Modelling device: one client write of `k2` after the saga's decision, which gives `NoResurrection` a newer value to protect. |
| `ackOn[c][k]` | What has been acknowledged that copy `c` must hold | Ghost variable with no production counterpart. A write acknowledged on the old copy while a resize is in flight obliges both copies; one acknowledged on the resized copy obliges only it, because an undo discards it by contract ([consistency](../../docs/lattice/consistency.md)). |
| `vis[k]` | The highest value a fresh reader has been served | Ghost variable with no production counterpart: the history `OwnerMonotonic` is stated over. The undo's swap back resets it, because the undo discards the resized copy's writes by contract. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `SplitBegin` | An adaptive split opens its shadow-write window | `TreeShardSplitGrain.SplitAsync` into `TreeShardSplitGrain.InitiateSplitStateAsync` and `ShardRootGrain.BeginSplitAsync`, admitted by `ShardMapCommitFence.Admits`. A split the reshard drives relies on the reshard's own interlock; an autonomous split refusing a resize until the old copy is purged or the resize undone is the intended design (#4452). **Over-approximation:** the guard admits a split whenever the source is unfenced; production also refuses a source mid-migration, which only removes behaviours. | Partial: `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map` pins the alias-cutover fence. No test pins the split/resize interlock, which production lacks until #4452's fix lands. |
| `SplitSweep` | The retroactive sweep of prepares that predate the window | `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`: an undecided prepare is replayed to the destination, a decided one is resolved there with the committed-values backstop. Modelled as one step; its non-atomic window (a decision landing between the pre-check and the replay) and the post-sweep cleanup that closes it are the companion module's late forward. | Yes: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` and `TreeShardSplitGrainTests.RetroactiveSweep_skips_replay_and_applies_commit_terminal_when_saga_already_committed`. |
| `SplitFreeze` | The source refuses the moved slot | `TreeShardSplitGrain.SwapAsync`: `MarkLeavesMovedAwayAsync`, then `EnterRejectPhaseAsync`, before the map moves. | Yes: `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map` and `TreeShardSplitGrainTests.Swap_calls_source_enter_reject_phase_exactly_once`. |
| `SplitCommit` | Final drain, then the map moves | `TreeShardSplitGrain.SwapAsync`: the authoritative final drain (`ForwardMovedSlotEntriesAtomicallyAsync`), then the fenced `ILatticeRegistry.ReassignSlotsAsync`, then `TreeShardSplitGrain.FinaliseAsync`. The background drain is omitted: it is last-writer-wins and dominated by the final drain, and no router can reach the destination before the map moves. | Yes: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip`. |
| `ReshardStart` | A reshard begins | `TreeReshardGrain.ReshardAsync`, refused while a resize is in flight. | Yes: `TreeReshardGrainTests.ReshardAsync_refused_while_resize_in_flight`. |
| `ReshardFinish` | The reshard reaches its target | `TreeReshardGrain` advancing to `ReshardPhase.Complete` once the map names the target shard count. | Yes: `TreeReshardGrainTests.Migrate_advances_to_Complete_once_the_target_shard_count_is_reached` and `TreeReshardGrainTests.RunReshardPass_drives_a_Planning_reshard_through_to_completion`. |
| `ResizeBegin` | A resize captures its shard set and starts the snapshot | `TreeResizeGrain.ResizeCoreAsync` (refused while a reshard is in flight) and `TreeResizeGrain.InitiateResizeStateAsync`, whose shard set is `RoutedShardIndices.Resolve` over the logical map. Refusing while a split is in flight is the intended design (#4452). | Partial: `TreeResizeGrainTests.ResizeAsync_refused_while_reshard_in_flight` and `RoutedShardIndicesTests.Resolve_adds_a_shard_a_split_allocated_above_the_pinned_count`. The split interlock is missing in production until #4452's fix lands. |
| `SnapCopy` | The online snapshot copies the old copy index-for-index | `TreeSnapshotGrain`'s online drain over `RoutedShardIndices.OrContiguous`, keeping an entry only on the shard the copy's map routes it to. Carrying prepared buckets is the intended design (#4455). | Partial: `TreeSnapshotGrainTests.BeginShadowForward_covers_a_shard_a_split_allocated_above_the_pinned_count`. Prepared buckets are not copied in production (#4455). |
| `ResizeFence(s)` | One old shard enters Rejecting before the flip | `TreeResizeGrain.SwapAliasAsync` calling `ShardRootGrain.EnterRejectingAsync` on every shard of `TreeResizeState.ShardIndices` (#4362). | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`. |
| `ResizeFlip` | The alias and map move to the resized copy in one write | `ILatticeRegistry.SwapAliasAsync` from `TreeResizeGrain.SwapAliasAsync`, only after every fence landed. | Yes: `TreeResizeGrainTests.SwapAlias_does_not_move_the_alias_when_an_old_shard_cannot_be_fenced` and `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row`. |
| `ResizeFlipRefused` | A refused or failed flip lifts the fence unless the alias moved | `TreeResizeGrain.LiftFenceUnlessSwappedAsync` deciding through `ResizeFence.LiftsFenceAfterFailedFlip`, then `ShardRootGrain.ExitRejectingAsync`. **Over-approximation:** budgeted to one refusal so the resize is not refused forever; production retries on every tick, so each refusal it makes is one the model makes. | Yes: `TreeResizeGrainTests.SwapAlias_lifts_the_fence_when_the_alias_cannot_move` and `TreeResizeGrainTests.SwapAlias_keeps_the_fence_when_a_failed_flip_reached_the_registry`. |
| `ResizeRetire` | Reject, then soft-delete the old copy | `TreeResizeGrain.RejectOldShardsAsync` and `TreeResizeGrain.CleanupOldTreeAsync`. The old copy stays fenced and admits the saga bound to it. | Yes: `TreeResizeGrainTests.RejectOldShards_rejects_a_shard_a_split_allocated_above_the_pinned_count` and `TreeResizeGrainTests.Cleanup_soft_deletes_a_later_resizes_old_physical_tree`. |
| `ResizePurge` | The purge after `SoftDeleteDuration` clears the old copy | `ShardRootGrain.PurgeAsync` driven by the tree-deletion grain. **Timing assumption, not an over-approximation:** the action drops every pair naming the old copy from `published`. A purged shard row answers an unseeded read as empty rather than refusing it, so the model is sound only because a routing activation idle for longer than `SoftDeleteDuration` has been collected; the undo-discard design makes the same argument (#3930). | Yes: `ShardRootGrainPurgeTests.PurgeAsync_clears_the_single_root_leaf_when_tree_is_flat`. |
| `UndoBeforeFlip` | Undo during the snapshot | `TreeResizeGrain.UndoResizeCoreAsync`'s before-swap branch: abort the snapshot, `ShardRootGrain.ClearShadowForwardAsync` on every old shard, discard the destination. | Yes: `TreeResizeGrainTests.UndoResize_at_snapshot_phase_discards_destination_without_recovering` and `TreeResizeGrainTests.UndoResize_during_drain_releases_a_split_allocated_shard`. |
| `UndoArm` | The resized copy is armed to redirect, before the swap | `AliasCutoverShardMaps.ArmRedirectsAsync` from `TreeResizeGrain.UndoResizeCoreAsync`, before the swap and again from the swap's own read (#4453). | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it` and `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_to_redirect_onto_the_old_tree`. |
| `UndoSwap` | The alias and the old map move back in one write | `ILatticeRegistry.SwapAliasAsync` from `TreeResizeGrain.UndoResizeCoreAsync`, with `TreeResizeState.OldRegistryEntry`'s map. | Yes: `TreeResizeGrainTests.UndoResize_recovers_old_tree_and_removes_alias`. |
| `UndoClear` | The old copy's fence lifts, after the swap | `ShardRootGrain.ClearShadowForwardAsync` on every old shard, after the old copy is recovered and the alias names it (#4453). | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it` and `TreeResizeGrainTests.UndoResize_after_swap_releases_a_split_allocated_shard`. |
| `SagaStart` | The saga binds to the copy the tree resolves to | `AtomicWriteGrain` binding `AtomicWriteState.BoundPhysicalTreeId` when it prepares, and `AtomicWriteGrain.BindUnboundSagaAsync` for a saga resumed from older state. | Yes: `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on` and `AtomicWriteGrainTests.ReceiveReminder_binds_a_saga_resumed_from_state_without_a_binding_before_it_dispatches`. |
| `SagaPrepare(k, p)` | One key's prepared write through a routing activation | `LatticeGrain.SetManyAsyncCore` deciding through `SagaCopyBinding.AdmitsDispatch` (cached pair, then one re-read) and handing the binding to the bound copy's shards, which admit it through a resize fence via `ShardRootGrain.AdmitsBoundSagaWhileFenced` and `ResizeFence.AdmitsBoundSaga` (#4358, #4369). Dispatching to the bound copy while it mirrors into the resolved one is the intended design (#4454). **Over-approximation:** one key per step, where production places a whole slice in one call; the extra partial interleavings stand for a transient shard failure part way through a call. | Partial: `LatticeGrainTests.SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy`, `LatticeGrainTests.SetManyAsync_under_a_saga_binding_rereads_a_pair_cached_before_a_swap`, `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_to_the_fenced_copy_is_applied_and_forwarded` and `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_elsewhere_is_rejected_by_the_fenced_copy`. The routing tier refuses rather than dispatches while the bound copy mirrors (#4454). |
| `SagaRebindOnRefusal` | The saga re-binds after a mid-dispatch refusal | `AtomicWriteGrain.TryRebindToResolvedCopyAsync`, entered when `SagaCopyBinding.RebindsAfterRefusal` holds. Staying bound while the bound copy mirrors is the intended design (#4454). | Partial: `AtomicWriteGrainTests.ExecuteAsync_rebinds_and_commits_on_the_new_copy_when_its_bound_copy_moved_during_dispatch`. Production re-binds without the mirror check (#4454). |
| `SagaRebindBeforeDecision` | The pre-decision check re-binds | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` when `SagaCopyBinding.BeforeDecision` answers `Rebind`. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaDecide` | The commit decision is recorded | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` answering `Commit` or `StayBound`, then the registry decision. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaAbort` | The abort decision is recorded; the broadcast compensates | `AtomicWriteGrain.BroadcastTerminalsAsync` with an abort after a failed execute phase. **Environment action:** unguarded through the execute phase and not fair, because production aborts on any prepare failure past its retries or on the caller going away, which the model does not otherwise represent. | Yes: `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. |
| `SagaTerminal(s)` | One shard of the terminal broadcast | `AtomicWriteGrain.MarkOneShardAsync` into `ShardRootGrain.AppendTxTerminalAsync` (a direct terminal passes a resize fence, #4369, and is mirrored), the leaf applying it through `MigrationTerminalCore.DecideBucketAction` (`DiscardOrphan` when the activation already applied it). Counting a terminal the discarded copy refuses as delivered (#4474), and delivering one a purged copy refuses to the copy it mirrored into (#4475), are the intended design. | Partial: `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_addressed_to_the_fenced_copy_directly_is_applied_and_forwarded`, `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_routed_through_the_alias_after_the_fence_is_rejected_and_not_forwarded` and `BPlusLeafGrainTests.ApplyTxTerminalAsync_with_already_terminalled_txid_discards_orphan_pending_bucket`. Production re-sends a terminal the discarded copy refuses to the old copy (#4474) and fails the broadcast on a purged copy (#4475). |
| `SagaComplete` | The broadcast finished; the caller is acknowledged | `AtomicWriteGrain.CompleteSagaAsync` after the broadcast has reached every touched shard. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `LaterWrite(p)` | A client write of `k2` through a routing activation | A routed write through `LatticeGrain` into a shard, mirrored by `ShardRootGrain.ForwardShadowAsync` while the shard forwards. **Environment action:** any pair the registry ever published may carry it. | Yes: `ShardRootGrainShadowForwardTests.SetAsync_forwards_during_draining`, `ShardRootGrainShadowForwardTests.SetManyAsync_forwards_full_batch_in_single_call_to_destination` and `LatticeGrainTests.SetAsync_retries_on_stale_alias`. |
| `Stutter` | Quiescence | Not a protocol step: a stuttering successor once nothing is in flight. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `UniqueOwner` | Every routing pair a `LatticeGrain` activation may hold is either refused for a key (split Reject, resize `Rejecting`, a retained redirect) or reaches that key's one owner. The fences exist for exactly this (#4362, #4357, #4453). | Partial: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`, `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map`, `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it` and `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole`. Production lets a split target serve after a flip until #4452's fix lands. |
| `NoKeyLost` | The owner's location holds every value acknowledged to a writer, so the final drain, the snapshot and the mirror carry every acknowledged write across a split or a flip. | Partial: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip` and `ShardRootGrainShadowForwardTests.SetManyAsync_forwards_full_batch_in_single_call_to_destination`. Writes to a split target are lost at a flip in production until #4452's fix lands. |
| `NoResurrection` | No served read returns a value older than one already acknowledged: no stale old copy after a flip (#4362) and no stale resized copy after an undo (#4453). The late-orphan form (#4445) is the companion module's. | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias` and `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`. |
| `SagaBatchOnOneCopy` | Once committed, a saga holds prepared buckets only on its bound copy and the copy that copy mirrors into (#4357, #4358, #4369). | Partial: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy` and `LatticeGrainTests.SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy`. The mid-dispatch re-bind is open (#4454). |
| `AtomicOnOwner` | A fresh reader sees a saga's batch on every key or on none. | Partial: `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole` and `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on`. A batch decided before a flip reads torn on the resized copy (#4455), and a terminal re-sent to the old copy after an undo lands part of a batch there (#4474). |
| `OwnerMonotonic` | The value a fresh reader gets never moves backwards, except across an undo's swap, which discards the resized copy's writes by contract. Stated over the history the ghost `vis` records. | Partial: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` and `BPlusLeafGrainTests.Materialiser_replays_prepared_set_into_pending_tx`. A commit decided before a flip reverts on the resized copy (#4455). |
| `SplitCompletes` | A split that opened its window finishes, under weakly fair coordinator steps. | Yes: `TreeShardSplitGrainTests.RunSplitPass_resumes_from_BeginShadowWrite_after_crash` and `TreeShardSplitGrainTests.ProcessNextPhase_drives_the_shadow_write_phase_through_the_full_split_pass`. |
| `ReshardCompletes` | A reshard that started reaches its target. | Yes: `TreeReshardGrainTests.RunReshardPass_drives_a_Planning_reshard_through_to_completion`. |
| `ResizeCompletes` | A resize that started is purged or undone. | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias` and `TreeResizeGrainTests.Cleanup_soft_deletes_a_later_resizes_old_physical_tree`. |
| `SagaCompletes` | A saga that started completes. | Partial: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. A saga bound to an old copy that is purged before its broadcast finishes never completes (#4475). |
| `RoutingConverges` | Eventually the registry's own pair serves every key: no fence, Reject or redirect outlives the operation that set it, so a refreshed router stops being refused. | Partial: `LatticeGrainTests.GetAsync_retries_on_stale_alias` and `TreeResizeGrainTests.UndoResize_after_swap_releases_a_split_allocated_shard`. An undo after a split that committed during the resize restores a map the old copy refuses forever until #4452's fix lands. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Over-approximation arguments

Every environment action, and every guard the base keeps weaker than
production, is argued in its row above. The general rule the module follows:
a guard may be weaker than production's (the model then explores behaviours
production cannot reach, so a clean result covers production), never stronger,
except where a row names a timing assumption (`ResizePurge`) or an intended
design that production lacks (the table at the top).

Three modelling choices are worth stating because they are easy to misread:

- **Routers.** A routing activation is not a variable. Any pair the registry
  ever published may be used by any call, at any time, which subsumes any
  number of activations and any cache age. That is why the routing tier's
  publish rule (`RoutingPairPublishGate`) has no action here: the spec assumes
  only that a router never holds a pair the registry did not publish, and
  `RoutingPairPublishModel` (Coyote) checks the rule that makes that true,
  including that a pair already invalidated is never published again.
- **Read precedence.** A surfaced bucket stamped before the later write is
  last-writer-wins merged with the row; one stamped after it outranks the row.
  Production's supersession rule also declines to let a migrated row
  supersede a bucket. The model does not distinguish migrated rows, so where
  production serves an older bucket over a newer migrated row the model
  serves the row: an under-approximation, listed below.
- **The registry always answers.** `RegistryView` is the recorded decision. A
  registry that declines to answer, or has retired the row, is strictly more
  behaviour, and the companion module checks it.

## Coyote models of the cores

| Model | Core it drives | Spec construct it refines | Guards (each must be found) |
|-------|----------------|---------------------------|-----------------------------|
| `ResizeFenceModel` | `ResizeFence` | `ResizeFence`, `ResizeFlip`, `ResizeFlipRefused`, the fenced half of `SagaPrepare` | the fence refusing the bound saga (pre-#4376), lifting after a landed flip, flipping before fencing (#4362) |
| `SagaCopyBindingModel` | `SagaCopyBinding` | `SagaPrepare`'s routing half, `SagaRebindOnRefusal`, `SagaRebindBeforeDecision`, `SagaDecide`, and `SagaBatchOnOneCopy` | the router ignoring the binding (#4358), the pre-decision check ignoring the mirror (#4369); plus a characterisation of #4454 that must flip when it is fixed |
| `RoutingPairPublishModel` | `RoutingPairPublishGate` | the assumption behind `published` | no version check, no epoch check |

Every guard also has a specificity test that disables exactly the assertion
it targets and requires a clean run.

## Deliberate abstraction gaps

What the module does **not** cover, stated so that coverage of one part is
not read as coverage of another:

- **Registry retention and leaf reactivation.** This module's registry always
  reports the decision and its leaves never lose memory. Both are the
  companion module's, which keeps the split, the resize and the undo they act
  through but not the reshard, the refused flip, the undo before a flip, the
  re-binds or stale writers. A defect needing retention together with one of
  those is outside both modules; see the README's account of the seam.
- **Alias cutovers other than a resize.** A shadow-cutover restore and its
  revert, an explicit `SetTreeAliasAsync`, and schema remediation all move the
  alias too (#4357 fixed all of them); none is modelled. With them goes the
  split's abandon path (`TreeShardSplitGrain.AbandonRetargetedSplitAsync`),
  which only an alias move can reach and which the base's interlock makes
  unreachable; mutations that break the interlock stand in for it by letting a
  stranded split count as finished. This is blindly inexpressible: production
  can do it and the spec cannot.
- **Consolidation (a shrinking reshard).** `TreeShardConsolidationGrain` folds
  shards together through the same shadow-write window; the reshard here only
  grows. #4452's fix applies the same hold to it.
- **The leaf-level moved-away handoff.** The seal a split leaves on the source
  leaves and its inheritance across a leaf split are modelled as one freeze
  step; the existing `SplitPivotAdmissionModel`, `SpanAdmissionMigrationModel`
  and `MovedAwaySealInheritanceModel` cover them at leaf granularity.
- **Undo's failure paths.** A swap-back that fails after the arm is released
  by `TreeResizeGrain.ReleaseUndoRedirectUnlessSwappedAsync`; the model has no
  failing undo action.
- **Migrated-row supersession**, as above: production can serve an older
  bucket over a newer migrated row in cases the model cannot express.
- **More than one of anything.** One split, one reshard, one resize, one saga
  writing two keys, one later write. A second saga contending for a key, a
  second split, and a split of the resized copy during the resize are bounded
  out.
- **Time.** No timers, retention windows or deadlines. The purge relies on the
  timing assumption in its row.

## Territory owned by other open issues

| Issue | Claim it owns |
|-------|---------------|
| #4452 | The split/resize interlock (`SplitBegin`, `ResizeBegin`, `UniqueOwner`, `NoKeyLost`, `RoutingConverges`). |
| #4454 | The mid-dispatch re-bind (`SagaRebindOnRefusal`, `SagaPrepare`, `SagaBatchOnOneCopy`). |
| #4455 | Prepared buckets in the online snapshot (`SnapCopy`, `AtomicOnOwner`, `OwnerMonotonic`). |
| #4474 | A terminal the copy an undo discarded refuses (`SagaTerminal`, `AtomicOnOwner`). |
| #4475 | A terminal a purged old copy refuses (`SagaTerminal`, `SagaCompletes`). |

When one of these lands, its rows move from `Partial` to `Yes` with the fix's
regression test named, after that test is shown red against the mutation that
reproduces the defect.

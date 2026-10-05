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

The base specification is clean because it models the **intended** design.
Where production had an open defect in this module's territory, the base kept
the intended design and a mutation in [`mutations/`](mutations/) restored
production's behaviour and made a property fire. No such defect is open now.
These rows have left this section's table because their fixes landed, and their
mutations stay as standing regression checks for the behaviour they replaced:

- **The saga value's stamp** (#4522, fixed by #4566, #4610 and #4629). A
  marked bucket drains at its prepare stamp P (#4566); the coordinator reads
  every key's P back before it decides, commits only with one for every marked
  key, and carries each to the copy that minted it, whose backstop installs
  the key at P (#4610); the resize mirror forwards a prepare marked at P and a
  plain write as the old copy's stored row at its own stamp, the snapshot's
  sweep carries buckets at their stamps, and the mirrored terminal carries P
  (#4629): the base's `TermRow`, `BVal`, `WVal` and `SnapCopy`.
  `NoKeyLostFreshStampDrainOverMigratedRow`, `NoKeyLostFreshStampBackstop`,
  `NoKeyLostResizeMirrorUnmarkedPrepare` and
  `NoKeyLostSnapshotResolvesAtFreshStamp` reproduce production before each fix.
- **A terminal the copy an undo discarded refuses** (#4474, fixed by #4516). It
  counts as delivered and is never re-sent to the old copy: the base's
  `SagaTerminal`. `SagaCompletesDiscardedCopyRefusesTerminal` reproduces
  production before the fix, and `AtomicOnOwnerDiscardedCopyTerminalRedirects`
  stands against a fix that follows the refusal.
- **A terminal a purged old copy refuses** (#4475, fixed by #4531 and #4581). It
  is redelivered to the copy it mirrored into, following that copy's layout and
  carrying no committed values: the base's `TermCopy` and `TermTargets`.
  `SagaCompletesPurgedCopyRefusesTerminal` reproduces production before the fix.
- **A migration import over a resolved saga's row** (#4564, fixed by #4600). A
  value stored at a prepare stamp carried from another shard is stored migrated,
  durably through the WAL, so a later migration import competes with it by
  last-writer-wins: the base's `SplitCommit` and `LaterWrite`.
  `NoKeyLostMigrationImportDropped` reproduces production before the fix.
- **A routed operation on a purged old copy** (#4503, fixed by #4528). The purge
  leaves a tombstone, and a routed call on it is refused with
  `StaleTreeRoutingException` unless its logical tree resolves there, so a
  router that cached the old pair refreshes: the base's `Gone` in
  `RoutedRefused`. `NoResurrectionPurgedCopyServesEmpty` and
  `NoKeyLostPurgedCopyAcceptsWrites` reproduce production before the fix.
- **Prepared buckets in the online snapshot** (#4455, fixed by #4506). The
  snapshot sweeps each source shard's prepared buckets onto the resized copy
  through `PreparedBucketSweep.RunAsync`, which the split's sweep shares: the
  base's `SnapCopy`. `OwnerMonotonicSnapshotSkipsBuckets` reproduces production
  before the fix.
- **The mid-dispatch re-bind** (#4454, fixed by #4521). The routing tier places
  a bound batch on its bound copy while that copy mirrors into the resolved one
  (`SagaCopyBinding.DispatchCopy`), and a refused saga stays bound under the
  same check (`SagaCopyBinding.AfterRefusal`): the base's `SagaPrepare` and
  `SagaRebindOnRefusal`. `SagaBatchOnOneCopyRebindIgnoresMirror` reproduces
  production before the fix.
- **The undo's order** (#4453, fixed by #4457). The base's `UndoArm`,
  `UndoSwap` and `UndoClear` are production's order;
  `UniqueOwnerUndoClearsBeforeSwap` reproduces the order it replaced.
- **The split/resize interlock** (#4452, fixed by #4466). A consolidation
  refuses while a resize is in flight, while an undo is pending or running, and
  after the resize completes for as long as any shard of the replaced copy
  still mirrors into the resized one; a split refuses while a resize is in
  flight or an undo is pending or running, and once the resize completes it may
  run while the replaced copy mirrors, because the mirror follows its refusal
  (#4478); a resize refuses while a split is in flight. `ResizeBegin` and
  `SplitBegin` are the base's.
  `UniqueOwnerSplitDuringResize` and `NoKeyLostResizeDuringSplit` reproduce
  production before the fix. The extent of the hold is pinned too: the rule
  first proposed admitted a split in the soft-delete window without the
  mirror and the terminal following it, which loses an acknowledged write once
  the registry retires the saga's row; its standing mutation,
  `NoKeyLostSplitInSoftDeleteWindow`, lives in the companion module, the one
  that models retirement.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `alias` | The physical copy the logical tree resolves to | `TreeRegistryEntry.PhysicalTreeId` on the logical tree's registry row, written with its map in one row write by `ILatticeRegistry.SwapAliasAsync` (#4357). |
| `rmap` | The logical row's routing map | `TreeRegistryEntry.ShardMap`, abstracted to the shard `k2`'s virtual slot routes to. Adaptive splits write it through `ILatticeRegistry.ReassignSlotsAsync`; every alias swap re-versions it above the row's previous `ShardMap.Version`. |
| `published` | Every (copy, map) pair any routing activation may hold | The `RoutingInfo` pairs `LatticeGrain` caches per activation, resolved together from one registry row by `LatticeGrain.GetRoutingSlowAsync` and published only under `RoutingPairPublishGate.ShouldPublish`. Activations cache for their lifetime, so the spec lets a router hold any pair the registry ever published. |
| `rmapR` | The resized copy's own map | The map `TreeSnapshotGrain.InitiateSnapshotStateAsync` registers the destination with, which `TreeResizeGrain.SwapAliasAsync` carries onto the logical row, and which a split of the resized copy moves (#4478). |
| `rmapOld` | The map an undo restores | `TreeResizeState.OldRegistryEntry`, captured by `TreeResizeGrain.InitiateResizeStateAsync`. |
| `row[c][s][k]` | A shard's committed projection value for a key | The leaf projection rows of shard `s` of copy `c`. A value is a version of a write: `Rank` is its real-time order, which acknowledgement and reads are judged by, and `Stamp` its HLC stamp, which last-writer-wins (`LWW`) compares. The base stamps versions in commit order; `LaterLowV`, `SagaUV` and `FreshV` let a mutation express a stamp that disagrees with real time (#4522). |
| `mig[c][s][k]` | Whether the row was last written by a cross-shard migration | `LwwValue.IsMigrated`, set by `MergeManyAsync` with `isCrossShardMigration` (the split's moved-slot drain and its shadow-forward of a plain write) and cleared when a non-migration write wins. The resize mirror and the snapshot never set it. |
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
| `SplitBegin` | An adaptive split opens its shadow-write window | `TreeShardSplitGrain.SplitAsync` into `TreeShardSplitGrain.InitiateSplitStateAsync` and `ShardRootGrain.BeginSplitAsync`, admitted by `ShardMapCommitFence.Admits`. The split refuses while `ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync` reports a hold, read before the split allocates anything and again once the source's record is open, which closes the race with a resize starting; the hold is `TreeResizeGrain.HoldsShardSplitsAsync`, true while a resize is in flight or an undo is pending, running or not yet persisted, and false once the resize completes, while the replaced copy still mirrors into the resized one (#4478): that mirror follows a split's refusal to the slot's current owner (`ShardRootGrain.ForwardShadowAsync`), and a bound saga's terminal reaches the resized copy's split closure. Consolidations and reshards keep `TreeResizeGrain.HoldsShardMigrationsAsync`, true until no shard of the replaced copy mirrors into the resized one (#4452). A split the reshard drives relies on the reshard's own hold. **Over-approximation:** the guard admits a split whenever the source is unfenced; production also refuses a source mid-migration, and fails closed when the hold cannot be read, which only removes behaviours. The base admits a split once the resize has retired its old copy, as production does: the mirror chases a refusal to the slot's current owner (`RTarget`), a bound saga's mirrored terminal reaches the split closure (`TermClosure`), and an undo during the split abandons it (`SplitAbandon`). | Yes: `ResizeMigrationHoldDetectorIntegrationTests.A_split_is_refused_by_the_resize_hold_while_the_resize_is_in_flight` and `ResizeMigrationHoldDetectorIntegrationTests.A_split_proceeds_while_the_replaced_copy_still_mirrors_because_the_mirror_follows_it` (real grains end to end, red when the split hold is lifted while the resize is in flight or kept until the purge), `ResizeSplitInSoftDeleteWindowIntegrationTests.A_bound_sagas_prepare_follows_a_split_of_the_resized_copy_to_the_slots_owner` and `ResizeSplitInSoftDeleteWindowIntegrationTests.A_bound_sagas_terminal_reaches_the_buckets_a_split_of_the_resized_copy_moved` (the mirror and the bound terminal follow the split), `TreeResizeGrainTests.HoldsShardSplits_is_true_while_an_undo_is_pending`, `TreeResizeGrainTests.HoldsShardSplits_is_false_once_complete_while_the_replaced_copy_still_mirrors`, and the caller tests `TreeShardSplitGrainTests.SplitAsync_refuses_while_a_resize_of_the_tree_is_in_flight`, `TreeShardSplitGrainTests.SplitAsync_proceeds_while_a_completed_resize_still_has_the_replaced_copy_mirroring` and `TreeShardSplitGrainTests.InitiateSplit_backs_out_when_a_resize_is_in_flight_once_the_source_record_is_open`, which stub the hold and so cover only the split's reading of it. The fence's two arms: `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map` (the alias already moved) and `TreeShardSplitGrainTests.Swap_while_a_cutover_has_carried_the_copy_map_but_not_swapped_the_alias_does_not_apply_the_diff` (a cutover carried the map but has not swapped). |
| `SplitSweep` | The retroactive sweep of prepares that predate the window | `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`: an undecided prepare is replayed to the destination, a decided one is resolved there with the committed-values backstop. Modelled as one step; its non-atomic window (a decision landing between the pre-check and the replay) and the post-sweep cleanup that closes it are the companion module's late forward. | Yes: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` and `TreeShardSplitGrainTests.RetroactiveSweep_skips_replay_and_applies_commit_terminal_when_saga_already_committed`. |
| `SplitFreeze` | The source refuses the moved slot | `TreeShardSplitGrain.SwapAsync`: `MarkLeavesMovedAwayAsync`, then `EnterRejectPhaseAsync`, before the map moves. | Yes: `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map` and `TreeShardSplitGrainTests.Swap_calls_source_enter_reject_phase_exactly_once`. |
| `SplitCommit` | Final drain, then the map moves | `TreeShardSplitGrain.SwapAsync`: the authoritative final drain (`ForwardMovedSlotEntriesAtomicallyAsync`), then the fenced `ILatticeRegistry.ReassignSlotsAsync`, then `TreeShardSplitGrain.FinaliseAsync`. A split of the resized copy moves that copy's own map too (`rmapR`). The background drain is omitted: it is last-writer-wins and dominated by the final drain, and no router can reach the destination before the map moves. The final drain is a migration import: a row it writes is flagged migrated, and it merges last-writer-wins over a row a carried prepare stamp stored, which is stored migrated (#4564). | Yes: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip`, `BPlusLeafGrainTests.A_later_migration_import_wins_over_a_value_drained_at_a_carried_original_stamp`, `BPlusLeafGrainTests.A_later_migration_import_wins_over_a_backstop_at_a_carried_original_stamp`, `BPlusLeafGrainTests.A_value_drained_at_a_carried_stamp_stays_migrated_across_a_replay`, `BPlusLeafGrainTests.A_backstop_at_a_carried_stamp_stays_migrated_across_a_replay` and `SplitMigrationImportAfterSagaIntegrationTests.A_write_acknowledged_after_the_sweep_resolved_a_decided_saga_survives_the_split`. |
| `SplitAbandon` | An alias move retargets a split before it commits | `TreeShardSplitGrain.AbandonRetargetedSplitAsync` (#4264), once `ShardMapCommitFence.Admits` refuses the commit because the tree no longer resolves to the split's copy. Reachable since a split may run on the resized copy while its resize can still be undone (#4478). | Yes: `ResizeSplitInSoftDeleteWindowIntegrationTests.An_undo_during_a_split_of_the_resized_copy_abandons_the_split` (red when the abandon does nothing) and `ResizeSplitInSoftDeleteWindowIntegrationTests.An_undo_after_a_split_of_the_resized_copy_committed_discards_the_copy_with_its_map`. |
| `ReshardStart` | A reshard begins | `TreeReshardGrain.ReshardAsync`, refused while `ShardMigrationResizeInterlock.ReadResizeHoldAsync` reports a hold (a resize in flight, an undo pending or running, or a replaced copy still mirroring), as the adaptive split is (#4452). | Yes: `ResizeMigrationHoldDetectorIntegrationTests.A_reshard_is_refused_by_the_resize_hold_while_the_replaced_copy_still_mirrors` (real grains, red when the hold stops holding), `TreeReshardGrainTests.ReshardAsync_is_refused_as_resize_undoable_while_a_completed_resize_holds_migrations` (red when the reshard stops reading the completed resize's hold) and `TreeReshardGrainTests.ReshardAsync_refused_while_resize_in_flight`. A consolidation reads the same hold: `ResizeMigrationHoldDetectorIntegrationTests.A_consolidation_is_refused_by_the_resize_hold_while_the_resize_is_in_flight` and `ResizeMigrationHoldDetectorIntegrationTests.A_consolidation_is_refused_by_the_resize_hold_while_the_replaced_copy_still_mirrors`. |
| `ReshardFinish` | The reshard reaches its target | `TreeReshardGrain` advancing to `ReshardPhase.Complete` once the map names the target shard count. | Yes: `TreeReshardGrainTests.Migrate_advances_to_Complete_once_the_target_shard_count_is_reached` and `TreeReshardGrainTests.RunReshardPass_drives_a_Planning_reshard_through_to_completion`. |
| `ResizeBegin` | A resize captures its shard set and starts the snapshot | `TreeResizeGrain.ResizeCoreAsync` (refused while a reshard is in flight) and `TreeResizeGrain.InitiateResizeStateAsync`, whose shard set is `RoutedShardIndices.Resolve` over the logical map. It refuses while a split is in flight, reading every routed shard's migration record through `ShardMigrationResizeInterlock.FindMigratingShardAsync` after its own intent is persisted (#4452). | Yes: `TreeResizeGrainTests.InitiateResize_refuses_while_a_shard_split_is_in_flight_and_starts_no_snapshot`, `TreeResizeGrainTests.ResizeAsync_refused_while_reshard_in_flight` and `RoutedShardIndicesTests.Resolve_adds_a_shard_a_split_allocated_above_the_pinned_count`. |
| `SnapCopy` | The online snapshot copies the old copy index-for-index | `TreeSnapshotGrain`'s online drain over `RoutedShardIndices.OrContiguous`, keeping an entry only on the shard the copy's map routes it to, and `TreeSnapshotGrain.SweepPreparedBucketsAsync`, which carries each source shard's prepared buckets onto the resized copy through `PreparedBucketSweep.RunAsync` (#4455). Carrying them at their stamps (`carryOriginalStamps`) is #4522's fix (#4629). | Yes: `TreeSnapshotGrainTests.BeginShadowForward_covers_a_shard_a_split_allocated_above_the_pinned_count`, `TreeSnapshotGrainTests.Online_shadow_begin_carries_an_in_flight_prepared_bucket_onto_the_destination_shard`, `TreeSnapshotGrainTests.Online_shadow_begin_applies_the_terminal_of_a_saga_decided_before_the_sweep`., `TreeSnapshotGrainTests.Online_shadow_begin_replays_a_marked_in_flight_prepare_carrying_its_original_stamp` and `TreeSnapshotGrainTests.Online_shadow_begin_backstops_a_decided_marked_prepare_at_its_original_stamp`. |
| `ResizeFence(s)` | One old shard enters Rejecting before the flip | `TreeResizeGrain.SwapAliasAsync` calling `ShardRootGrain.EnterRejectingAsync` on every shard of `TreeResizeState.ShardIndices` (#4362). | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`. |
| `ResizeFlip` | The alias and map move to the resized copy in one write | `ILatticeRegistry.SwapAliasAsync` from `TreeResizeGrain.SwapAliasAsync`, only after every fence landed. | Yes: `TreeResizeGrainTests.SwapAlias_does_not_move_the_alias_when_an_old_shard_cannot_be_fenced`, `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row` and `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_replaces_a_persisted_map_on_the_logical_row` (the swap replaces a map the logical row already persists, rather than keeping its slots under a new version). |
| `ResizeFlipRefused` | A refused or failed flip lifts the fence unless the alias moved | `TreeResizeGrain.LiftFenceUnlessSwappedAsync` deciding through `ResizeFence.LiftsFenceAfterFailedFlip`, then `ShardRootGrain.ExitRejectingAsync`. **Over-approximation:** budgeted to one refusal so the resize is not refused forever; production retries on every tick, so each refusal it makes is one the model makes. | Yes: `TreeResizeGrainTests.SwapAlias_lifts_the_fence_when_the_alias_cannot_move` and `TreeResizeGrainTests.SwapAlias_keeps_the_fence_when_a_failed_flip_reached_the_registry`. |
| `ResizeRetire` | Reject, then soft-delete the old copy | `TreeResizeGrain.RejectOldShardsAsync` and `TreeResizeGrain.CleanupOldTreeAsync`. The old copy stays fenced and admits the saga bound to it. | Yes: `TreeResizeGrainTests.RejectOldShards_rejects_a_shard_a_split_allocated_above_the_pinned_count` and `TreeResizeGrainTests.Cleanup_soft_deletes_a_later_resizes_old_physical_tree`. |
| `ResizePurge` | The purge after `SoftDeleteDuration` clears the old copy | `ShardRootGrain.PurgeAsync` driven by the tree-deletion grain. Pairs naming the old copy stay published: nothing bounds a routing activation's lifetime below `SoftDeleteDuration`, which may be zero. The purge leaves a tombstone (`ShardRootState.IsPurged`), so a routed operation on the purged copy is refused with `StaleTreeRoutingException` unless its logical tree resolves there (#4503). | Yes: `ShardRootGrainPurgeTests.PurgeAsync_clears_the_single_root_leaf_when_tree_is_flat`, `ShardRootGrainPurgeTests.PurgeAsync_leaves_only_a_purge_tombstone`, `ShardRootGrainPurgeTests.A_routed_call_whose_tree_resolves_elsewhere_is_refused_as_stale`, `PurgedCopyStaleRoutingIntegrationTests.A_stale_router_is_refused_by_a_purged_first_resize_copy` and `PurgedCopyStaleRoutingIntegrationTests.A_stale_router_is_refused_by_a_purged_later_resize_copy_which_is_not_resurrected`. |
| `UndoBeforeFlip` | Undo during the snapshot | `TreeResizeGrain.UndoResizeCoreAsync`'s before-swap branch: abort the snapshot, `ShardRootGrain.ClearShadowForwardAsync` on every old shard, discard the destination. | Yes: `TreeResizeGrainTests.UndoResize_at_snapshot_phase_discards_destination_without_recovering` and `TreeResizeGrainTests.UndoResize_during_drain_releases_a_split_allocated_shard`. |
| `UndoArm` | The resized copy is armed to redirect, before the swap | `AliasCutoverShardMaps.ArmRedirectsAsync` from `TreeResizeGrain.UndoResizeCoreAsync`, before the swap and again from the swap's own read (#4453). | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it` pins the order (arm before the swap); `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_to_redirect_onto_the_old_tree` pins only that the arm happens and its target, and stays green when the arm moves after the swap. |
| `UndoSwap` | The alias and the old map move back in one write | `ILatticeRegistry.SwapAliasAsync` from `TreeResizeGrain.UndoResizeCoreAsync`, with `TreeResizeState.OldRegistryEntry`'s map. | Yes: `TreeResizeGrainTests.UndoResize_recovers_old_tree_and_removes_alias`. |
| `UndoClear` | The old copy's fence lifts, after the swap | `ShardRootGrain.ClearShadowForwardAsync` on every old shard, after the old copy is recovered and the alias names it (#4453). | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it` and `TreeResizeGrainTests.UndoResize_after_swap_releases_a_split_allocated_shard`. |
| `SagaStart` | The saga binds to the copy the tree resolves to | `AtomicWriteGrain` binding `AtomicWriteState.BoundPhysicalTreeId` when it prepares, and `AtomicWriteGrain.BindUnboundSagaAsync` for a saga resumed from older state. | Yes: `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on` and `AtomicWriteGrainTests.ReceiveReminder_binds_a_saga_resumed_from_state_without_a_binding_before_it_dispatches`. |
| `SagaPrepare(k, p)` | One key's prepared write through a routing activation | `LatticeGrain.SetManyAsyncCore` deciding through `SagaCopyBinding.AdmitsDispatch` against the cached pair, then `SagaCopyBinding.DispatchCopy` against one re-read, which places the batch on the bound copy while it mirrors into the resolved one (#4454), and handing the binding to the bound copy's shards, which admit it through a resize fence via `ShardRootGrain.AdmitsBoundSagaWhileFenced` and `ResizeFence.AdmitsBoundSaga` (#4358, #4369). **Over-approximation:** one key per step, where production places a whole slice in one call; the extra partial interleavings stand for a transient shard failure part way through a call. | Yes: `LatticeGrainTests.SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy`, `LatticeGrainTests.SetManyAsync_under_a_saga_binding_rereads_a_pair_cached_before_a_swap`, `LatticeGrainTests.SetManyAsync_under_a_saga_binding_places_the_batch_on_a_bound_copy_that_mirrors_into_the_resolved_one`, `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_to_the_fenced_copy_is_applied_and_forwarded` and `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_elsewhere_is_rejected_by_the_fenced_copy`. |
| `SagaRebindOnRefusal` | The saga re-binds after a mid-dispatch refusal | `AtomicWriteGrain.TryRebindToResolvedCopyAsync`, entered when `SagaCopyBinding.RebindsAfterRefusal` holds, re-binding only when `SagaCopyBinding.AfterRefusal` answers `Rebind`: it stays bound while the bound copy mirrors into the resolved one (#4454). | Yes: `AtomicWriteGrainTests.ExecuteAsync_rebinds_and_commits_on_the_new_copy_when_its_bound_copy_moved_during_dispatch` and `AtomicWriteGrainTests.ExecuteAsync_stays_bound_after_a_refusal_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaRebindBeforeDecision` | The pre-decision check re-binds | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` when `SagaCopyBinding.BeforeDecision` answers `Rebind`. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaDecide` | The commit decision is recorded | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` answering `Commit` or `StayBound`, then the registry decision. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaAbort` | The abort decision is recorded; the broadcast compensates | `AtomicWriteGrain.BroadcastTerminalsAsync` with an abort after a failed execute phase. **Environment action:** unguarded through the execute phase and not fair, because production aborts on any prepare failure past its retries or on the caller going away, which the model does not otherwise represent. | Yes: `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. |
| `SagaTerminal(s)` | One shard of the terminal broadcast | `AtomicWriteGrain.MarkOneShardAsync` into `ShardRootGrain.AppendTxTerminalAsync` (a direct terminal passes a resize fence, #4369, and is mirrored), the leaf applying it through `MigrationTerminalCore.DecideBucketAction` (`DiscardOrphan` when the activation already applied it). The committed-values backstop is `TermRow`: a key the terminal finds no bucket for is installed last-writer-wins at the saga's own stamp. A terminal the copy an undo discarded refuses counts as delivered (`AtomicWriteGrain`'s discarded-copy check through `ITreeDeletionGrain.IsDiscardedAsync`, #4474); one a purged old copy refuses is redelivered to the copy it mirrored into, following that copy's layout (`PurgedCopyTerminalTargets.Resolve`, #4475); and a mirrored terminal also reaches the resized copy's split closure without committed values (`TermClosure`, #4478). The coordinator reads every key's original prepare stamp back before it decides and carries it to the copy that minted it, and the mirrored terminal carries it to the resized copy (#4522, fixed by #4610 and #4629). | Yes: `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_addressed_to_the_fenced_copy_directly_is_applied_and_forwarded`, `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_routed_through_the_alias_after_the_fence_is_rejected_and_not_forwarded` and `BPlusLeafGrainTests.ApplyTxTerminalAsync_with_already_terminalled_txid_discards_orphan_pending_bucket`, `AtomicWriteGrainTests.MarkOneShardAsync_counts_a_deleted_tree_refusal_by_a_discarded_copy_as_delivered`, `AtomicWriteGrainTests.MarkOneShardAsync_never_follows_a_stale_tree_refusal_by_a_discarded_copy`, `AtomicWriteGrainTests.MarkOneShardAsync_redelivers_a_terminal_a_purged_copy_refuses_to_the_resized_copy`, `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_of_a_rejecting_copy_reaches_every_shard_the_resized_copys_splits_lead_to`, `BPlusLeafGrainTests.A_backstop_carrying_an_original_stamp_keeps_a_later_write` and `OriginalPrepareStampIntegrationTests.A_committed_saga_never_overwrites_a_later_write_imported_as_a_migration`, `AtomicWriteGrainTests.A_commit_carries_each_backstop_key_original_prepare_stamp_to_the_copy_that_minted_it`, `AtomicWriteGrainTests.A_key_no_pass_finds_fails_the_batch_and_the_saga_never_commits_without_its_stamps`, `AtomicWriteGrainTests.A_read_back_that_faults_fails_the_batch_and_the_saga_never_commits_without_its_stamps`, `AtomicWriteGrainTests.A_key_the_fast_pass_misses_is_found_by_the_exhaustive_pass_and_carried`, `AtomicWriteGrainTests.The_lineage_guard_carries_stamps_only_to_the_copy_that_minted_them`, `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_mirrors_a_terminal_to_the_resize_destination_with_its_original_stamps` and `AtomicWriteBackstopOriginalStampIntegrationTests.A_backstop_after_a_post_decision_leaf_split_never_overwrites_a_later_write`. |
| `SagaComplete` | The broadcast finished; the caller is acknowledged | `AtomicWriteGrain.CompleteSagaAsync` after the broadcast has reached every touched shard. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `LaterWrite(p)` | A client write of `k2` through a routing activation | A routed write through `LatticeGrain` into a shard, mirrored by `ShardRootGrain.ForwardShadowAsync` while the shard forwards; a mirror the resized copy refuses because a split of it moved the slot is re-sent to the shard the refusal names (`ShadowForwardRefusal.NextShard`, `RTarget`, #4478), and the write fails once the hops run out. Its stamp is `WVal`: above the saga's P when its leaf has seen P (property H), and every forward carries it: the resize mirror forwards the old copy's stored row at its own stamp through `ShardRootGrain.MergeManyAsync`, refused for a slot a split of the resized copy moved and forwarded on to that split's destination (#4522, fixed by #4629); the split's shadow-forward is a migration import, flagged migrated. **Environment action:** any pair the registry ever published may carry it, including one naming a purged copy. | Yes: `ShardRootGrainShadowForwardTests.SetAsync_forwards_during_draining`, `ShardRootGrainShadowForwardTests.SetManyAsync_forwards_full_batch_in_single_call_to_destination` and `LatticeGrainTests.SetAsync_retries_on_stale_alias`, `ShardRootGrainShadowForwardTests.SetAsync_mirror_refused_for_a_moved_slot_is_resent_to_the_shard_that_owns_it_now` and `ShardRootGrainShadowForwardTests.SetAsync_fails_once_the_mirror_is_still_refused_after_the_last_hop`, `SplitMigrationImportAfterSagaIntegrationTests.A_write_acknowledged_after_the_sweep_resolved_a_decided_saga_survives_the_split` (the split's shadow-forward of the write, #4564)., `ResizeMirrorOriginalStampIntegrationTests.A_plain_write_is_mirrored_at_the_source_copys_own_stamp`, `ShardRootGrainSplitShadowForwardTests.A_merge_for_a_moved_slot_is_refused_during_the_reject_phase` and `ShardRootGrainSplitShadowForwardTests.A_merge_for_a_moved_slot_is_forwarded_to_the_split_destination_during_the_drain`. |
| `Stutter` | Quiescence | Not a protocol step: a stuttering successor once nothing is in flight. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `UniqueOwner` | Every routing pair a `LatticeGrain` activation may hold is either refused for a key (split Reject, resize `Rejecting`, a retained redirect) or reaches that key's one owner. The fences exist for exactly this (#4362, #4357, #4453), and the split/resize hold keeps a split target the resize never fences from existing (#4452). | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`, `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map`, `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`, `TreeShardSplitGrainTests.SplitAsync_refuses_while_a_resize_of_the_tree_is_in_flight` , `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole` and `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole_when_the_logical_row_persists_a_map`. |
| `NoKeyLost` | The owner's location holds every value acknowledged to a writer, so the final drain, the snapshot and the mirror carry every acknowledged write across a split or a flip. | Yes: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip`, `ShardRootGrainShadowForwardTests.SetManyAsync_forwards_full_batch_in_single_call_to_destination` and `TreeResizeGrainTests.InitiateResize_refuses_while_a_shard_split_is_in_flight_and_starts_no_snapshot`, `OriginalPrepareStampIntegrationTests.A_committed_saga_never_overwrites_a_later_write_imported_as_a_migration` and `SplitMigrationImportAfterSagaIntegrationTests.A_write_acknowledged_after_the_sweep_resolved_a_decided_saga_survives_the_split`., `AtomicWriteBackstopOriginalStampIntegrationTests.A_backstop_after_a_post_decision_leaf_split_never_overwrites_a_later_write` and `ResizeMirrorOriginalStampIntegrationTests.A_mirrored_prepare_is_bucketed_on_the_destination_at_its_original_stamp`. |
| `NoResurrection` | No served read returns a value older than one already acknowledged: no stale old copy after a flip (#4362) and no stale resized copy after an undo (#4453). The late-orphan form (#4445) is the companion module's. | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias` and `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it` and `PurgedCopyStaleRoutingIntegrationTests.A_stale_router_is_refused_by_a_purged_first_resize_copy`. |
| `SagaBatchOnOneCopy` | Once committed, a saga holds prepared buckets only on its bound copy and the copy that copy mirrors into (#4357, #4369). It constrains which copies hold buckets, not the shard a bucket lands on, so it cannot see the routing tier ignoring the binding on its own (#4358): that is caught by `AtomicOnOwner` (mutation `AtomicOnOwnerRouterIgnoresBinding`), and this property sees it only together with a flip that does not wait for the fence. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`, `AtomicWriteGrainTests.ExecuteAsync_stays_bound_after_a_refusal_when_its_bound_copy_mirrors_into_the_new_copy`, `LatticeGrainTests.SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy` and `LatticeGrainTests.SetManyAsync_under_a_saga_binding_places_the_batch_on_a_bound_copy_that_mirrors_into_the_resolved_one`. |
| `AtomicOnOwner` | A fresh reader sees a saga's batch on every key or on none. It is the property that catches the routing tier ignoring the binding (#4358, mutation `AtomicOnOwnerRouterIgnoresBinding`). | Yes: `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole`, `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole_when_the_logical_row_persists_a_map`, `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on` and `TreeSnapshotGrainTests.Online_shadow_begin_carries_an_in_flight_prepared_bucket_onto_the_destination_shard`. A fix for #4474 that followed the discarded copy's refusal to the old copy would land part of a batch there (`AtomicOnOwnerDiscardedCopyTerminalRedirects`). |
| `OwnerMonotonic` | The value a fresh reader gets never moves backwards, except across an undo's swap, which discards the resized copy's writes by contract. Stated over the history the ghost `vis` records. | Yes: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight`, `BPlusLeafGrainTests.Materialiser_replays_prepared_set_into_pending_tx` and `TreeSnapshotGrainTests.Online_shadow_begin_applies_the_terminal_of_a_saga_decided_before_the_sweep`. |
| `SplitCompletes` | A split that opened its window finishes, under weakly fair coordinator steps. | Yes: `TreeShardSplitGrainTests.RunSplitPass_resumes_from_BeginShadowWrite_after_crash` and `TreeShardSplitGrainTests.ProcessNextPhase_drives_the_shadow_write_phase_through_the_full_split_pass`. |
| `ReshardCompletes` | A reshard that started reaches its target. | Yes: `TreeReshardGrainTests.RunReshardPass_drives_a_Planning_reshard_through_to_completion`. |
| `ResizeCompletes` | A resize that started is purged or undone. | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias` and `TreeResizeGrainTests.Cleanup_soft_deletes_a_later_resizes_old_physical_tree`. |
| `SagaCompletes` | A saga that started completes. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`, `ResizeUndoBoundSagaTerminalIntegrationTests.A_saga_bound_to_the_resized_copy_is_discarded_whole_when_the_resize_is_undone_mid_broadcast` (a copy an undo discarded, #4474), `AtomicWriteGrainTests.MarkOneShardAsync_redelivers_a_terminal_a_purged_copy_refuses_to_the_resized_copy` and `AtomicWriteGrainTests.MarkOneShardAsync_redelivers_on_a_tombstoned_copy_refusal_without_reading_its_deletion_record` (a purged copy, #4475). |
| `RoutingConverges` | Eventually the registry's own pair serves every key: no fence, Reject or redirect outlives the operation that set it, so a refreshed router stops being refused. | Yes: `LatticeGrainTests.GetAsync_retries_on_stale_alias`, `TreeResizeGrainTests.UndoResize_after_swap_releases_a_split_allocated_shard` and `TreeShardSplitGrainTests.InitiateSplit_backs_out_when_a_resize_is_in_flight_once_the_source_record_is_open`. An undo can no longer restore a map that predates a split, because no split commits while a resize can still be undone (#4452). |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Over-approximation arguments

Every environment action, and every guard the base keeps weaker than
production, is argued in its row above. The general rule the module follows:
a guard may be weaker than production's (the model then explores behaviours
production cannot reach, so a clean result covers production), never stronger,
except where a row names an intended design that production lacks (the table
at the top). The module once kept one timing assumption, that a router idle
for longer than `SoftDeleteDuration` had been collected, and pruned the old
copy's pairs at the purge; review #4435 showed it false (`SoftDeleteDuration`
may be zero, and nothing bounds a routing activation's lifetime), so the pairs
now stay published and the purged copy's behaviour is a row of the table.

These modelling choices are worth stating because they are easy to misread:

- **Routers.** A routing activation is not a variable. Any pair the registry
  ever published may be used by any call, at any time, which subsumes any
  number of activations and any cache age. That is why the routing tier's
  publish rule (`RoutingPairPublishGate`) has no action here: the spec assumes
  only that a router never holds a pair the registry did not publish, and
  `RoutingPairPublishModel` (Coyote) checks the rule that makes that true,
  including that a pair already invalidated is never published again;
  `LatticeGrainTests.GetRoutingAsync_does_not_publish_a_pair_read_before_an_invalidation`
  pins that `LatticeGrain` feeds the rule the epoch it captured when the
  resolve started.
- **Read precedence and stamps.** A surfaced bucket is last-writer-wins merged
  with the row at the bucket's stamp (`BVal`), and a later write is stamped
  above the saga's P exactly when its leaf's clock has seen P (`KnowsP`,
  `WVal`), which is property H. #4566's read gate supersedes a marked prepare
  iff the row's stamp is at or above P, with no migrated test, which is this
  gate.
- **A read the split source refuses.** During a split's freeze the source
  refuses the moved slot, and the reader retries once the map has moved, so it
  sees the destination after the final drain. `OwnerValue` reads it that way,
  which matters because the mirror's chase (#4478) can land a write on the
  destination before the map moves.
- **The registry always answers.** `RegistryView` is the recorded decision. A
  registry that declines to answer, or has retired the row, is strictly more
  behaviour, and the companion module checks it.

## Coyote models of the cores

| Model | Core it drives | Spec construct it refines | Guards (each must be found) |
|-------|----------------|---------------------------|-----------------------------|
| `ResizeFenceModel` | `ResizeFence` | `ResizeFence`, `ResizeFlip`, `ResizeFlipRefused`, the fenced half of `SagaPrepare` | the fence refusing the bound saga (pre-#4376), lifting after a landed flip, flipping before fencing (#4362), and the fence admitting a stale routed call (`ResizeFence.AdmitsBoundSaga` over-admitting). The stale router's probe goes through `ResizeFence.AdmitsBoundSaga` as an unbound or foreign-bound call, so the core's refusal arm is what the stale-read assertion checks, not the model's own reading of the fence |
| `SagaCopyBindingModel` | `SagaCopyBinding` | `SagaPrepare`'s routing half, `SagaRebindOnRefusal`, `SagaRebindBeforeDecision`, `SagaDecide`, and `SagaBatchOnOneCopy` | the router ignoring the binding (#4358), the pre-decision check ignoring the mirror (#4369), the pre-decision check always staying bound (a saga deciding on the copy an undo discards, caught by the bound-copy-live assertion), and a refusal that never re-binds (caught by the bounded-progress assertion), and the mid-dispatch re-bind ignoring the mirror (#4454 before #4521) |
| `RoutingPairPublishModel` | `RoutingPairPublishGate` | the assumption behind `published` | no version check, no epoch check |

Every guard also has a specificity test that disables exactly the assertion
it targets and requires a clean run. Each model checks its core, not the grain
that calls it; where the grain's composition of the core matters, a grain-level
test pins it: `LatticeGrainTests.GetRoutingAsync_does_not_publish_a_pair_read_before_an_invalidation`
for the publish gate's call site, and the tests named in the `SagaPrepare`,
`SagaRebindOnRefusal` and `SagaRebindBeforeDecision` rows for the binding.

## Deliberate abstraction gaps

What the module does **not** cover, stated so that coverage of one part is
not read as coverage of another:

- **Registry retention and leaf reactivation.** This module's registry always
  reports the decision and its leaves never lose memory. Both are the
  companion module's, which keeps the split, the resize and the undo they act
  through but not the reshard, the refused flip, the undo before a flip, the
  re-binds or stale writers. Their composition was checked once and is clean,
  all of this module's properties included: 497,105 distinct states at depth
  29, 9 min 54 s on two workers, which is the measured cost of composing the
  two modules and why it is not a CI gate (see the README's account of the
  seam).
- **Alias cutovers other than a resize.** A shadow-cutover restore and its
  revert move the alias against a bound saga, and the companion module
  `ShardOwnershipCutover` checks them
  ([`RefinementCutover.md`](RefinementCutover.md)). There the copy the saga
  leaves mirrors nowhere, so the saga re-binds and discards what it left behind
  before it decides (#4689, fixed). An explicit `SetTreeAliasAsync` and schema remediation
  also move the alias (#4357 fixed all three), with no shadow to revert to, and
  are not modelled. With them goes the split's abandon path for those moves. The
  abandon an undo of a resize reaches is modelled (`SplitAbandon`, #4478), and
  for a cutover it is pinned by
  `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map`.
- **Consolidation (a shrinking reshard).** `TreeShardConsolidationGrain` folds
  shards together through the same shadow-write window; the reshard here only
  grows. The #4452 hold applies to it too
  (`TreeShardConsolidationGrainTests.StartAsync_refuses_while_a_completed_resize_still_has_the_replaced_copy_mirroring`).
- **The leaf-level moved-away handoff.** The seal a split leaves on the source
  leaves and its inheritance across a leaf split are modelled as one freeze
  step; the existing `SplitPivotAdmissionModel`, `SpanAdmissionMigrationModel`
  and `MovedAwaySealInheritanceModel` cover them at leaf granularity.
- **Undo's failure paths.** A swap-back that fails after the arm is released
  by `TreeResizeGrain.ReleaseUndoRedirectUnlessSwappedAsync`; the model has no
  failing undo action.
- **Unmarked buckets.** A bucket forwarded by an older silo, or one whose P
  could not be read back, carries the destination's clock rather than P and
  keeps today's drain rule (#4522). It can lose a later write a split shadow-
  forward landed as a migrated row below its stamp. That is a named exception
  to `NoKeyLost` for a mixed-version cluster, recorded here and not modelled.
- **A plain write before the prepare.** The base has no write before the
  saga's prepare. One acknowledged on T then, re-minted on R at a clock running
  ahead of P, would beat the saga's marked bucket there and tear the batch
  (`AtomicOnOwner`, checked once by adding that write: depth 10). Since #4629 the
  mirror forwards it at T's stamp, which is clean (123,933 states); the detector
  is `ResizeMirrorOriginalStampIntegrationTests.A_plain_write_is_mirrored_at_the_source_copys_own_stamp`,
  red with the mirror re-minting the write on R.
- **Named exception to H.** An idempotent retry that reuses its issue stamp
  can be stamped below a prepare on the same leaf; `NoKeyLostLaterWriteBelowP`
  shows what that costs. A range delete no longer is one: it is stamped above
  the highest clock of every leaf it covers (#4530, fixed by #4568; detector
  `DeleteRangeAfterSagaDecisionIntegrationTests.Range_delete_acknowledged_after_the_decision_survives_the_terminal_drain`,
  red with the facade's wall-clock stamp restored).
- **More than one of anything.** One split, one reshard, one resize, one saga
  writing two keys, one later write. A second saga contending for a key, a
  second split, and a split of the resized copy during the resize are bounded
  out.
- **Time.** No timers, retention windows or deadlines.

## Territory owned by other open issues

No open issue currently owns a claim here. The section stays, empty of owners,
so a later census can see the question was asked; re-populate it when an open
issue next takes ownership of a claim made here.

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
| A routed operation on a purged old copy is refused, so the caller refreshes its pair (`Gone` in `RoutedRefused`) | A routed read answers as the empty tree and a routed write is accepted, re-seeding the purged copy; a routing activation that cached the old pair across `SoftDeleteDuration` is never told to refresh | #4503 | `NoResurrectionPurgedCopyServesEmpty`, `NoKeyLostPurgedCopyAcceptsWrites` |
| A mid-dispatch re-bind stays bound while the bound copy mirrors into the resolved one, and the routing tier dispatches to it (`SagaRebindOnRefusal`, `SagaPrepare`) | It re-binds unconditionally | #4454 | `SagaBatchOnOneCopyRebindIgnoresMirror` |
| The online snapshot carries prepared buckets (`SnapCopy`) | It copies committed entries only | #4455 | `OwnerMonotonicSnapshotSkipsBuckets` |
| A terminal the copy an undo discarded refuses counts as delivered (`SagaTerminal`); a purged copy is not discarded in this sense | `AtomicWriteGrain.MarkOneShardAsync` strips the routed stamp, so the armed redirect never refuses a direct terminal: the resized copy takes it until the undo discards that copy, and from then on `ShardRootGrain.AppendTxTerminalAsync` throws `InvalidOperationException`, which the broadcast does not follow, so the saga retries forever and never completes. Following the refusal to the old copy instead, the naive fix, lands part of the batch there | #4474 | `SagaCompletesDiscardedCopyRefusesTerminal` (production today); `AtomicOnOwnerDiscardedCopyTerminalRedirects` stands against the naive fix |
| The terminal's committed-values backstop installs a key it finds no bucket for last-writer-wins at the saga's own stamp (`TermRow`) | `BPlusLeafGrain`'s backstop stamps `HybridLogicalClock.Tick` over the row's stamp, above whatever the row holds, so a terminal reaching a split destination after a later write of the moved key overwrites that acknowledged write | #4522 | `NoKeyLostFreshStampBackstop` |
| A terminal a purged old copy refuses is delivered to the copy it mirrored into, following that copy's own layout (`SagaTerminal`, `TermTargets`) | `ShardRootGrain.AppendTxTerminalAsync` on a purged copy throws `InvalidOperationException`, which the broadcast does not follow, so the saga never completes | #4475 | `SagaCompletesPurgedCopyRefusesTerminal` |

Two rows have left this table because their fixes landed, and their mutations
stay as standing regression checks for the behaviour they replaced:

- **The undo's order** (#4453, fixed by #4457). The base's `UndoArm`,
  `UndoSwap` and `UndoClear` are production's order;
  `UniqueOwnerUndoClearsBeforeSwap` reproduces the order it replaced.
- **The split/resize interlock** (#4452, fixed by #4466). A consolidation
  refuses while a resize is in flight, while an undo is pending or running, and
  after the resize completes for as long as any shard of the replaced copy
  still mirrors into the resized one; a split refuses while a resize is in
  flight or an undo is pending or running, and once the resize completes it may
  run while the replaced copy mirrors, because the mirror follows its refusal
  (#4478); a resize refuses while a split is in flight. `ResizeBegin` is the
  base's; the base's `SplitBegin` keeps the stricter until-purge hold until the
  bases move (see the `SplitBegin` row).
  `UniqueOwnerSplitDuringResize` and `NoKeyLostResizeDuringSplit` reproduce
  production before the fix. The extent of the hold is pinned too: the rule
  first proposed refused a split only while a resize was in flight, which loses
  an acknowledged write once the registry retires the saga's row, so its
  standing mutation, `NoKeyLostSplitInSoftDeleteWindow`, lives in the companion
  module, the one that models retirement.

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
| `SplitBegin` | An adaptive split opens its shadow-write window | `TreeShardSplitGrain.SplitAsync` into `TreeShardSplitGrain.InitiateSplitStateAsync` and `ShardRootGrain.BeginSplitAsync`, admitted by `ShardMapCommitFence.Admits`. The split refuses while `ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync` reports a hold, read before the split allocates anything and again once the source's record is open, which closes the race with a resize starting; the hold is `TreeResizeGrain.HoldsShardSplitsAsync`, true while a resize is in flight or an undo is pending, running or not yet persisted, and false once the resize completes, while the replaced copy still mirrors into the resized one (#4478): that mirror follows a split's refusal to the slot's current owner (`ShardRootGrain.ForwardShadowAsync`), and a bound saga's terminal reaches the resized copy's split closure. Consolidations and reshards keep `TreeResizeGrain.HoldsShardMigrationsAsync`, true until no shard of the replaced copy mirrors into the resized one (#4452). A split the reshard drives relies on the reshard's own hold. **Over-approximation:** the guard admits a split whenever the source is unfenced; production also refuses a source mid-migration, and fails closed when the hold cannot be read, which only removes behaviours. **Under-approximation (until the bases move):** the base's `SplitBegin` keeps the until-purge hold, so it does not explore a split of the resized copy while the replaced copy still mirrors, which production now allows (#4478). A one-off scratch variant of both modules checked that behaviour - the refusal chase with hop budgets of one and zero, the terminal to R/s plus its split closure, and `SplitAbandon` on an undo - clean at 86,576 and 124,030 distinct states. It is not a CI gate yet; the bases move to it in a follow-up member PR. | Yes: `ResizeMigrationHoldDetectorIntegrationTests.A_split_is_refused_by_the_resize_hold_while_the_resize_is_in_flight` and `ResizeMigrationHoldDetectorIntegrationTests.A_split_proceeds_while_the_replaced_copy_still_mirrors_because_the_mirror_follows_it` (real grains end to end, red when the split hold is lifted while the resize is in flight or kept until the purge), `ResizeSplitInSoftDeleteWindowIntegrationTests.A_bound_sagas_prepare_follows_a_split_of_the_resized_copy_to_the_slots_owner` and `ResizeSplitInSoftDeleteWindowIntegrationTests.A_bound_sagas_terminal_reaches_the_buckets_a_split_of_the_resized_copy_moved` (the mirror and the bound terminal follow the split), `TreeResizeGrainTests.HoldsShardSplits_is_true_while_an_undo_is_pending`, `TreeResizeGrainTests.HoldsShardSplits_is_false_once_complete_while_the_replaced_copy_still_mirrors`, and the caller tests `TreeShardSplitGrainTests.SplitAsync_refuses_while_a_resize_of_the_tree_is_in_flight`, `TreeShardSplitGrainTests.SplitAsync_proceeds_while_a_completed_resize_still_has_the_replaced_copy_mirroring` and `TreeShardSplitGrainTests.InitiateSplit_backs_out_when_a_resize_is_in_flight_once_the_source_record_is_open`, which stub the hold and so cover only the split's reading of it. The fence's two arms: `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map` (the alias already moved) and `TreeShardSplitGrainTests.Swap_while_a_cutover_has_carried_the_copy_map_but_not_swapped_the_alias_does_not_apply_the_diff` (a cutover carried the map but has not swapped). |
| `SplitSweep` | The retroactive sweep of prepares that predate the window | `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`: an undecided prepare is replayed to the destination, a decided one is resolved there with the committed-values backstop. Modelled as one step; its non-atomic window (a decision landing between the pre-check and the replay) and the post-sweep cleanup that closes it are the companion module's late forward. | Yes: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` and `TreeShardSplitGrainTests.RetroactiveSweep_skips_replay_and_applies_commit_terminal_when_saga_already_committed`. |
| `SplitFreeze` | The source refuses the moved slot | `TreeShardSplitGrain.SwapAsync`: `MarkLeavesMovedAwayAsync`, then `EnterRejectPhaseAsync`, before the map moves. | Yes: `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map` and `TreeShardSplitGrainTests.Swap_calls_source_enter_reject_phase_exactly_once`. |
| `SplitCommit` | Final drain, then the map moves | `TreeShardSplitGrain.SwapAsync`: the authoritative final drain (`ForwardMovedSlotEntriesAtomicallyAsync`), then the fenced `ILatticeRegistry.ReassignSlotsAsync`, then `TreeShardSplitGrain.FinaliseAsync`. The background drain is omitted: it is last-writer-wins and dominated by the final drain, and no router can reach the destination before the map moves. | Yes: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip`. |
| `ReshardStart` | A reshard begins | `TreeReshardGrain.ReshardAsync`, refused while `ShardMigrationResizeInterlock.ReadResizeHoldAsync` reports a hold (a resize in flight, an undo pending or running, or a replaced copy still mirroring), as the adaptive split is (#4452). | Yes: `ResizeMigrationHoldDetectorIntegrationTests.A_reshard_is_refused_by_the_resize_hold_while_the_replaced_copy_still_mirrors` (real grains, red when the hold stops holding), `TreeReshardGrainTests.ReshardAsync_is_refused_as_resize_undoable_while_a_completed_resize_holds_migrations` (red when the reshard stops reading the completed resize's hold) and `TreeReshardGrainTests.ReshardAsync_refused_while_resize_in_flight`. A consolidation reads the same hold: `ResizeMigrationHoldDetectorIntegrationTests.A_consolidation_is_refused_by_the_resize_hold_while_the_resize_is_in_flight` and `ResizeMigrationHoldDetectorIntegrationTests.A_consolidation_is_refused_by_the_resize_hold_while_the_replaced_copy_still_mirrors`. |
| `ReshardFinish` | The reshard reaches its target | `TreeReshardGrain` advancing to `ReshardPhase.Complete` once the map names the target shard count. | Yes: `TreeReshardGrainTests.Migrate_advances_to_Complete_once_the_target_shard_count_is_reached` and `TreeReshardGrainTests.RunReshardPass_drives_a_Planning_reshard_through_to_completion`. |
| `ResizeBegin` | A resize captures its shard set and starts the snapshot | `TreeResizeGrain.ResizeCoreAsync` (refused while a reshard is in flight) and `TreeResizeGrain.InitiateResizeStateAsync`, whose shard set is `RoutedShardIndices.Resolve` over the logical map. It refuses while a split is in flight, reading every routed shard's migration record through `ShardMigrationResizeInterlock.FindMigratingShardAsync` after its own intent is persisted (#4452). | Yes: `TreeResizeGrainTests.InitiateResize_refuses_while_a_shard_split_is_in_flight_and_starts_no_snapshot`, `TreeResizeGrainTests.ResizeAsync_refused_while_reshard_in_flight` and `RoutedShardIndicesTests.Resolve_adds_a_shard_a_split_allocated_above_the_pinned_count`. |
| `SnapCopy` | The online snapshot copies the old copy index-for-index | `TreeSnapshotGrain`'s online drain over `RoutedShardIndices.OrContiguous`, keeping an entry only on the shard the copy's map routes it to. Carrying prepared buckets is the intended design (#4455). | Partial: `TreeSnapshotGrainTests.BeginShadowForward_covers_a_shard_a_split_allocated_above_the_pinned_count`. Prepared buckets are not copied in production (#4455). |
| `ResizeFence(s)` | One old shard enters Rejecting before the flip | `TreeResizeGrain.SwapAliasAsync` calling `ShardRootGrain.EnterRejectingAsync` on every shard of `TreeResizeState.ShardIndices` (#4362). | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`. |
| `ResizeFlip` | The alias and map move to the resized copy in one write | `ILatticeRegistry.SwapAliasAsync` from `TreeResizeGrain.SwapAliasAsync`, only after every fence landed. | Yes: `TreeResizeGrainTests.SwapAlias_does_not_move_the_alias_when_an_old_shard_cannot_be_fenced`, `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row` and `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_replaces_a_persisted_map_on_the_logical_row` (the swap replaces a map the logical row already persists, rather than keeping its slots under a new version). |
| `ResizeFlipRefused` | A refused or failed flip lifts the fence unless the alias moved | `TreeResizeGrain.LiftFenceUnlessSwappedAsync` deciding through `ResizeFence.LiftsFenceAfterFailedFlip`, then `ShardRootGrain.ExitRejectingAsync`. **Over-approximation:** budgeted to one refusal so the resize is not refused forever; production retries on every tick, so each refusal it makes is one the model makes. | Yes: `TreeResizeGrainTests.SwapAlias_lifts_the_fence_when_the_alias_cannot_move` and `TreeResizeGrainTests.SwapAlias_keeps_the_fence_when_a_failed_flip_reached_the_registry`. |
| `ResizeRetire` | Reject, then soft-delete the old copy | `TreeResizeGrain.RejectOldShardsAsync` and `TreeResizeGrain.CleanupOldTreeAsync`. The old copy stays fenced and admits the saga bound to it. | Yes: `TreeResizeGrainTests.RejectOldShards_rejects_a_shard_a_split_allocated_above_the_pinned_count` and `TreeResizeGrainTests.Cleanup_soft_deletes_a_later_resizes_old_physical_tree`. |
| `ResizePurge` | The purge after `SoftDeleteDuration` clears the old copy | `ShardRootGrain.PurgeAsync` driven by the tree-deletion grain. Pairs naming the old copy stay published: nothing bounds a routing activation's lifetime below `SoftDeleteDuration`, which may be zero. That a routed operation on the purged copy is then refused is the intended design (#4503). | Partial: `ShardRootGrainPurgeTests.PurgeAsync_clears_the_single_root_leaf_when_tree_is_flat`. A routed read on the purged copy answers empty and a routed write is accepted until #4503's fix lands. |
| `UndoBeforeFlip` | Undo during the snapshot | `TreeResizeGrain.UndoResizeCoreAsync`'s before-swap branch: abort the snapshot, `ShardRootGrain.ClearShadowForwardAsync` on every old shard, discard the destination. | Yes: `TreeResizeGrainTests.UndoResize_at_snapshot_phase_discards_destination_without_recovering` and `TreeResizeGrainTests.UndoResize_during_drain_releases_a_split_allocated_shard`. |
| `UndoArm` | The resized copy is armed to redirect, before the swap | `AliasCutoverShardMaps.ArmRedirectsAsync` from `TreeResizeGrain.UndoResizeCoreAsync`, before the swap and again from the swap's own read (#4453). | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it` pins the order (arm before the swap); `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_to_redirect_onto_the_old_tree` pins only that the arm happens and its target, and stays green when the arm moves after the swap. |
| `UndoSwap` | The alias and the old map move back in one write | `ILatticeRegistry.SwapAliasAsync` from `TreeResizeGrain.UndoResizeCoreAsync`, with `TreeResizeState.OldRegistryEntry`'s map. | Yes: `TreeResizeGrainTests.UndoResize_recovers_old_tree_and_removes_alias`. |
| `UndoClear` | The old copy's fence lifts, after the swap | `ShardRootGrain.ClearShadowForwardAsync` on every old shard, after the old copy is recovered and the alias names it (#4453). | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it` and `TreeResizeGrainTests.UndoResize_after_swap_releases_a_split_allocated_shard`. |
| `SagaStart` | The saga binds to the copy the tree resolves to | `AtomicWriteGrain` binding `AtomicWriteState.BoundPhysicalTreeId` when it prepares, and `AtomicWriteGrain.BindUnboundSagaAsync` for a saga resumed from older state. | Yes: `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on` and `AtomicWriteGrainTests.ReceiveReminder_binds_a_saga_resumed_from_state_without_a_binding_before_it_dispatches`. |
| `SagaPrepare(k, p)` | One key's prepared write through a routing activation | `LatticeGrain.SetManyAsyncCore` deciding through `SagaCopyBinding.AdmitsDispatch` (cached pair, then one re-read) and handing the binding to the bound copy's shards, which admit it through a resize fence via `ShardRootGrain.AdmitsBoundSagaWhileFenced` and `ResizeFence.AdmitsBoundSaga` (#4358, #4369). Dispatching to the bound copy while it mirrors into the resolved one is the intended design (#4454). **Over-approximation:** one key per step, where production places a whole slice in one call; the extra partial interleavings stand for a transient shard failure part way through a call. | Partial: `LatticeGrainTests.SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy`, `LatticeGrainTests.SetManyAsync_under_a_saga_binding_rereads_a_pair_cached_before_a_swap`, `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_to_the_fenced_copy_is_applied_and_forwarded` and `ShardRootGrainShadowForwardTests.SetManyAsync_prepared_by_a_saga_bound_elsewhere_is_rejected_by_the_fenced_copy`. The routing tier refuses rather than dispatches while the bound copy mirrors (#4454). |
| `SagaRebindOnRefusal` | The saga re-binds after a mid-dispatch refusal | `AtomicWriteGrain.TryRebindToResolvedCopyAsync`, entered when `SagaCopyBinding.RebindsAfterRefusal` holds. Staying bound while the bound copy mirrors is the intended design (#4454). | Partial: `AtomicWriteGrainTests.ExecuteAsync_rebinds_and_commits_on_the_new_copy_when_its_bound_copy_moved_during_dispatch`. Production re-binds without the mirror check (#4454). |
| `SagaRebindBeforeDecision` | The pre-decision check re-binds | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` when `SagaCopyBinding.BeforeDecision` answers `Rebind`. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaDecide` | The commit decision is recorded | `AtomicWriteGrain.RebindAcrossAliasSwapAsync` answering `Commit` or `StayBound`, then the registry decision. | Yes: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy`. |
| `SagaAbort` | The abort decision is recorded; the broadcast compensates | `AtomicWriteGrain.BroadcastTerminalsAsync` with an abort after a failed execute phase. **Environment action:** unguarded through the execute phase and not fair, because production aborts on any prepare failure past its retries or on the caller going away, which the model does not otherwise represent. | Yes: `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. |
| `SagaTerminal(s)` | One shard of the terminal broadcast | `AtomicWriteGrain.MarkOneShardAsync` into `ShardRootGrain.AppendTxTerminalAsync` (a direct terminal passes a resize fence, #4369, and is mirrored), the leaf applying it through `MigrationTerminalCore.DecideBucketAction` (`DiscardOrphan` when the activation already applied it). The committed-values backstop is `TermRow`: a key the terminal finds no bucket for is installed last-writer-wins at the saga's own stamp. Counting a terminal the discarded copy refuses as delivered (#4474), delivering one a purged copy refuses to the copy it mirrored into (#4475), and stamping the backstop at the saga's stamp (#4522) are the intended design. | Partial: `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_addressed_to_the_fenced_copy_directly_is_applied_and_forwarded`, `ShardRootGrainShadowForwardTests.AppendTxTerminalAsync_routed_through_the_alias_after_the_fence_is_rejected_and_not_forwarded` and `BPlusLeafGrainTests.ApplyTxTerminalAsync_with_already_terminalled_txid_discards_orphan_pending_bucket`. Production fails the broadcast on a copy the undo discarded (#4474) and on a purged copy (#4475), and stamps the backstop above the row (#4522). |
| `SagaComplete` | The broadcast finished; the caller is acknowledged | `AtomicWriteGrain.CompleteSagaAsync` after the broadcast has reached every touched shard. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `LaterWrite(p)` | A client write of `k2` through a routing activation | A routed write through `LatticeGrain` into a shard, mirrored by `ShardRootGrain.ForwardShadowAsync` while the shard forwards. **Environment action:** any pair the registry ever published may carry it, including one naming a purged copy. | Partial: `ShardRootGrainShadowForwardTests.SetAsync_forwards_during_draining`, `ShardRootGrainShadowForwardTests.SetManyAsync_forwards_full_batch_in_single_call_to_destination` and `LatticeGrainTests.SetAsync_retries_on_stale_alias`. A write through a pair naming the purged copy is accepted and lost until #4503's fix lands. |
| `Stutter` | Quiescence | Not a protocol step: a stuttering successor once nothing is in flight. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `UniqueOwner` | Every routing pair a `LatticeGrain` activation may hold is either refused for a key (split Reject, resize `Rejecting`, a retained redirect) or reaches that key's one owner. The fences exist for exactly this (#4362, #4357, #4453), and the split/resize hold keeps a split target the resize never fences from existing (#4452). | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`, `TreeShardSplitGrainTests.Swap_enters_reject_phase_before_setting_shard_map`, `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`, `TreeShardSplitGrainTests.SplitAsync_refuses_while_a_resize_of_the_tree_is_in_flight` , `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole` and `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole_when_the_logical_row_persists_a_map`. |
| `NoKeyLost` | The owner's location holds every value acknowledged to a writer, so the final drain, the snapshot and the mirror carry every acknowledged write across a split or a flip. | Partial: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip`, `ShardRootGrainShadowForwardTests.SetManyAsync_forwards_full_batch_in_single_call_to_destination` and `TreeResizeGrainTests.InitiateResize_refuses_while_a_shard_split_is_in_flight_and_starts_no_snapshot`. A write through a pair naming the purged old copy is acknowledged and lost (#4503), and a backstop stamped above the row overwrites a later write of a moved key (#4522). |
| `NoResurrection` | No served read returns a value older than one already acknowledged: no stale old copy after a flip (#4362) and no stale resized copy after an undo (#4453). The late-orphan form (#4445) is the companion module's. | Partial: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias` and `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`. A read through a pair naming the purged old copy answers empty (#4503). |
| `SagaBatchOnOneCopy` | Once committed, a saga holds prepared buckets only on its bound copy and the copy that copy mirrors into (#4357, #4369). It constrains which copies hold buckets, not the shard a bucket lands on, so it cannot see the routing tier ignoring the binding on its own (#4358): that is caught by `AtomicOnOwner` (mutation `AtomicOnOwnerRouterIgnoresBinding`), and this property sees it only together with a flip that does not wait for the fence. | Partial: `AtomicWriteGrainTests.ExecuteAsync_stays_bound_across_a_move_only_when_its_bound_copy_mirrors_into_the_new_copy` and `LatticeGrainTests.SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy`. The mid-dispatch re-bind is open (#4454). |
| `AtomicOnOwner` | A fresh reader sees a saga's batch on every key or on none. It is the property that catches the routing tier ignoring the binding (#4358, mutation `AtomicOnOwnerRouterIgnoresBinding`). | Partial: `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole`, `AliasSwapRoutingAtomicityIntegrationTests.A_warm_multi_get_after_a_swap_reads_the_new_copy_whole_when_the_logical_row_persists_a_map` and `AtomicWriteGrainTests.ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on`. A batch decided before a flip reads torn on the resized copy (#4455). A fix for #4474 that followed the discarded copy's refusal to the old copy would land part of a batch there (`AtomicOnOwnerDiscardedCopyTerminalRedirects`). |
| `OwnerMonotonic` | The value a fresh reader gets never moves backwards, except across an undo's swap, which discards the resized copy's writes by contract. Stated over the history the ghost `vis` records. | Partial: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` and `BPlusLeafGrainTests.Materialiser_replays_prepared_set_into_pending_tx`. A commit decided before a flip reverts on the resized copy (#4455). |
| `SplitCompletes` | A split that opened its window finishes, under weakly fair coordinator steps. | Yes: `TreeShardSplitGrainTests.RunSplitPass_resumes_from_BeginShadowWrite_after_crash` and `TreeShardSplitGrainTests.ProcessNextPhase_drives_the_shadow_write_phase_through_the_full_split_pass`. |
| `ReshardCompletes` | A reshard that started reaches its target. | Yes: `TreeReshardGrainTests.RunReshardPass_drives_a_Planning_reshard_through_to_completion`. |
| `ResizeCompletes` | A resize that started is purged or undone. | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias` and `TreeResizeGrainTests.Cleanup_soft_deletes_a_later_resizes_old_physical_tree`. |
| `SagaCompletes` | A saga that started completes. | Partial: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. A saga bound to an old copy that is purged before its broadcast finishes never completes (#4475), nor does one bound to a resized copy an undo discards before its broadcast finishes (#4474). |
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

Three modelling choices are worth stating because they are easy to misread:

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
| `ResizeFenceModel` | `ResizeFence` | `ResizeFence`, `ResizeFlip`, `ResizeFlipRefused`, the fenced half of `SagaPrepare` | the fence refusing the bound saga (pre-#4376), lifting after a landed flip, flipping before fencing (#4362), and the fence admitting a stale routed call (`ResizeFence.AdmitsBoundSaga` over-admitting). The stale router's probe goes through `ResizeFence.AdmitsBoundSaga` as an unbound or foreign-bound call, so the core's refusal arm is what the stale-read assertion checks, not the model's own reading of the fence |
| `SagaCopyBindingModel` | `SagaCopyBinding` | `SagaPrepare`'s routing half, `SagaRebindOnRefusal`, `SagaRebindBeforeDecision`, `SagaDecide`, and `SagaBatchOnOneCopy` | the router ignoring the binding (#4358), the pre-decision check ignoring the mirror (#4369), the pre-decision check always staying bound (a saga deciding on the copy an undo discards, caught by the bound-copy-live assertion), and a refusal that never re-binds (caught by the bounded-progress assertion); plus a characterisation of #4454 that must flip when it is fixed |
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
  revert, an explicit `SetTreeAliasAsync`, and schema remediation all move the
  alias too (#4357 fixed all of them); none is modelled. With them goes the
  split's abandon path (`TreeShardSplitGrain.AbandonRetargetedSplitAsync`),
  which only an alias move can reach and which the base's interlock makes
  unreachable; mutations that break the interlock stand in for it by letting a
  stranded split count as finished. This is blindly inexpressible: production
  can do it and the spec cannot.
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
- **Migrated-row supersession**, as above: production can serve an older
  bucket over a newer migrated row in cases the model cannot express.
- **More than one of anything.** One split, one reshard, one resize, one saga
  writing two keys, one later write. A second saga contending for a key, a
  second split, and a split of the resized copy during the resize are bounded
  out.
- **Time.** No timers, retention windows or deadlines.

## Territory owned by other open issues

| Issue | Claim it owns |
|-------|---------------|
| #4454 | The mid-dispatch re-bind (`SagaRebindOnRefusal`, `SagaPrepare`, `SagaBatchOnOneCopy`). |
| #4455 | Prepared buckets in the online snapshot (`SnapCopy`, `AtomicOnOwner`, `OwnerMonotonic`). |
| #4474 | A terminal the copy an undo discarded refuses (`SagaTerminal`, `SagaCompletes`, and `AtomicOnOwner` against a fix that follows the refusal). |
| #4475 | A terminal a purged old copy refuses (`SagaTerminal`, `SagaCompletes`). |
| #4503 | A routed operation on a purged old copy (`ResizePurge`, `LaterWrite`, `NoKeyLost`, `NoResurrection`). |
| #4522 | The stamp of the terminal's committed-values backstop (`SagaTerminal`, `NoKeyLost`). |

When one of these lands, its rows move from `Partial` to `Yes` with the fix's
regression test named, after that test is shown red against the mutation that
reproduces the defect.

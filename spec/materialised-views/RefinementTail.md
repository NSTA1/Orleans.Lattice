# MaterialisedViewTail refinement

This is a documented abstraction, not a machine-checked refinement proof.
The model checks one view's tail and lifecycle state across the operations;
the detectors exercise the specific production seams, not the complete model
in one fixture.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `logs`, `trim` | Each physical copy's WAL and its GC trim point | `ILatticeWalGc.RunOnceAsync`, `LatticeWalGc.RunOnceAsync`. |
| `alias`, `snap`, `swapped` | The physical copy the logical source names | `ILatticeRegistry.SwapAliasAsync`. |
| `bound` | The physical copy the view tails | `ViewMaintainerGrain.ResolveSourcePhysicalAsync`, `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`. |
| `vval`, `active`, `gens` | Served generation tree and generations awaiting reclaim | `ViewMaintainerGrain.SwapToShadowAsync`, `ViewMaintainerGrain.TryReclaimPendingGenerationAsync`. |
| `applied`, `ckpt` | In-memory drain progress and the durable resume checkpoint | `ViewMaintainerGrain.DrainAsync`. |
| `hw`, `rbHw` | The log prefix the served (or scanned shadow) view already reflects; replay below it re-applies entries | Modelling device for at-least-once replay; production keeps no high-water mark, which is why replay transiently regresses. |
| `readReg`, `pubTree`, `pubPos` | Offset-consumer registration and published read position | `ViewMaintainerGrain.EnsureReadRegisteredAsync`, `ViewMaintainerGrain.PublishReadPositions`, `IWalOffsetConsumer.GetDurableReadPositionsAsync`. |
| `pinned`, `pin` | GC cursor pin per physical copy | `IWalCursorRegistry.ReportCursorAsync`, `LatticeWalGc.ReadDurablePinOffsetsAsync`. |
| `rb`, `rbTree`, `rbFloor`, `rbVal` | Shadow rebuild phase, target, resume floor and scanned state | `ViewMaintainerGrain.BuildShadowAsync`, `ViewMaintainerGrain.CaptureResumeFloorsAsync`. |
| `shadow` | Physical shadow generation tree holding rebuild rows | `ViewMaintainerGrain.BuildShadowAsync`, `ViewMaintainerGrain.DeleteBackingGenerationsAsync`. |
| `deleted` | Decommissioned view | `ViewMaintainerGrain.DecommissionAsync`. |
| `written`, `ttlUsed`, `driftUsed`, `drifted`, `crashes` | Finite environment budgets | Modelling devices; no production counterpart. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write` | A source write appends to the copy the alias names | `ILattice.SetAsync`. | Yes: `MaterialisedViewIntegrationTests.View_update_wins_by_hlc`. |
| `SourceSwap` | Resize, reshard or restore moves the alias onto another physical copy | `ILatticeRegistry.SwapAliasAsync`. | Yes: `MaterialisedViewIdentitySwapHealTests.View_tails_new_identity_after_swap`. |
| `CursorTrim` | GC trims only below every cursor and published read position | `LatticeWalGc.RunOnceAsync`, `LatticeWalGc.ReadOffsetConsumerFloorsAsync`, `LatticeWalGc.ReadDurablePinOffsetsAsync`. | Yes: `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints`. |
| `RetentionTrim` | The retention TTL trims past a reader | `LatticeWalGc.RunOnceAsync`. | Yes: `WalGcViewOffsetFloorTests.A_view_a_retention_trim_overtakes_falls_off_and_rebuilds_from_source_state`, `HistoryViewWalFallOffTests.A_history_view_a_retention_trim_overtakes_logs_the_fall_off_and_collapses_to_current_state`. |
| `Drift` | An out-of-band write regresses the view tree | `ILattice.SetAsync`. | Yes: `MaterialisedViewShadowSwapTests.ReconcileAsync_detects_and_repairs_out_of_band_drift`, `MaterialisedViewIntegrationTests.RebuildAsync_reconverges_after_view_drift`. |
| `Crash` | Lose in-memory progress; republish the persisted bound log at its resume point on activation | `ViewMaintainerGrain.PublishActivationReadPositions`. | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point`, `ViewMaintainerReadPositionTests.A_maintainer_answers_for_the_whole_log_before_its_activation_publishes`, `MaterialisedViewShadowSwapTests.Rebuild_converges_after_a_simulated_crash_before_swap`, `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `Drain` | Apply the next entry of the bound log as a plain view-tree overwrite (colliding keys within one drain batch resolve by source HLC; across batches and on replay the later write simply lands) | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.DrainAggregationAsync`, `ViewMaintainerGrain.ApplyAsync`. | Yes: `MaterialisedViewIntegrationTests.GetLagAsync_reports_backlog_then_zero_after_drain`, `MaterialisedViewIntegrationTests.View_update_wins_by_hlc`. |
| `Checkpoint` | Persist, publish the read position and report the cursor under the bound physical copy | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.PublishReadPositions`, `IWalCursorRegistry.ReportCursorAsync`. | Yes: `MaterialisedViewIdentitySwapHealTests.View_reports_its_wal_cursor_under_the_bound_physical_identity_after_swap`, `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints`. |
| `FallOff` | A trim past the applied offset forces a rebuild from source state | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.RebuildAsync`. | Yes: `WalGcViewOffsetFloorTests.A_view_a_retention_trim_overtakes_falls_off_and_rebuilds_from_source_state`. |
| `LagEvict` | Lag past the budget evicts and rebuilds | `ViewMaintainerGrain.TryEvictForLagBudgetAsync`. | Yes: `MaterialisedViewReplicationLifecycleTests.Drain_evicts_and_rebuilds_when_lag_exceeds_budget`, `MaterialisedViewReplicationLifecycleTests.Drain_does_not_re_evict_within_the_lag_eviction_cooldown`. |
| `Rebind` | Detect a new source identity and rebuild against it | `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`, `ViewMaintainerGrain.NotifySourceIdentityChangedAsync`. | Yes: `MaterialisedViewIdentitySwapHealTests.View_heals_after_source_physical_identity_swap`, `MaterialisedViewIdentitySwapHealTests.View_retries_heal_when_rebuild_fails_then_converges`. |
| `Reconcile` | A digest mismatch triggers a rebuild | `ViewMaintainerGrain.ReconcileAsync`, `ViewMaintainerGrain.ComputeViewDigestAsync`. | Yes: `MaterialisedViewShadowSwapTests.ReconcileAsync_detects_and_repairs_out_of_band_drift`. |
| `RebuildScan` | Capture the resume floor and scan source state into a shadow generation | `ViewMaintainerGrain.BuildShadowAsync`, `ViewMaintainerGrain.CaptureResumeFloorsAsync`. | Yes: `MaterialisedViewShadowSwapTests.Rebuild_never_exposes_empty_or_partial_view_to_a_concurrent_reader`, `MaterialisedViewIntegrationTests.RebuildAsync_reconverges_after_view_drift`. |
| `RebuildSwap` | Flip the generation, binding and resume point in one durable write | `ViewMaintainerGrain.SwapToShadowAsync`. | Yes: `MaterialisedViewShadowSwapTests.Rebuild_never_exposes_empty_or_partial_view_to_a_concurrent_reader`, `MaterialisedViewIdentitySwapHealTests.View_tails_new_identity_after_swap`. |
| `Reclaim` | Delete a superseded generation | `ViewMaintainerGrain.TryReclaimPendingGenerationAsync`. | Yes: `MaterialisedViewShadowSwapTests.Rebuild_reclaims_the_old_generation_tree`. |
| `Delete` | Withdraw registration, release logical and physical pins, delete generations | `ViewMaintainerGrain.DecommissionAsync`, `ViewMaintainerGrain.PublishNoReadPositions`, `ViewMaintainerGrain.WithdrawReadRegistrationAsync`, `ViewMaintainerGrain.DeleteBackingGenerationsAsync`. | Yes: `ViewMaintainerDecommissionTests.Decommission_releases_the_pin_deletes_the_tree_and_clears_the_checkpoint`, `ViewMaintainerDecommissionTests.Decommission_deletes_every_generation_up_to_and_including_the_active_one`, `MaterialisedViewIdentitySwapHealTests.Deleting_a_view_after_a_swap_releases_the_pin_on_the_physical_identity`, `ViewMaintainerReadPositionTests.A_decommissioned_maintainer_holds_no_log`, `ViewDecommissionOrphanShadowTests.Decommission_deletes_the_shadow_generation_an_interrupted_rebuild_left_behind`. |
| `Stutter` | No step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `ServedNeverAhead` | The view holds no version its bound source lacks | Yes: `MaterialisedViewIntegrationTests.View_update_wins_by_hlc`. |
| `NoLostWrite` | A replay or a trimmed log never loses an applied write | Yes: `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`, `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints`. |
| `CheckpointWithinApplied` | The read position names the bound log at the durable checkpoint | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point`, `ViewMaintainerReadPositionTests.A_restarted_maintainer_that_never_bound_a_log_holds_none`. |
| `PinOnlyOnBinding` | The cursor pin is reported under the bound physical copy only | Yes: `MaterialisedViewIdentitySwapHealTests.View_reports_its_wal_cursor_under_the_bound_physical_identity_after_swap`. |
| `DecommissionReleases` | Decommission releases every pin, read position and generation | Yes: `ViewMaintainerDecommissionTests.Decommission_also_deletes_a_generation_awaiting_reclamation`, `MaterialisedViewLifecycleManagementTests.DeleteAsync_releases_source_wal_pin_even_after_catalog_loss`, `MaterialisedViewIdentitySwapHealTests.Deleting_a_view_after_a_swap_releases_the_pin_on_the_physical_identity`, `ViewDecommissionOrphanShadowTests.Decommission_deletes_the_shadow_generation_an_interrupted_rebuild_left_behind`, `ViewMaintainerDecommissionTests.Decommission_deletes_the_shadow_generation_an_interrupted_rebuild_may_have_left`. |
| `CursorTrimEntitled` | Cursor GC never trims an entry the view has not read | Yes: `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints`. |
| `ServedMonotone` | Readers never see an empty or partial generation, and outside a replay window never see a key regress | Yes: `MaterialisedViewShadowSwapTests.Rebuild_never_exposes_empty_or_partial_view_to_a_concurrent_reader`. |
| `ReplayConverges` | A crash replay or post-rebuild catch-up, once past the prefix the view already reflected, leaves the view equal to its source prefix | Yes: `ViewMaintainerReplayFlushTests.A_reactivation_that_replays_an_already_flushed_batch_keeps_the_batch_value_over_the_older_ordinary_write`, `ViewMaintainerReplayFlushTests.A_pass_replayed_after_a_lost_flush_response_keeps_the_batch_value_over_the_older_ordinary_write`, `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `ViewConverges` | The view converges to its current source copy | Yes: `MaterialisedViewIdentitySwapHealTests.View_heals_after_source_physical_identity_swap`, `MaterialisedViewIntegrationTests.GetLagAsync_reports_backlog_then_zero_after_drain`. |
| `PinConverges` | The cursor pin follows the binding to the current copy's head | Yes: `MaterialisedViewIdentitySwapHealTests.View_reports_its_wal_cursor_under_the_bound_physical_identity_after_swap`. |
| `GenerationsReclaimed` | Superseded generations are deleted | Yes: `MaterialisedViewShadowSwapTests.Rebuild_reclaims_the_old_generation_tree`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain constraint, not a production behaviour. |

## Evidence and limits

`Yes` identifies a concrete detector of the mapped seam. It does not claim an
automatic refinement proof or crash injection at every await. The `Crash` row's
detectors exercise the activation-time read-position publish and replay from a
persisted checkpoint, not a crash between every drain step. The defects this
model found (#4853, #4856 and #4857, listed in the README) were fixed with
regression tests, and those tests are the detectors cited; they are not
refinement gaps. Production deletes the shadow tree on decommission; the model
abstracts the partial shadow as one boolean, so the number of partial rows is
not modelled. View writes are overwrites, not merges by source HLC, so a
replay below the high-water mark (after a crash, or a rebuild catching up from
its resume floor) can transiently serve an older value for a key it already
served newer; the model checks that this window closes (`ReplayConverges`) and
that no regression happens outside it (`ServedMonotone`), not that reads are
monotonic across a replay. No production detector observes the transient
regression itself; it follows from `ViewMaintainerGrain.ApplyAsync`. The
model's per-action mutations demonstrate non-vacuity of the abstract
properties; they are distinct from production perturbations. The scope
exclusions are stated in the README.

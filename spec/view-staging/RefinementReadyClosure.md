# Independent-clock ready-closure refinement

This is a documented bounded abstraction, not a machine-checked refinement
proof. Candidate detector names below were supplied by the production owner.
Pending is deliberate: these rows must not be promoted to Yes merely because
a finite model or a final-value test passes.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `read`, `ckpt` | Bounded subscription progress and unresolved-entry holdback | `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`, `ViewCheckpointState.AppliedOffsets`. |
| `prepares`, `terminals` | Per-batch index/shard completeness | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`. |
| `done` | Durable effect/idempotency identity, not a new persisted maintainer field | `ViewMaintainerGrain.MarkResolved`; backing atomic-write idempotency is abstracted with the effect. |
| `value`, `history` | Current projected rows or revision-addressed history | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`, `HistoryLatticeViewProjection`. |
| `horizon` | Source-confirmation read entitlement | Candidate source-confirmation head capture in the maintainer; acceptance pending, not a completed-source assertion. |
| `snapshots`, `historySnapshots` | One consumer's observable complete read cut | `ILattice.GetManyAsync`; individual reads, not cross-view simultaneity. |
| `accumulative`, `crashes` | Registration branch and bounded failure environment | `ViewRegistration.Accumulative`; finite crash counter is a modelling device. |
| `deferred` | External eligibility hold on a staged replacement despite consumed heads | SourceAdmission rely-guarantee precondition; not an inference from terminal completeness. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Read` | Consume one partition entry and stage prepares/distinct terminals | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_that_a_deferred_newer_batch_replaces_waits_for_it`. |
| `Capture` | Arm post-source-confirmation head evidence before allowing omission | Candidate source-confirmation path in the maintainer. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_that_a_deferred_newer_batch_replaces_waits_for_it`. |
| `Publish` | Fold exact per-key stamps and emit one closed atomic slice | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`; candidate local closure replaces serial batch flips. | Yes: `ViewMaintainerReplayFlushTests.Crossed_prepare_clock_dependencies_publish_the_complete_local_closure_atomically`, `MaterialisedViewReadyClosureSnapshotTests.Crossed_prepare_clocks_publish_one_complete_winner_cut_in_public_GetMany` and `MaterialisedViewReadyClosureSnapshotTests.Public_snapshot_detector_rejects_split_crossed_clock_closure`. |
| `HistoryPublish` | Atomically retain both revisions without winner filtering | `HistoryLatticeViewProjection`, `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`. | Yes: `MaterialisedViewHistoryReadAtomicTests.Committed_history_batch_never_splits_its_revision_pair_in_one_public_GetMany_snapshot` and its split-publication detector control exercise the actual outgoing atomic history write. `MaterialisedViewHistoryRebuildCompositionTests.Rebuild_captured_floor_keeps_post_floor_revisions_after_lost_response_and_reactivation` retains ordinary and overlapping committed revisions across response loss and reactivation; skipping ordinary revisions in the production drain callback makes this positive fail. |
| `Checkpoint` | Persist only resolved progress or a floor below unresolved entries | `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Pending: existing regression detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `Crash` | Lose volatile staging and replay; retain completed atomic effects | `ViewMaintainerGrain.ArmReplayFence`, `ViewMaintainerGrain.MarkResolved`. | Pending: existing regression detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `Observe` | Publicly read one complete local cut, independent of other consumers' lag | `ILattice.GetManyAsync`. | Yes: `ViewMaintainerReplayFlushTests.Crossed_prepare_clock_dependencies_publish_the_complete_local_closure_atomically`, `MaterialisedViewReadyClosureSnapshotTests.Crossed_prepare_clocks_publish_one_complete_winner_cut_in_public_GetMany` and `MaterialisedViewReadyClosureSnapshotTests.Public_snapshot_detector_rejects_split_crossed_clock_closure`. |
| `Release` | Fair external progress makes the held replacement eligible without changing its WAL progress | Candidate source-confirmation/eligibility path; environment progress is separate from local read completeness. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.An_omitting_batch_flushes_after_the_overlapping_newer_batch_that_holds_the_replacement`. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `CheckpointHeld` | Reading a prepare does not authorize checkpointing past its unresolved identity | Pending: existing regression detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `SnapshotExact` | Crossed independent clocks publish one correct winner cut, never a scalar-batch or partial-key cut | Yes: `ViewMaintainerReplayFlushTests.Crossed_prepare_clock_dependencies_publish_the_complete_local_closure_atomically`, `MaterialisedViewReadyClosureSnapshotTests.Crossed_prepare_clocks_publish_one_complete_winner_cut_in_public_GetMany` and `MaterialisedViewReadyClosureSnapshotTests.Public_snapshot_detector_rejects_split_crossed_clock_closure`. |
| `HistoryExact` | Accumulative projections retain every committed revision, including low-clock losers | Yes: the two `ViewMaintainerHistoryAdmissionTests.Accumulative_history_retains_ordinary_and_overlapping_atomic_revisions_with_independent_clocks_and_replay` cases retain low-clock revisions through normal and lost-response replay. `MaterialisedViewHistoryRebuildCompositionTests.Rebuild_captured_floor_keeps_post_floor_revisions_after_lost_response_and_reactivation` checks public post-floor history after rebuild, overlapping commits, a lost atomic-publication response and reactivation; its paired detector control rejects split publication. The boundary is the captured floor: older or trimmed revisions are not promised. |
| `HistoryReadAtomic` | One history batch's two revision rows are not split by a public snapshot | Yes: `MaterialisedViewHistoryReadAtomicTests.Committed_history_batch_never_splits_its_revision_pair_in_one_public_GetMany_snapshot` and `MaterialisedViewHistoryReadAtomicTests.History_snapshot_detector_rejects_split_publication_at_the_captured_atomic_call`. |
| `PublicationEntitled` | Every omitted key's ready replacement dependency closes transitively with head evidence | Pending: existing regression detector `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_that_a_deferred_newer_batch_replaces_waits_for_it`. |
| `Converges` | Each fairly scheduled consumer completes independently after finite crashes | Pending: existing regression detector `ViewMaintainerReplayFlushTests.An_omitting_batch_flushes_after_the_overlapping_newer_batch_that_holds_the_replacement`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite domain check, not a production behaviour. |

## Evidence and limits
The crossed-clock closure has both actual maintainer call-path and real public
read controls. Replacing `PublishLocalClosureSliceAsync` atomic publication with
sequential production point calls makes the unit snapshot detector fail; its
restored control passes. The integration pair stages two committed batches with
opposed per-key clocks on distinct source shards, holds the actual outgoing
atomic publication, and checks one public `GetManyAsync` result against the
complete winner cut. Its deliberate split publishes one key, holds before the
second, and must fail that same snapshot assertion. The restored pair and the
independent history split-publication pair execute four passing cases, including
two expected-failure controls. HistoryReadAtomic covers indivisible revision-pair
reads only; captured-floor history convergence retains its separate obligation.

The three-batch ring is a reachable committed/installed source prefix. It is
not arbitrary transaction creation or a replacement for source admission.
Each key's prepare/terminal offsets 1..4 are unrelated to source HLCs 10/30.
All current-state batches depend on another ready batch's replacement, so the
whole ring must close before publication. Durable `done` stands for the
backing store's keyed, idempotent effect and history identity, not persistence
of the maintainer's volatile resolved set. One physical identity is fixed; a
reused transaction ID on a different physical log must not inherit this `done`
state. The separate [ResolvedIdentity companion](../view-resolved-identity/README.md)
discharges that bounded scoping obligation (#4895) through actual-tail ownership,
same-log retention and set/eviction-queue discrimination. It does not enlarge
this model or the existing single-transaction Rebind abstraction. Crash after
publication but before checkpoint includes a lost response/replay boundary
without modelling every await.

See the module README's explicit SourceAdmission rely-guarantee contract.
History bypass is a separate registration branch; it does not erase the
current-state admission obligation. The TwoConsumers instance has independent
positions and snapshot cuts; nothing requires equal progress or simultaneous
completion. Coordinator degraded paths and cross-tree atomic-write internals
are outside this local closure model.

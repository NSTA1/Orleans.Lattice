# Refinement note: BackupCapture to code

This note maps [`BackupCapture.tla`](BackupCapture.tla) - a backup capture racing
in-flight atomic sagas - to the production symbols that play each role, and to
the detector tests that would notice production deviating from it. It is a
documented mapping, not a machine-checked refinement proof.

## The model is the decision gate of #4485

Before #4485 was fixed, production captured each shard's frozen baseline at its
own moment and served a bucket still pending at that moment pre-saga, because
the snapshot path discarded the registry decisions. Issue #4485 found, and
confirmed by execution, that a full backup could therefore hold an atomic batch
torn, and that a cross-tree set could be torn even with the drain gate and both
fences working.

The module specifies the fix that shipped for that issue: a
**lease-fenced decision gate**. While the gate is held no decision can be
recorded on the gated tree, a decision snapshot `d0` of the tree's LOCAL
decisions is taken under it, and every shard's still-pending bucket is resolved
against `d0`. A set capture additionally fences new cross-tree registrations,
drains outside the gate, gates every member, re-checks the delegation rows, and
validates the leases and epochs before it accepts. Two mutations reproduce
production as it was before the fix: `BackupSagaConsistentPendingReadsPre` and
`BackupSagaConsistentShardsCapturedApart`. Their code analogues - a leaf fold
that does not resolve its pending buckets against `d0`, and a registry that
records a decision under a held gate - were applied to production and turn the
#4485 regression tests red (the detector-proof log of #4517), so the rows below
cite the fix's symbols and tests.

## Saga abstraction: restated, not instanced

The saga actions restate `spec/atomic-commit/AtomicCommit.tla` rather than
`INSTANCE` it. A sibling module directory is not copied into TLC's scratch
directory, so an `INSTANCE` across directories cannot be checked by the gates;
and the atomic-commit module's instance (two sagas, three keys, fixed write
sets) and its reader-side variables are not the shape a capture needs, which is
two trees, per-tree LOCAL decisions, and cross-tree delegation rows. The
restatement keeps only the saga steps a capture can observe:

| This module | `AtomicCommit.tla` | What is kept, what is dropped |
|-------------|--------------------|-------------------------------|
| `Prepare(t, sh)` | `PrepareTx(t)` | One shard per step here (one atomic step there), because a capture can fall between two shards' prepares. Votes are dropped. |
| `Decide(t)` | `DecideTx(t)` | The outcome is nondeterministic; `CommitIntegrity` is the atomic-commit module's. A single-tree saga's decision is its tree's local Mark. |
| `Finalize(t, T)` | (none) | Cross-tree sagas only: each tree's own Mark, which the atomic-commit module does not separate from the decision. |
| `Broadcast(t, sh)` | `BroadcastStep(t, k)` | Guarded on the tree's local decision (invariant I1), where `BroadcastStep` is guarded on the saga phase. |
| (none) | `ShadowForwardOrphan`, `OrphanDrain`, `ForgetDecision`, `RegistryMask` | Dropped as actions of their own. Their safety is the atomic-commit module's; the capture resolves against `d0`, so a later retirement cannot change a captured image. The two that reach a capture are kept in their capture-visible form: an expired decision tombstone (`Expire`, the registry mask's input) and the row's removal once forgotten (`Prune`, the decision's retirement). |

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `reg[t][T]` | Cross-tree saga registered its decision authority with tree `T` | `TxRegistryGrain.RegisterExternalDecisionAuthorityAsync` (authoring side) or `TxRegistryGrain.RegisterReceiverDecisionAuthorityAsync` (receiver side). |
| `deleg[t][T]` | A live delegation row: the tree's in-flight count | The `ExternalAuthorities` / `ReceiverDecisionAuthorities` maps in `TxRegistryState`, counted by `TxRegistryGrain.ObserveCrossTreeInFlightAsync` as `CrossTreeInFlightObservation.InFlightCount`. |
| `epoch[T]` | Monotonic registration epoch | `TxRegistryState.CrossTreeRegistrationEpoch`, reported as `CrossTreeInFlightObservation.RegistrationEpoch`. |
| `prep[t][sh]` | A staged, hidden prepared bucket | The leaf's per-transaction pending bucket (`BPlusLeafGrain.PendingTx`). |
| `dec[t]` | The coordinator's outcome | `AtomicWriteGrain.RecordTerminalDecisionAsync` for a single-tree saga; the `LatticeCrossTreeTxGrain` decision for a cross-tree saga. |
| `loc[t][T]` | Tree `T`'s LOCAL decision record | `TxRegistryState.Decisions` on the registry shard the txid routes to, written by `TxRegistryGrain.MarkCommittedAsync` / `TxRegistryGrain.MarkAbortedAsync`, or by a cached delegated verdict (`TxRegistryGrain.ResolveDelegatedAsync`). |
| `term[t][sh]` | The terminal a shard applied | The leaf's applied terminal after `AtomicWriteGrain.BroadcastTerminalsAsync`. |
| `expired` | Saga `s`'s committed decision tombstone on T1 has outlived its retention while its row is still stored | `TxRegistryGrain.IsTombstoneExpired` past `LatticeOptions.TxDecisionRetention`, with the row still in `TxRegistryState.Decisions`. |
| `lease[T]`, `held[T]`, `gated[T]` | The registry lease (none / fence / gate) and its continuity | A capture hold of mode `TxRegistryCaptureGateMode`, taken by `TxRegistryGrain.AcquireCaptureGateAsync` on every registry shard of the tree (`TxRegistryFanOut.AcquireCaptureGateAsync`), kept by `TxRegistryGrain.RenewCaptureGateAsync`, and live while `TxRegistryGrain.IsHoldActive` holds. |
| `d0[T]` | The gate's snapshot of local decisions, each RECORDED verdict included, an expired tombstone whose row is still stored as well | `TxRegistryGrain.CaptureLocalDecisions`, taken once the gate is in force and every pending decision write is durable, and read back through `TxRegistryGrain.GetCaptureGateStatusManyAsync`. Production reported an expired tombstone as Indeterminate before #4619 was fixed; `BackupSagaConsistentMaskedDecisionHidden` reproduces production before the fix. |
| `kind` | Standalone capture or set capture | `LatticeBackupCaptureService.CaptureAsync` versus `LatticeBackupCaptureService.CaptureSetAsync` with `CrossTreeConsistent` set over more than one tree. |
| `phase`, `attempt` | The capture's progress and its fence attempt | `LatticeBackupCaptureService.CaptureFencedSetAsync`'s attempt loop, bounded by `LatticeBackupOptions.MaxCrossTreeFenceAttempts`. |
| `before[T]` | The epoch observed at the drained moment | The `epochBefore` array `LatticeBackupCaptureService.DrainCrossTreeInFlightAsync` returns. |
| `capd[sh]`, `img[sh][t]` | Which shards a member capture has read, and what it holds: nothing, pre-saga, post-saga, or absent (a key the fold hid) | The per-shard frozen baselines `ShardRootGrain.CaptureSnapshotBaselineAsync` seeds, streamed into the artifact by `RawEntryCollector`; `SnapshotProjectionFolder.ResolvePendingAgainst` hides a key whose decision reads Indeterminate. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Register(t, T)` | A cross-tree saga registers on a tree | `TxRegistryGrain.RegisterExternalDecisionAuthorityAsync` (and the receiver form), which advances the epoch on a first registration and is refused under a fence by `TxRegistryGrain.ThrowIfRegistrationFenced`. | Yes: `TxRegistryGrainTests.ObserveCrossTreeInFlightAsync_epoch_advances_once_per_distinct_saga_not_per_reregistration` and `TxRegistryGrainTests.ObserveCrossTreeInFlightAsync_epoch_advances_for_a_receiver_side_delegation`. |
| `RegisterRefused(t, T)` | A lease refuses a NEW registration; the sub-saga compensates and its coordinator aborts | `TxRegistryGrain.ThrowIfRegistrationFenced` raises `TxDecisionGateRefusedException` with `TxDecisionGateRefusal.RegistrationFenced`; `TxRegistryWriteRetry.RunAsync` does not retry it, and `AtomicWriteGrain` parks the sub-saga to compensate. | Yes: `TxRegistryGrainTests.Fence_admits_decisions_but_refuses_a_new_external_delegation`, `TxRegistryGrainTests.Fence_refuses_a_new_receiver_delegation`, `TxRegistryWriteRetryTests.RunAsync_does_not_retry_a_fenced_registration` and `SnapshotDecisionGateIntegrationTests.A_fenced_tree_refuses_a_new_cross_tree_write_without_stranding_a_delegation`. |
| `Prepare(t, sh)` | One shard of the prepare fan-out stages its bucket | `AtomicWriteGrain.ExecutePhaseAsync` staging through the prepared path. | Yes: `AtomicWriteGrainTests.ExecuteAsync_routes_execute_phase_writes_through_the_prepared_path`. |
| `Decide(t)` | The coordinator's decision; for a single-tree saga, its local Mark | `AtomicWriteGrain.RecordTerminalDecisionAsync`. Under a held gate the Mark is refused by `TxRegistryGrain.ThrowIfDecisionGated` (`TxDecisionGateRefusal.DecisionGated`) and waited out by `TxRegistryWriteRetry.RunAsync`. **Environment argument:** the outcome is unconstrained, which over-approximates every vote set. **Abstraction argument:** the decision waits for every shard's prepare, even an abort, while production can abort a partly prepared saga. That removes no capture image: a shard never prepared holds nothing of the saga, and a prepared one under an abort reads pre-saga, which is the image the modelled abort gives once the remaining prepares have run. | Yes: `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals` pins the decision preceding the broadcast; `TxRegistryGrainTests.Gate_refuses_a_new_commit_decision_as_retryable`, `TxRegistryGrainTests.Gate_refuses_a_new_abort_decision`, `TxRegistryWriteRetryTests.RunAsync_waits_out_a_decision_gate_refusal_and_reissues_the_call` and `SnapshotCaptureSagaAtomicityTests.Shards_captured_apart_hold_the_batch_on_one_side_and_do_not_block_writes` pin the refusal. |
| `Finalize(t, T)` | A tree records a cross-tree saga's decision and drops its row | `AtomicWriteGrain.FinalizeAsync` into `TxRegistryGrain.MarkCommittedAsync` / `TxRegistryGrain.MarkAbortedAsync`, which drop the delegation rows. | Yes: `TxRegistryGrainTests.MarkCommittedAsync_clears_delegation` and `TxRegistryGrainTests.ObserveCrossTreeInFlightAsync_drops_in_flight_count_once_the_coordinator_decides`. |
| `CacheVerdict(t, T)` | A reader caches a delegated verdict as a local decision | `TxRegistryGrain.ResolveDelegatedAsync` and `TxRegistryGrain.ResolveReceiverDelegatedAsync`. Under a held gate they return the verdict without caching it, and a reader's snapshot carries it through `TxRegistryGrain.MergeUncachedVerdicts`. **Environment argument:** not fair and enabled whenever a decided saga holds a row, which is every moment production can cache. | Yes: `TxRegistryGrainTests.GetStatusAsync_delegated_txid_caches_committed_verdict_and_clears_delegation` pins the cache and `TxRegistryGrainTests.Gated_registry_serves_a_delegated_verdict_to_readers_without_caching_it` its suppression under the gate. |
| `Broadcast(t, sh)` | The terminal fan-out reaches a shard after its tree's local decision (I1) | `AtomicWriteGrain.BroadcastTerminalsAsync`, which runs after `AtomicWriteGrain.RecordTerminalDecisionAsync` (single tree) or after `AtomicWriteGrain.FinalizeAsync` records the tree's decision (cross tree). | Yes: `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals` and `AtomicWriteGrainTests.FinalizeAsync_records_the_decision_before_broadcasting_terminals`. |
| `Sweep(t, sh)` | A sweep applies a terminal from a terminal-intent status read | `BPlusLeafGrain.SelfTerminaliseResolvedPreparesAsync`, `PreparedBucketSweep.RunAsync`, which runs the split's sweep (`TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`) and the online resize snapshot's, and the replication receiver's settlement of a re-shipped prepare (`LatticeGrain.TrySettleReplicatedPrepareAsync`, #4590), which stages the prepare rather than settling it when the read answers InFlight. Their reads go through `TxRegistryGrain.GetStatusForTerminalAsync` / `TxRegistryGrain.GetStatusManyForTerminalAsync`, which answer a delegated txid InFlight under a held gate and outside it cache the verdict durably before answering (`TxRegistryGrain.ReadStatusForTerminalAsync`). **Environment argument:** not fair, so sweeps may or may not run. | Yes: `TxRegistryGrainTests.Gated_registry_answers_a_terminal_intent_read_for_a_delegated_txid_as_InFlight`, `TxRegistryGrainTests.Ungated_terminal_intent_read_caches_a_delegated_verdict_before_reporting_it`, `BPlusLeafGrainTests.Cross_tree_prepare_left_pending_under_a_capture_gate_drains_on_the_first_activation_after_release`, `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` and `BootstrapAtomicVisibilityTests.Reshipped_prepare_of_a_saga_its_decided_receiver_coordinator_owns_is_staged_not_written_under_a_capture_gate`. |
| `Expire` | Saga `s`'s committed decision tombstone on T1 outlives its retention; the row is still stored | `TxRegistryGrain.IsTombstoneExpired`: a live read then answers Indeterminate (#2318), while the sweeps and the capture's `d0` keep the recorded verdict (`TxRegistryGrain.GetRecordedStatusAsync`). **Environment argument:** not fair, and enabled as soon as the decision is recorded, which over-approximates any retention, a shard whose terminal is still held included. Enabled for a single-tree capture only: a set's gate builds `d0` through the same `TxRegistryGrain.CaptureLocalDecisions`, so the set adds no behaviour and only multiplies the state space. | Yes: `TxRegistryGrainTests.GetStatusAsync_reports_an_aged_out_decision_as_indeterminate_not_in_flight` and `TxRegistryGrainTests.GetRecordedStatusAsync_returns_the_stored_verdict_behind_an_indeterminate_answer`. |
| `Prune` | The expired row is removed, once every shard of the saga has applied its terminal | `TxRegistryGrain.PruneExpired`, reached only after the saga's `TxRegistryGrain.ForgetAsync`, which `AtomicWriteGrain` calls once its terminal broadcast has been acknowledged by every touched shard. **Abstraction argument:** the retention, the #4508 WAL purge guard and the replay barrier that make a pruned saga's terminal visible to every later reader are folded into the precondition that every shard holds its terminal; `BackupSagaConsistentPruneBeforeTerminals` shows that precondition is load-bearing. | Yes: `AtomicWriteGrainTests.RunSagaAsync_forgets_the_decision_only_after_the_terminal_broadcast` pins the precondition, and `TxRegistryGrainTests.ForgetAsync_prunes_expired_tombstones_when_next_saga_completes` the prune. |
| `Fence` | A set capture fences new registrations on every member | Step 0 of `LatticeBackupCaptureService.CaptureFencedSetAsync`: `TxRegistryFanOut.AcquireCaptureGateAsync` with `TxRegistryCaptureGateMode.Fence` on every member. | Yes: `LatticeBackupSetCaptureHoldLossTests.A_delegation_attempted_between_the_drain_and_the_gate_is_refused_by_the_set_fence` pins the call site, and `TxRegistryGrainTests.Fence_admits_decisions_but_refuses_a_new_external_delegation` and `TxRegistryGrainTests.Fence_refuses_a_new_receiver_delegation` the refusal it relies on. Inside the window the gate refuses a registration too (`LatticeBackupSetCaptureWindowTests.A_cross_tree_write_starting_inside_the_capture_window_is_refused_and_rolled_back`), so the fence alone matters between the drain and the gate. |
| `DrainGate` | The drain, outside the gate, records the drained epochs | `LatticeBackupCaptureService.DrainCrossTreeInFlightAsync`, deciding through `CrossTreeFenceWindow.IsDrained`. | Yes: `LatticeBackupCaptureServiceDrainTests.CaptureSetAsync_throws_on_drain_timeout_when_sagas_stay_in_flight`, `CrossTreeFenceWindowTests.IsDrained_only_when_nothing_is_in_flight`, and `CrossTreeFenceCaptureCoyoteTests.The_drain_gate_alone_keeps_a_saga_registered_before_the_capture_whole`. |
| `GateAcquire(T)` | The capture takes the gate on a member and snapshots `d0` from the recorded decisions | `LatticeGrain.CaptureGatedBaselinesAsync` for a single tree, step 2 of `LatticeBackupCaptureService.CaptureFencedSetAsync` for a set; both take `TxRegistryCaptureGateMode.Gate`, and `TxRegistryGrain.AcquireCaptureGateAsync` snapshots `d0` once the hold is in force. | Yes: `TxRegistryGrainTests.Gate_status_lookup_keeps_a_decision_the_live_map_drops_under_the_gate` pins that the lookup answers from the snapshot, not the live map (`ForgetAsync` is not gated); `TxRegistryGrainTests.Gate_status_lookup_answers_from_the_decisions_at_acquisition`, `TxRegistryGrainTests.Upgrading_a_fence_to_a_gate_captures_the_decisions_at_the_upgrade` and `SnapshotCaptureSagaAtomicityTests.Shards_captured_apart_hold_the_batch_on_one_side_and_do_not_block_writes` the gate itself; `TxRegistryGrainTests.Gate_captures_an_expired_but_stored_tombstone_at_its_recorded_verdict` and `SnapshotCaptureExpiredTombstoneTests.A_capture_holds_both_keys_of_a_committed_batch_whose_tombstone_expired_with_one_bucket_pending` the recorded verdict of an expired tombstone (both red against the code analogue of `BackupSagaConsistentMaskedDecisionHidden`). |
| `Recheck` | A set capture re-checks the delegation rows under the gate | Step 3 of `LatticeBackupCaptureService.CaptureFencedSetAsync`, deciding through `CrossTreeFenceWindow.IsRecheckClean` (no live and no unresolvable delegation on any member under the gate). | Yes: `CrossTreeFenceWindowTests.IsRecheckClean_refuses_a_delegation_still_live_under_the_gate` and `CrossTreeFenceWindowTests.IsRecheckClean_refuses_a_delegation_it_cannot_resolve` pin the core; `LatticeBackupSetCaptureHoldLossTests.A_delegation_registered_while_the_fence_was_lost_refuses_the_attempt` pins the call site together with `Validate`'s, and `CrossTreeFenceCaptureCoyoteTests.The_gated_recheck_alone_refuses_a_registration_a_lost_fence_admitted` the rule as the only defence left. Removing this call site alone was measured green, which the paired mutation `SetSagaConsistentRecheckAndEpochSkipped` predicts. |
| `CaptureShard(sh)` | One shard's baseline is captured and its pending buckets resolved | `ShardRootGrain.CaptureGatedSnapshotBaselineAsync`, streamed by `RawEntryCollector`; the leaf's fold resolves each still-pending bucket against `d0` with `SnapshotProjectionFolder.ResolvePendingAgainst`. | Yes: `ShardRootGrainSnapshotBaselineCaptureTests.Fanned_out_fold_produces_the_same_baseline_as_a_serial_fold` pins the per-shard capture; `SnapshotProjectionFolderGateResolutionTests.A_saga_committed_in_the_decision_snapshot_reads_post_saga`, `SnapshotProjectionFolderGateResolutionTests.An_undecided_or_aborted_saga_reads_pre_saga` and `SnapshotCaptureSagaAtomicityTests.Capture_during_a_half_broadcast_saga_holds_the_batch_on_one_side` pin the resolution. |
| `Validate` | Release with validation: every lease held throughout, and for a set no epoch moved and nothing in flight | The post-capture re-observation in `LatticeBackupCaptureService.CaptureFencedSetAsync`, deciding through `CrossTreeFenceWindow.IsStable`, then step 6's release, where `TxRegistryGrain.ReleaseCaptureGateAsync` reports whether the hold stayed live (the single-tree form is `LatticeGrain.CaptureGatedBaselinesAsync`'s release). | Yes: `LatticeBackupSetCaptureHoldLossTests.A_member_gate_lost_while_a_single_tree_saga_decides_on_it_forces_a_second_attempt` pins the set's lease validation (red when the set ignores the release result, alone or with the renewal: #4441 F4); `TxRegistryGrainTests.Lapsed_gate_readmits_decisions_and_reports_invalid_everywhere` the release's answer on a lapse; `LatticeBackupSetCaptureHoldLossTests.A_cross_tree_write_completed_while_the_fence_was_lost_forces_a_second_attempt` the epoch call site; `CrossTreeFenceWindowTests.IsStable_refuses_a_registration_that_completed_inside_the_window` and `CrossTreeFenceWindowTests.IsStable_refuses_a_saga_still_in_flight` the core; `CrossTreeFenceCaptureCoyoteTests.The_reobserved_epoch_alone_refuses_a_registration_a_lost_fence_admitted` and `CrossTreeFenceCaptureCoyoteTests.The_reobserved_in_flight_count_alone_refuses_a_registration_a_lost_fence_admitted` each clause as the only defence left; and `SnapshotDecisionGateIntegrationTests.A_capture_that_loses_its_gate_is_retried_with_a_fresh_gate_and_then_accepted` the single-tree path's lost hold. |
| `LeaseLapse(T)` | A lease expires, or is lost, before it is released | A hold past its expiry: `TxRegistryGrain.IsHoldActive` stops counting it, so decisions are admitted again, and the release or a gated status read reports the lapse; a registry reactivation drops a hold outright, and a registry shard minted above the high-water the gate covered was never gated, both of which the release reports the same way (`TxRegistryFanOut.ReleaseCaptureGateAsync` compares the shard high-water with the one it covered). **Environment argument:** not fair and enabled whenever a lease is held, which is every moment a holder can stall past its expiry. | Yes: `TxRegistryGrainTests.Lapsed_gate_readmits_decisions_and_reports_invalid_everywhere` (expiry) and `TxRegistryGrainTests.Release_of_a_token_never_acquired_reports_invalid` (loss) and `TxRegistryFanOutTests.ReleaseCaptureGateAsync_is_invalid_when_the_high_water_grew_past_the_covered_keys` (an ungated new shard); `SnapshotDecisionGateIntegrationTests.A_crashed_captures_gate_lapses_so_sagas_resume_and_writes_are_never_blocked` the readmission end to end, and `SnapshotDecisionGateIntegrationTests.A_capture_that_can_never_hold_its_gate_fails_closed` the lost-hold branch's explicit failure. |
| `CaptureFault` | The capture throws before it is accepted | Every throwing path of `LatticeBackupCaptureService.CaptureFencedSetAsync`: the drain timeout (`LatticeBackupOptions.CrossTreeFenceDrainTimeout`), a member fault, cancellation. **Environment argument:** not fair and enabled at every step before acceptance. | Yes: `LatticeBackupCaptureServiceDrainTests.CaptureSetAsync_throws_on_drain_timeout_when_sagas_stay_in_flight`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `BackupSagaConsistent` | An accepted capture never holds an atomic batch with one key post-saga and another key of the same tree pre-saga or absent. Production violated it before #4485 was fixed, and with an expired decision tombstone before #4619. | Yes: `SnapshotCaptureSagaAtomicityTests.Capture_during_a_half_broadcast_saga_holds_the_batch_on_one_side` (red against the code analogue of `BackupSagaConsistentPendingReadsPre`), `SnapshotCaptureSagaAtomicityTests.Shards_captured_apart_hold_the_batch_on_one_side_and_do_not_block_writes` (red against that of `BackupSagaConsistentShardsCapturedApart`), and, for a set capture's within-member half, `LatticeBackupSetCaptureHoldLossTests.A_member_gate_lost_while_a_single_tree_saga_decides_on_it_forces_a_second_attempt` (red against that of `BackupSagaConsistentSetIgnoresLeaseLoss`), and `SnapshotCaptureExpiredTombstoneTests.A_capture_holds_both_keys_of_a_committed_batch_whose_tombstone_expired_with_one_bucket_pending` (red against that of `BackupSagaConsistentMaskedDecisionHidden`). |
| `SetSagaConsistent` | An accepted cross-tree-consistent set never holds a cross-tree batch on one member and not another. | Yes: `SnapshotCaptureSagaAtomicityTests.Backup_set_holds_a_cross_tree_batch_on_one_side_while_its_terminal_broadcast_straddles_the_set` and `CrossTreeFenceCaptureCoyoteTests.Production_window_never_accepts_a_torn_set`. |
| `SetComplete` | An accepted set holds every member: the set manifest is built only after every member capture returned. | Yes: `LatticeBackupSetCaptureIntegrationTests.CaptureSetAsync_multi_tree_stamps_shared_set_membership_on_every_member`. |
| `CaptureStrictIsolation` | A capture never holds an uncommitted saga's writes: a prepared write is staged hidden, and a capture serves only applied terminals and committed resolutions. | Yes: `SnapshotProjectionFolderGateResolutionTests.An_undecided_or_aborted_saga_reads_pre_saga` pins the full capture's fold, `LatticeBackupIncrementalSagaConsistencyTests.An_increment_taken_while_a_saga_is_in_flight_does_not_restore_its_writes` and `LatticeBackupIncrementalSagaConsistencyTests.An_increment_taken_after_a_saga_aborted_does_not_restore_its_writes` the incremental capture's (#4589), and `AtomicWriteGrainTests.ExecuteAsync_routes_execute_phase_writes_through_the_prepared_path` the staging. |
| `SetCaptureCompletes` | Every set capture reaches a verdict by the protocol's own steps. It fails on a protocol defect under the spec's fairness: an abort's finalize that keeps its delegation row, a fence that refuses the finalize Marks, or a refused registration that is retried instead of aborting. | Yes: `LatticeBackupCaptureServiceDrainTests.CaptureSetAsync_throws_on_drain_timeout_when_sagas_stay_in_flight` pins the explicit failure the drain timeout gives when rows never drain. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

Per #2321's taxonomy:

- BackupSagaConsistent: reached and falsifiable for both capture kinds. Twelve
  mutations targeting it fire it (CaptureStrictIsolationPrepareWritesVisible
  and SetCompleteFaultAccepts fire it as well). Until #4441 the property was
  blind for a set capture: the single-tree saga did not run under a set, and the
  cross-tree saga had one shard per member, so no batch could tear inside a
  member. Both sagas now run under both kinds, and the cross-tree saga has two
  shards on T1. With Init pinned to a set capture, eight of the eleven mutations
  that then target it fire it, among them BackupSagaConsistentSetIgnoresLeaseLoss,
  which perturbs only the set path. The other three (a delegated verdict cached
  under the gate, a sweep reading the coordinator under the gate, a terminal
  before its tree's mark) perturb the cross-tree saga's delegation, which a set
  additionally defends by its drain and gated re-check; each fires under a set
  once those are skipped as well. That is defence in depth, not blindness.
- SetSagaConsistent: reached and falsifiable; SetSagaConsistentSkipsDrainAndRecheck
  and SetSagaConsistentRecheckAndEpochSkipped fire it.
- SetComplete: reached and falsifiable; SetCompleteFaultAccepts fires it.
- CaptureStrictIsolation: reached and falsifiable;
  CaptureStrictIsolationPrepareWritesVisible fires it.
- SetCaptureCompletes: liveness, falsifiable under the asserted fairness; three
  protocol-level mutations fire it, each leaving the fairness of Spec intact.

No mutation in this module adds an action; every one perturbs an existing
action or definition. BackupSagaConsistentPruneBeforeTerminals declares
`DEADLOCK: off`: a row pruned while a bucket of its saga is pending strands the
bucket for good, so the saga never quiesces and TLC would report that deadlock
before the torn capture. Two checks are mutually redundant by design and are
paired in one mutation: skipping the gated in-flight re-check alone, or the
epoch validation alone, is measured clean (a row can be live at the gate only if
a lease lapse admitted a registration after the drain, which also moves the
drained epoch). Production implements both, as step 3 and step 5 of
`LatticeBackupCaptureService.CaptureFencedSetAsync`, and the re-observation's
in-flight clause as a third; `CrossTreeFenceCaptureCoyoteTests` pins each of the
three, and the drain, as the only defence left in an arm of its own.

## Scope at the seams

Each item below is checked elsewhere, or argued here; none is an unchecked claim.

- **Shard topology.** The model's shards are fixed. A split or reshard during a
  capture moves a pending bucket to another shard of the same tree
  (`PreparedBucketSweep`, whose terminal-intent reads `Sweep` maps), where the
  capture still resolves it against the same `d0`: a txid's decision routes to
  one registry key whichever shard holds its bucket. That a move never drops or
  duplicates a bucket is the shard-ownership modules' (`spec/shard-ownership/`,
  #4455 and #4506). A registry shard minted mid-capture is `LeaseLapse`'s.
- **Unresolvable delegations.** The model has no unreachable coordinator. In
  production `d0` holds local decisions only and dials no coordinator, so a
  delegated saga with no local decision resolves InFlight and, by invariant I1,
  reads pre-saga on every shard of the tree - exactly the model's `d0` entry of
  none. A set capture's re-check also refuses any unresolvable delegation
  (`CrossTreeFenceWindow.IsRecheckClean`, pinned by
  `CrossTreeFenceWindowTests.IsRecheckClean_refuses_a_delegation_it_cannot_resolve`).
- **The receiver path.** Receiver delegations register and finalize exactly as
  authoring ones here, through the same registry calls. That a terminal never
  overtakes its prepare across shipping is #4480's, fixed by #4493 and checked
  by the replication modules (`spec/replication/`).
- **One cut per member, not one cut per set.** Members are captured one tree
  after another, each against its own `d0`; the set is consistent for
  cross-tree batches only, as `BackupSetFence` documents and `SetSagaConsistent`
  checks, and each member is consistent within itself, as `BackupSagaConsistent`
  checks for both capture kinds.
- **Forgetting.** Only the single-tree saga forgets and is pruned here. A
  cross-tree sub-saga forgets through the same `AtomicWriteGrain` completion
  path after its own terminal broadcast, so the precondition `Prune` carries is
  the same.
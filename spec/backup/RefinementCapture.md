# Refinement note: BackupCapture to code

This note maps [`BackupCapture.tla`](BackupCapture.tla) - a backup capture racing
in-flight atomic sagas - to the production symbols that play each role, and to
the detector tests that would notice production deviating from it. It is a
documented mapping, not a machine-checked refinement proof.

## The model is the intended design of #4485

Production today captures each shard's frozen baseline at its own moment
(`LatticeGrain.OpenSnapshotCursorAsync` fanning out
`ShardRootGrain.CaptureSnapshotBaselineAsync`) and serves a bucket still
pending at that moment pre-saga, because the snapshot path discards the
registry decisions. Issue #4485 found, and confirmed by execution, that a full
backup can therefore hold an atomic batch torn, and that a cross-tree set can be
torn even with the drain gate and both fences working.

The module specifies the fix the coordinator approved on that issue: a
**lease-fenced decision gate**. While the gate is held no decision can be
recorded on the gated tree, a decision snapshot `d0` of the tree's LOCAL
decisions is taken under it, and every shard's still-pending bucket is resolved
against `d0`. A set capture additionally fences new cross-tree registrations,
drains outside the gate, gates every member, re-checks the delegation rows, and
validates the leases and epochs before it accepts. Two mutations reproduce
production as it is today: `BackupSagaConsistentPendingReadsPre` and
`BackupSagaConsistentShardsCapturedApart`. Rows whose code counterpart is the
gate say so and cite #4485; when the fix lands, those rows are re-pointed at
its symbols and their detectors re-proved red against the two mutations.

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
| (none) | `ShadowForwardOrphan`, `OrphanDrain`, `ForgetDecision`, `RegistryMask` | Dropped. Their safety is the atomic-commit module's; the capture resolves against `d0`, so a later retirement cannot change a captured image. The registry mask is an abstraction gap below. |

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
| `lease[T]`, `held[T]`, `gated[T]` | The registry lease (none / fence / gate) and its continuity | The #4485 fix's per-registry-shard lease; no production symbol yet. |
| `d0[T]` | The gate's snapshot of local decisions | The #4485 fix's decision snapshot; no production symbol yet. |
| `kind` | Standalone capture or set capture | `LatticeBackupCaptureService.CaptureAsync` versus `LatticeBackupCaptureService.CaptureSetAsync` with `CrossTreeConsistent` set over more than one tree. |
| `phase`, `attempt` | The capture's progress and its fence attempt | `LatticeBackupCaptureService.CaptureFencedSetAsync`'s attempt loop, bounded by `LatticeBackupOptions.MaxCrossTreeFenceAttempts`. |
| `before[T]` | The epoch observed at the drained moment | The `epochBefore` array `LatticeBackupCaptureService.DrainCrossTreeInFlightAsync` returns. |
| `capd[sh]`, `img[sh][t]` | Which shards a member capture has read, and what it holds | The per-shard frozen baselines `ShardRootGrain.CaptureSnapshotBaselineAsync` seeds, streamed into the artifact by `RawEntryCollector`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Register(t, T)` | A cross-tree saga registers on a tree | `TxRegistryGrain.RegisterExternalDecisionAuthorityAsync` (and the receiver form), which advances the epoch on a first registration. Refusal under a lease is the #4485 fix. | Yes: `TxRegistryGrainTests.ObserveCrossTreeInFlightAsync_epoch_advances_once_per_distinct_saga_not_per_reregistration` and `TxRegistryGrainTests.ObserveCrossTreeInFlightAsync_epoch_advances_for_a_receiver_side_delegation`. |
| `RegisterRefused(t, T)` | A lease refuses a NEW registration; the sub-saga compensates and its coordinator aborts | The #4485 fix's FENCE refusal (not retried). Today nothing refuses a registration. | Partial: #4485. Today's coverage is only the abort a failed sub-saga produces, `CrossClusterSagaCoordinatorGrainTests.RunAsync_one_abort_aborts_and_compensates_only_prepared_participants`; the refusal itself lands with the fix. |
| `Prepare(t, sh)` | One shard of the prepare fan-out stages its bucket | `AtomicWriteGrain.ExecutePhaseAsync` staging through the prepared path. | Yes: `AtomicWriteGrainTests.ExecuteAsync_routes_execute_phase_writes_through_the_prepared_path`. |
| `Decide(t)` | The coordinator's decision; for a single-tree saga, its local Mark | `AtomicWriteGrain.RecordTerminalDecisionAsync`. Refusal of a Mark under the gate (`TxRegistryWriteRetry` waits) is the #4485 fix. **Environment argument:** the outcome is unconstrained, which over-approximates every vote set. | Partial: #4485. `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals` pins the decision preceding the broadcast; the gate refusal lands with the fix. |
| `Finalize(t, T)` | A tree records a cross-tree saga's decision and drops its row | `AtomicWriteGrain.FinalizeAsync` into `TxRegistryGrain.MarkCommittedAsync` / `TxRegistryGrain.MarkAbortedAsync`, which drop the delegation rows. | Yes: `TxRegistryGrainTests.MarkCommittedAsync_clears_delegation` and `TxRegistryGrainTests.ObserveCrossTreeInFlightAsync_drops_in_flight_count_once_the_coordinator_decides`. |
| `CacheVerdict(t, T)` | A reader caches a delegated verdict as a local decision | `TxRegistryGrain.ResolveDelegatedAsync`. **Environment argument:** not fair and enabled whenever a decided saga holds a row, which is every moment production can cache. Suppression under the gate is the #4485 fix. | Partial: #4485. `TxRegistryGrainTests.GetStatusAsync_delegated_txid_caches_committed_verdict_and_clears_delegation` pins the cache; the suppression lands with the fix. |
| `Broadcast(t, sh)` | The terminal fan-out reaches a shard after its tree's local decision (I1) | `AtomicWriteGrain.BroadcastTerminalsAsync`, which runs after `AtomicWriteGrain.RecordTerminalDecisionAsync` (single tree) or after `AtomicWriteGrain.FinalizeAsync` records the tree's decision (cross tree). | Yes: `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals` and `AtomicWriteGrainTests.FinalizeAsync_records_the_decision_before_broadcasting_terminals`. |
| `Sweep(t, sh)` | A sweep applies a terminal from a terminal-intent status read | `BPlusLeafGrain.SelfTerminaliseResolvedPreparesAsync` and `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`. Their reads answering from LOCAL decisions only (and outside the gate, caching durably before answering) is the #4485 fix. **Environment argument:** not fair, so sweeps may or may not run. | Partial: #4485. `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` exercises the split sweep; the terminal-intent read lands with the fix. |
| `Fence` | A set capture fences new registrations on every member | The #4485 fix's FENCE lease; no production step today. | Partial: #4485. `LatticeBackupSetCaptureIntegrationTests.CaptureSetAsync_cross_tree_consistent_never_captures_a_partial_cross_tree_batch` exercises today's set capture; the fence lands with the fix. |
| `DrainGate` | The drain, outside the gate, records the drained epochs | `LatticeBackupCaptureService.DrainCrossTreeInFlightAsync`, deciding through `CrossTreeFenceWindow.IsDrained`. | Yes: `LatticeBackupCaptureServiceDrainTests.CaptureSetAsync_throws_on_drain_timeout_when_sagas_stay_in_flight`, `CrossTreeFenceWindowTests.IsDrained_only_when_nothing_is_in_flight`, and `CrossTreeFenceCaptureCoyoteTests.Skipping_the_drain_gate_accepts_a_torn_set`. |
| `GateAcquire(T)` | The capture takes the gate on a member and snapshots `d0` | The #4485 fix's GATE acquire. Today the snapshot open (`LatticeGrain.OpenSnapshotCursorAsync`) raises no gate, which `BackupSagaConsistentShardsCapturedApart` reproduces. | Partial: #4485. `LatticeBackupEndToEndTests.CaptureAsync_atomic_saga_is_never_split_across_the_consistency_cut` covers a capture taken after a saga completed; the concurrent case is the issue's repro, which becomes the regression test. |
| `Recheck` | A set capture re-checks the delegation rows under the gate | The #4485 fix's gated in-flight re-check. | Partial: #4485. `CrossTreeFenceWindowTests.IsStable_refuses_a_saga_still_in_flight` pins the in-flight clause the re-check reuses; the gated step lands with the fix. |
| `CaptureShard(sh)` | One shard's baseline is captured and its pending buckets resolved | `ShardRootGrain.CaptureSnapshotBaselineAsync`, streamed by `RawEntryCollector`. Resolving pending buckets against `d0` is the #4485 fix; today they read pre-saga (`BackupSagaConsistentPendingReadsPre`). | Partial: #4485. `ShardRootGrainSnapshotBaselineCaptureTests.Fanned_out_fold_produces_the_same_baseline_as_a_serial_fold` pins the per-shard capture; the resolution against `d0` lands with the fix. |
| `Validate` | Release with validation: every lease held throughout, and for a set no epoch moved | The post-capture re-observation in `LatticeBackupCaptureService.CaptureFencedSetAsync`, deciding through `CrossTreeFenceWindow.IsStable`. The lease validation is the #4485 fix. | Yes: `LatticeBackupSetCaptureWindowTests.A_cross_tree_write_completing_inside_the_capture_window_forces_a_second_attempt` pins the call site, `CrossTreeFenceWindowTests.IsStable_refuses_a_registration_that_completed_inside_the_window` the core, and `CrossTreeFenceCaptureCoyoteTests.Reobservation_ignoring_the_epoch_accepts_a_torn_set` the race. |
| `LeaseLapse(T)` | A lease expires before it is released | The #4485 fix's lease expiry. **Environment argument:** not fair and enabled whenever a lease is held, which is every moment a holder can stall past its expiry. | Partial: #4485. No production lease exists yet; `CrossTreeFenceCaptureCoyoteTests.Production_window_never_accepts_a_torn_set` covers today's window. |
| `CaptureFault` | The capture throws before it is accepted | Every throwing path of `LatticeBackupCaptureService.CaptureFencedSetAsync`: the drain timeout (`LatticeBackupOptions.CrossTreeFenceDrainTimeout`), a member fault, cancellation. **Environment argument:** not fair and enabled at every step before acceptance. | Yes: `LatticeBackupCaptureServiceDrainTests.CaptureSetAsync_throws_on_drain_timeout_when_sagas_stay_in_flight`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `BackupSagaConsistent` | An accepted capture never holds an atomic batch with one key post-saga and another key of the same tree pre-saga. Production violates it today (#4485). | Partial: #4485. `LatticeBackupEndToEndTests.CaptureAsync_atomic_saga_is_never_split_across_the_consistency_cut` holds only for a capture taken after the saga completed; the concurrent repro in #4485 is the regression test the fix adds. |
| `SetSagaConsistent` | An accepted cross-tree-consistent set never holds a cross-tree batch on one member and not another. | Partial: #4485. `LatticeBackupSetCaptureIntegrationTests.CaptureSetAsync_cross_tree_consistent_never_captures_a_partial_cross_tree_batch` and `CrossTreeFenceCaptureCoyoteTests.Production_window_never_accepts_a_torn_set` (which assumes the fix's per-tree resolution) cover the window; the per-tree half is #4485. |
| `SetComplete` | An accepted set holds every member: the set manifest is built only after every member capture returned. | Yes: `LatticeBackupSetCaptureIntegrationTests.CaptureSetAsync_multi_tree_stamps_shared_set_membership_on_every_member`. |
| `CaptureStrictIsolation` | A capture never holds an uncommitted saga's writes: a prepared write is staged hidden, and a capture serves only applied terminals and committed resolutions. | Yes: `AtomicWriteGrainTests.ExecuteAsync_routes_execute_phase_writes_through_the_prepared_path`. |
| `SetCaptureCompletes` | Every set capture reaches a verdict by the protocol's own steps. It fails on a protocol defect under the spec's fairness: an abort's finalize that keeps its delegation row, a fence that refuses the finalize Marks, or a refused registration that is retried instead of aborting. | Yes: `LatticeBackupCaptureServiceDrainTests.CaptureSetAsync_throws_on_drain_timeout_when_sagas_stay_in_flight` pins the explicit failure the drain timeout gives when rows never drain. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

Per #2321's taxonomy:

- BackupSagaConsistent: reached and falsifiable; ten protocol-level mutations
  fire it.
- SetSagaConsistent: reached and falsifiable; SetSagaConsistentSkipsDrainAndRecheck
  and SetSagaConsistentRecheckAndEpochSkipped fire it.
- SetComplete: reached and falsifiable; SetCompleteFaultAccepts fires it.
- CaptureStrictIsolation: reached and falsifiable;
  CaptureStrictIsolationPrepareWritesVisible fires it.
- SetCaptureCompletes: liveness, falsifiable under the asserted fairness; three
  protocol-level mutations fire it, each leaving the fairness of Spec intact.

No mutation in this module adds an action; every one perturbs an existing
action or definition. Two checks are mutually redundant by design and are
paired in one mutation: skipping the gated in-flight re-check alone, or the
epoch validation alone, is measured clean (a row can be live at the gate only if
a lease lapse admitted a registration after the drain, which also moves the
drained epoch). The #4485 fix implements both.

## Deliberate abstraction gaps

- **The registry mask.** `d0` holds only committed, aborted or no decision. In
  the #4485 design an expired tombstone snapshots as Indeterminate and reads
  hidden (the export vocabulary of #2328). Not modelled; the atomic-commit
  module's `RegistryMask` covers what a hidden answer may and may not be.
- **Topology.** The capture's high-water re-read (a shard split or reshard
  mid-capture fails it closed) and the pinned shard map are not modelled; shard
  ownership is the shard-ownership module's.
- **Unresolvable delegations.** A re-check that cannot dial a coordinator fails
  closed in the #4485 design; the model has no unreachable coordinator.
- **The receiver path.** Receiver delegations register and finalize exactly as
  authoring ones here. Receiver-side consistency also depends on a terminal
  never overtaking its prepare across shipping, which is #4480's.
- **One cut per member, not one cut per set.** Members are captured one tree
  after another, each against its own `d0`; the set is consistent for
  cross-tree batches only, as `BackupSetFence` documents.

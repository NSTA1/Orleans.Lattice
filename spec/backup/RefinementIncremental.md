# Refinement note: BackupIncremental to code

This note maps [`BackupIncremental.tla`](BackupIncremental.tla) - an incremental
backup racing an atomic saga - to the production symbols that play each role,
and to the detector tests that would notice production deviating from it. It is a
documented mapping, not a machine-checked refinement proof.

## The model is the fix of #4589

Before #4589 was fixed, `IncrementalDeltaCollector.OnEntry` copied every
prepared write in the delta window as an ordinary entry and dropped the saga's
`TxCommit` / `TxAbort` terminals. An increment could therefore restore an aborted
or undecided batch's writes, or a batch partially.
`BackupSagaConsistentIncrementCopiesPrepares` reproduces that collector.

The module specifies the fix. An increment takes the #4485 decision gate before it
drains, so its decision snapshot `d0` holds every saga whose prepares it can meet.
It then resolves each saga's records against `d0` through one core,
`IncrementalSagaStaging`, which `IncrementalDeltaCollector` routes every saga entry
through:

- a batch committed in `d0` is emitted whole once its prepares cover it;
- an aborted one is dropped;
- an undecided one is dropped and holds the recorded frontier back, so the next
  increment reads it again;
- a committed batch whose prepares the window does not cover falls back to a full
  backup.

Model checking found one more case the window cannot see. A saga can be undecided
at a full base with every prepare before the base's frontier, then commit before
any of its terminals reaches the WAL. The fix therefore also records, in the
snapshot coordinate and the backup's consistency cut, the sagas a capture held
pre-saga because they were undecided (`LatticeSnapshotCoordinate.UndecidedSagaIds`,
`BackupConsistencyCut.UndecidedSagaIds`). Each increment looks them up again and
hands on the ones still undecided.

## Saga abstraction

The saga steps restate [`BackupCapture.tla`](BackupCapture.tla)'s, which restate
`spec/atomic-commit/AtomicCommit.tla`. One saga writes two keys; votes, the
coordinator, and cross-tree delegation are dropped (the capture module covers the
set capture's fence). Unlike `BackupCapture`, a record's WAL partition is modelled,
because a prepare is routed by its key's hash and a shard's terminal by the shard
index (`ShardRootGrain.AppendTxTerminalAsync`), so a saga's prepares and terminals
need not share a partition, and the frontier is per partition.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `wal[p]` | A WAL partition's records in offset order | The tree's commit log, read forward by `IncrementalDeltaCollector.StreamAsync` through `IWalSubscriber.DrainAsync`. A record is a `LatticeMutation` with `LatticeMutation.IsPrepared`, or a terminal whose `LatticeMutation.ShardIndex` names its shard. |
| `dec` | The saga's recorded decision | The tree's local Mark, `TxRegistryGrain.MarkCommittedAsync` / `TxRegistryGrain.MarkAbortedAsync`, recorded by `AtomicWriteGrain.RecordTerminalDecisionAsync`. |
| `phase`, `pending` | The capture engine's step, and the WAL heads a full capture read before its snapshot | `LatticeBackupCaptureService.CaptureWalHeadsAsync`, recorded as `BackupConsistencyCut.WalPartitionOffsets`. |
| `gated`, `d0` | The decision gate and its snapshot | `TxRegistryFanOut.AcquireCaptureGateAsync` in `TxRegistryCaptureGateMode.Gate`, read back through `TxRegistryFanOut.GetCaptureGateStatusManyAsync`. |
| `chain` | The backup chain | `BackupManifest.BaseBackupId` links; each link's `heads` is `BackupConsistencyCut.WalPartitionOffsets`, `emit` the artifact's entries, `und` its `BackupConsistencyCut.UndecidedSagaIds`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Prepare(x)` | One key's prepared write reaches its key's WAL partition | `AtomicWriteGrain.ExecutePhaseAsync` staging the batch through `IShardRootGrain.SetManyAsync` under `LatticePreparedContext`, each record stamped with `LatticeMutation.AtomicBatchIndex` and `LatticeMutation.AtomicBatchSize`. **Environment argument:** any key may prepare until the saga is decided, in any order. | Yes: `AtomicWriteGrainTests.ExecuteAsync_routes_execute_phase_writes_through_the_prepared_path`, and `LatticeBackupIncrementalSagaConsistencyTests.An_increment_taken_while_a_saga_is_in_flight_does_not_restore_its_writes`, which reads both prepares in the window. |
| `Decide(o)` | The decision, refused while the gate is held; a commit only once every key has prepared | `AtomicWriteGrain.RecordTerminalDecisionAsync`, refused under the gate by `TxRegistryGrain.ThrowIfDecisionGated`. **Environment argument:** the outcome is unconstrained, and an abort may come at any point before the commit. | Yes: `TxRegistryGrainTests.Gate_refuses_a_new_commit_decision_as_retryable` and `TxRegistryGrainTests.Gate_refuses_a_new_abort_decision`. |
| `Terminal(x)` | A shard's terminal reaches its shard's partition, only after the decision | `ShardRootGrain.AppendTxTerminalAsync`, called by `AtomicWriteGrain.BroadcastTerminalsAsync` after the decision. **Environment argument:** the terminals land in any order and at any time after the decision, including after an increment. | Yes: `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals`, and `LatticeBackupIncrementalSagaConsistencyTests.A_saga_undecided_at_the_base_and_committed_with_no_record_in_the_window_is_restored_whole`, which holds the terminals back. |
| `FullHeads` | A full capture reads the WAL heads before its snapshot | `LatticeBackupCaptureService.CaptureWalHeadsAsync`. | Yes: `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_captures_only_the_delta_committed_after_the_base`. |
| `FullSnap` | The full snapshot resolves every pending bucket against its gate, and records the sagas it held pre-saga because they were undecided | `LatticeGrain.CaptureGatedBaselinesAsync` (through `SnapshotProjectionFolder.ResolvePendingAgainst`), recording `LatticeSnapshotCoordinate.UndecidedSagaIds` from `TxRegistryFanOut.GetCaptureGateUndecidedAsync`, which `TxRegistryGrain.GetCaptureGateUndecidedAsync` answers from its lookups; `LatticeBackupCaptureService.BuildConsistencyCut` copies the set into the cut. | Yes: `SnapshotCaptureSagaAtomicityTests.Capture_during_a_half_broadcast_saga_holds_the_batch_on_one_side`, `TxRegistryGrainTests.Gate_remembers_only_the_txids_its_status_lookups_answered_undecided`, and `LatticeBackupIncrementalSagaConsistencyTests.A_saga_undecided_at_the_base_and_committed_with_no_record_in_the_window_is_restored_whole`, which asserts the base recorded the saga. |
| `GateAcquire` | An increment takes the decision gate and its snapshot before it drains | `LatticeBackupCaptureService.CaptureIncrementalAsync` through `TxRegistryFanOut.AcquireCaptureGateAsync`, renewed for the whole drain and released with validation; a gate not held throughout falls back to a full backup. That validation is defence in depth and was measured green when removed: a decision recorded after `d0` reads undecided, so its saga is held back, and its commit terminal is one `d0` does not explain, so the increment falls back anyway. | Yes: `LatticeBackupIncrementalSagaConsistencyTests.A_saga_left_out_while_undecided_is_restored_whole_by_the_next_increment` and `LatticeBackupIncrementalSagaConsistencyTests.An_increment_taken_while_a_saga_is_in_flight_does_not_restore_its_writes`, both red when the increment takes a fence instead of the gate. |
| `Drain` | The increment settles its window: emit, drop, hold back, hand on, or fall back | `IncrementalDeltaCollector.OnEntry` and `IncrementalDeltaCollector.StreamAsync` routing through `IncrementalSagaStaging.TryStage`, `IncrementalSagaStaging.ApplyDecisions`, `IncrementalSagaStaging.DrainCommitted` and `IncrementalSagaStaging.Finish`; the frontier from `IncrementalDeltaCollector.NewPartitionOffsets` (held back by `IncrementalSagaStaging.HeldOffsets`), the WAL pin from `IncrementalSagaStaging.BlockedFloor`, the hand-on from `IncrementalSagaStaging.CarriedUndecided`, and the fall back on `IncrementalDeltaCollector.RequiresSagaFallback`. | Yes: every `LatticeBackupIncrementalSagaConsistencyTests` detector, `IncrementalDeltaCollectorTests.An_aborted_sagas_prepared_write_is_not_captured`, and `IncrementalSagaStagingTests.An_undecided_batch_is_dropped_and_holds_the_frontier_at_its_earliest_entry_per_partition`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `BackupSagaConsistent` | No restore of a chain holds an atomic batch partially. Production violated it before #4589 was fixed. | Yes: `LatticeBackupIncrementalSagaConsistencyTests.A_saga_committed_across_the_base_frontier_is_restored_whole` (red against the bucket's collector) and `IncrementalSagaStagingTests.A_committed_batch_the_window_does_not_cover_requires_a_full_backup`. |
| `CaptureStrictIsolation` | No restore of a chain holds a write of a saga that did not commit. Production violated it before #4589 was fixed. | Yes: `LatticeBackupIncrementalSagaConsistencyTests.An_increment_taken_after_a_saga_aborted_does_not_restore_its_writes` and `LatticeBackupIncrementalSagaConsistencyTests.An_increment_taken_while_a_saga_is_in_flight_does_not_restore_its_writes` (both red against the bucket's collector). |
| `ChainCoversCommitted` | A link whose decision snapshot holds a saga committed restores it whole, including a saga none of whose records reaches the window. | Yes: `LatticeBackupIncrementalSagaConsistencyTests.A_saga_undecided_at_the_base_and_committed_with_no_record_in_the_window_is_restored_whole`, `LatticeBackupIncrementalSagaConsistencyTests.A_saga_undecided_across_an_increment_is_handed_on_and_restored_whole_once_it_commits`, and `LatticeBackupIncrementalSagaConsistencyTests.A_saga_committed_across_the_base_frontier_is_restored_whole`. |
| `SagaFallbackOnlyAcrossFull` | An increment falls back to a full backup only for a batch that straddles the full capture's frontier: an increment never strands a saga it has read part of. | Yes: `LatticeBackupIncrementalSagaConsistencyTests.A_saga_left_out_while_undecided_is_restored_whole_by_the_next_increment` and `IncrementalSagaStagingTests.A_committed_batch_is_emitted_but_holds_the_frontier_until_every_shard_terminal_is_read`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

Per #2321's taxonomy every property is reached and falsifiable, each by at least
one protocol-level mutation:

- BackupSagaConsistent by BackupSagaConsistentIncrementCopiesPrepares (today's
  collector) and BackupSagaConsistentStraddleEmittedWithoutFallback;
- CaptureStrictIsolation by CaptureStrictIsolationIncrementIgnoresDecision;
- ChainCoversCommitted by ChainCoversCommittedBaseUndecidedIgnored,
  ChainCoversCommittedUndecidedNotHandedOn and ChainCoversCommittedFullSnapReadsPre;
- SagaFallbackOnlyAcrossFull by five mutations, one for each rule that keeps an
  increment from stranding a saga: the held-back frontier, waiting for every
  shard terminal, the gate refusing decisions, a terminal following its decision,
  and a decision snapshot read from the registry.

No mutation adds an action. The module has no liveness property: an increment is
one step, and nothing in it waits on anything.

## Deliberate abstraction gaps

- **The drain is one step.** Production drains each partition in pages while
  writes continue. The model reads the whole window at once. Splitting it changes
  nothing the properties see: `d0` is fixed for the drain, a saga committed in it
  had every prepare in the WAL before the gate, and a record the drain misses
  lies beyond the recorded frontier, so the next increment reads it.
- **One saga, no ordinary writes.** Ordinary writes are emitted unchanged and
  restored last-writer-wins, as before the fix; the model keeps only the saga's
  records, whose resolution is what the fix changes.
- **Retried prepares and duplicated terminals.** The core keys a prepare by its
  `LatticeMutation.AtomicBatchIndex`, so a retried prepare never completes a batch
  twice (`IncrementalSagaStagingTests.A_retried_prepare_replaces_its_index_rather_than_completing_the_batch`).
  A terminal redelivered after its saga settled reaches a later increment as a
  commit with no prepares and falls back to a full backup: safe, and outside the
  model.
- **Gate lapse.** A lease that lapses during the drain makes the decision
  snapshot unreadable; the increment then falls back to a full backup.
  `BackupCapture` models the lease and its lapse.
- **A base captured before the fix.** Its cut carries no undecided set, so an
  increment on it cannot look those sagas up. Every capture taken after the fix
  records the set.

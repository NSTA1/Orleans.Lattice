# Refinement note: tag-index reconciliation scheduling

## Model scope in one paragraph

One non-reentrant per-index coordinator snapshots a finite covered-tree set,
probes fingerprints in bounded tree batches and repairs divergent trees one at
a time. A recurring schedule, phase-pump recovery, one queued on-demand request,
one transient failed repair/checkpoint and finite subject-version changes can
overlap. Probe-only is snapshotted for the instance. Successful complete repair
is the only operation that certifies its probe-time fingerprint. The model
checks scheduling, not the membership CRDT or row-level repair algorithm.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `phase`, `sweeps` | Exclusive sweep lifecycle; sweeps is an audit counter. | `TagIndexReconcileState.InProgress`, `TagIndexReconcileState.Phase`. |
| `cursor` | Probe cursor or completed-repair prefix, by phase. | `TagIndexReconcileState.NextProbeIndex`, `TagIndexReconcileState.NextRepairIndex`. |
| `dirty` | Ordered repair work, selected by lowest tree ordinal. | `TagIndexReconcileState.DirtyTrees`. |
| `baseline` | Last durably certified probe snapshot. | `TagIndexReconcileState.Baselines`. |
| `fingerprint` | Subject version, with a finite environment change. | `TagIndexReconcileGrain.ComputeFingerprintAsync`. |
| `pendingBaseline` | Probe snapshot awaiting complete repair. | `TagIndexReconcileState.PendingBaselines`. |
| `repaired` | Ghost witness of the snapshot whose whole-tree repair completed. | `LatticeTagIndexContext.ReconcileSubjectAsync`. |
| `probeOnly` | Audit-only option fixed for the instance. | `TagIndexReconcileState.ProbeOnlySweep`. |
| `pending`, `demand`, `servingDemand` | Arrival, queued call and completion acknowledgement. | `TagIndexReconcileGrain.RunSweepAsync`, `TagIndexReconcileGrain.DrainActiveSweepAsync`. |
| `pump`, `armNeeded` | Runtime timer and outstanding reminder-registry operation. | `CoordinatorGrain.StartCoordinatorAsync`, `CoordinatorGrain.StartPhaseTimer`. |
| `busy`, `selected`, `failureUsed` | Fallible tree repair and checkpoint; one transient failure. | `TagIndexReconcileGrain.ProcessRepairChunkAsync`. |
| `work` | Tree work in a phase callback, not rows, bytes or time. | `TagIndexReconcileGrain.ProcessProbeChunkAsync`, `TagIndexReconcileGrain.ProcessRepairChunkAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Mutate` | Change a subject fingerprint; it must be re-probed. | `TagIndexReconcileGrain.ComputeFingerprintAsync`. | Yes: `TagIndexReconcileGrainTests.Sweeps_reprobe_subject_changes_without_certifying_unrepaired_fingerprints`. |
| `Demand` | Queue a synchronous on-demand call behind activation-local work. | `TagIndexReconcileGrain.RunSweepAsync`. | Yes: `TagIndexReconcileGrainTests.On_demand_finishes_the_active_repair_before_starting_its_fresh_sweep`. |
| `Reminder` | Recurring schedule requests work or recovers the existing pump, without rewind. | `TagIndexReconcileGrain.ReceiveReminder`. | Yes: `TagIndexReconcileGrainTests.Phase_callback_bounds_probe_work_and_preserves_progress_across_reminders`, `TagIndexReconcileGrainTests.Schedule_recovers_the_pump_after_initial_keepalive_registration_failed`. |
| `Rearm` | Successful registry completion arms one timer. | `CoordinatorGrain.StartCoordinatorAsync`, `CoordinatorGrain.StartPhaseTimer`. | Yes: `TagIndexReconcileGrainTests.Schedule_recovers_the_pump_after_initial_keepalive_registration_failed`. |
| `BeginSweep` | Start only after the older sweep terminates. | `TagIndexReconcileGrain.InitSweepStateAsync`, `TagIndexReconcileGrain.DrainActiveSweepAsync`. | Yes: `TagIndexReconcileGrainTests.On_demand_finishes_the_active_repair_before_starting_its_fresh_sweep`. |
| `ProbeChunk` | Probe a bounded number of covered trees; do not bank baselines. | `TagIndexReconcileGrain.ProcessProbeChunkAsync`. | Yes: `TagIndexReconcileGrainTests.Phase_callback_bounds_probe_work_and_preserves_progress_across_reminders`. |
| `BeginRepair` | Enter repair only for a non-audit divergent sweep. | `TagIndexReconcileGrain.ProcessProbeChunkAsync`. | Yes: `TagIndexReconcileGrainTests.Sweeps_reprobe_subject_changes_without_certifying_unrepaired_fingerprints`. |
| `RepairStart` | Begin the fallible RPC without advancing the cursor. | `TagIndexReconcileGrain.ProcessRepairChunkAsync`. | Yes: `TagIndexReconcileGrainTests.ProcessNextPhase_failed_repair_retries_the_same_tree_before_committing_progress`. |
| `CompleteRepair` | All required reads, writes and checkpoint complete successfully. | `TagIndexReconcileGrain.ProcessRepairChunkAsync`, `LatticeTagIndexContext.ReconcileSubjectAsync`. | Yes: `TagIndexReconcileGrainTests.ReceiveReminder_incomplete_repair_does_not_bank_a_baseline_and_recovers_with_unchanged_digest`, `TagIndexReconcileGrainTests.Sweeps_reprobe_subject_changes_without_certifying_unrepaired_fingerprints`. |
| `FailRepair` | Fail the RPC/checkpoint and retain the retry boundary. | `TagIndexReconcileGrain.ProcessRepairChunkAsync`, `TagIndexReconcileGrain.FinishSweepAsync`. | Yes: `TagIndexReconcileGrainTests.ProcessNextPhase_failed_repair_checkpoint_restores_retryable_state_and_durable_progress`. |
| `Finish` | Persist Idle before releasing the pump or beginning the queued sweep. | `TagIndexReconcileGrain.FinishSweepAsync`, `TagIndexReconcileGrain.RunSweepAsync`. | Yes: `TagIndexReconcileGrainTests.Cooperative_reminder_demand_and_subject_changes_preserve_sweep_progress`, `TagIndexReconcileGrainTests.ProcessNextPhase_failed_repair_checkpoint_restores_retryable_state_and_durable_progress`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `BaselineOnlyAfterRepair` | Only a complete successful repair certifies its probe snapshot; incomplete gate reads cannot bank completion. | Yes: `TagIndexReconcileGrainTests.ReceiveReminder_incomplete_repair_does_not_bank_a_baseline_and_recovers_with_unchanged_digest`, `TagIndexReconcileGrainTests.Sweeps_reprobe_subject_changes_without_certifying_unrepaired_fingerprints`. |
| `ProbeOnlyNeverRepairs` | Audit sweeps never repair or establish a repair baseline. | Yes: `TagIndexReconcileGrainTests.Sweeps_reprobe_subject_changes_without_certifying_unrepaired_fingerprints`. |
| `SingleSweep` | Reminder/demand cannot start a second sweep over a live cursor; the queued demand gets a fresh sweep after the old terminal write. | Yes: `TagIndexReconcileGrainTests.On_demand_finishes_the_active_repair_before_starting_its_fresh_sweep`, `TagIndexReconcileGrainTests.Cooperative_reminder_demand_and_subject_changes_preserve_sweep_progress`. |
| `ChunkBounded` | A phase callback probes at most the clamped ChunkSize trees or repairs one tree. | Yes: `TagIndexReconcileGrainTests.Phase_callback_bounds_probe_work_and_preserves_progress_across_reminders`. |
| `RepairCursorOnlyAfterCompletion` | A failed repair/checkpoint cannot consume a tree on the next live tick or reactivation. | Yes: `TagIndexReconcileGrainTests.ProcessNextPhase_failed_repair_retries_the_same_tree_before_committing_progress`, `TagIndexReconcileGrainTests.ProcessNextPhase_failed_repair_checkpoint_restores_retryable_state_and_durable_progress`. |
| `DivergenceEventuallyRepaired` | Stable subject changes converge when recurring schedule, registry, storage and phase deliveries continue successfully. | Yes: `TagIndexReconcileGrainTests.Sweeps_reprobe_subject_changes_without_certifying_unrepaired_fingerprints`, `TagIndexReconcileGrainTests.Schedule_recovers_the_pump_after_initial_keepalive_registration_failed`. |
| `SweepTerminates` | A finite covered set and finite transient failures eventually drain; reminders never rewind it. | Yes: `TagIndexReconcileGrainTests.Cooperative_reminder_demand_and_subject_changes_preserve_sweep_progress`, `TagIndexReconcileGrainTests.Phase_callback_bounds_probe_work_and_preserves_progress_across_reminders`. |
| `TriggerNotLost` | A queued synchronous on-demand call returns only after its own fresh sweep completes. | Yes: `TagIndexReconcileGrainTests.On_demand_finishes_the_active_repair_before_starting_its_fresh_sweep`, `TagIndexReconcileGrainTests.Cooperative_reminder_demand_and_subject_changes_preserve_sweep_progress`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

## Property classification

TypeOK, ProbeOnlyNeverRepairs and ChunkBounded are state safety.
BaselineOnlyAfterRepair, SingleSweep and RepairCursorOnlyAfterCompletion
are action safety. DivergenceEventuallyRepaired, SweepTerminates and
TriggerNotLost are temporal liveness. ThreeTrees retains every state/action
check; only the temporal checks are bounded-out there for the TLC liveness
budget, with every temporal check retained on the base.

Every experiment is an anchored action perturbation, not a transition splice:

| Mutation | Classification |
|----------|----------------|
| TypeOKInvalidDemand | Anchored action: Demand. |
| BaselineOnlyAfterRepairBaselineFirst | Anchored action: ProbeChunk. |
| BaselineOnlyAfterRepairEnvironmentBanks | Anchored action: Mutate. |
| SingleSweepDemandResetsCursor | Anchored action: Demand. |
| SingleSweepBeginWhileActive | Anchored action: BeginSweep. |
| ChunkBoundedUnboundedChunk | Anchored action: ProbeChunk. |
| ProbeOnlyNeverRepairsRepairsInProbe | Anchored action: BeginRepair. |
| SweepTerminatesResetCursorOnReminder | Anchored action: Reminder. |
| SweepTerminatesNeverEntersRepair | Anchored action: BeginRepair. |
| DivergenceEventuallyRepairedReminderNeverReArms | Anchored action: Reminder. |
| TriggerNotLostRearmDropsPump | Anchored action: Rearm. |
| TriggerNotLostTriggerOverwritten | Anchored action: Finish. |
| RepairCursorOnlyAfterCompletionCursorFirst | Anchored action: RepairStart. |
| RepairCursorOnlyAfterCompletionFailedCheckpoint | Anchored action: FailRepair. |
| BaselineOnlyAfterRepairWrongFingerprint | Anchored action: CompleteRepair. |

## Detectors

The cooperative Coyote detector drives the real grain methods and context
against completed, state-backed collaborators. Choices interleave subject
changes, reminder deliveries, phase callbacks and on-demand calls at the
non-reentrant message boundary. A finite delivery tail is explicit, not random
luck or a scheduler-fairness claim. The harness requires every collaborator
task to have completed before synchronously observing it; no uncontrolled async
task is blocked inside Coyote. Separate async tests exercise registration
failure, gate-pruned reads and failed RPC/checkpoint retries through the real
production paths. These detectors do not substitute a second phase machine.

### Executed production negative controls

Each control below edits the real grain, runs the named mapped detector family,
requires an assertion failure (not a build failure), restores the exact source
bytes, then reruns the clean production scope. Cooperative controls run both
audit and repair instances; their counterexamples retain the Coyote trace.

| Production perturbation | Detecting family | Observed failure |
|-------------------------|------------------|------------------|
| Omit active-sweep coordinator restart. | Schedule recovery. | Registration called once instead of twice. |
| Reset the old sweep instead of draining it. | On-demand and cooperative Coyote. | Probe published before old Idle; SingleSweep trace. |
| Ignore the probe chunk setting. | Phase callback bounds. | Three trees observed in a one/two-tree callback. |
| Bank a baseline during probe. | Phase bounds, re-probe and Coyote. | Baseline changes before repair. |
| Ignore ProbeOnly at repair entry. | Re-probe and Coyote. | Audit publishes a repair baseline. |
| Advance the repair cursor before the RPC. | Failed-repair retry. | Failed tree consumed without completion. |
| Retain the cursor after checkpoint failure. | Failed-checkpoint retry. | Uncommitted tree consumed after a failed write. |
| Bank the wrong repair fingerprint. | Re-probe. | Published baseline differs from pending snapshot. |
| Return after draining only the prior sweep. | On-demand. | One terminal write instead of two. |
| Never enter the repair phase. | Phase callback bounds. | Probe makes no progress and cannot drain. |
| Rewind the cursor on a schedule reminder. | Phase bounds and Coyote. | Cursor regression; SingleSweep trace. |
| Skip the whole-tree repair RPC but bank completion. | Re-probe and Coyote. | Orphan remains after final stable sweep; divergence trace. |

The separate #4992 cancellation regression protects ownership of the old scheduled
sweep: a cancelled tracked request must not abandon work it has not started.
It was red with an unconditional cancellation cleanup and green after cleanup
was restricted to the new request's own sweep. It is separate from the
keepalive-registration defect and does not claim an independently proved
on-demand data-loss incident.

## Defect mutations whose fixes have landed

RepairCursorOnlyAfterCompletionCursorFirst and
RepairCursorOnlyAfterCompletionFailedCheckpoint reinstate the cursor/checkpoint
failure mechanism repaired by [PR #4978](https://github.com/NSTA1/Orleans.Lattice/pull/4978).
The mapped original production regressions failed before that fix and pass
with it. The complete-read boundary also relies on
[PR #4980](https://github.com/NSTA1/Orleans.Lattice/pull/4980):
its real scheduled detectors reject success-shaped incomplete gate reads.
This is integration-branch evidence, not a claim those repairs are released.

## Replaced draft assertions

The draft's "per-turn" bound conflates three different units. ChunkSize bounds
trees probed by one phase callback, and repair handles one entire tree per
callback. RunSweepAsync intentionally drains several callbacks synchronously;
neither that caller's exclusive turn nor a tree's scan duration is bounded by
ChunkSize. The model checks the exact callback work unit and does not promise
row, byte or latency bounds.

Weak fairness cannot establish bounded latency. RuntimeStep is a delivered
activation opportunity: its phase guards are disjoint and execute the
production-selected step, not a fair choice among competing policies. Reminder
delivery, reminder-registry success and storage completion are environment
premises. One failed repair is modelled explicitly; arbitrary permanent faults
are outside the convergence premise. Subject changes are finite and monotone.
Probe-only has no divergence-repair obligation. Reminder overlap coalesces into
the active sweep rather than acknowledging every periodic tick separately.
On-demand arrival is queued by Orleans, and its acknowledgement is after a
fresh sweep, never a second concurrently running coordinator.

## Deliberate abstraction gaps

Fixed covered-tree membership and fixed per-instance ProbeOnly/ChunkSize;
no disabled schedule, cancellation, repeated process crashes, digest
unavailability/collisions or unbounded changing subjects. An accepted call is
not made durable before Orleans dispatches it: client retry is required if the
activation dies. The model does not claim exactly-once RPC delivery. Tree repair
is opaque, including flag convergence, mirror reconstruction, bounded read/write
windows and re-verification races. Their real context regressions remain
required independently; this model only treats complete successful return as
repair completion. Environment fairness is conditional, never an uptime or
timer-delivery guarantee.

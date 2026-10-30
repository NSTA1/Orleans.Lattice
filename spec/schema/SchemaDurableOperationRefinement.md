# Refinement note: durable schema operations

## Model scope in one paragraph

Tracked schema operations - remediation, eager migration and
advance-and-migrate - over one tree. A start is admitted or refused before any
operation record exists; an admitted start creates a durable queued record,
returns its handle, and launches the work in the accepting silo's process. The
work accepts on the tree's durable remediation coordinator, which either
records a new remediation, lets the start follow the one already in flight with
the same parameters, or refuses one with different parameters in-band. The
work then drives the remediation it follows one bounded slice at a time
(dry run, build, cutover swap, completion), persisting a cursor after each
slice, and maps the terminal report to the recorded outcome. A cancel request
is recorded durably and observed by the runner, which cancels the remediation
it follows while it is in its dry run or build; cutover is the point of no
return. A runner's silo can die, losing the work but not the record or the
remediation: the record then reads failed, and a fresh start with the same
parameters follows the remediation still in flight and drives it on from its
last slice. A lapsed heartbeat lease fails a record whose runner is alive. The
model checks the follower contract #5267 fixes: a driver never slices, cancels
or reports a remediation it does not follow, and an operation whose followed
remediation ended under another operation reports that outcome or that it was
superseded, naming the followed remediation.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `alive`, `crashes` | Which silos are alive; silo deaths so far. | `ILatticeOperationSiloLiveness.IsDead`, `ClusterMembershipOperationSiloLiveness.IsDead`. |
| `stalls` | Heartbeat leases that have lapsed while a runner lived. | `LatticeOperationGrain.HeartbeatAsync`, `LatticeOperationOptions.HeartbeatLease`. |
| `rejects` | Starts refused at admission. | `SchemaOperationService.StartRemediationAsync`, `SchemaOperationService.StartMigrationAsync`, `SchemaOperationService.StartAdvanceAndMigrateAsync`. |
| `handles`, `launches` | Ghosts: the handles returned to callers and how often each id's work was launched. | `LatticeOperationLaunch`, `LatticeOperationRunner.StartAsync`. |
| `opState`, `opOutcome`, `opResultRem`, `opRunner`, `opParams`, `opCancel` | The durable operation record: state, recorded outcome and the remediation its result names, the runner silo, the start's parameters, and the cancel request. | `LatticeOperationGrain`, `LatticeOperationRecord`, `SchemaOperationResultKeys.RemediationOperationId`. |
| `opWork`, `opFollows`, `opSeen` | The in-process work: accepting, driving, reporting or done; the remediation it follows; the terminal report its loop ended with. | `LatticeOperationRunner.StartAsync`, `SchemaRemediationDriver.DriveAsync`. |
| `bad`, `remInProgress`, `remOwner`, `remParams`, `remPhase`, `remCursor`, `remLast`, `remResult`, `cutOver` | The tree's durable remediation: the unremediable value, whether one is in flight, its identity, parameters, phase and cursor, the last terminal report, each remediation's outcome, and which ones cut the tree over. | `LatticeSchemaRemediationGrain`, `SchemaRemediationState`, `LatticeSchemaRemediationGrain.GetStatusAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `StartRejected(o,s)` | Admission refuses a start (authority, installed support, an uncompilable policy, a missing version config or descriptor, a target that does not advance, an id held by another kind or scope) before any record exists. | `SchemaOperationService.StartRemediationAsync`, `SchemaOperationService.StartMigrationAsync`, `SchemaOperationService.StartAdvanceAndMigrateAsync`, `LatticeSchemaControl.StartRemediationAsync`, `LatticeOperationGrain.BeginAsync`. | Yes: `SchemaOperationServiceTests.StartRemediationAsync_refuses_an_uncompilable_policy_before_accepting_anything`, `SchemaOperationServiceTests.StartMigrationAsync_and_StartAdvanceAndMigrateAsync_refuse_when_versioning_is_not_registered`, `LatticeSchemaOperationsIntegrationTests.An_advance_that_does_not_advance_rejects_before_creating_an_operation`, `LatticeSchemaOperationsIntegrationTests.Missing_migration_configuration_rejects_before_an_operation_is_registered`, `LatticeOperationGrainTests.Begin_again_with_a_different_kind_is_refused`. |
| `Begin(o,s,p)` | An admitted start creates the queued record and launches the work in the accepting silo. | `LatticeOperationRunner.StartAsync`, `LatticeOperationGrain.BeginAsync`. | Yes: `LatticeOperationGrainTests.Begin_records_a_queued_operation_and_indexes_it`, `LatticeOperationRunnerTests.Start_runs_the_work_and_records_its_success`. |
| `Retry(o)` | A start with an existing id returns that operation and starts nothing. | `LatticeOperationGrain.BeginAsync`, `LatticeOperationRunner.StartAsync`. | Yes: `LatticeOperationGrainTests.Begin_again_with_the_same_kind_is_idempotent`, `LatticeOperationRunnerTests.Starting_an_existing_id_starts_nothing`, `LatticeSchemaOperationsIntegrationTests.A_retried_start_with_the_same_id_starts_nothing`. |
| `Accept(o)` | The work accepts on the coordinator: a new remediation, the in-flight one with the same parameters, or an in-band refusal of different parameters. | `LatticeSchemaRemediationGrain.AcceptAsync`, `LatticeSchemaRemediationGrain.AcceptVersionMigrationAsync`. | Yes: `LatticeSchemaRemediationGrainTests.AcceptAsync_persists_intent_without_scanning_and_records_the_operation_id`, `LatticeSchemaRemediationGrainTests.AcceptAsync_with_the_in_flight_parameters_returns_that_operation_and_accepts_nothing_new`, `LatticeSchemaRemediationGrainTests.StartAsync_different_parameters_while_in_flight_throws_invalid_operation`. |
| `RequestCancel(o)` | A cancel request is recorded durably on the operation. | `LatticeOperationRunner.RequestCancelAsync`, `LatticeOperationGrain.RequestCancelAsync`. | Yes: `LatticeOperationGrainTests.Report_and_heartbeat_tell_the_runner_to_stop_once_cancellation_is_requested`, `LatticeOperationRunnerTests.A_local_cancel_request_cancels_the_work_and_records_cancelled`. |
| `DriveCancel(o)` | The driver, its token cancelled, cancels the remediation it follows while that one is in its dry run or build. | `SchemaRemediationDriver.DriveAsync`, `LatticeSchemaRemediationGrain.CancelAsync`, `LatticeOperationProgressSink.ReportAsync`. | Yes: `SchemaRemediationDriverTests.DriveAsync_turns_a_cancellation_into_a_cancel_of_the_followed_remediation`, `LatticeSchemaRemediationGrainTests.CancelAsync_during_the_build_discards_the_destination_and_records_cancelled`, `LatticeSchemaRemediationGrainTests.CancelAsync_for_another_operation_leaves_the_remediation_running`, `LatticeOperationRunnerTests.A_stop_signal_on_the_heartbeat_cancels_the_work`. |
| `Slice(o)` | The driver runs one bounded, persisted slice of the remediation it follows. | `LatticeSchemaRemediationGrain.RunSliceAsync`, `SchemaRemediationDriver.DriveAsync`. | Yes: `LatticeSchemaRemediationGrainTests.RunSliceAsync_processes_one_bounded_slice_at_a_time_through_every_phase`, `SchemaRemediationDriverTests.DriveAsync_reports_each_slice_phase_values_and_total_until_terminal`, `LatticeSchemaRemediationGrainTests.StartAsync_dry_run_violation_aborts_with_no_cutover_and_leaves_original_untouched`, `LatticeSchemaRemediationGrainTests.A_joined_driver_does_not_slice_or_report_a_later_remediation_after_the_followed_one_ended`, `SchemaRemediationDriverTests.DriveAsync_stops_without_reporting_a_slice_of_another_operations_remediation`. |
| `Observe(o)` | A driver whose followed remediation ended under another driver is returned its report, or ends as superseded when another remediation has since been accepted. | `SchemaRemediationDriver.DriveAsync`, `SchemaRemediationDriver.IsSuperseded`, `LatticeSchemaRemediationGrain.RunSliceAsync`. | Yes: `LatticeSchemaRemediationGrainTests.A_joined_driver_does_not_slice_or_report_a_later_remediation_after_the_followed_one_ended`, `LatticeSchemaRemediationGrainTests.A_joined_driver_reports_the_followed_remediation_outcome_while_the_coordinator_still_holds_it`, `SchemaRemediationDriverTests.DriveAsync_stops_without_reporting_a_slice_of_another_operations_remediation`, `SchemaRemediationDriverTests.DriveAsync_reports_the_followed_remediations_terminal_report_the_coordinator_still_holds`. |
| `Finish(o)` | The runner maps the report its loop ended with to the recorded completion, a superseded follower to a failure naming the followed remediation; a record already terminal keeps its outcome. | `SchemaOperationService.ToCompletion`, `SchemaRemediationDriver.SupersededReason`, `SchemaOperationResultKeys.Superseded`, `LatticeOperationGrain.CompleteAsync`. | Yes: `SchemaOperationServiceTests.ToCompletion_records_a_completed_remediation_as_succeeded`, `SchemaOperationServiceTests.ToCompletion_records_an_aborted_remediation_as_failed_naming_the_offending_value`, `SchemaOperationServiceTests.ToCompletion_records_a_cancelled_remediation_as_cancelled`, `LatticeOperationGrainTests.Complete_records_the_outcome_once_and_marks_the_index`. |
| `Crash(s)` | A silo dies; the work in its process is lost, the record and the remediation are durable. | `LatticeOperationRunner.StartAsync`, `LatticeOperationGrainState`, `SchemaRemediationState`. | Yes: `SchemaOperationSiloLossChaosTests.A_remediation_whose_runner_silo_is_killed_fails_then_resumes_when_started_again`, `LatticeOperationSiloLossChaosTests.An_operation_whose_runner_silo_is_killed_reads_as_failed_from_a_survivor`. |
| `DetectLoss(o)` | A read of a live record whose runner's silo is dead records it failed. | `LatticeOperationGrain.GetAsync`, `ILatticeOperationSiloLiveness.IsDead`. | Yes: `LatticeOperationGrainTests.A_running_operation_whose_runner_silo_is_dead_reads_as_failed`, `SchemaOperationSiloLossChaosTests.A_remediation_whose_runner_silo_is_killed_fails_then_resumes_when_started_again`. |
| `LeaseExpire(o)` | A read of a live record past its heartbeat lease records it failed although its runner lives; the runner then stops. | `LatticeOperationGrain.GetAsync`, `LatticeOperationGrain.HeartbeatAsync`. | Yes: `LatticeOperationGrainTests.A_running_operation_past_its_heartbeat_lease_reads_as_failed`, `LatticeOperationGrainTests.Reports_on_a_missing_or_finished_operation_tell_the_runner_to_stop`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `HandleHasRecord` | A handle is returned only for an operation record that exists. | Yes: `LatticeOperationRunnerTests.Start_runs_the_work_and_records_its_success`, `SchemaOperationServiceTests.StartRemediationAsync_refuses_an_uncompilable_policy_before_accepting_anything`, `SchemaOperationSiloLossChaosTests.A_remediation_whose_runner_silo_is_killed_fails_then_resumes_when_started_again`. |
| `OneExecutionPerId` | One operation id runs its work at most once. | Yes: `LatticeOperationRunnerTests.Starting_an_existing_id_starts_nothing`, `LatticeSchemaOperationsIntegrationTests.A_retried_start_with_the_same_id_starts_nothing`. |
| `SucceededMeansCutOver` | An operation reads succeeded only when its remediation completed and cut the tree over. | Yes: `SchemaOperationServiceTests.ToCompletion_records_a_completed_remediation_as_succeeded`, `SchemaOperationServiceTests.ToCompletion_of_a_non_terminal_report_records_a_failure`, `LatticeSchemaRemediationGrainTests.StartAsync_all_values_remediate_cuts_over_and_installs_policy`. |
| `NothingCutOverUnlessCompleted` | An aborted or cancelled remediation never cut the tree over; a cancel at cutover is declined. | Yes: `LatticeSchemaRemediationGrainTests.CancelAsync_at_cutover_is_declined_and_the_remediation_completes`, `LatticeSchemaRemediationGrainTests.StartAsync_dry_run_violation_aborts_with_no_cutover_and_leaves_original_untouched`, `LatticeSchemaOperationsIntegrationTests.Cancelling_a_remediation_held_in_its_build_cancels_it_and_leaves_the_tree_untouched`. |
| `OutcomeMatchesRemediation` | The recorded outcome and state are the ones the remediation's terminal report maps to. | Yes: `SchemaOperationServiceTests.ToCompletion_records_an_aborted_remediation_as_failed_naming_the_offending_value`, `SchemaOperationServiceTests.ToCompletion_records_a_cancelled_remediation_as_cancelled`, `LatticeSchemaOperationsIntegrationTests.A_remediation_that_meets_an_unremediable_value_fails_naming_it_with_nothing_cut_over`. |
| `OutcomeOfFollowed` | An operation reports the outcome of the remediation it accepted or followed, or that it was superseded, naming that remediation; never another's. | Yes: `LatticeSchemaRemediationGrainTests.A_joined_driver_does_not_slice_or_report_a_later_remediation_after_the_followed_one_ended`, `LatticeSchemaRemediationGrainTests.A_cancelled_joined_driver_does_not_cancel_slice_or_report_a_later_remediation`, `SchemaRemediationDriverTests.DriveAsync_stops_without_reporting_a_slice_of_another_operations_remediation`, `SchemaRemediationDriverTests.RequireTerminal_throws_for_a_superseded_report_and_returns_a_terminal_one`. |
| `CancelledOnlyOnRequest` | A remediation is cancelled only for an operation that follows it and whose cancel was requested or whose record was declared lost. | Yes: `LatticeSchemaRemediationGrainTests.CancelAsync_for_another_operation_leaves_the_remediation_running`, `SchemaRemediationDriverTests.DriveAsync_turns_a_cancellation_into_a_cancel_of_the_followed_remediation`, `SchemaRemediationDriverTests.DriveAsync_stops_when_a_cancel_finds_another_operations_remediation`. |
| `RejectedStartCreatesNothing` | A start refused at admission creates no record, handle, work or remediation. | Yes: `SchemaOperationServiceTests.StartRemediationAsync_refuses_an_uncompilable_policy_before_accepting_anything`, `LatticeSchemaOperationsIntegrationTests.An_advance_that_does_not_advance_rejects_before_creating_an_operation`, `LatticeSchemaOperationsIntegrationTests.Missing_migration_configuration_rejects_before_an_operation_is_registered`. |
| `TerminalIsFinal` | A terminal record never changes: a late completion after loss is ignored. | Yes: `LatticeOperationGrainTests.Complete_records_the_outcome_once_and_marks_the_index`, `LatticeOperationGrainTests.Cancel_of_a_missing_operation_returns_null_and_of_a_finished_one_is_unchanged`. |
| `InFlightRemediationKept` | An in-flight remediation keeps its identity and parameters, and its durable progress never moves backwards, whoever drives it. | Yes: `LatticeSchemaRemediationGrainTests.AcceptAsync_with_the_in_flight_parameters_returns_that_operation_and_accepts_nothing_new`, `LatticeSchemaRemediationGrainTests.StartAsync_same_parameters_while_in_flight_resumes_idempotently`, `LatticeSchemaRemediationGrainTests.A_faulted_build_slice_banks_the_values_it_wrote_and_the_next_slice_resumes_after_them`, `SchemaOperationSiloLossChaosTests.A_remediation_whose_runner_silo_is_killed_fails_then_resumes_when_started_again`. |
| `DrivenOnlyByFollowers` | A remediation advances only while a live driver follows it; no driver slices a remediation it does not follow. | Yes: `LatticeSchemaRemediationGrainTests.A_joined_driver_does_not_slice_or_report_a_later_remediation_after_the_followed_one_ended`, `LatticeSchemaRemediationGrainTests.A_cancelled_joined_driver_does_not_cancel_slice_or_report_a_later_remediation`, `SchemaRemediationDriverTests.DriveAsync_stops_when_a_cancel_finds_another_operations_remediation`. |
| `LostRunnerReadsFailed` | An operation whose runner's silo is lost eventually reads as failed. | Yes: `LatticeOperationGrainTests.A_running_operation_whose_runner_silo_is_dead_reads_as_failed`, `LatticeOperationSiloLossChaosTests.An_operation_whose_runner_silo_is_killed_reads_as_failed_from_a_survivor`. |
| `OperationTerminates` | Every accepted operation reaches a terminal state. | Yes: `LatticeOperationRunnerTests.A_failing_work_item_records_failure_banks_progress_and_rethrows_its_own_exception`, `LatticeOperationGrainTests.A_running_operation_past_its_heartbeat_lease_reads_as_failed`, `SchemaRemediationDriverTests.DriveAsync_drives_on_to_completion_when_cutover_declines_the_cancel`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, HandleHasRecord, OneExecutionPerId, SucceededMeansCutOver,
NothingCutOverUnlessCompleted, OutcomeMatchesRemediation, OutcomeOfFollowed
and CancelledOnlyOnRequest are state safety. RejectedStartCreatesNothing,
TerminalIsFinal, InFlightRemediationKept and DrivenOnlyByFollowers are action
safety. LostRunnerReadsFailed and OperationTerminates are temporal liveness
under weak fairness of each operation's runner steps and of loss detection
(status polling). The ThreeOps variant re-checks every state and action check
with a third operation, which is the smallest bound where a followed
remediation can be superseded; the TwoParams variant re-checks them with a
second parameter set, which reaches the in-band refusal of a start whose
parameters differ from the in-flight remediation. Both leave the temporal
checks on the base for the TLC liveness budget.

Every experiment is an anchored action perturbation:

| Mutation | Classification |
|----------|----------------|
| TypeOKBeginUndeclaredState | Anchored action: Begin. |
| HandleHasRecordRejectedHandle | Anchored action: StartRejected. |
| RejectedStartCreatesNothingRecordFirst | Anchored action: StartRejected. |
| OneExecutionPerIdRetryRelaunches | Anchored action: Retry. |
| SucceededMeansCutOverSkipsSwap | Anchored action: Slice. |
| NothingCutOverUnlessCompletedCancelAtCutover | Anchored action: DriveCancel. |
| OutcomeMatchesRemediationEveryReportSucceeds | Anchored action: Finish. |
| OutcomeMatchesRemediationCancelRecordsAtOnce | Anchored action: RequestCancel. |
| OutcomeOfFollowedDrivesAnother | Anchored action: Slice, Observe. |
| CancelledOnlyOnRequestCancelsAnother | Anchored action: DriveCancel. |
| TerminalIsFinalLateCompletionOverwrites | Anchored action: Finish. |
| InFlightRemediationKeptJoinRestarts | Anchored action: Accept. |
| DrivenOnlyByFollowersSlicesAnother | Anchored action: Slice. |
| LostRunnerReadsFailedNeverDetected | Anchored action: DetectLoss. |
| HandleHasRecordLostWithSilo | Anchored action: Crash. |
| OperationTerminatesFollowerNeverReturns | Anchored action: Observe. |
| TerminalIsFinalStallRewritesFinished | Anchored action: LeaseExpire. |

## Detectors

The operation-record detectors drive the real `LatticeOperationGrain` with a
controllable clock and liveness, and the real `LatticeOperationRunner` against
a fake record, so they observe exactly the admission, idempotent start,
completion, loss and lease transitions the actions name. The remediation
detectors drive the real coordinator grain over substituted trees one slice at
a time, and the driver detectors drive `SchemaRemediationDriver` against a
scripted coordinator. `LatticeSchemaOperationsIntegrationTests` starts, polls,
retries and cancels operations through the schema facade on a real cluster.
The two silo-loss chaos fixtures kill a runner's silo mid-build and observe the
record fail and a fresh start resume the remediation; they run in the chaos
tier, and every row that cites one also cites a deterministic detector.

## Defect mutations whose fixes have landed

LostRunnerReadsFailedNeverDetected is the shape #4123 removed: an operation
whose runner's silo was lost read running forever. HandleHasRecordRejectedHandle
and RejectedStartCreatesNothingRecordFirst are the shape #4966 removed: a start
whose prerequisites were missing returned a handle to an operation that failed
at once instead of being refused. OutcomeOfFollowedDrivesAnother and
DrivenOnlyByFollowersSlicesAnother are the #5267 defect this model found: a
driver that joined a remediation sliced, and then reported the outcome of, a
remediation another operation started after the followed one ended. It is
fixed in the schema-assurance integration bucket; this is integration-branch
evidence, not a claim the fix is released.

## Deliberate abstraction gaps

- **Values and slices.** A remediation has a bounded number of values, one
  value per slice, and at most the last one unremediable. Real slices batch up
  to a configured size, a build-time violation aborts like a dry-run one, and
  the transform and policy semantics are those of the stage-ordering model.
  Production symbols: `LatticeSchemaRemediationGrain.DefaultSliceSize`.
- **One tree, one remediation kind.** The model tracks one tree and treats
  remediation, migration and advance-and-migrate as one remediation kind with
  abstract parameters. The advance step of advance-and-migrate, the no-op
  success of a migration to a version already reached, and migration upcast
  aborts are owned by the version-family and remediation models.
  Production symbols: `SchemaOperationService.StartAdvanceAndMigrateAsync`, `LatticeSchemaRemediationGrain.AcceptVersionMigrationAsync`.
- **Cutover, reservation and destination.** The cutover is one swap step and
  one completion step; the alias reservation a remediation holds while in
  flight, the shadow destination, policy installation and the shard-map swap
  are owned by the remediation and cutover model.
  No production symbol: the gap delegates to another model, not to a production mechanism.
- **No autonomous resume.** A remediation whose runner was lost stays in
  flight until a fresh start follows it; nothing resumes it on its own, so
  liveness of the remediation itself depends on an operator restarting it and
  is not checked.
  Production symbols: `LatticeSchemaRemediationGrain.RunRemediationPassAsync`.
- **A lapsed lease over a live runner.** A record failed by a lapsed lease
  while its runner lives keeps that outcome; the runner then observes the
  terminal record as a stop signal and cancels the remediation if it is still
  in its dry run or build, but past that point the remediation completes and
  the tree's remediation report, not the operation record, is authoritative.
  No production symbol: the gap states a consequence of first-terminal-wins, not a separate mechanism.
- **Record retention, the index and unrecorded outcomes.** Retention pruning,
  the operation index and its outbox, progress coalescing, and an outcome the
  runner could not record (later failed by the lease) are not modelled; a
  pruned id can be begun again, which the model's one-launch-per-id ghost does
  not represent.
  Production symbols: `LatticeOperationOptions.Retention`, `LatticeOperationRunner.ListAsync`, `LatticeOperationProgressSink`.
- **Status reads under silo loss.** A status read can itself time out while the
  cluster reconverges after a kill (#4842); polling retries the read, which the
  model abstracts as weak fairness of loss detection.
  No production symbol: the gap states a premise of the model, not a production mechanism.
- **A remediation accepted with no operation id.** Only the in-grain blocking
  start accepts a remediation without an operation id, and it drives that
  remediation to its end within one grain turn; a driver following such a
  remediation has no identity to compare and keeps the unnamed slice call. The
  model gives every remediation the id of the operation that accepted it.
  Production symbols: `LatticeSchemaRemediationGrain.StartAsync`.
- **Ids taken in order.** Operation ids are started in a fixed order to keep
  the state space small; ids are names with no protocol meaning.
  No production symbol: the gap states a bound of the model, not a production mechanism.

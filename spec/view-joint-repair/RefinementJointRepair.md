# Joint-repair refinement

This is a bounded documented abstraction, not a machine-checked refinement
proof. Coordinator API regressions establish coordinator behaviours only.
Pending caller rows retain a resolvable seam detector but explicitly do not
claim that it proves the maintainer or actual joint-publication behaviour.
There is no supported shared multi-view snapshot cursor; independent consumer
opens can straddle a valid joint commit. No join-view or view-DAG API is implied.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `legacy` | Historical original contract versus current mandatory repair | `ViewCrossTreeCoordinatorState.RepairProtocolVersion`, `ViewCrossTreeDecision.RequiresRepair`; first admission and cluster capability are separately guarded. |
| `registered`, `decided`, `applied`, `degraded` | Original readiness and durable joint decision/completion | `ViewCrossTreeCoordinatorState.Slices`, `ViewCrossTreeCoordinatorState.Decided`, `ViewCrossTreeCoordinatorState.Applied`, `ViewCrossTreeCoordinatorState.Degraded`. |
| `captured`, `target`, `repairDecided`, `repairApplied` | Independently immutable fresh readiness and repair decision | `ViewCrossTreeCoordinatorState.RepairSlices`, `ViewCrossTreeCoordinatorState.RepairDecided`, `ViewCrossTreeCoordinatorState.RepairApplied`. |
| `first`, `firstTarget` | Ghost witnesses of first submitted payload and physical target | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`, `ViewCrossTreeCoordinatorGrain.GetRepairReadinessAsync`. |
| `acknowledged`, `cleared` | Durable participant acknowledgements and retention cleanup | `ViewCrossTreeCoordinatorState.RepairAcknowledged`, `ViewCrossTreeCoordinatorGrain.AcknowledgeRepairAsync`, `ViewCrossTreeCoordinatorGrain.OnTtlExpiredAsync`. |
| `source` | Coherent admitted current source tuple per participant | Caller source-admission/replacement-closure rely, not inferred from projected rows. |
| `bound`, `retired` | Physical identity and durable checkpoint retirement | Caller durable hold/rebind fence and successful checkpoint-write rely; implementation acceptance pending. |
| `rows`, `stage`, `ordinarySeen` | Joint installed effects and legitimate later ordinary successors | `ILatticeCrossTreeTxGrain.CommitAsync`, `ILattice.SetManyAtomicAsync`; backing publication/idempotency is abstracted. |
| `reported`, `faults` | Returned success and bounded response/persistence failures | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`, `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`; fault counter is a modelling device. |
| `observed`, `snapshotRows`, `snapshotOrdinary` | Conceptual joint-publication cut and row/phase witnesses | `ViewCrossTreeCoordinatorGrain.IssueJointFlipAsync`; runtime evidence must hold/capture the actual joint write, not infer a common cut from independent consumer opens. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Register` | Admit original participant | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_incomplete_wait_set_returns_not_ready_and_persists` checks the missing peer remains undecided and persisted. |
| `DecideOriginal` | Persist original joint intent | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterDegradedAsync_a_decided_publication_cannot_abandon_its_joint_repair_obligation`. |
| `PublishOriginal` | Apply original joint effect | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`. | Partial: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision`; actual joint-publication witness acceptance remains pending under #4876. |
| `CompleteOriginal` | Persist original completion before success | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_failed_completion_persistence_cannot_memoize_an_undurable_result`. |
| `Timeout` | Degrade only before durable original intent | `ViewCrossTreeCoordinatorGrain.RegisterDegradedAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterDegradedAsync_a_decided_publication_cannot_abandon_its_joint_repair_obligation`. |
| `Advance` | Admit newer coherent source replacement | Caller admission rely. | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision` checks submitted replacement bytes, not source admission or replacement closure (#4876). |
| `Capture` | Freeze first fresh source-derived slice and physical target | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Partial: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation`; caller horizon/closure and later-source perturbation remain pending (#4876). |
| `Retry` | Recover/resubmit immutable stored readiness without replaying completed repair | `ViewCrossTreeCoordinatorGrain.GetRepairReadinessAsync`, `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `DecideRepair` | Require every original participant's fresh contribution | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision`. |
| `PublishRepair` | One fresh joint operation applies all frozen slices | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Partial: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision`; held actual joint-write cut witnesses require caller acceptance (#4876). |
| `CompleteRepair` | Persist repair completion before success | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `PersistFailure` | Failed completion does not certify success | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`, `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_failed_completion_persistence_cannot_memoize_an_undurable_result`, `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `Crash` | Preserve durable decision, capture and target through reactivation | `ViewCrossTreeCoordinatorState`, `ViewCrossTreeCoordinatorGrain.GetRepairReadinessAsync`. | Partial: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation`; maintainer hold/pending-ack restart is separate pending acceptance (#4876). |
| `Retire` | Successful checkpoint write retires held frontier and persists pending ack | Caller checkpoint-write rely. | Pending: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement` checks coordinator ack retention, not caller checkpoint durability (#4876). |
| `Acknowledge` | Record post-checkpoint participant acknowledgement | `ViewCrossTreeCoordinatorGrain.AcknowledgeRepairAsync`. | Partial: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`; caller ack ordering remains pending (#4876). |
| `Cleanup` | Expire current work only after all durable participant acknowledgements | `ViewCrossTreeCoordinatorGrain.OnTtlExpiredAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`. |
| `CleanupLegacy` | Automatically expire historical original-contract completion without fresh certification | `ViewCrossTreeCoordinatorGrain.OnTtlExpiredAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_released_applied_state_completes_without_claiming_repair`; an actual coordinator TTL-retention mutation trips its cleanup assertion. |
| `Ordinary` | Resume ordinary/local publication only after repair completes | Caller publication-fence/deferred replay rely. | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target` proves no coordinator replay, not the caller-wide fence (#4876). |
| `Rebind` | Heal physical identity only after durable retirement | Caller durable physical hold/rebind rely. | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation` proves stored target retention, not alias/rebuild/eviction fencing (#4876). |
| `Observe` | Witness one conceptual joint-publication cut; independent consumer opens may straddle commits | `ViewCrossTreeCoordinatorGrain.IssueJointFlipAsync`. | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision` inspects collaborator batches, not an actual held joint-write witness (#4876). |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `OriginalIntentHeld` | Durable original intent cannot degrade after response loss | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterDegradedAsync_a_decided_publication_cannot_abandon_its_joint_repair_obligation`. |
| `OriginalEntitled` | Original decision requires all original participants | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_incomplete_wait_set_returns_not_ready_and_persists`; premature durable original intent trips its exact Decided-state assertion. |
| `FreshEntitled` | Fresh repair decision requires original completion and all fresh participants | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision`. |
| `CaptureFrozen` | First payload and physical target survive changed retries/reactivation | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation`. |
| `PhysicalHeld` | Physical identity remains pinned until durable checkpoint retirement | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation` checks coordinator target, not caller rebind fencing (#4876). |
| `JointVisible` | Original/repair effects install jointly; this does not synchronize independent consumer reads | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision` checks one joint call, not the actual held-joint-write discriminator (#4876). |
| `CompletionDurable` | Failed persistence never returns completion success | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_failed_completion_persistence_cannot_memoize_an_undurable_result`, `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `CheckpointHeld` | Repair obligation pins checkpoints until repair applies | Pending: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement` checks retention only; caller holdback acceptance remains pending (#4876). |
| `PublicationFenced` | All ordinary/local publication is fenced across unresolved repair | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target` checks no coordinator replay only (#4876). |
| `AckEntitled` | Participant ack follows successful durable retirement | Pending: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement` validates coordinator retention, not caller write/ack ordering (#4876). |
| `CleanupEntitled` | Current cleanup requires applied repair and every participant ack; historical completion retains its original TTL contract | Yes: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`, `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_released_applied_state_completes_without_claiming_repair`. |
| `LegacyNeverCertified` | Original-contract completion cannot fabricate current fresh readiness or repair certification | Yes: `ViewCrossTreeCoordinatorGrainTests.Repair_apis_do_not_certify_released_original_completion`, `ViewCrossTreeCoordinatorGrainTests.Serialization_released_applied_state_has_no_repair_certificate`; the latter reads an actual old-shape Orleans binary payload. |
| `NewerPeerPreserved` | Completed repair retry cannot resurrect an old peer projection | Partial: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`; newer public-source perturbation is pending (#4876). |
| `EffectExact` | Frozen payload applies to its exact retained physical target | Partial: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`; actual-store and caller projection acceptance remain pending (#4876). |
| `SnapshotExact` | Conceptual publication-cut row witnesses match the installed effect; separate-tree opens may straddle commits | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision` does not establish the actual installed joint-cut witness (#4876). |
| `FreshCaptureCoherent` | First repair capture derives coherent current source tuples/targets | Pending: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision` inspects supplied bytes, not caller admission/replacement closure (#4876). |
| `RepairClosed` | Decided joint work eventually repairs, retires and acknowledges after recovery | Partial: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`; unavailability is fail-closed, convergence assumes peer recovery/fair maintenance (#4876). |
| `LifecycleEnds` | Pre-decision degradation or post-decision full repair closure eventually occurs | Partial: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`; bounded faults and fairness are rely assumptions, not runtime scheduling guarantees (#4876). |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite domain check, not a production behaviour. |

## Automatic upgrade boundary

Historical version-zero state remains an original-contract operation, not a new
repair obligation. The bounded legacy mode imports an already-completed record;
legacy in-flight history, lost response and completion-write failure are
separate coordinator controls, not modelled source-history reconstruction.

`ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_closed_cluster_gate_does_not_mint_an_operation_or_block_legacy_cleanup`
reaches the actual public admission path and rejects a bypass mutation.
`ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_lost_first_marker_write_is_durable_before_retry_completion`
proves the read-only protocol query does not promote a volatile failed marker.
The cluster helper includes non-dead joining/draining members and invalidates
its cached result when membership or manifest versions change. Neither that
helper nor finite TLC proves the caller checks admission before querying an old
RPC endpoint or minting a hold; those caller/runtime controls remain Pending
under #4902 until composed acceptance. No host-overridable version selector or
unsupported old-binary downgrade is assumed safe.

## Runtime acceptance boundary

The source owner reports two executed caller cases named
`MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement`.
They inspect pairs at stable held coordinator boundaries before publication
and after completion before maintainers resume, lose repair-completion and
post-checkpoint ack responses, and optionally reactivate maintainers. These
observations do not promise simultaneous independent read/opens. Final combined-chain and
caller-mutation acceptance are still pending, so this note does not promote
those reported cases to Yes.

Separate physical alias/rebuild/lag-eviction fencing, failed checkpoint writes
before durable ack, and a later-source perturbation after immutable capture and
restart require distinct caller discriminators. Source projection coherence,
transitive atomic replacement closure, numeric memberships/rekey retractions,
and checkpoint-frontier replay are explicit relies. Neither finite TLC nor the
coordinator's collaborator assertions discharge them.

The source owner is also validating
`ViewMaintainerReplayFlushTests.Repair_acknowledgement_never_precedes_successful_checkpoint_retirement`,
`ViewMaintainerReplayFlushTests.A_frozen_repair_waits_for_its_peer_without_accumulating_more_WAL_staging`,
`ViewMaintainerReplayFlushTests.A_cross_tree_batch_resolved_by_a_pass_that_reads_nothing_is_not_overwritten_by_a_later_replay_of_an_older_ordinary_write`,
and the runtime case
`MaterialisedViewCrossTreeStaleSliceTests.A_submitted_repair_pins_its_physical_source_and_generation_until_checkpoint_retirement`.
These are candidate names, not accepted Yes mappings. The pair detector is
being strengthened with a changed source after freeze before restart.
Persisted repair-source HLC witnesses and the consumed retirement frontier
must protect against older ordinary replay. Fair capture assumes sufficient
staging capacity; capacity exhaustion must remain fail-closed rather than
silently dropping a hold or degrading a decided repair.

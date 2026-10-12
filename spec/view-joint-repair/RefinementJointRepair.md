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
| `Capture` | Freeze first fresh source-derived slice and physical target | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Partial: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation`, `MaterialisedViewCrossTreeStaleSliceTests.Fresh_repair_uses_the_held_physical_sources_current_shard_map_after_split_and_alias_cutover`, and `ViewMaintainerReplayFlushTests.A_frozen_repair_waits_for_its_peer_without_accumulating_more_WAL_staging` cover immutable capture/physical generation and bounded peer waiting. Complete transitive replacement closure remains a caller rely (#4876). |
| `Retry` | Recover/resubmit immutable stored readiness without replaying completed repair | `ViewCrossTreeCoordinatorGrain.GetRepairReadinessAsync`, `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `DecideRepair` | Require every original participant's fresh contribution | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision`. |
| `PublishRepair` | One fresh joint operation applies all frozen slices | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: for held public-view boundaries: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` inspects the paired views around the joint publication and catches split/ordinary-ordering regressions. It does not create a shared cross-view read snapshot. |
| `CompleteRepair` | Persist repair completion before success | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `PersistFailure` | Failed completion does not certify success | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`, `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_failed_completion_persistence_cannot_memoize_an_undurable_result`, `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `Crash` | Preserve durable decision, capture and target through reactivation | `ViewCrossTreeCoordinatorState`, `ViewCrossTreeCoordinatorGrain.GetRepairReadinessAsync`. | Partial: coordinator reactivation is covered by `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation`; caller lost-completion/ack reactivation is covered by `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` and `ViewMaintainerReplayFlushTests.Repair_acknowledgement_never_precedes_successful_checkpoint_retirement`. This does not establish recovery under permanent peer unavailability. |
| `Retire` | Successful checkpoint write retires held frontier and persists pending ack | Caller checkpoint-write rely. | Yes: `ViewMaintainerReplayFlushTests.Repair_acknowledgement_never_precedes_successful_checkpoint_retirement` observes the independently cloned durable unretired hold at the ack callback and covers restart; two callback-order mutation controls fail. This establishes caller write-before-ack ordering, not an atomic snapshot across views. |
| `Acknowledge` | Record post-checkpoint participant acknowledgement | `ViewCrossTreeCoordinatorGrain.AcknowledgeRepairAsync`. | Yes: `ViewMaintainerReplayFlushTests.Repair_acknowledgement_never_precedes_successful_checkpoint_retirement` covers both restart modes and kills ack-before-checkpoint mutations; coordinator retention is separately covered by `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`. |
| `Cleanup` | Expire current work only after all durable participant acknowledgements | `ViewCrossTreeCoordinatorGrain.OnTtlExpiredAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`. |
| `CleanupLegacy` | Automatically expire historical original-contract completion without fresh certification | `ViewCrossTreeCoordinatorGrain.OnTtlExpiredAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_released_applied_state_completes_without_claiming_repair`; an actual coordinator TTL-retention mutation trips its cleanup assertion. |
| `Ordinary` | Resume ordinary/local publication only after repair completes | Caller publication-fence/deferred replay rely. | Yes: for the bounded cross-tree successor scenario: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` holds the original A / replacement B pair at stable public-view boundaries, including lost completion/ack responses and reactivation; both ordinary-publication bypass mutations fail. The separate next-drain HLC witness is `ViewMaintainerReplayFlushTests.A_cross_tree_reactivation_replay_never_rewrites_the_jointly_flipped_slice_non_atomically`. |
| `Rebind` | Heal physical identity only after durable retirement | Caller durable physical hold/rebind rely. | Yes: for held physical routing across the exercised split/alias-cutover cases: `MaterialisedViewCrossTreeStaleSliceTests.Fresh_repair_uses_the_held_physical_sources_current_shard_map_after_split_and_alias_cutover` fails all three physical-routing mutation cases. Active-alias rebuild topology is separately covered by `MaterialisedViewCrossTreeStaleSliceTests.Rebuilding_an_active_alias_after_a_split_uses_its_physical_sources_live_topology` (reconcile false/true). This does not generalise to unmodelled placement transitions. |
| `Observe` | Witness one conceptual joint-publication cut; independent consumer opens may straddle commits | `ViewCrossTreeCoordinatorGrain.IssueJointFlipAsync`. | Yes: for stable held boundaries: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` inspects public paired-view state around the held joint publication and kills the ordinary-ordering bypass. It does not provide a shared multi-view snapshot cursor or promise simultaneous independent opens. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `OriginalIntentHeld` | Durable original intent cannot degrade after response loss | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterDegradedAsync_a_decided_publication_cannot_abandon_its_joint_repair_obligation`. |
| `OriginalEntitled` | Original decision requires all original participants | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_incomplete_wait_set_returns_not_ready_and_persists`; premature durable original intent trips its exact Decided-state assertion. |
| `FreshEntitled` | Fresh repair decision requires original completion and all fresh participants | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision`. |
| `CaptureFrozen` | First payload and physical target survive changed retries/reactivation | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation`. |
| `PhysicalHeld` | Physical identity remains pinned until durable checkpoint retirement | Yes: for the tested split/alias-cutover routes: `MaterialisedViewCrossTreeStaleSliceTests.Fresh_repair_uses_the_held_physical_sources_current_shard_map_after_split_and_alias_cutover`; its three-case mutation control rejects a foreign shadow or stale physical mapping. Broader topology changes remain outside the bounded detector. |
| `JointVisible` | Original/repair effects install jointly; this does not synchronize independent consumer reads | Yes: for the held before/after publication boundaries: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` inspects public paired views while publication is held and after completion. This is not a simultaneous read guarantee; independent view opens can straddle a valid commit. |
| `CompletionDurable` | Failed persistence never returns completion success | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_failed_completion_persistence_cannot_memoize_an_undurable_result`, `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `CheckpointHeld` | Repair obligation pins checkpoints until repair applies | Yes: for the exercised live-debt boundary: `ViewMaintainerReplayFlushTests.A_closed_gate_after_current_original_publication_retains_durable_repair_debt_without_new_RPC` verifies a closed gate retains current repair debt without issuing a new RPC; `Repair_acknowledgement_never_precedes_successful_checkpoint_retirement` observes the durable hold before checkpoint persistence. |
| `PublicationFenced` | All ordinary/local publication is fenced across unresolved repair | Yes: for the paired original/successor path: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` holds the newer ordinary successor from becoming public before peer repair closes; both publication-bypass mutations fail. |
| `AckEntitled` | Participant ack follows successful durable retirement | Yes: `ViewMaintainerReplayFlushTests.Repair_acknowledgement_never_precedes_successful_checkpoint_retirement` covers both restart settings and both ack-before-checkpoint mutation controls fail at the callback boundary; coordinator retention is covered separately by `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`. |
| `CleanupEntitled` | Current cleanup requires applied repair and every participant ack; historical completion retains its original TTL contract | Yes: `ViewCrossTreeCoordinatorGrainTests.Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement`, `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_released_applied_state_completes_without_claiming_repair`. |
| `LegacyNeverCertified` | Original-contract completion cannot fabricate current fresh readiness or repair certification | Yes: `ViewCrossTreeCoordinatorGrainTests.Repair_apis_do_not_certify_released_original_completion`, `ViewCrossTreeCoordinatorGrainTests.Serialization_released_applied_state_has_no_repair_certificate`; the latter reads an actual old-shape Orleans binary payload. |
| `NewerPeerPreserved` | Completed repair retry cannot resurrect an old peer projection | Yes: for the tested newer ordinary successor and reactivation path: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement`; the follow-on drain/HLC detector is `ViewMaintainerReplayFlushTests.A_cross_tree_reactivation_replay_never_rewrites_the_jointly_flipped_slice_non_atomically` and explicitly executes the next ordinary pass. |
| `EffectExact` | Frozen payload applies to its exact retained physical target | Yes: for the exercised physical routing and public projection cases: `MaterialisedViewCrossTreeStaleSliceTests.Fresh_repair_uses_the_held_physical_sources_current_shard_map_after_split_and_alias_cutover`; the separate current-topology rebuild control is `Rebuilding_an_active_alias_after_a_split_uses_its_physical_sources_live_topology`. |
| `SnapshotExact` | Conceptual publication-cut row witnesses match the installed effect; separate-tree opens may straddle commits | Yes: for the tested stable held before/after boundaries: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` reads public paired-view projections at the held publication boundary. This is not a claim that arbitrary independent reads share a global cursor or cannot straddle a valid joint commit. |
| `FreshCaptureCoherent` | First repair capture derives coherent current source tuples/targets | Partial: `ViewMaintainerReplayFlushTests.A_cross_tree_reactivation_replay_never_rewrites_the_jointly_flipped_slice_non_atomically` kills replay of an older source point when version metadata is unavailable while its readable-metadata control remains green; `MaterialisedViewNumericIntentCompositionTests` covers Count/Sum numeric intent. Complete transitive source admission/replacement closure and every projection encoding remain explicit caller relies, not proved by this bounded detector. |
| `RepairClosed` | Decided joint work eventually repairs, retires and acknowledges after recovery | Partial: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement`, `ViewMaintainerReplayFlushTests.Repair_acknowledgement_never_precedes_successful_checkpoint_retirement`, and `A_frozen_repair_waits_for_its_peer_without_accumulating_more_WAL_staging` cover bounded response loss, restart, durable retirement and non-accumulating peer wait. Eventual closure remains conditional on peer recovery, fair maintenance and staging capacity; permanent unavailability is fail-closed, not convergent. |
| `LifecycleEnds` | Pre-decision degradation or post-decision full repair closure eventually occurs | Partial: bounded actual caller cases cover recoverable response loss, ordinary deferral and checkpoint/ack ordering, but the liveness claim still relies on eventual peer recovery, fair maintenance and available staging capacity; none of the detectors establishes progress under permanent unavailability. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite domain check, not a production behaviour. |

## Automatic upgrade boundary

Historical version-zero state remains an original-contract operation, not a new
repair obligation. The bounded legacy mode imports an already-completed record;
legacy in-flight history, lost response and completion-write failure are
separate coordinator controls, not modelled source-history reconstruction.

`ViewMaintainerReplayFlushTests.Closed_upgrade_gate_pauses_before_any_coordinator_RPC_and_resumes_automatically`
proves legacy-v0 and unknown-operation drains pause before the protocol query and
resume after admission opens; its two-case query-bypass mutation fails.
`A_closed_gate_after_current_original_publication_retains_durable_repair_debt_without_new_RPC`
proves a current v1 obligation remains retained if admission closes after the
original publication. These join the coordinator public-entry and manifest-cache
controls above. No host-overridable version selector or unsupported old-binary
downgrade is assumed safe.

The public maintenance boundary is separately covered by
`ViewMaintainerReplayFlushTests.Closed_upgrade_gate_blocks_public_maintenance_with_pending_acknowledgement_without_effects`
for decommission, rebuild and reconcile. With pending acknowledgement and closed
admission, each entry rejects before coordinator/tree calls or checkpoint and
generation writes. The original three cases failed before the shared admission
guard; the corrected eight-case upgrade/ack union and the parent's composed
65-case replay/decommission neighbours pass. Reopening public decommission
reaches the retained acknowledgement before an injected response fault stops
teardown. This does not certify complete rebuild/reconcile after reopening or
arbitrary binary downgrade. Maintenance may retry once admission reopens; it
need not wait until every acknowledgement has already completed.

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

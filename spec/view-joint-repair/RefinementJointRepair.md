# Joint-repair refinement

This is a bounded documented abstraction, not a machine-checked refinement
proof. Coordinator API regressions establish coordinator behaviours only.
Named actual-caller controls separately establish local admission, transitive
fresh capture, held public publication boundaries and durable retirement.
Conditional recovery assumes eventual peers, fair maintenance and sufficient capacity.
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
| `bound`, `retired` | Physical identity and durable checkpoint retirement | Caller durable hold/rebind fence and successful checkpoint-write rely, exercised separately by physical-route and checkpoint-before-ack controls. |
| `rows`, `stage`, `ordinarySeen` | Joint installed effects and legitimate later ordinary successors | `ILatticeCrossTreeTxGrain.CommitAsync`, `ILattice.SetManyAtomicAsync`; backing publication/idempotency is abstracted. |
| `reported`, `faults` | Returned success and bounded response/persistence failures | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`, `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`; fault counter is a modelling device. |
| `observed`, `snapshotRows`, `snapshotOrdinary` | Conceptual joint-publication cut and row/phase witnesses | `ViewCrossTreeCoordinatorGrain.IssueJointFlipAsync`; runtime evidence must hold/capture the actual joint write, not infer a common cut from independent consumer opens. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Register` | Admit original participant | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_incomplete_wait_set_returns_not_ready_and_persists` checks the missing peer remains undecided and persisted. |
| `DecideOriginal` | Persist original joint intent | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterDegradedAsync_a_decided_publication_cannot_abandon_its_joint_repair_obligation`. |
| `PublishOriginal` | Apply original joint effect | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`. | Yes: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` observes both original public rows before allowing fresh repair to decide. Omitting the actual original `IssueJointFlipAsync` call makes both cases fail on null original rows. This is installation at held public boundaries, with backend atomicity a separate rely; it does not synchronize independent consumer opens. |
| `CompleteOriginal` | Persist original completion before success | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_failed_completion_persistence_cannot_memoize_an_undurable_result`. |
| `Timeout` | Degrade only before durable original intent | `ViewCrossTreeCoordinatorGrain.RegisterDegradedAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterDegradedAsync_a_decided_publication_cannot_abandon_its_joint_repair_obligation`. |
| `Advance` | Admit newer coherent source replacement | Caller admission rely. | Yes: `ViewMaintainerReplayFlushTests.A_cross_tree_participant_registers_only_after_local_source_admission_and_waits_for_its_expected_peer` rejects absent/older local tuples; `ViewMaintainerReplayFlushTests.A_fresh_joint_repair_captures_the_transitive_source_key_closure_before_freezing_readiness` derives six current row/HLC witnesses from five connected batches and waits for the post-observation consumed horizon. Actual admission, one-pass closure, unconsumed-head and pre-observation-head bypasses fail independently. |
| `Capture` | Freeze first fresh source-derived slice and physical target | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewMaintainerReplayFlushTests.A_fresh_joint_repair_captures_the_transitive_source_key_closure_before_freezing_readiness` freezes the complete six-key current payload/HLC set, survives lost registration response/reactivation and rejects one-pass closure or premature head certification. `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation` separately freezes stored payload/target; `MaterialisedViewCrossTreeStaleSliceTests.Fresh_repair_uses_the_held_physical_sources_current_shard_map_after_split_and_alias_cutover` checks actual held physical routing. Per-row source coherence is not source-wide atomic capture. |
| `Retry` | Recover/resubmit immutable stored readiness without replaying completed repair | `ViewCrossTreeCoordinatorGrain.GetRepairReadinessAsync`, `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `DecideRepair` | Require every original participant's fresh contribution | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision`. |
| `PublishRepair` | One fresh joint operation applies all frozen slices | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: for held public-view boundaries: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` inspects the paired views at held publication boundaries and rejects the actual ordinary-ordering bypass; backend atomicity remains a separate rely. It does not create a shared cross-view read snapshot. |
| `CompleteRepair` | Persist repair completion before success | `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `PersistFailure` | Failed completion does not certify success | `ViewCrossTreeCoordinatorGrain.CompleteOriginalPublicationAsync`, `ViewCrossTreeCoordinatorGrain.RegisterRepairReadyAsync`. | Yes: `ViewCrossTreeCoordinatorGrainTests.RegisterReadyAsync_failed_completion_persistence_cannot_memoize_an_undurable_result`, `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target`. |
| `Crash` | Preserve durable decision, capture and target through reactivation | `ViewCrossTreeCoordinatorState`, `ViewCrossTreeCoordinatorGrain.GetRepairReadinessAsync`. | Yes: for finite response-loss/reactivation boundaries: `ViewMaintainerReplayFlushTests.A_fresh_joint_repair_captures_the_transitive_source_key_closure_before_freezing_readiness` recovers frozen registration after loss; `ViewCrossTreeCoordinatorGrainTests.RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation` retains durable coordinator capture/target; `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` changes source after frozen completion and recovers lost completion/ack; `ViewMaintainerReplayFlushTests.Repair_acknowledgement_never_precedes_successful_checkpoint_retirement` separately checks cloned durable caller state. Permanent peer unavailability is excluded. |
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
| `FreshCaptureCoherent` | First repair capture derives coherent current source tuples/targets | Yes: for the bounded per-row/source-key seam: `ViewMaintainerReplayFlushTests.A_fresh_joint_repair_captures_the_transitive_source_key_closure_before_freezing_readiness` asserts complete transitive current payload/HLC witnesses and consumed post-observation heads; erasing actual captured row timestamps fails all four cases, and one-pass/head bypasses fail separately. `BPlusLeafGrainTests.GetVisibleRawEntries_committed_pending_value_and_version_come_from_one_row` supplies the logical-row API prerequisite; `MaterialisedViewNumericIntentCompositionTests.Source_suppression_and_a_no_output_successor_complete_numeric_intent_before_checkpoint` separately covers Count/Sum source suppression. Not every projection encoding, joint numeric fresh capture or source-wide transaction snapshot is modelled. |
| `RepairClosed` | Decided joint work eventually repairs, retires and acknowledges after recovery | Yes: conditional on eventual peers, fair maintenance, finite faults and adequate staging capacity: `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` completes both peers to zero lag after lost completion/ack and optional reactivation with later source writes; `ViewMaintainerReplayFlushTests.Repair_acknowledgement_never_precedes_successful_checkpoint_retirement` checks the durable retirement boundary; `ViewMaintainerReplayFlushTests.A_frozen_repair_waits_for_its_peer_without_accumulating_more_WAL_staging` checks fail-closed bounded waiting. No progress under permanent unavailability is claimed. |
| `LifecycleEnds` | Pre-decision degradation or post-decision full repair closure eventually occurs | Yes: under the stated finite-fault/fairness/capacity relies: `ViewCrossTreeCoordinatorGrainTests.RegisterDegradedAsync_fresh_terminally_degrades_and_persists` covers terminal predecision degradation; `ViewCrossTreeCoordinatorGrainTests.RegisterDegradedAsync_a_decided_publication_cannot_abandon_its_joint_repair_obligation` prohibits postdecision abandonment; `MaterialisedViewCrossTreeStaleSliceTests.Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement` completes the postdecision repair/retirement/ack path after bounded response loss and restart. Permanent unavailability remains fail-closed, not convergent. |

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

The parent composed the final source and runtime chain: 92 caller/physical
positives in `parent-source-capture-HLC-final-restored.trx` and 35 runtime
positives in `parent-joint-and-horizon-live-final35.trx`. The latter includes
23 stale-slice/joint cases, two Count/Sum cases and the ten accepted Horizon
runtime cases. There are no failed, skipped, inconclusive or unexecuted cases.
Source-owner paired controls remain distinct: ordinary publication bypass
fails two stable public pairs, ack-before-checkpoint fails two cloned durable
hold observations, held-physical routing fails three split/alias routes, and
active-alias rebuild topology bypass fails both public maintenance paths.

The additional parent original-publication control omits the actual original
joint flip and fails both original-row assertions, before fresh repair can
heal output (`parent-original-joint-flip-omitted-red.trx`). This is not a claim
about simultaneous arbitrary consumers or every backend crash interleaving.
The backend atomic operation and existing atomic-commit contracts remain relies.

The fresh caller detector uses five deterministic connected batches with equal
maximum prepare clocks and six keys. The far member precedes its bridges and
adds a unique q only on a fixed-point revisit. The actual one-pass capture
mutation fails normal/lost-response cases on missing q
(`parent-joint-fresh-one-pass-red.trx`).
The advancing-head axis changes the physical head inside the logical source
read. Independent actual `Covers` and pre-observation head mutations fail the
isolated normal/advancing case at premature readiness registration
(`parent-joint-fresh-unconsumed-head-red.trx`,
`parent-joint-fresh-preobservation-head-red.trx`).
Replacing captured mutation timestamps with zero fails all four durable six-key
HLC witness assertions (`parent-joint-fresh-source-HLC-erased-red.trx`);
the rebuilt restored four cases and complete 92-case population pass.
All accepted negative outcomes are assertion failures, not injected timeouts.

This fixture deliberately retains a NotReady peer and checkpoint -1. It proves
capture, registration horizon and frozen response-loss recovery, not final
joint publication. Its shared-generation fake is unsuitable for a complete
automatic shadow-rebuild final-state assertion; exploratory failed baselines
from that unsupported harness are not runtime defects or accepted controls.
Separate actual runtime pairs prove the bounded publication/recovery seam.
Per-row logical source coherence, finite staging capacity, eventual peer
recovery and fair maintenance remain explicit relies. There is no source-wide
transaction-atomic capture, arbitrary retention-gap convergence, complete
numeric projection proof, join/view-DAG API or shared cross-view cursor.

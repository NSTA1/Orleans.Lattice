# Schema/view composition refinement

This is a documented abstraction, not a machine-checked refinement proof. It
composes the schema write path, the version family and a rewrite (an eager
version migration or a transform remediation) with an independent Count/Sum
projection, and checks the projection input contract between them: on a
versioning-enabled source a projection receives the envelope-stripped body at
its stamped version, never upcast, on every derivation path, and a target
advance re-projects nothing. The source is two keys over a fixed script: a raw
write, a write an accepting stage transforms or the policy refuses, and an
overlapping two-key atomic batch. The environment advances the target, replaces
the projection definition, delivers one value below the check with a newer
source stamp, retries a write whose response was lost, runs one rewrite, and
crashes the maintainer.

## Model scope in one paragraph

Each value is stored as a body token at the version it was stamped with. A
version changes what a token's shape means: version 2 regroups one token,
rescales another's numeric and puts a third outside the filter, so an upcast
changes a contribution's group, numeric or filter membership. Projection
definition 1 reads only the version-1 shape and definition 2 reads both. The
view applies each WAL entry by replacing the key's membership with the
projection of the entry's input (`Input`, the stripped value at its stamp) and
moving its slots; an atomic batch publishes its net slice in one step; a
changed definition or a moved alias rebuilds from the bound copy's stored
values; a crash replays the retained entries at their own stamps. The schema
side is the environment its own modules guarantee, restated below. The checked
contract is that a caught-up view is the projection of every key's stored value
at its stamp and, end to end, of the value the write path promised: the
accepted (transformed) body at its stamp, or after a rewrite the rewritten body
at the rewrite's stamp.

## Relied-on guarantees

The module instances neither side. Each assumption it makes about the schema
modules or the numeric companion is restated as an action of this module, and
discharged by the checked properties of the module named here.

| Assumption | Restated as | Discharged by |
|------------|-------------|---------------|
| A local write is judged before the WAL append; a refused write appends nothing; an accepted raw value is stamped at the target, after any accepting stage's transform; an enveloped value is kept verbatim. | `Write`, `Retry` | `SchemaStageOrdering`: ValidationBeforeCommit, RefusalAppliesNothing, StampedWhenVersioned, EnvelopedValuesVerbatim, EnforcementSeesSubmittedValue. `SchemaPolicyConfig`: AcceptedWriteValid, RejectLeavesNoMutation. |
| Strict ingest judges a replicated entry on its decoded body and diverts a refused one, which is never committed; a committed or unjudged entry is stored with its source stamp. | `Replicate` | `SchemaStrictIngest`: StoredValidWhenJudged, DeadLettersOnlyRefused, DiversionAckedOnlyWhenRecorded. `SchemaCrossRegion` (the cross-region interactions model): the receiver stores the source stamp below the check. |
| A target advance is strictly monotonic, to a registered and reachable version, and rewrites no stored value. | `Advance` | `SchemaVersionFamily`: ValidatedTarget, AdvanceStrict, ConfigNeverRewritesData. |
| A migration captures the target when it begins, re-stamps every value from its own stamp to that target, and either cuts over read-equivalently or aborts naming the value with the data untouched. | `BeginRewrite`, `BuildRewrite` | `SchemaVersionFamily`: ShadowAtOwnTarget, UpcastOrExplicitFailure, MigrationReadEquivalent, AbortLeavesDataUntouched. |
| A remediation writes every rewritten value through the destination copy's ordinary write path, stamped at the target the transform ran on; an abort publishes nothing; cutover moves the alias only while it still names the captured source, and a superseded publication is refused. | `BuildRewrite`, `Cutover` | `SchemaRemediationCutover` (the remediation copy-and-cutover model): DestinationStampedAtTarget, AbortPublishesNothing, SupersededPublicationRefused, QuiescentCutoverComplete. `SchemaDurableOperation`: NothingCutOverUnlessCompleted. |
| The maintainer applies per-key replace membership with slots that are its fold, retires every publication obligation before the checkpoint, replays from the checkpoint without resubmitting an operation id with another payload, and rebuilds under a fresh generation from the current copy on a changed version or swapped copy. | `Apply`, `ApplyBatch`, `Fail`, `Rebind`, `Reconcile` | `MaterialisedViewNumeric`: SlotsMatchMembership, IntentCoversStale, NoIdentityConflict, QuiescentExact, CheckpointAfterPublication, Converges. |

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `appended`, `submitted`, `retried`, `replicated` | Source script position, each key's last submitted body, and the retry and below-check delivery budgets | `ILattice.SetAsync`, `ILattice.SetManyAtomicAsync`, `ILattice.MergeAsync`. |
| `target` | The tree's target schema version | `LatticeSchemaVersionConfig.TargetVersion`, `ILatticeSchemaVersionAdmin.AdvanceTargetVersionAsync`. |
| `defn`, `viewDefn` | Registered projection definition and the definition the view was built under | `AggregationLatticeViewProjection.ProjectionVersion`, `ViewCheckpointState.ProjectionVersion`. |
| `phys`, `stored`, `wal` | The physical copy the alias names, each copy's stored value per key with its envelope, and each copy's WAL | `ILatticeRegistry.ResolveAsync`, `IReplicationApplyGrain.ReadStoredWithVersionAsync`, `LatticeSchemaEnvelope`. |
| `rw`, `cap`, `promised` | Rewrite phase, the version it re-stamps at, and the version it promised readers | `LatticeSchemaRemediationReport.Phase`, `ILatticeSchemaVersionAdmin.MigrateToTargetVersionAsync`, `ILatticeSchemaRemediationAdmin.RemediateAsync`. |
| `bound`, `genCopy`, `read`, `checkpoint` | Bound physical copy, the copy the current generation was built from, volatile cursor and resume floor | `ViewCheckpointState.BoundPhysicalTreeId`, `ViewCheckpointState.RebuildGeneration`, `ViewCheckpointState.AppliedOffsets`. |
| `mem`, `slots` | Per-source-key membership and per-group count/sum accumulator | `AggregationRowCodec.MembershipKey`, `AggregationApplier.ContributeNumericAsync`, `AggregationApplier.RetractNumericAsync`. |
| `intended`, `mustRefuse`, `applied` | What the write path promised each key, the units it must refuse, and the units the view applied incrementally | Modelling devices, not production fields. |
| `faults`, `reconciles` | Crash and reconcile budgets | Modelling devices, not production fields. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write` | A local raw write judged by the composed chain before the append: refused, or transformed by an accepting stage and stamped at the target; and a decided two-key atomic batch | `CompositeLatticeWriteInterceptor.OnWriteAsync`, `LatticeSchemaWriteInterceptor.OnWriteAsync`, `LatticeSchemaVersionWriteInterceptor.OnWriteAsync`, `ILattice.SetManyAtomicAsync`. | Yes: `CompositeLatticeWriteInterceptorTests.OnWriteAsync_threads_transformed_value_into_next_stage`, `SchemaEnforcementEnvelopedBodyTests.Plain_local_write_is_validated_then_stamped_at_the_target`, `SchemaViewCompositionIntegrationTests.Refused_write_contributes_nothing_and_a_retried_write_is_counted_once`. |
| `Retry` | A writer whose response was lost re-submits; the re-submission is judged and stamped afresh | `ILattice.SetAsync`, `LatticeSchemaVersionWriteInterceptor.OnWriteAsync`. | Yes: `SchemaViewCompositionIntegrationTests.Refused_write_contributes_nothing_and_a_retried_write_is_counted_once`. |
| `Replicate` | A value arrives below the check with a newer source stamp; strict ingest judges its decoded body and diverts it when it cannot be decoded, otherwise it is stored verbatim | `ILattice.MergeAsync`, `IReplicationApplyGrain.ApplyMergeManyAsync`, `LatticeSchemaDeadLetterStore.AppendAsync`. | Yes: `SchemaViewCompositionIntegrationTests.Value_merged_below_the_check_is_projected_at_its_source_stamp_and_stops_a_migration_that_cannot_decode_it`, `SchemaStrictIngestDeadLetterTests.Undecodable_enveloped_entry_is_diverted_never_admitted`, `SchemaStrictIngestDeadLetterTests.Valid_enveloped_entry_is_admitted_verbatim_with_no_record`. |
| `Advance` | The target advances to a registered version; nothing is rewritten and no view is touched | `ILatticeSchemaVersionAdmin.AdvanceTargetVersionAsync`. | Yes: `ProjectionInputSeamCharacterizationTests.Incremental_maintenance_projects_the_stripped_body_at_its_stamped_version`, `SchemaTargetAdvanceIntegrationTests.AdvanceTargetVersionAsync_rewrites_no_stored_value_and_a_later_registered_hop_upcasts_it_on_read`. |
| `ChangeDefinition` | The operator replaces the projection with one that reads the new shape, changing its version | `AggregationLatticeViewProjection.ProjectionVersion`, `ViewMaintainerGrain.EnsureActiveAsync`. | Yes: `ViewMaintainerAggregationDrainTests.Aggregation_activation_rebuilds_when_the_persisted_projection_version_differs`, `ViewMaintainerAggregationDrainTests.Aggregation_activation_keeps_the_view_when_the_projection_version_matches`. |
| `BeginRewrite` | An eager migration or a transform remediation of the source begins; a migration captures the target | `ILatticeSchemaVersionAdmin.MigrateToTargetVersionAsync`, `ILatticeSchemaRemediationAdmin.RemediateAsync`. | Yes: `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaRemediationVersionStampIntegrationTests.RemediateAsync_over_an_upcast_value_stamps_it_at_the_version_the_transform_ran_on`. |
| `BuildRewrite` | The rewrite reads every stored value in its stored form and either aborts on one it cannot decode or writes each through the destination's ordinary write path, re-stamped | `LatticeSchemaVersionMigration.Migrate`, `LatticeSchemaVersionDecoder.DecodeForRestampAsync`. | Yes: `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaViewCompositionIntegrationTests.Migration_that_aborts_leaves_the_source_its_alias_and_every_view_untouched`, `SchemaVersionMigrationAbortIntegrationTests.MigrateToTargetVersionAsync_over_a_value_newer_than_the_target_aborts_and_releases_the_reservation`. |
| `Cutover` | The alias moves to the rewritten copy while it still names the captured source, and the view is told the source identity changed | `ILatticeRegistry.SetAliasAsync`, `ViewMaintainerGrain.NotifySourceIdentityChangedAsync`. | Yes: `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaViewCompositionIntegrationTests.Transform_remediation_reprojects_each_rewritten_body_at_the_target_stamp`. |
| `Apply` | Incremental apply of an ordinary entry through the projection-input seam: the stripped body at its stamp | `ViewMaintainerGrain.DrainAggregationAsync`, `ILatticeEnvelopeCodec.StripForFold`, `AggregationApplier.ContributeNumericAsync`. | Yes: `ProjectionInputSeamCharacterizationTests.Incremental_maintenance_projects_the_stripped_body_at_its_stamped_version`, `ViewProjectionInputSeamCallSiteTests.Every_projection_call_in_the_view_maintainer_goes_through_the_projection_input_seam`. |
| `ApplyBatch` | Atomic publication of a decided batch's net slice through the same seam; an aborted batch contributes nothing | `ViewMaintainerGrain.FlushCompletedAggregationBatchesAsync`. | Yes: `ProjectionInputSeamCharacterizationTests.Atomic_publication_projects_the_same_input_and_reconciles_without_drift`, `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced`. |
| `Fail` | A crash or lost response replays the retained entries from the checkpoint at their own stamps | `ViewMaintainerGrain.DrainAggregationAsync`, `ViewCheckpointState.AppliedOffsets`. | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`, `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `Rebind` | A changed definition or a moved alias rebuilds from the current copy's stored form, binding generation, cursor and checkpoint to it | `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`, `ViewMaintainerGrain.RebuildAsync`, `IReplicationApplyGrain.ReadStoredWithVersionAsync`. | Yes: `ProjectionInputSeamCharacterizationTests.Aliased_source_rebuild_and_reconcile_rederive_exactly_the_incrementally_maintained_view`, `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `MaterialisedViewIdentitySwapHealTests.View_heals_after_source_physical_identity_swap`. |
| `Reconcile` | An operator reconcile or rebuild of a live view re-derives it from the stored form | `ViewMaintainerGrain.ReconcileAsync`, `ViewMaintainerGrain.BuildShadowAsync`. | Yes: `ProjectionInputSeamCharacterizationTests.Rebuild_and_reconcile_rederive_exactly_the_incrementally_maintained_view`. |
| `WaitHead` | A reader's head barrier returns once the view is bound to the copy the alias names and has applied that copy's entries | `ILatticeView.WaitForSourceHeadAsync`, `ViewMaintainerGrain.WaitForSourceHeadAsync`. | Yes: `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaViewCompositionIntegrationTests.Transform_remediation_reprojects_each_rewritten_body_at_the_target_stamp`. |
| `Stutter` | No step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `NoDuplicateContribution` | No source key is counted twice across a retry, a replay, a rebuild or a rewrite's cutover | Yes: `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaViewCompositionIntegrationTests.Refused_write_contributes_nothing_and_a_retried_write_is_counted_once`, `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`. |
| `RejectedContributesNothing` | A refused local write or retry, and a strict-ingest diversion, never reach a view | Yes: `SchemaViewCompositionIntegrationTests.Refused_write_contributes_nothing_and_a_retried_write_is_counted_once`, `SchemaStrictIngestDeadLetterTests.Undecodable_enveloped_entry_is_diverted_never_admitted`. |
| `QuiescentAtStampedVersion` | Every derivation path projects the stripped body at its stamp, so a caught-up view is identical whichever path derived it | Yes: `ProjectionInputSeamCharacterizationTests.Incremental_maintenance_projects_the_stripped_body_at_its_stamped_version`, `ProjectionInputSeamCharacterizationTests.Rebuild_and_reconcile_rederive_exactly_the_incrementally_maintained_view`, `ProjectionInputSeamCharacterizationTests.Atomic_publication_projects_the_same_input_and_reconciles_without_drift`, `ProjectionInputSeamCharacterizationTests.Aliased_source_rebuild_and_reconcile_rederive_exactly_the_incrementally_maintained_view`. |
| `IntendedContribution` | End to end, a caught-up view holds exactly the contribution of each accepted, transformed or rewritten value at the stamp it was given | Yes: `SchemaViewCompositionIntegrationTests.Transform_remediation_reprojects_each_rewritten_body_at_the_target_stamp`, `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaViewCompositionIntegrationTests.Value_merged_below_the_check_is_projected_at_its_source_stamp_and_stops_a_migration_that_cannot_decode_it`. |
| `BindingConsistent` | After a cutover, generation, cursor and checkpoint name the copy the view is bound to | Yes: `MaterialisedViewIdentitySwapHealTests.View_reports_its_wal_cursor_under_the_bound_physical_identity_after_swap`, `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`. |
| `BarrierExact` | A head barrier that returns after a cutover shows the reader the projection of the rewritten copy, never the old copy's membership | Yes: `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaViewCompositionIntegrationTests.Transform_remediation_reprojects_each_rewritten_body_at_the_target_stamp`. |
| `AdvanceReprojectsNothing` | A target advance emits no WAL entry, rewrites no value and changes no view | Yes: `ProjectionInputSeamCharacterizationTests.Incremental_maintenance_projects_the_stripped_body_at_its_stamped_version`, `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaTargetAdvanceIntegrationTests.AdvanceTargetVersionAsync_rewrites_no_stored_value_and_a_later_registered_hop_upcasts_it_on_read`. |
| `AbortTouchesNothing` | A rewrite that aborts changes no copy, no alias and no view | Yes: `SchemaViewCompositionIntegrationTests.Migration_that_aborts_leaves_the_source_its_alias_and_every_view_untouched`, `SchemaViewCompositionIntegrationTests.Value_merged_below_the_check_is_projected_at_its_source_stamp_and_stops_a_migration_that_cannot_decode_it`. |
| `Converges` | Once writes stop and no rewrite is in flight, the view reaches the intended contributions, including after a cutover | Yes: `SchemaViewCompositionIntegrationTests.Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy`, `SchemaViewCompositionIntegrationTests.Transform_remediation_reprojects_each_rewritten_body_at_the_target_stamp`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain constraint, not a production behaviour. |

## Property classification (issue #2321)

TypeOK, NoDuplicateContribution, RejectedContributesNothing,
QuiescentAtStampedVersion, IntendedContribution, BindingConsistent and
BarrierExact are state safety. AdvanceReprojectsNothing and AbortTouchesNothing are action safety.
Converges is temporal liveness under weak fairness of the rewrite's build and
cutover and of the maintainer's apply, batch publication and rebind; the
environment steps are unfair and finitely budgeted.

The base checks every property with an eager migration, ingest that is not
strict, one crash and no retry or reconcile. The `Remediate` variant switches
the rewrite to a transform remediation, allows one reconcile and no crash, and
checks every property, so liveness holds under both rewrites. The `Strict`
variant makes ingest strict and allows a retry, and checks the invariants and
both action properties; its liveness is bounded-out for the TLC budget, with
`Converges` retained on the base and on `Remediate`.

Every experiment is an anchored action perturbation except the one fairness
removal:

| Mutation | Classification |
|----------|----------------|
| TypeOKAdvancePastCatalog | Anchored action: Advance. |
| AdvanceReprojectsNothingAdvanceRebuilds | Anchored action: Advance. |
| NoDuplicateContributionApplyNeverRetracts | Anchored action: Apply. |
| NoDuplicateContributionRebindKeepsSlots | Anchored action: Rebind. |
| RejectedContributesNothingRefusedAppended | Anchored action: Write. |
| RejectedContributesNothingRetryUnjudged | Anchored action: Retry, under `MaxRetries = 1`. |
| RejectedContributesNothingDivertCommits | Anchored action: Replicate, under `StrictIngest = TRUE`. |
| IntendedContributionReplicaRestamped | Anchored action: Replicate. |
| IntendedContributionTransformDropped | Anchored action: Write. |
| QuiescentAtStampedVersionApplySeesEnvelope | Anchored action: Apply. |
| QuiescentAtStampedVersionBatchUpcasts | Anchored action: ApplyBatch. |
| QuiescentAtStampedVersionAbortedBatchApplied | Anchored action: ApplyBatch. |
| QuiescentAtStampedVersionRebuildUpcasts | Anchored action: Rebind. |
| QuiescentAtStampedVersionReconcileUpcasts | Anchored action: Reconcile, under `MaxReconciles = 1`. |
| QuiescentAtStampedVersionCrashSkipsRetained | Anchored action: Fail. |
| BindingConsistentRebindKeepsCursor | Anchored action: Rebind. |
| BarrierExactIgnoresSourceIdentity | Anchored action: WaitHead. |
| IntendedContributionRewriteDropsEnvelope | Anchored action: BuildRewrite. |
| AbortTouchesNothingAbortPublishes | Anchored action: BuildRewrite. |
| IntendedContributionBeginCapturesWrongTarget | Anchored action: BeginRewrite. |
| IntendedContributionCutoverNotPublished | Anchored action: Cutover. |
| IntendedContributionDefinitionAdoptedForward | Anchored action: ChangeDefinition. |
| ConvergesCutoverRebindUnfair | Fairness removal on Rebind. |

## Detectors

`ProjectionInputSeamCharacterizationTests` is the normative detector of the
projection input contract: a real versioned source written across a target
advance, with real Count, Sum and filter views read through their public
handles, on the incremental, atomic, rebuild and reconcile paths and over an
aliased source. Every Project call site in the maintainer is pinned to the seam
by `ViewProjectionInputSeamCallSiteTests`.

`SchemaViewCompositionIntegrationTests` is the caller-level detector of the
composition: a single silo with enforcement, versioning (versions 1 to 3, one
upcaster from 1 to 2 that renames `Amount` to `Total`) and startup-declared
Count and Sum views. It drives an eager migration and checks that every value
is re-projected once at the captured target and that a later write reaches the
view through the new copy; an aborted migration that leaves the source, its
alias and the views untouched; a transform remediation whose rewritten body is
projected at the target stamp; a refused write and a retried write; and a value
merged below the check that is projected at its newer source stamp and stops a
migration that cannot decode it. After each cutover it reads the views straight
after `ILatticeView.WaitForSourceHeadAsync`, so it is also the caller-level
detector of the head barrier across a source identity change (#5308).

Production perturbations for this area are in
[`perturbations/`](perturbations/README.md).

## Defect mutations whose fixes have landed

QuiescentAtStampedVersionApplySeesEnvelope, QuiescentAtStampedVersionBatchUpcasts,
QuiescentAtStampedVersionRebuildUpcasts and
QuiescentAtStampedVersionReconcileUpcasts reinstate the per-path projection
input the #5225 characterization recorded and #5240 aligned: stored bytes with
the envelope on the incremental and atomic paths, and decoded, upcast bodies on
an unaliased rebuild and reconcile. IntendedContributionRewriteDropsEnvelope
reinstates the un-enveloped remediation write fixed by #5243, and
BarrierExactIgnoresSourceIdentity the head barrier fixed by #5308, which waited
only for the copy the view was bound to and so returned before the rebind that
follows a cutover.

## Deliberate abstraction gaps

- **The barrier is driven once, after a cutover.** The model checks the head
  barrier where the source identity changed under the view; elsewhere its result
  is what `QuiescentAtStampedVersion` already checks. A value merged below the
  check with an older source clock is covered by the fixed barrier's
  per-partition WAL positions, not by a step of this model.
  Production symbols: `ILatticeView.WaitForSourceHeadAsync`, `ILattice.MergeAsync`.
- **The rebind is asynchronous.** The model rebinds whenever the view is not
  live, under weak fairness. Production learns of a moved alias from the
  alias-change push or a backstop re-resolve, so a reader that does not use the
  head barrier can observe the old copy's membership for a bounded time after a
  cutover.
  Production symbols: `ViewMaintainerGrain.NotifySourceIdentityChangedAsync`, `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`.
- **A projection must handle every live stamp.** The contract hands a projection
  each value at its own stamp and carries no stamped-version field, no upcast to
  the target and no per-view opt-in. A value whose shape the projection cannot
  read contributes nothing here; a projection that throws instead stops the
  drain, which the numeric companion records. Refreshing contributions to the
  target shape takes a projection change or a rewrite through the WAL.
  Production symbols: `ILatticeAggregationProjection.Project`, `ILatticeViewProjection.Project`.
- **Fixed shapes, one upcaster, one transform.** Two versions, three body
  tokens, a total upcaster and one remediation transform stand for the version
  family's upcaster graph and arbitrary user transforms; arbitrary upcaster
  chains, schema families, JSON predicates and projection encodings are not
  verified.
  No production symbol: the gap names the bounded script.
- **The accepting transform is the core seam.** No shipped enforcement policy
  transforms a value; the only transform the schema add-ons make is the version
  stamp. The accepting transform modelled is the one a composed stage may return
  through `LatticeWriteDecision.AcceptTransformed`, which the composed chain
  threads to the next stage.
  Production symbols: `LatticeWriteDecision.AcceptTransformed`, `CompositeLatticeWriteInterceptor`.
- **A retry is re-stamped at the target current when it lands.** A writer that
  re-sends raw bytes after a target advance has them stamped at the new target,
  whatever shape they carry; matching the bytes to the target is the writer's
  responsibility, and the model only checks that the view follows the stamp.
  Production symbols: `LatticeSchemaVersionWriteInterceptor.OnWriteAsync`.
- **Writes during a rewrite are quiesced and only one rewrite runs.** The model
  admits no source write while a rewrite builds and runs no other alias move, so
  the refusal of a superseded cutover and a write landing in the build window
  are relied on rather than exercised here.
  Production symbols: `ILatticeRegistry.SetAliasAsync`.
- **No source-wide atomic capture.** A rebuild scans one physical copy in one
  step here. Interrupted scans, read holds and late alias moves during a scan
  are the rebind and tail models' obligations; independent views or sources are
  never read at one snapshot, and there is no shared snapshot API.
  Production symbols: `ViewMaintainerGrain.BuildShadowAsync`.
- **Accumulative history views.** A history view over a versioned source receives
  the same stripped input at its stamp, but how its retained revisions carry
  across a cutover is not modelled.
  Production symbols: `HistoryLatticeViewProjection`.
- **One silo, one config.** The writer's silo always holds the current target and
  policy; the per-silo staleness window, batch-level refusal, typed CRDT deltas
  and full-state rows belong to the policy-configuration, stage-ordering and
  cross-region models.
  No production symbol: the gap names model boundaries owned by other modules.
- **Merge paths below the check stay unvalidated.** A value written below the
  check is stored with its source stamp; strict ingest judges replicated
  entries, while a tree merge or a merging restore stores what it copies
  unjudged, so it can carry a stamp the target cannot decode until a hop is
  registered or the target advances.
  Production symbols: `ILattice.MergeAsync`.
- **The maintainer's internals are relied on.** Publication obligations,
  checkpoint banking past the generation floor, source-superseded drops and
  operation identities are the numeric companion's checked contract; this
  module's checkpoint is the generation floor and its replay is per-key replace.
  No production symbol: the gap names model boundaries owned by other modules.

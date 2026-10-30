# Refinement note: schema version family, upcaster catalog and eager migration

## Model scope in one paragraph

One versioned tree's schema family. Its upcaster catalog is the union of the
host's startup declarations and the tree's durable runtime registry, which is
append-only: a registration lands whole or not at all, an identical replay
succeeds and changes nothing, and a registration that renames a version, adds a
second hop from a version, or adds a hop that does not ascend between registered
versions is refused. The facade lets the version config's target move only to a
registered target the current one reaches through the catalog (while the catalog
is non-empty), and an advance is strictly monotonic. No config change or
registration rewrites a stored value. A read decodes each value from its own
stamp: an un-stamped value, a stamp with no config, and another family's stamp
read back as stored; a stamp at the target reads as stored; an older stamp is
upcast along the unique chain of hops, refusing a hop that overshoots the target;
anything else (newer than the target, or no chain) is an explicit failure. An
eager migration captures the target when it starts, holds the tree's alias
reservation, re-stamps every value from its own stamp to that target, validates
the upcast body against the tree's policy, and either cuts over to a build whose
every value reads back exactly as the read-time upcast did, or aborts naming the
value it could not migrate, with the tree's data untouched and the reservation
released.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `rtD`, `rtH` | The tree's runtime-registered descriptors and hops; the host's declarations are the constants `HostD`, `HostH`. | `LatticeSchemaRuntimeRegistryGrain.RegisterAsync`, `SchemaRuntimeRegistryState`, `LatticeSchemaRegistry.Describe`. |
| `cfg` | The version config's target (0 when the tree is unversioned); the family is fixed. | `LatticeSchemaVersionStore.SetConfigAsync`, `LatticeSchemaVersionConfig`. |
| `data` | Each key's stored value: absent, un-stamped, or stamped with a family and version over a body of some shape. | `LatticeSchemaEnvelope.Encode`, `LatticeSchemaEnvelope.TryReadHeader`. |
| `mig`, `migT`, `shadow`, `reserved` | A running eager migration, the target it captured, the values it has built, and the tree's alias reservation. | `LatticeSchemaVersionAdmin.MigrateToTargetVersionAsync`, `SchemaRemediationDriver.DriveAsync`, `LatticeSchemaVersionMigration.Migrate`. |
| `out` | What the last step returned to its caller: a registration or config verdict, or a migration report. | `LatticeSchemaRemediationReport.DidAbort`, `LatticeSchemaRemediationReport.OffendingKey`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Register(a)` | Merge a registration into the tree's runtime catalog whole, or refuse it and change nothing. | `SchemaRuntimeRegistry.RegisterAsync`, `LatticeSchemaRuntimeRegistryGrain.RegisterAsync`, `SchemaRuntimeCatalog.Merge`. | Yes: `SchemaRuntimeRegistryTests.Register_persists_round_trips_and_resolves_after_new_activation`, `SchemaRuntimeRegistryTests.Identical_retry_is_idempotent_conflicts_leave_persisted_catalog_unchanged`, `SchemaRuntimeRegistryTests.Invalid_hops_and_unknown_host_ids_are_rejected`. |
| `SetConfig(t)` | The facade's replace of the version config, validated against a non-empty catalog. | `LatticeSchemaControl.SetVersionConfigAsync`, `SchemaRuntimeRegistry.ValidateTargetAsync`, `LatticeSchemaVersionAdmin.SetVersionConfigAsync`. | Yes: `LatticeSchemaControlVersionTargetTests.SetVersionConfig_refuses_a_target_with_no_registered_descriptor`, `LatticeSchemaControlVersionTargetTests.SetVersionConfig_refuses_a_target_the_current_target_cannot_reach`, `LatticeSchemaControlVersionTargetTests.SetVersionConfig_accepts_a_registered_target_the_current_target_reaches`, `LatticeSchemaControlVersionTargetTests.SetVersionConfig_with_an_empty_catalog_accepts_any_target`. |
| `Advance(t)` | The facade's advance, validated against the catalog, then strictly monotonic in the admin. | `LatticeSchemaControl.AdvanceTargetVersionAsync`, `SchemaRuntimeRegistry.ValidateTargetAsync`, `LatticeSchemaVersionAdmin.AdvanceTargetVersionAsync`. | Yes: `LatticeSchemaVersionAdminTests.AdvanceTargetVersionAsync_advances_and_persists`, `LatticeSchemaVersionAdminTests.AdvanceTargetVersionAsync_non_advancing_throws`, `LatticeSchemaControlVersionTargetTests.AdvanceTargetVersion_refuses_a_target_with_no_registered_path`, `LatticeSchemaControlVersionTargetTests.AdvanceTargetVersion_accepts_a_registered_reachable_target`. |
| `ClearConfig` | Remove the version config; the tree is unversioned again. | `LatticeSchemaVersionAdmin.ClearVersionConfigAsync`, `LatticeSchemaVersionStore.ClearConfigAsync`. | Yes: `LatticeSchemaVersionAdminTests.ClearVersionConfigAsync_removes_and_invalidates`, `SchemaTargetAdvanceIntegrationTests.ClearVersionConfigAsync_rewrites_no_stored_value_and_reads_return_the_stored_body`. |
| `Put(k,v)` | Environment: a value lands under a key by any path, or the key is deleted. | No single production counterpart: local writes stamped at a silo's target, legacy unversioned writes, values replicated, restored or merged below the check, and deletes all land here. | Not applicable. |
| `MigrateBegin` | Start an eager migration at the current target, holding the tree's alias reservation. | `LatticeSchemaVersionAdmin.MigrateToTargetVersionAsync`, `LatticeSchemaVersionAdmin.AdvanceAndMigrateAsync`, `SchemaOperationService.StartMigrationAsync`. | Yes: `LatticeSchemaVersionAdminTests.MigrateToTargetVersionAsync_drives_migration_to_current_target`, `LatticeSchemaVersionAdminTests.AdvanceAndMigrateAsync_advances_config_then_drives_migration_to_new_target`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_different_target_while_in_flight_throws`. |
| `MigrateStep(k)` | Re-stamp one stored value from its own stamp to the captured target and validate its upcast body, or abort naming it. | `LatticeSchemaVersionMigration.Migrate`, `SchemaRemediationDriver.DriveAsync`. | Yes: `LatticeSchemaVersionMigrationTests.Migrate_below_target_value_is_upcast_and_reenveloped_at_target`, `LatticeSchemaVersionMigrationTests.Migrate_legacy_unstamped_value_is_stamped_at_target_without_upcast`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_un_upcastable_value_aborts_and_leaves_tree_untouched`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_reads_the_stored_form_never_the_decoding_client_read`. |
| `MigrateCutover` | Every value is built: the tree cuts over to the build and releases the reservation. | `SchemaRemediationDriver.DriveAsync`, `LatticeSchemaVersionAdmin.MigrateToTargetVersionAsync`. | Yes: `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_restamps_mixed_versions_to_target_and_cuts_over`, `LatticeSchemaOperationsIntegrationTests.Runtime_registration_drives_lazy_reads_and_eager_migration_and_is_tree_scoped`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `CatalogConsistent` | The effective catalog names each version of a family once, holds at most one hop from any version, and every hop ascends between registered versions; a runtime entry never redefines a host declaration. | Yes: `SchemaRuntimeRegistryTests.Identical_retry_is_idempotent_conflicts_leave_persisted_catalog_unchanged`, `SchemaRuntimeRegistryTests.Invalid_hops_and_unknown_host_ids_are_rejected`, `SchemaRuntimeRegistryTests.A_registration_conflicting_with_a_host_declaration_is_refused_and_a_replay_of_one_persists_nothing`, `LatticeSchemaRegistryTests.AddSchema_duplicate_throws`, `LatticeSchemaRegistryTests.AddUpcaster_duplicate_from_version_throws`. |
| `ReservationHeldWhileRunning` | A migration holds the tree's alias reservation exactly while it runs, and releases it on completion and on abort. | Yes: `SchemaVersionMigrationAbortIntegrationTests.AdvanceAndMigrateAsync_with_no_upcast_hop_aborts_naming_the_value_and_releases_the_reservation`, `SchemaVersionMigrationAbortIntegrationTests.MigrateToTargetVersionAsync_over_a_value_newer_than_the_target_aborts_and_releases_the_reservation`. |
| `ShadowAtOwnTarget` | Every value a migration builds is stamped at the target the migration captured, from the value's own stamp. | Yes: `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_restamps_mixed_versions_to_target_and_cuts_over`, `LatticeSchemaVersionMigrationTests.Migrate_below_target_value_is_upcast_and_reenveloped_at_target`, `LatticeSchemaVersionMigrationTests.Migrate_value_already_at_target_is_returned_unchanged`. |
| `UpcastOrExplicitFailure` | A read of a value stamped with the tree's family returns the body upcast to the target when a chain of hops reaches it, and otherwise fails explicitly, as newer than the target or with no path. | Yes: `LatticeSchemaVersionDecoderTests.DecodeAsync_stale_value_is_upcast_to_target`, `LatticeSchemaVersionDecoderTests.DecodeAsync_newer_than_target_throws_not_supported`, `LatticeSchemaVersionDecoderTests.DecodeAsync_stale_value_with_no_upcast_path_throws_not_supported`, `LatticeSchemaRegistryTests.CanUpcast_hop_overshooting_the_target_is_false`, `LatticeSchemaRegistryTests.Upcast_hop_overshooting_the_target_throws_not_supported`, `SchemaTargetAdvanceIntegrationTests.AdvanceTargetVersionAsync_rewrites_no_stored_value_and_a_later_registered_hop_upcasts_it_on_read`. |
| `UnversionedReadsStoredBody` | An un-stamped value, a stamp read with no config, and another family's stamp read back as the body they were stored with. | Yes: `LatticeSchemaVersionDecoderTests.DecodeAsync_un_stamped_value_returns_same_reference`, `LatticeSchemaVersionDecoderTests.DecodeAsync_enveloped_with_no_config_strips_stored_body`, `LatticeSchemaVersionDecoderTests.DecodeAsync_enveloped_with_mismatched_schema_strips_stored_body`. |
| `CatalogAppendOnly` | No registration removes or redefines a runtime entry. | Yes: `SchemaRuntimeRegistryTests.A_later_registration_keeps_every_earlier_entry`, `SchemaRuntimeRegistryTests.Identical_retry_is_idempotent_conflicts_leave_persisted_catalog_unchanged`. |
| `RegistrationAllOrNothing` | A registration lands whole when every entry merges, and otherwise is refused with nothing persisted. | Yes: `SchemaRuntimeRegistryTests.A_registration_with_any_conflicting_entry_persists_none_of_it`, `SchemaRuntimeRegistryTests.Failed_persistence_rolls_back_the_live_catalog`. |
| `ReplayIdempotent` | Re-registering entries the catalog already holds, declared or registered, succeeds and persists nothing new. | Yes: `SchemaRuntimeRegistryTests.Identical_retry_is_idempotent_conflicts_leave_persisted_catalog_unchanged`, `SchemaRuntimeRegistryTests.A_registration_conflicting_with_a_host_declaration_is_refused_and_a_replay_of_one_persists_nothing`. |
| `ValidatedTarget` | While the tree's catalog is non-empty, the facade sets or advances the target only to a registered version, not below the current target, that the current target reaches through registered hops. | Yes: `LatticeSchemaControlVersionTargetTests.SetVersionConfig_refuses_a_target_with_no_registered_descriptor`, `LatticeSchemaControlVersionTargetTests.SetVersionConfig_refuses_a_lower_target_or_another_family`, `LatticeSchemaControlVersionTargetTests.SetVersionConfig_refuses_a_target_the_current_target_cannot_reach`, `LatticeSchemaControlVersionTargetTests.AdvanceTargetVersion_refuses_a_target_with_no_registered_path`. |
| `AdvanceStrict` | An accepted advance moves an existing target strictly forward. | Yes: `LatticeSchemaVersionAdminTests.AdvanceTargetVersionAsync_non_advancing_throws`, `LatticeSchemaVersionAdminTests.AdvanceAndMigrateAsync_non_advancing_target_throws`, `LatticeSchemaVersionAdminTests.AdvanceTargetVersionAsync_unversioned_tree_throws`. |
| `ConfigNeverRewritesData` | Advancing or clearing the target, or registering a hop, rewrites no stored value, so it re-projects nothing. | Yes: `SchemaTargetAdvanceIntegrationTests.AdvanceTargetVersionAsync_rewrites_no_stored_value_and_a_later_registered_hop_upcasts_it_on_read`, `SchemaTargetAdvanceIntegrationTests.ClearVersionConfigAsync_rewrites_no_stored_value_and_reads_return_the_stored_body`, `ProjectionInputSeamCharacterizationTests.Incremental_maintenance_projects_the_stripped_body_at_its_stamped_version`. |
| `AbortLeavesDataUntouched` | A migration that reaches a value it cannot upcast, or whose upcast body the policy rejects, aborts naming that value, publishes nothing and releases the reservation. | Yes: `SchemaVersionMigrationAbortIntegrationTests.AdvanceAndMigrateAsync_with_no_upcast_hop_aborts_naming_the_value_and_releases_the_reservation`, `SchemaVersionMigrationAbortIntegrationTests.MigrateToTargetVersionAsync_over_a_value_newer_than_the_target_aborts_and_releases_the_reservation`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_un_upcastable_value_aborts_and_leaves_tree_untouched`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_with_policy_aborts_when_post_upcast_body_violates`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_with_policy_validates_post_upcast_body_and_cuts_over`. |
| `MigrationReadEquivalent` | A completed migration publishes, under every key, a value that reads back at the migration's target exactly as the read-time upcast of the old value did. | Yes: `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_restamps_mixed_versions_to_target_and_cuts_over`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_legacy_unstamped_value_is_stamped_at_target`, `SchemaVersionMigrationAbortIntegrationTests.MigrateToTargetVersionAsync_with_no_upcast_hop_aborts_then_succeeds_once_the_hop_is_registered`. |
| `MigrationTerminates` | Every migration ends, completed or aborted, rather than staying in flight. | Yes: `SchemaVersionMigrationAbortIntegrationTests.StartMigrationAsync_with_no_upcast_hop_records_a_terminal_aborted_operation_and_releases_the_reservation`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_resumes_in_flight_migration_and_cuts_over`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, CatalogConsistent, ReservationHeldWhileRunning, ShadowAtOwnTarget,
UpcastOrExplicitFailure and UnversionedReadsStoredBody are state safety.
CatalogAppendOnly, RegistrationAllOrNothing, ReplayIdempotent, ValidatedTarget,
AdvanceStrict, ConfigNeverRewritesData, AbortLeavesDataUntouched and
MigrationReadEquivalent are action safety. MigrationTerminates is temporal
liveness under weak fairness of the per-value step and the cutover. The base
checks one key against a host that declares versions 1 and 3 with a hop from 1
to 3, so a target of 2 makes the declared hop overshoot. The TwoKeys variant
re-checks every state and action check with two keys, so a build holds one value
while the other aborts it; the Undeclared variant re-checks them with no host
declarations, so the facade starts ungoverned and the whole catalog, an
overshooting hop included, is built at run time. MigrationTerminates is
bounded-out in both variants for the TLC liveness budget and is retained on the
base.

Every experiment is an anchored perturbation:

| Mutation | Classification |
|----------|----------------|
| TypeOKPutUndeclaredValue | Anchored action: Put. |
| CatalogConsistentRenameAdmitted | Anchored definition: Admissible. |
| ReservationHeldWhileRunningBeginUnreserved | Anchored action: MigrateBegin. |
| ShadowAtOwnTargetLiveTarget | Anchored action: MigrateStep. |
| UpcastOrExplicitFailureOvershootAccepted | Anchored definition: Walk. |
| UnversionedReadsStoredBodyForeignUpcast | Anchored definition: Decode. |
| CatalogAppendOnlyReplaces | Anchored action: Register. |
| RegistrationAllOrNothingPartialCommit | Anchored action: Register. |
| ReplayIdempotentReplayRefused | Anchored action: Register. |
| ValidatedTargetPathSkipped | Anchored action: SetConfig. |
| AdvanceStrictEqualAccepted | Anchored action: Advance. |
| ConfigNeverRewritesDataClearStrips | Anchored action: ClearConfig. |
| AbortLeavesDataUntouchedValidatesStored | Anchored action: MigrateStep. |
| MigrationReadEquivalentCutoverDropsKey | Anchored action: MigrateCutover. |
| MigrationTerminatesNoStepFairness | Anchored fairness: Spec. |

## Detectors

The registry detectors drive the real runtime-registry grain over a persistent
state fake and a real host registry, and read back what was persisted. The
facade detectors drive the real schema control over a real runtime registry
whose grain is substituted, and assert both the refusal and that the version
admin was never reached. The decoder, registry and per-value migration tests run
the real components over real envelopes. The migration grain tests and the
integration fixtures drive the real migration through a cluster or a faked
lattice and read back the data, the remediation status, the alias and the
operation record; the target-advance fixture reads a stamped value back through
the decoding read before and after a hop is registered, so a target change that
rewrote stored values would read back differently.

## Defect mutations whose fixes have landed

AbortLeavesDataUntouched and MigrationTerminates are the #5208 contract: a
migration that reached a value it could not upcast read the slice through the
decoding client read, threw before the per-value abort, and stayed in flight
holding the alias reservation. ShadowAtOwnTargetLiveTarget is the side of that
fix that makes each value re-stamp from its own stamp to the migration's own
target rather than through the live config. ConfigNeverRewritesData is the
#5238 contract that a target advance re-projects nothing. Both fixes are in the
schema-assurance integration bucket; this is integration-branch evidence, not a
claim the fixes are released.

## Deliberate abstraction gaps

- **One family, one tree, a bounded catalog.** The model tracks one tree, one
  configured family plus a stamp of another family, three versions and a fixed
  set of candidate registrations. The catalog size limits, the serialized-size
  bound, transform-tree validation and the snapshotting that stops callers
  mutating persisted arrays are checked by unit detectors, not modelled.
  Production symbols: `SchemaRuntimeCatalog.ValidateSize`, `SchemaRuntimeCatalog.Snapshot`.
- **Family changes.** The configured family is fixed. The facade refuses a
  family change while the catalog is non-empty, and an unversioned tree may be
  opted in under any family; neither is modelled.
  Production symbols: `SchemaRuntimeRegistry.ValidateTargetAsync`.
- **In-process admins bypass the catalog check.** Only the facade validates a
  target against the catalog. The in-process version admin replaces the config
  unchecked, so it can lower the target or change the family, and advances with
  only the monotonic check. The model's config actions are the facade's.
  Production symbols: `LatticeSchemaVersionAdmin.SetVersionConfigAsync`, `LatticeSchemaVersionAdmin.AdvanceTargetVersionAsync`.
- **Per-silo config caches.** The model reads the durable config. A silo's cached
  config is evicted on the admin's own silo before the change returns and on every
  other silo within the propagation interval; within that window a lagging silo
  stamps and decodes at the previous target. That propagation is the
  `SchemaPolicyConfig` module's.
  No production symbol: the gap delegates to another model, not to a production mechanism.
- **Upcaster semantics.** A hop is an abstract, total, deterministic function
  that moves a body to its target's shape and preserves whether the policy
  accepts it. A transform that throws on a malformed body, a host transform id
  not installed on a serving silo, and arbitrary transform code are not
  modelled; each surfaces as the same explicit failure as a missing hop.
  Production symbols: `LatticeSchemaUpcaster.Apply`, `LatticeValueTransformEvaluation.Evaluate`.
- **Runtime registration support.** A custom host registry keeps every catalog
  host-provisioned and refuses runtime registration; the model assumes the
  standard registry.
  Production symbols: `SchemaRuntimeRegistry.IsSupported`.
- **Catalog provenance.** The effective catalog merges host declarations and
  runtime registrations without recording which source each entry came from, so
  a caller reading it cannot tell them apart. The model distinguishes them only
  to check that a runtime entry never redefines a declaration.
  Production symbols: `SchemaRuntimeRegistry.GetAsync`.
- **Per-region catalog.** The runtime catalog is per cluster and is not
  replicated. A value replicated or restored from a region whose catalog holds
  hops this one lacks lands through the environment like any other stamp, and
  reads or migrates as a value with no path. Replication itself is the atomic,
  replication and tenant model's.
  Production symbols: `LatticeSchemaRuntimeRegistryGrain`.
- **Write stamping and strict ingest.** Values land through an environment step
  that may carry any stamp. The write stage that stamps local writes at the
  target, and strict ingest that checks a replicated stamp against the catalog,
  are the stage-ordering and strict-ingest models'.
  No production symbol: the gap delegates to other models, not to a production mechanism.
- **Migration phases and concurrency.** A migration is one capture, one step per
  key and one cutover. Its dry run, slices, durable progress, cancellation,
  shadow tree layout and alias swap, and the quiescence it requires (no write
  is forwarded into the build, so writes are excluded while it runs), are the
  remediation model's, as is the transform remediation that shares its
  machinery.
  No production symbol: the gap delegates to another model, not to a production mechanism.
- **Tree policy.** A policy is a fixed abstract judgement over a body's shape and
  validity, installed for the whole check. A tree with no policy migrates
  without validation, and rule semantics are not modelled.
  No production symbol: the gap states a bound of the model, not a production mechanism.
- **CRDT deltas.** Typed-CRDT deltas are lifted to the target once at the write
  stage and folded at their stored stamp; that path is not modelled here.
  Production symbols: `LatticeSchemaEnvelopeCodec`.

# Refinement note: schema remediation copy and cutover

## Model scope in one paragraph

The copy-and-cutover protocol beneath a schema remediation (transform mode) and
an eager version migration, as `LatticeSchemaRemediationGrain` runs it for one
logical tree. The coordinator takes the tree's alias reservation, then persists
its intent: the mode, a fresh destination id and the physical copy the tree
resolves to now. A read-only dry run judges every value; the build registers
the destination with the source's layout, writes each rewritten value into it
(re-stamped at the target on a versioned tree, in both modes) and banks its
progress slice by slice. The cutover installs a transform's target policy,
swaps the alias together with the destination's map in one registry write, and
arms every shard of the captured source to redirect stale routers, then records
completion and releases the reservation. A crash anywhere loses only in-turn
progress, and the next slice resumes from the banked cursor. An abort or cancel
publishes nothing, discards any destination and releases the reservation. A
cutover whose captured source has been superseded refuses to publish (#5289).
The environment writes to the tree, routes stale writes at superseded copies,
reshards the live copy, moves the tree by a resize, restore or revert that
takes the reservation, and sets the alias explicitly, which is refused while
the reservation is held but may have been admitted just before it was taken.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `phase`, `mode`, `cursor` | The durable phase, build mode and banked slice cursor. | `SchemaRemediationState.Phase`, `SchemaRemediationState.Mode`, `SchemaRemediationState.ScanCursor`, `SchemaRemediationState.ScannedCount`. |
| `pend`, `cstep`, `begin` | In-turn progress a crash loses: a destination write not yet banked, the cutover step reached, a reservation taken before intent was persisted. | `LatticeSchemaRemediationGrain.RunBuildSliceAsync`, `LatticeSchemaRemediationGrain.CutoverAsync`, `LatticeSchemaRemediationGrain.AcceptAsync`. |
| `dest`, `srcPhys`, `destReg`, `inherited` | The destination copy, the source copy captured at intent, whether the destination is registered, and the layout it inherited. | `SchemaRemediationState.DestinationTreeId`, `SchemaRemediationState.SourcePhysicalTreeId`, `DerivedTreeEntries.InheritingAsync`. |
| `reserved` | The tree's alias reservation. | `SchemaRemediationState.AliasReservationId`, `ITreeDeletionGrain.BeginAliasChangeAsync`, `ITreeDeletionGrain.EndAliasChangeAsync`. |
| `alias`, `mapOf`, `shards` | The physical copy the logical row names, the copy its map describes, and each copy's shard layout. | `ILatticeRegistry.SwapAliasAsync`, `TreeRegistryEntry`, `ShardMap`. |
| `armed` | Retained redirects armed on a copy's shards. | `IShardRootGrain.MarkRetainedRedirectAsync`. |
| `policy` | The tree's enforcement policy. | `ILatticeSchemaPolicyStore.SetPolicyAsync`. |
| `data`, `discarded` | Each copy's values, and the destinations soft-deleted on an abort, cancel or supersession. | `ILattice.SetAsync`, `ILattice.DeleteTreeAsync`. |
| `report`, `abortKey` | The terminal report and the value an abort names. | `LatticeSchemaRemediationReport`, `LatticeSchemaRemediationReport.OffendingKey`. |
| `nextId`, `retired`, `rems`, `writes`, `crashes`, `moved`, `resharded`, `explicit`, `dirty`, `late`, `last` | Ghosts and environment bounds: fresh ids taken in order, copies a cutover or alias move retired, budgets, whether a write landed during the remediation, whether a stale write landed on a retired copy, and the last step's name. | No production counterpart. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Reserve` | The coordinator takes the alias reservation, persisting its id, before it persists any intent. | `LatticeSchemaRemediationGrain.AcceptAsync`, `ITreeDeletionGrain.BeginAliasChangeAsync`. | Yes: `LatticeSchemaRemediationGrainTests.ReleaseAlias_write_failure_restores_the_reservation_id`, `SchemaVersionMigrationAbortIntegrationTests.AdvanceAndMigrateAsync_with_no_upcast_hop_aborts_naming_the_value_and_releases_the_reservation`, `SchemaRemediationSupersededCutoverIntegrationTests.An_explicit_alias_while_a_remediation_holds_the_reservation_is_refused_naming_it`. |
| `Initiate(m)` | Intent is persisted before any side effect: the mode, a fresh destination suffix never derived from the operation id, and the source physical copy read from the registry entry. | `LatticeSchemaRemediationGrain.AcceptAsync`, `LatticeSchemaRemediationGrain.AcceptVersionMigrationAsync`. | Yes: `LatticeSchemaRemediationGrainTests.AcceptAsync_persists_intent_without_scanning_and_records_the_operation_id`, `LatticeSchemaRemediationGrainTests.InitiateCoreAsync_reads_entry_directly_for_source_physical_tree_id`, `LatticeSchemaRemediationGrainTests.Initiate_write_failure_restores_every_field_it_staged`. |
| `DryStep(k)` | One dry-run value is rewritten and judged; the first offender aborts with no destination built, naming it. | `LatticeSchemaRemediationGrain.RunSliceAsync`. | Yes: `LatticeSchemaRemediationGrainTests.StartAsync_dry_run_violation_aborts_with_no_cutover_and_leaves_original_untouched`, `SchemaRemediationIntegrationTests.Remediation_abort_leaves_the_original_tree_untouched`, `SchemaRemediationUpcastAbortIntegrationTests.RemediateAsync_over_a_value_with_no_upcast_hop_aborts_naming_the_value_and_releases_the_reservation`. |
| `BuildStep(k)` | The first build slice registers the destination with the source's layout; each value is rewritten, re-stamped at the target, judged and written to the destination; an offender discards the destination and aborts. | `LatticeSchemaRemediationGrain.RunSliceAsync`, `DerivedTreeEntries.InheritingAsync`, `LatticeSchemaVersionMigration.Migrate`. | Yes: `LatticeSchemaRemediationGrainTests.RunRemediationPassAsync_build_time_violation_discards_partial_destination_and_does_not_cut_over`, `LatticeSchemaRemediationGrainTests.The_destination_inherits_the_source_topology_pins_and_overrides`, `SchemaRemediationTopologyInheritanceIntegrationTests.Remediation_of_a_resharded_pinned_tree_keeps_its_map_shard_count_and_pins`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_restamps_mixed_versions_to_target_and_cuts_over`, `LatticeSchemaRemediationGrainTests.StartAsync_over_a_versioned_tree_writes_every_value_enveloped_at_the_target_version`. |
| `Bank` | A slice persists how far it got; the exhausted source moves the build to the cutover. | `LatticeSchemaRemediationGrain.RunSliceAsync`. | Yes: `LatticeSchemaRemediationGrainTests.RunSliceAsync_processes_one_bounded_slice_at_a_time_through_every_phase`, `LatticeSchemaRemediationGrainTests.A_faulted_build_slice_banks_the_values_it_wrote_and_the_next_slice_resumes_after_them`. |
| `Supersede` | A cutover that finds the alias on neither its captured source nor its destination refuses, before installing anything or when its fenced swap is refused: no swap, a policy this attempt installed put back, the destination discarded, the reservation released, and a terminal aborted report naming the supersession. | `LatticeSchemaRemediationGrain.CutoverAsync`, `LatticeSchemaRemediationReport.Superseded`. | Yes: `LatticeSchemaRemediationGrainTests.Cutover_superseded_before_it_starts_installs_no_policy_swaps_nothing_and_aborts_naming_it`, `LatticeSchemaRemediationGrainTests.Cutover_superseded_at_its_swap_restores_the_prior_policy_and_aborts`, `SchemaRemediationSupersededCutoverIntegrationTests.Cutover_after_the_alias_moved_to_another_tree_is_refused_with_no_swap_and_the_new_alias_intact`, `SchemaOperationServiceTests.ToCompletion_records_a_superseded_publication_as_an_abort_naming_the_supersession`. |
| `CutPolicy` | A transform remediation records the tree's policy, then installs its target policy and evicts the local cache before the swap; a migration keeps the tree's policy. | `LatticeSchemaRemediationGrain.CutoverAsync`, `ILatticeSchemaPolicyStore.SetPolicyAsync`. | Yes: `LatticeSchemaRemediationGrainTests.Cutover_arms_the_target_policy_before_repointing_the_alias`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_with_policy_validates_post_upcast_body_and_cuts_over`. |
| `CutSwap` | The alias and the destination's map move together in one registry write, fenced on the captured source; a swap resumed after it landed changes nothing. | `AliasCutoverShardMaps.SwapCutoverAsync`, `ILatticeRegistry.SwapAliasAsync`. | Yes: `AliasCutoverShardMapsRefusalTests.SwapCutoverAsync_over_the_captured_source_swaps_fenced_on_it`, `SchemaRemediationCutoverShardMapIntegrationTests.Cutover_of_a_resharded_tree_keeps_every_value_readable`, `SchemaRemediationCutoverShardMapIntegrationTests.Cutover_of_a_resharded_aliased_tree_keeps_every_value_readable`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_resumes_in_flight_migration_and_cuts_over`. |
| `CutArm` | Every shard of the captured source, at its live layout, is armed to redirect logical traffic onto the destination. | `LatticeSchemaRemediationGrain.CutoverAsync`, `IShardRootGrain.MarkRetainedRedirectAsync`. | Yes: `SchemaRemediationCutoverAfterReshardIntegrationTests.Cutover_after_a_reshard_arms_every_source_shard`, `SchemaRemediationCutoverShardMapIntegrationTests.Cutover_of_a_resharded_aliased_tree_arms_every_source_shard`. |
| `Complete` | Completion is recorded and the reservation released. | `LatticeSchemaRemediationGrain.RunSliceAsync`. | Yes: `LatticeSchemaRemediationGrainTests.StartAsync_all_values_remediate_cuts_over_and_installs_policy`, `LatticeSchemaRemediationGrainTests.Complete_write_failure_restores_progress_phase_and_report`. |
| `Cancel` | A cancel in the dry run or build discards any destination and releases the reservation. | `LatticeSchemaRemediationGrain.CancelAsync`. | Yes: `LatticeSchemaRemediationGrainTests.CancelAsync_during_the_build_discards_the_destination_and_records_cancelled`, `LatticeSchemaRemediationGrainTests.CancelAsync_during_the_dry_run_records_cancelled_without_touching_a_destination`. |
| `IdlePass` | An idle pass releases a reservation whose intent was never persisted. | `LatticeSchemaRemediationGrain.RunRemediationPassAsync`. | Yes: `LatticeSchemaRemediationGrainTests.Idle_pass_releases_an_orphan_preparation_idempotently`, `LatticeSchemaRemediationGrainTests.External_idle_pass_cannot_release_a_control_plane_reservation`. |
| `Crash` | The coordinator's activation is lost: in-turn progress goes, the phase, cursor, reservation and destination writes stay, and the next slice resumes. | `SchemaRemediationState`, `LatticeSchemaRemediationGrain.RunRemediationPassAsync`. | Yes: `LatticeSchemaRemediationGrainTests.RunRemediationPassAsync_resumes_a_durable_build_after_reactivation`, `LatticeSchemaRemediationGrainTests.A_faulted_dry_run_slice_banks_its_progress_and_resumes_after_it`, `LatticeSchemaRemediationGrainTests.AdvancePhase_write_failure_restores_the_phase_and_scanned_count`. |
| `Write(k,v)` | Environment: an ordinary write through the logical tree. | No production counterpart. | Not applicable. |
| `StaleWrite(c,s,k)` | Environment: a router caching a superseded copy routes a write at one of its shards; the armed redirect forwards it to the live copy. | `IShardRootGrain.MarkRetainedRedirectAsync`, `StaleTreeRoutingException`. | Yes: `SchemaRemediationCutoverAfterReshardIntegrationTests.Cutover_after_a_reshard_arms_every_source_shard`, `BareAliasRoutingIntegrationTests.SetAliasAsync_moves_every_warmed_router_and_never_writes_the_retained_copy`. |
| `Move` | Environment: a resize, restore or revert takes the reservation, moves the tree onto a fresh copy and arms the old one; refused while the reservation is held. | `ITreeDeletionGrain.BeginAliasChangeAsync`, `TreeResizeGrain`. | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`, `SchemaRemediationTopologyInheritanceIntegrationTests.Repeated_remediation_follows_a_reshard_through_the_alias`. |
| `Reshard` | Environment: an online reshard grows the layout of the copy the tree resolves to; it takes no reservation. | `ILattice.ReshardAsync`. | Yes: `SchemaRemediationCutoverAfterReshardIntegrationTests.Cutover_after_a_reshard_arms_every_source_shard`, `SchemaRemediationTopologyInheritanceIntegrationTests.Repeated_remediation_follows_a_reshard_through_the_alias`. |
| `ExplicitAdmit` | Environment: an explicit alias set is admitted only while no alias reservation is held, checked before anything is written and again in the registry's turn. | `AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync`, `LatticeRegistryGrain.SetAliasAsync`, `ITreeDeletionGrain.EnsureAliasUnreservedAsync`. | Yes: `TreeDeletionGrainTests.Explicit_alias_is_refused_while_a_reservation_is_held_naming_the_holder`, `LatticeTreeAdminAliasShardMapIntegrationTests.SetTreeAliasAsync_while_an_alias_operation_holds_the_reservation_is_refused_naming_it`, `TreeResizeAliasLifecycleIntegrationTests.Explicit_alias_while_an_alias_operation_holds_the_reservation_is_refused_naming_it`. |
| `ExplicitPublish` | Environment: an admitted explicit alias set moves the tree onto another copy with its map and arms the copy it replaces. | `AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync`. | Yes: `AliasSwapRoutingAtomicityIntegrationTests.An_explicit_alias_moves_warm_point_reads_onto_the_target_and_back`, `AliasCutoverShardMapsTests.CarryAcrossExplicitAliasAsync_moves_the_alias_with_the_target_map_and_stamps_the_replaced_tree`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `ReservationHeldWhileInFlight` | The alias reservation is held for the whole of a remediation and released only once it is terminal. | Yes: `SchemaVersionMigrationAbortIntegrationTests.MigrateToTargetVersionAsync_over_a_value_newer_than_the_target_aborts_and_releases_the_reservation`, `LatticeSchemaRemediationGrainTests.Idle_pass_releases_an_orphan_preparation_idempotently`, `SchemaRemediationSupersededCutoverIntegrationTests.An_explicit_alias_while_a_remediation_holds_the_reservation_is_refused_naming_it`. |
| `FreshDestination` | The destination is a fresh physical tree, never named by the operation id, so a reused id never names a copy an earlier remediation cut over to. | Yes: `LatticeSchemaRemediationGrainTests.AcceptAsync_persists_intent_without_scanning_and_records_the_operation_id`. |
| `DestinationInheritsTopology` | The destination is registered with the source's routing map, pins and overrides, so a resharded tree keeps its layout. | Yes: `LatticeSchemaRemediationGrainTests.The_destination_inherits_the_source_topology_pins_and_overrides`, `SchemaRemediationTopologyInheritanceIntegrationTests.Remediation_of_a_resharded_pinned_tree_keeps_its_map_shard_count_and_pins`. |
| `AliasMapCoherent` | The logical row's map always describes the copy its alias names (#4336). | Yes: `SchemaRemediationCutoverShardMapIntegrationTests.Cutover_of_a_resharded_tree_keeps_every_value_readable`, `SchemaRemediationCutoverShardMapIntegrationTests.Cutover_of_a_resharded_aliased_tree_keeps_every_value_readable`. |
| `EnforcedBeforeLive` | A transform remediation's destination is never live without its target policy. | Yes: `LatticeSchemaRemediationGrainTests.Cutover_arms_the_target_policy_before_repointing_the_alias`, `SchemaRemediationIntegrationTests.Remediation_rewrites_values_cuts_over_and_enforces_the_new_policy`. |
| `DestinationStampedAtTarget` | On a versioned tree every value the destination holds is stamped at the target, in both modes (#5243). | Yes: `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_restamps_mixed_versions_to_target_and_cuts_over`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_legacy_unstamped_value_is_stamped_at_target`, `LatticeSchemaRemediationGrainTests.StartAsync_over_a_versioned_tree_writes_every_value_enveloped_at_the_target_version`, `SchemaRemediationVersionStampIntegrationTests.RemediateAsync_over_a_versioned_tree_stamps_every_remediated_value_at_the_target_version`, `SchemaRemediationVersionStampIntegrationTests.RemediateAsync_over_an_upcast_value_stamps_it_at_the_version_the_transform_ran_on`. |
| `SupersededCopyTakesNoWrite` | A write a stale router routes at a copy a cutover retired is redirected to the live copy, never stored on the retired one. | Yes: `SchemaRemediationCutoverAfterReshardIntegrationTests.Cutover_after_a_reshard_arms_every_source_shard`, `BareAliasRoutingIntegrationTests.SetAliasAsync_moves_every_warmed_router_and_never_writes_the_retained_copy`. |
| `AbortPublishesNothing` | An abort or cancel leaves the alias and policy untouched, discards any destination, releases the reservation, and an abort names a value the remediation could not rewrite (#5250, #5257). | Yes: `LatticeSchemaRemediationGrainTests.RunRemediationPassAsync_build_time_violation_discards_partial_destination_and_does_not_cut_over`, `SchemaRemediationUpcastAbortIntegrationTests.StartRemediationAsync_over_a_value_with_no_upcast_hop_records_a_terminal_aborted_operation_and_releases_the_reservation`, `LatticeSchemaRemediationGrainTests.CancelAsync_during_the_build_discards_the_destination_and_records_cancelled`, `SchemaRemediationIntegrationTests.Remediation_abort_leaves_the_original_tree_untouched`. |
| `SupersededPublicationRefused` | A cutover swaps the alias only off the source it captured, or resumes over its own destination (#5289). | Yes: `AliasCutoverShardMapsRefusalTests.SwapCutoverAsync_superseded_since_the_captured_source_is_refused_without_writing`, `SchemaRemediationSupersededCutoverIntegrationTests.Cutover_after_the_alias_moved_to_another_tree_is_refused_with_no_swap_and_the_new_alias_intact`, `AliasCutoverShardMapsRefusalTests.SwapCutoverAsync_resumed_after_its_swap_publishes_nothing_whatever_it_captured`, `SchemaRemediationSupersededCutoverIntegrationTests.Cutover_resumed_after_its_swap_still_completes`. |
| `SupersededLeavesNothing` | A superseded cutover makes no swap, puts back any policy it installed in the same attempt, discards its destination and releases the reservation (#5289). | Yes: `LatticeSchemaRemediationGrainTests.Cutover_superseded_at_its_swap_restores_the_prior_policy_and_aborts`, `LatticeSchemaRemediationGrainTests.Cutover_superseded_before_it_starts_installs_no_policy_swaps_nothing_and_aborts_naming_it`, `SchemaRemediationSupersededCutoverIntegrationTests.Cutover_after_the_alias_moved_to_another_tree_is_refused_with_no_swap_and_the_new_alias_intact`. |
| `MigrationKeepsPolicy` | An eager migration never installs or changes the tree's policy. | Yes: `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_with_policy_validates_post_upcast_body_and_cuts_over`. |
| `ProgressNeverRegresses` | Banked progress survives a fault or reactivation; the next slice resumes after it. | Yes: `LatticeSchemaRemediationGrainTests.A_faulted_build_slice_banks_the_values_it_wrote_and_the_next_slice_resumes_after_them`, `LatticeSchemaRemediationGrainTests.A_faulted_dry_run_slice_banks_its_progress_and_resumes_after_it`. |
| `RedirectsCoverSource` | A completed cutover has armed every shard of the source at its live layout (#4206). | Yes: `SchemaRemediationCutoverAfterReshardIntegrationTests.Cutover_after_a_reshard_arms_every_source_shard`, `SchemaRemediationCutoverShardMapIntegrationTests.Cutover_of_a_resharded_aliased_tree_arms_every_source_shard`. |
| `ReservationExcludesAliasChanges` | No resize, restore, revert, delete or explicit alias set is admitted while the remediation holds the reservation (#5289). | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`, `SchemaRemediationSupersededCutoverIntegrationTests.An_explicit_alias_while_a_remediation_holds_the_reservation_is_refused_naming_it`, `TreeDeletionGrainTests.Explicit_alias_is_refused_while_a_reservation_is_held_naming_the_holder`. |
| `QuiescentCutoverComplete` | With no write during the remediation, the published copy holds exactly the rewrite of what a point read of the source served. | Yes: `SchemaRemediationIntegrationTests.Remediation_rewrites_values_cuts_over_and_enforces_the_new_policy`, `SchemaRemediationForeignRowIntegrationTests.The_remediated_copy_holds_what_a_point_read_of_the_original_served`, `LatticeSchemaRemediationGrainTests.StartVersionMigrationAsync_restamps_mixed_versions_to_target_and_cuts_over`. |
| `RemediationTerminates` | A driven remediation ends completed, aborted, cancelled or superseded. | Yes: `LatticeSchemaRemediationGrainTests.RunSliceAsync_processes_one_bounded_slice_at_a_time_through_every_phase`, `LatticeSchemaRemediationGrainTests.RunRemediationPassAsync_resumes_a_durable_build_after_reactivation`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, ReservationHeldWhileInFlight, FreshDestination,
DestinationInheritsTopology, AliasMapCoherent, EnforcedBeforeLive,
DestinationStampedAtTarget and SupersededCopyTakesNoWrite are state safety.
AbortPublishesNothing, SupersededPublicationRefused, SupersededLeavesNothing,
MigrationKeepsPolicy, ProgressNeverRegresses, RedirectsCoverSource,
ReservationExcludesAliasChanges and QuiescentCutoverComplete are action safety.
RemediationTerminates is temporal liveness under weak fairness of the
coordinator's steps, which a driver supplies by slicing the remediation (the
driver itself is `SchemaDurableOperation`'s). The TwoRemediations variant
re-checks every state and action check over two remediations in turn, the
smallest bound where a destination id could be reused and a later remediation
starts from a copy an earlier one published; the TwoKeys variant re-checks
them over two keys, which reaches a write after one key was copied and an
abort part-way through the build. Both leave the temporal check on the base for
the TLC liveness budget.

Every experiment is an anchored action or fairness perturbation:

| Mutation | Classification |
|----------|----------------|
| TypeOKInitiateUndeclaredMode | Anchored action: Initiate. |
| ReservationHeldWhileInFlightIdlePassReleasesLive | Anchored action: IdlePass. |
| ReservationHeldWhileInFlightIntentBeforeReservation | Anchored action: Reserve, Initiate. |
| FreshDestinationReusesDestination | Anchored action: Initiate. |
| DestinationInheritsTopologyDefaultLayout | Anchored action: BuildStep. |
| DestinationInheritsTopologyReshardHitsDestination | Anchored action: Reshard. |
| AliasMapCoherentSwapLeavesMap | Anchored action: CutSwap. |
| EnforcedBeforeLivePolicyNeverInstalled | Anchored action: CutPolicy. |
| DestinationStampedAtTargetTransformDropsStamp | Anchored action: BuildStep. |
| SupersededCopyTakesNoWriteRedirectIgnored | Anchored action: StaleWrite. |
| SupersededCopyTakesNoWriteExplicitAliasUnarmed | Anchored action: ExplicitPublish. |
| AbortPublishesNothingBuildAbortKeepsDestination | Anchored action: BuildStep. |
| AbortPublishesNothingDryRunNamesValidValue | Anchored action: DryStep. |
| AbortPublishesNothingCancelKeepsDestination | Anchored action: Cancel. |
| SupersededPublicationRefusedSwapUnfenced | Anchored action: CutSwap. |
| SupersededLeavesNothingKeepsDestination | Anchored action: Supersede. |
| SupersededLeavesNothingKeepsInstalledPolicy | Anchored action: Supersede. |
| MigrationKeepsPolicyMigrationInstalls | Anchored action: CutPolicy. |
| ProgressNeverRegressesCrashLosesCursor | Anchored action: Crash. |
| RedirectsCoverSourceArmsInheritedLayout | Anchored action: CutArm. |
| RedirectsCoverSourceCompleteBeforeArm | Anchored action: Complete. |
| ReservationExcludesAliasChangesAdmitIgnoresReservation | Anchored action: ExplicitAdmit. |
| ReservationExcludesAliasChangesMoveDuringRemediation | Anchored action: Move. |
| QuiescentCutoverCompleteCutsOverEarly | Anchored action: Bank. |
| RemediationTerminatesNoArmFairness | Anchored fairness: Spec. |

## Rely-guarantee composition

The module restates, rather than `INSTANCE`s, the guarantees of its neighbours:

- Which values a remediation cannot rewrite is abstracted to one flag per value.
  `SchemaVersionFamily` discharges it for a migration (`AbortLeavesDataUntouched`
  names exactly the values with no path to the target, and
  `MigrationReadEquivalent` makes the re-stamped copy read like the read-time
  upcast); `SchemaStageOrdering` and `SchemaPolicyConfig` own the policy
  judgement a rewritten body faces.
- Admission, the driver that slices the remediation, its cancellation and the
  superseded follower are `SchemaDurableOperation`'s; this module assumes a
  driver keeps slicing (weak fairness) and that a cancel arrives only in the dry
  run or build.
- The swap's atomic alias-and-map row, its expected-identity fence, retained
  redirects and repeated moves are `AliasRouting`'s and `AliasRevert`'s
  (`spec/aliasing/`); this module restates them as the one-step `CutSwap` and
  the redirect-following `StaleWrite`.
- The guarantee this module gives the schema and view composition model: a completed
  cutover publishes a copy every value of which was written through the
  destination's ordinary write path, stamped at the target on a versioned tree,
  so a view over the tree re-projects the stripped body at that stamp from the
  destination's WAL rather than keeping contributions derived from the source.

## Detectors

The grain detectors drive the real `LatticeSchemaRemediationGrain` over
substituted source, destination, registry and policy-store grains, slice by
slice, with injected state-write faults and seeded durable state; they observe
the reservation, intent, destination registration, writes, policy install,
alias swap and terminal report each action names. The integration detectors run
remediations and migrations on a real single-silo cluster, reshard, alias and
delete trees, and probe the source's shards for armed redirects through the
logical routing context. `SchemaRemediationCutoverChaosTests` re-checks reader
visibility across repeated cutovers of a resharded tree in the chaos tier; no
row relies on it alone.

## Defect mutations whose fixes have landed

SupersededPublicationRefusedSwapUnfenced and
ReservationExcludesAliasChangesAdmitIgnoresReservation are the #5289 defect this
model found: a remediation cutover published over an explicit alias set an
administrator made while it ran, mixing two copies, because the swap was fenced
only on the alias it read just before it and the explicit set never consulted
the reservation. DestinationStampedAtTargetTransformDropsStamp is the #5243
shape: transform remediation of a versioned tree wrote decoded bodies with no
envelope. Both are fixed in the schema-assurance integration bucket; this is
integration-branch evidence, not a claim the fixes are released.
AbortPublishesNothingDryRunNamesValidValue stands in for #5208 and #5252, whose
fixes made an un-upcastable value abort naming it rather than throw out of the
slice holding the reservation. RedirectsCoverSourceArmsInheritedLayout is the
#4206 shape and AliasMapCoherentSwapLeavesMap the #4336 shape.

## Deliberate abstraction gaps

- **Writes during the remediation.** The build copies at the logical level and
  does not shadow-forward writes, so the protocol requires the tree be
  write-quiesced from intent to completion: a write that lands after its key
  was copied is not carried into the destination and is superseded by the
  swap. The model lets writes land at any time and claims completeness only for
  a quiescent run (`QuiescentCutoverComplete`); it makes no claim for writes in
  the build window.
  Production symbols: `LatticeSchemaRemediationGrain`.
- **Values, slices and rewrites.** A value is abstracted to its stamp, whether
  the remediation can rewrite it and whether a transform rewrote it; one value
  is processed per step and slices are as large as the model needs. Transform
  semantics, rule semantics, the dry-run preview bound and the per-value decode
  are owned by the stage-ordering, policy and version-family models. One schema
  family is modelled: a value of another family keeps its own stamp when a
  transform remediation re-stamps the rest.
  Production symbols: `LatticeSchemaRemediationGrain.DefaultSliceSize`, `LatticeValueTransformEvaluation`.
- **One target, never advanced mid-run.** The target version is fixed for the
  run; a target advance while a migration or remediation runs, and the target a
  migration captures, are the version-family model's.
  No production symbol: the gap delegates to another model, not to a production mechanism.
- **Policy propagation.** The cutover's policy install is one step; eviction of
  other silos' caches within the propagation interval is the policy model's.
  Production symbols: `ILatticeSchemaPolicyProvider.Invalidate`.
- **A policy installed by a crashed cutover attempt.** A cutover that installed
  its target policy and was then lost before its swap resumes in a new attempt;
  if that attempt finds the source superseded it refuses before installing
  anything, so it does not know the policy the lost attempt replaced and leaves
  the target policy installed. Only a refusal within the attempt that installed
  the policy puts the earlier one back.
  Production symbols: `LatticeSchemaRemediationGrain`.
- **An ownership-guard refusal of the swap.** A registered tree-ownership guard
  can refuse the swap, leaving the remediation in its cutover with the target
  policy installed and the reservation held for retry or repair; the model's
  null guard never refuses.
  Production symbols: `ITreeOwnershipGuard`.
- **A superseded cutover after its swap.** An explicit alias set admitted before
  the reservation was taken and published after the swap but before the source
  was armed leaves a resumed cutover that refuses as superseded with the source
  unarmed; the explicit set armed the destination, not the source.
  No production symbol: the gap states a bound of the race the model admits, not a production mechanism.
- **Redirect chains, splits and folds.** Layouts are one or two shards, one
  reshard grows the live copy once, and a redirect is followed to a bounded
  depth; split and fold publication against a copy's map, and sparse slot
  movement, are the tree-lifecycle and ownership models'.
  No production symbol: the gap states a bound of the model, not a production mechanism.
- **Physical deletion and purge.** A discarded destination is soft-deleted and
  left for the sweeper when the delete fails; purge, recovery and deletion of a
  remediated tree's live copy are the tree-lifecycle model's.
  Production symbols: `ITreeDeletionGrain.DiscardIfAbandonedDerivedCopyAsync`.
- **Ids taken in order.** Fresh destination ids are taken in a fixed order to
  keep the state space small; production suffixes are random GUIDs.
  No production symbol: the gap states a bound of the model, not a production mechanism.

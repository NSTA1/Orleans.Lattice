# Refinement note: BackupRetention to code

This note maps [`BackupRetention.tla`](BackupRetention.tla) - backup retention
pruning racing backup captures - to the production symbols that play each role,
and to the detector tests that would notice production deviating from it. It is a
documented mapping, not a machine-checked refinement proof.

## Model scope in one paragraph

One backup scope under a retention policy (keep-last, max-age, or neither, the
unbounded no-op), whose `BackupSchedulerGrain` runs retention passes while full
and incremental captures of the same scope run concurrently: the scheduler's own,
the backup API's, and another silo's. A pass lists the catalog, computes the keep
set and its base closure, and prunes the rest one storage call at a time; an
increment reads its base, drains, writes its artifact and sink manifest, and
publishes its catalog row. Any capture and any pass may crash at any step, and the
clock advances. A backup id is a content address, as 10.2.0 documents: a capture
of identical content re-derives an existing backup's id - one still listed, one
a pass is deleting, or one long gone - and republishes it with a fresh artifact
(#5418). A full capture may be a member of a
multi-tree backup set, whose catalog row waits for the set's membership stamp
and is registered there, once (#5449). A
backup is restorable while its manifest resolves (catalog row first, then the sink
manifest, as the restore engine resolves it), its artifact is in the sink, and its
whole base chain is restorable. The model checks the fix of the defect it found
(#5357): the two-sided fence between a pass and an increment, the transitive keep
of what an increment outside the pass builds on, the tip-first delete order, and
the activation-local guard that runs one pass at a time, with the pass's
listing read under that guard (#5412); the per-id hold that keeps a republish
and a pass off the same id at once, so every listed backup's manifest stays in
the sink, and the ranking refresh that keeps a backup a capture has just
republished from being pruned as old (#5418); and that a set member is listed
only once it is stamped, so no pass can prune it before the stamp lists it
(#5449).

## The model is the fix of #5357

Before the fix, `BackupSchedulerGrain.PruneCoreAsync` deleted each doomed
manifest's artifacts, then its sink manifest, then its catalog row, from a plan
computed over a listing that cannot hold an increment still in flight; and
`LatticeBackupCaptureService.CaptureIncrementalAsync` read its base once, before
the drain, and registered its catalog row without looking at the base again. A
pass could therefore delete the base an in-flight increment had chosen, and the
increment was published with a base that no longer existed. The model found it
with three ids and one pass. Two mutations reproduce production before the fix:
`InFlightBaseSurvivesNoBaseRecheck` (the capture) and
`RetainedRestorableNoDependentScan` (the pass). Checking the fix found a second
hazard, `RetainedRestorableConcurrentPasses`: an operator's
`BackupSchedulerGrain.PruneAsync` and a scheduled cycle's prune
(`BackupSchedulerGrain.RunScheduledCycleAsync`, which is `[AlwaysInterleave]`)
can run at once on one activation, and one pass's restore re-opens the fence
behind the other's scan. The independent review of this module (#5352) found
that the guard as first written still let a pass read its catalog listing
before checking it, so a pass could wait out another and then plan from a
listing holding rows that pass had already pruned; its scan could restore the
row of a backup whose manifest and artifact were gone (#5412,
`ListedRestorableListingBeforeGuard`).

The fix, routed through one pure core, `BackupRetentionPlanner`:

- a pass removes every doomed catalog row **first**, then scans the sink once
  (`BackupRetentionPlanner.DependedOn`), restores the row of every doomed
  manifest an increment outside the pass builds on and of that manifest's own
  base chain, and deletes the rest tip first (`BackupRetentionPlanner.TipFirst`),
  each sink manifest before its artifacts;
- an increment writes its sink manifest, re-reads its base's catalog row, and
  registers only if `BackupRetentionPlanner.MayPublishIncrement` allows it;
  otherwise it deletes its own sink manifest and artifact and falls back to a
  full capture;
- a pass is skipped while another is in flight on the activation, and reads
  its listing only after it has taken the guard.

Whichever side moves second sees the other: either the pass's scan finds the
increment's sink manifest, or the increment finds its base's row gone.

## The republish is the fix of #5418

`lattice.backup` 10.2.0 documents a backup id as a content address: a full
backup's id is its payload digest and an increment's folds in its base, so an
identical capture or retry re-derives the same id and republishes that backup.
A pass that had listed and doomed the id could then delete the sink manifest a
republish had just rewritten and registered, leaving the catalog listing a
backup the sink no longer held (`ListedInSinkUngatedRepublish`). And a
republish kept the backup's first capture time, which was all a pass ranked by,
so the pass after a capture of an unchanged tree pruned the backup the capture
had just returned (`KeepPolicyHonouredStaleRank`). The fix keeps the ids, and
serialises per id on the scope's `BackupSchedulerGrain`:

- every publish to the catalog and sink - a capture, a set's membership stamp,
  a member registered unstamped, a catalog rebuild - runs through the
  publication gate (`IBackupPublicationGate`) as
  `BackupSchedulerGrain.PublishAsync`, which holds the id for the length of
  the publish and waits while a pass or a delete holds it;
- a pass decides its doom and reserves every doomed id as retiring in one step,
  with no await between its listing and the reservation, before it removes any
  row; it never dooms an id a capture is publishing, published while it was
  listing, or a set member awaits its stamp with, and it releases each id once
  it has deleted or restored it;
- an operator delete and a catalog scrub retire their id the same way
  (`BackupSchedulerGrain.RetireAsync`);
- a publish refreshes `BackupManifest.LastCapturedAtUtc`, and the planner ranks
  by `BackupRetentionPlanner.RankTime`, the later of it and the immutable
  `BackupManifest.CreatedAtUtc`.

A publish of an id that already has a sink manifest rewrites it in place; an
increment checks its base's row first and abandons without touching the
existing manifest when the row is gone. A publish of an id with no sink
manifest goes through the fence above. One call waits at most
`LatticeBackupOptions.PublishHoldWait`, inside the grain response timeout, and
returns busy having written nothing; `SchedulerBackupPublicationGate` retries
it, so a pass deleting a large artifact delays a capture without failing it.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `now` | The wall clock | The wall clock (UtcNow), read by `BackupSchedulerGrain.PruneCoreAsync` for the keep set and by `LatticeBackupCaptureService.CaptureIncrementalAsync` for `BackupManifest.CreatedAtUtc`. |
| `pol` | The scope's retention policy | `LatticeBackupScheduleOptions.RetentionKeepLast` and `LatticeBackupScheduleOptions.RetentionMaxAge`, with `LatticeBackupScheduleOptions.RetentionEnabled` set. |
| `next`, `meta` | Each backup's manifest under its content-addressed id: kind, base and ranking time | `BackupIdentity.Full` and `BackupIdentity.Increment` (the id), `BackupManifest.Kind`, `BackupManifest.BaseBackupId`, and `BackupRetentionPlanner.RankTime` over `BackupManifest.CreatedAtUtc` and `BackupManifest.LastCapturedAtUtc`. |
| `arts` | Artifacts in the sink | `ILatticeBackupSink.WriteArtifactAsync`, `ILatticeBackupSink.DeleteArtifactAsync`; one per capture (`BackupContentDescriptor.ArtifactId`). |
| `snk` | Manifests in the sink | `ILatticeBackupSink.WriteManifestAsync`, `ILatticeBackupSink.DeleteManifestAsync`, `ILatticeBackupSink.ListManifestsAsync`. |
| `cat` | Catalog rows | `ILatticeBackupCatalogStore.RegisterAsync`, `ILatticeBackupCatalogStore.RemoveAsync`, `ILatticeBackupCatalogStore.GetAsync`, `ILatticeBackupCatalogStore.ListAsync`. |
| `cap` | Each in-flight capture's progress and its start time | The activation-local progress of `LatticeBackupCaptureService.CaptureIncrementalAsync` and of a full capture (`LatticeBackupCaptureService.CaptureAsync`), and the publish hold `BackupSchedulerGrain._publishing`, with set members awaiting their stamp in `BackupSchedulerGrain._pendingMembers`; the start time is the capture's `BackupManifest.CreatedAtUtc`, which a publish records as the last capture. |
| `prPh`, `prDoomed`, `prGone`, `prBlocked`, `prManGone` | A retention pass's progress | The locals of `BackupSchedulerGrain.PruneCoreAsync`, the guard `BackupSchedulerGrain._pruneInFlight`, and the ids the pass holds retiring, `BackupSchedulerGrain._retiring`. |
| `crashes` | The crash budget | No production state: a bound on the checked instance. |
| `pub` | Ghost: ids registered and not since republished afresh | No production state: it lets `SinkChainIntact` tell a published increment from a crashed capture's leftover. |
| `lpt` | Ghost: when each backup was really last captured | No production state: the properties judge the policy by it, so a ranking time the manifest does not refresh is a defect they can see. |
| `reps` | The republish budget | No production state: a republish allocates no id, so this bounds the captures the checked instance can start. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Tick` | The clock advances | The wall clock read by `BackupSchedulerGrain.PruneCoreAsync`. **Environment argument:** the clock advances at any time by any amount the bound allows. The model's clock never steps back; production's wall clock can, which is the one premise stronger than production (see the gaps). | Yes: `BackupRetentionPlannerTests.Max_age_keeps_a_backup_created_exactly_at_the_cutoff` and `BackupSchedulerGrainUnitTests.RunScheduledCycleAsync_retention_max_age_keeps_recent_manifests`, which asserts the expired backup is pruned and the recent one kept. |
| `Start(c,k,b)` | A capture starts under a fresh id, or re-derives the id of a backup of identical content; an increment names a base whose sink manifest it can read | The id from `BackupIdentity.Full` (resolved by `BackupPublisher.ResolveFullIdAsync`) and `BackupIdentity.Increment` in `LatticeBackupCaptureService.CaptureIncrementalAsync` and the full capture; `BackupSchedulerGrain.TriggerIncrementalAsync` (base = the newest catalog row, `BackupSchedulerGrain.FindLatestForScopeAsync`), `BackupOperationService.StartIncrementalCaptureAsync` (base supplied by the caller), and the base read in `LatticeBackupCaptureService.CaptureIncrementalAsync`. **Environment argument:** any free slot starts any kind of capture at any time, and an increment may name any base with a sink manifest - a superset of the scheduler's choice and exactly the API's. A republish may re-derive any id of the same kind and base, listed, being pruned or deleted, which covers every identical capture or retry. | Yes: `BackupIdentityTests.A_full_backup_id_is_the_10_2_0_payload_hash`, `BackupIdentityTests.An_increment_id_is_the_10_2_0_hash_of_its_base_and_payload_hash`, `CapturingClusterStampTests.CaptureAsync_identical_content_republishes_the_same_backup` (all red with per-capture ids), `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_throws_when_the_base_backup_is_missing` and `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`. |
| `Acquire(c)` | A republish is admitted to publish its id only once no pass holds it retiring and no other capture holds it | `BackupSchedulerGrain.PublishAsync` taking the id's hold in `BackupSchedulerGrain.TryHoldAsync`, reached through `SchedulerBackupPublicationGate`, which retries a call that returns busy after `LatticeBackupOptions.PublishHoldWait`. Production holds a fresh id too, and only for the publish call; the model holds a republish's id from its artifact write, which is equivalent because a capture's artifact id is its own. | Yes: `BackupRetentionConcurrencyTests.A_full_capture_of_an_unchanged_tree_during_a_pass_waits_for_the_pass_and_republishes_the_backup` (red without the hold), `BackupRepublishHoldTests.A_republish_that_meets_a_pass_retiring_its_id_waits_beyond_one_call_and_then_publishes` (red without the retry), `SchedulerBackupPublicationGateTests.A_call_that_timed_out_is_retried_and_its_result_returned` (red when a call that timed out at the caller is not retried, though it is idempotent) and `BackupRepublishHoldTests.An_identical_increment_whose_base_row_is_gone_falls_back_without_withdrawing_the_published_one` (red when an existing manifest is withdrawn). |
| `WriteArt(c)` | The capture streams its artifact | `ILatticeBackupSink.WriteArtifactAsync` in `LatticeBackupCaptureService.CaptureIncrementalAsync`; a republish in place also writes its fresh artifact. | Yes: `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_captures_only_the_delta_committed_after_the_base` and `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`, which probes every listed chain's artifacts. |
| `WriteMan(c)` | The capture writes its sink manifest, before its check | `ILatticeBackupSink.WriteManifestAsync` in `BackupPublisher.PublishAsync`, under the id's hold. | Yes: `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`, whose pass finds the written manifest. |
| `Check(c)` | The increment re-reads its base's catalog row | `ILatticeBackupCatalogStore.GetAsync` and `BackupRetentionPlanner.MayPublishIncrement` in `BackupPublisher.PublishAsync`; a republish into an existing sink manifest checks first, in `Acquire`. | Yes: `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup` (red without the re-check) and `BackupRetentionPlannerTests.An_increment_publishes_only_while_its_base_row_is_present`. |
| `Register(c)` | The capture publishes its catalog row and refreshes the id's ranking time, or, as a member of a multi-tree set, defers it to the stamp | `ILatticeBackupCatalogStore.RegisterAsync` in `BackupPublisher.PublishAsync`, through `BackupManifestRegistration.Reconcile`, which keeps `BackupManifest.CreatedAtUtc` and records `BackupManifest.LastCapturedAtUtc`; a set member is captured with the registration deferred by `LatticeBackupCaptureService.CaptureMembersAsync`. | Yes: `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on` (red when the increment does not register), `BackupRetentionTests.PruneAsync_keep_last_one_preserves_the_base_of_a_retained_increment`, `BackupRepublishHoldTests.A_republish_refreshes_the_ranking_so_retention_keeps_the_backup_a_capture_just_returned` and `BackupManifestRegistrationTests.Reconcile_records_the_latest_capture_for_retention_to_rank_by` (both red without the refresh). |
| `Stamp(c)` | The set's membership stamp re-writes a member's sink manifest and registers its row, the member's only registration, and refreshes its ranking time | `LatticeBackupCaptureService.StampSetMembershipAsync`, through `IBackupPublicationGate.RegisterAsync` under the id's hold; a member nothing will stamp (a failed set, a discarded fenced attempt) is registered unstamped by `LatticeBackupCaptureService.RegisterUnstampedAsync` through the same gate, which the model's `Stamp` covers too. | Yes: `BackupRetentionConcurrencyTests.A_pass_during_a_set_capture_does_not_leave_a_member_listed_over_a_deleted_artifact` (red with the member registered at capture), `BackupRetentionConcurrencyTests.A_set_that_fails_after_a_member_was_captured_lists_that_member_unstamped` (red without the failed set's unstamped registration) and `LatticeBackupSetCaptureHoldLossTests.A_discarded_attempt_lists_its_members_unstamped_beside_the_stamped_ones` (red without the discarded attempt's unstamped registration). The stamp and the unstamped registration are pinned to the gate by the first two, which record it. A member whose stamp never comes - its capturing silo lost - is held only until its lease runs out: `BackupRepublishHoldTests.A_set_member_whose_stamp_never_comes_is_held_from_retention_only_until_its_lease_runs_out` (red without the lease, #5629). |
| `Abandon(c)` | An increment whose base is being pruned withdraws its sink manifest and falls back to a full capture | `ILatticeBackupSink.DeleteManifestAsync` in `BackupPublisher.PublishAsync`, under the id's hold, then at the end of `LatticeBackupCaptureService.CaptureIncrementalAsync` `ILatticeBackupSink.DeleteArtifactAsync` of the capture's own artifact and `LatticeBackupCaptureService.CaptureTreeAsync`. The model deletes the artifact or not, nondeterministically, which covers a crash between the two deletes, and starts the fallback full capture as a new `Start`. | Yes: `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup`, which asserts the result is a full backup, that no sink manifest names the pruned base, and that the withdrawal ran through the gate. |
| `CaptureCrash(c)` | A capture crashes at any step; what it wrote stays | A faulted or lost capture. **Environment argument:** a crash may strike any capture at any step, up to the bound. | Yes: `BackupSchedulerGrainUnitTests.RunScheduledCycleAsync_rethrows_when_capture_service_throws`. |
| `PruneList(p)` | One listing and plan: the keep set, its base closure and the doomed rest, read under the one-pass guard, less every id a capture holds, and each doomed id reserved as retiring in the same step | `BackupSchedulerGrain._pruneInFlight` checked and set in `BackupSchedulerGrain.PruneCoreAsync`, then `BackupSchedulerGrain.ListScopeAsync`, `BackupRetentionPlanner.ComputeRetained` and `BackupRetentionPlanner.Doomed`, filtered by `BackupSchedulerGrain.IsHeld` and reserved by `BackupSchedulerGrain.ReserveRetiring` in `BackupSchedulerGrain.PruneUnderGuardAsync`. Production sets the guard synchronously and then awaits the listing; the model takes both in one step, which is equivalent because nothing a capture does depends on the guard. Production records every id published while the listing is read (`BackupSchedulerGrain._publishedDuringListing`) and holds them out of the doom, which the model's atomic listing makes unnecessary. | Yes: `BackupRetentionTests.PruneAsync_keep_last_two_prunes_the_oldest_chain`, `BackupRetentionTests.PruneAsync_keep_last_one_preserves_the_base_of_a_retained_increment`, `BackupRetentionPlannerTests.Keep_last_keeps_the_newest_by_creation_time_then_ordinal_id`, `BackupSchedulerGrainUnitTests.A_retention_pass_is_skipped_while_another_is_in_flight` (red without the guard) and `BackupSchedulerGrainUnitTests.A_retention_pass_plans_from_a_listing_taken_under_the_one_pass_guard` (red with the listing read before the guard), `BackupRepublishHoldTests.A_pass_never_dooms_a_backup_a_capture_is_republishing` and `BackupRepublishHoldTests.A_pass_never_dooms_a_set_member_awaiting_its_stamp` (both red when the pass ignores a capture's hold). |
| `RemoveRow(p,m)` | The pass removes a doomed catalog row, before its scan | `ILatticeBackupCatalogStore.RemoveAsync` in `BackupSchedulerGrain.PruneUnderGuardAsync`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts`. |
| `Scan(p)` | One sink scan for what an increment outside the pass builds on, transitively | `ILatticeBackupSink.ListManifestsAsync` and `BackupRetentionPlanner.DependedOn`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_keeps_and_restores_a_doomed_base_an_in_flight_increment_builds_on` (red without the scan), `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on` (red without the scan), `BackupRetentionPlannerTests.The_base_chain_of_a_kept_manifest_is_kept_transitively` and `BackupRetentionPlannerTests.An_increment_the_pass_is_itself_pruning_does_not_keep_its_base`. |
| `RestoreRow(p,m)` | The pass restores a kept manifest's row and drops it from its plan, releasing its hold | `ILatticeBackupCatalogStore.RegisterAsync` in `BackupSchedulerGrain.PruneUnderGuardAsync`, then `BackupSchedulerGrain.ReleaseRetiring`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_keeps_and_restores_a_doomed_base_an_in_flight_increment_builds_on` and `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`. |
| `DelMan(p,m)` | The pass deletes a doomed sink manifest, tip first and after its scan | `ILatticeBackupSink.DeleteManifestAsync` over `BackupRetentionPlanner.TipFirst`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts`, `BackupSchedulerGrainUnitTests.PruneAsync_deletes_a_doomed_chain_tip_first` (red when the grain deletes in listing order) and `BackupRetentionPlannerTests.Deletion_runs_tip_first`. |
| `DelArt(p,m)` | The pass deletes the doomed manifest's unshared artifact, and releases the id's hold | `ILatticeBackupSink.DeleteArtifactAsync`, skipping `BackupRetentionPlanner.RetainedArtifacts`, then `BackupSchedulerGrain.ReleaseRetiring`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts` and `BackupRetentionPlannerTests.An_artifact_a_retained_manifest_references_is_retained`. |
| `PruneCrash(p)` | A pass crashes at any step; its plan and its holds are lost and its deletes stay | A faulted pass or a lost activation; `BackupSchedulerGrain._pruneInFlight` and the pass's retiring holds are cleared in a `finally` and are never persisted. **Environment argument:** a crash may strike a pass at any step, up to the bound. | Yes: `BackupSchedulerGrainUnitTests.A_faulted_retention_pass_does_not_wedge_the_scope`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `RetainedRestorable` | Every backup the retention policy keeps from the catalog, with its whole base chain, is restorable. Production violated it before #5357 was fixed. | Yes: `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on` (red without the scan), `BackupRetentionCoyoteTests.Production_retention_never_leaves_a_listed_backup_unrestorable`, `BackupRetentionCoyoteTests.Without_the_dependent_scan_a_pass_deletes_a_base_an_increment_published_over` and `BackupRetentionTests.PruneAsync_keep_last_one_preserves_the_base_of_a_retained_increment`. |
| `ListedRestorable` | Every backup the catalog lists is restorable, retained or not, so no pass leaves - or restores - a listed row whose manifest, artifact or base chain is gone, and no capture re-lists one a pass pruned. Production violated it before #5412 and #5449 were fixed. | Yes: `BackupSchedulerGrainUnitTests.A_retention_pass_plans_from_a_listing_taken_under_the_one_pass_guard` (red with the listing read before the guard), `BackupRetentionConcurrencyTests.A_pass_during_a_set_capture_does_not_leave_a_member_listed_over_a_deleted_artifact` (red with a set member registered at capture) and `BackupRetentionCoyoteTests.Production_retention_never_leaves_a_listed_backup_unrestorable`, which asserts every catalog row after every step. |
| `ListedInSink` | Every listed backup's manifest is in the sink, so a catalog scrub never finds a listed backup orphaned and a cold restore resolves every listed backup. Production violated it as 10.2.0 shipped it (#5418): a full capture of an unchanged tree republished the id of a backup a pass was deleting. | Yes: `BackupRetentionConcurrencyTests.A_full_capture_of_an_unchanged_tree_during_a_pass_waits_for_the_pass_and_republishes_the_backup` and `BackupRepublishHoldTests.A_pass_never_dooms_a_backup_a_capture_is_republishing` (both red without the per-id hold), `BackupMaintenanceHoldTests.A_rebuild_lists_each_manifest_under_its_hold_and_never_one_removed_while_it_walks`, `BackupMaintenanceHoldTests.A_scrub_keeps_an_orphan_row_a_capture_republished_after_the_scrub_found_it` and `LatticeBackupControlDeleteHoldTests.DeleteBackupAsync_removes_the_backup_under_its_per_id_hold` (each red when its path bypasses the gate). |
| `SinkChainIntact` | A published increment still in the sink keeps a restorable base, so an interrupted pass never leaves one a catalog rebuild would list unrestorable. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_deletes_a_doomed_chain_tip_first` (red when the grain deletes in listing order), `BackupRetentionPlannerTests.Deletion_runs_tip_first` and `BackupRetentionTests.PruneAsync_keep_last_two_prunes_the_oldest_chain`. |
| `InFlightBaseSurvives` | An increment is published only while its base is restorable: a pass never prunes the base an in-flight increment chose before it completes, or the increment does not publish. Production violated it before #5357 was fixed. | Yes: `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup` (red without the re-check) and `BackupRetentionCoyoteTests.Without_the_base_recheck_an_increment_is_published_over_a_pruned_base`. |
| `KeepPolicyHonoured` | A pass deletes a published backup's artifact only when the policy does not keep it, judged by when each backup was last captured: the newest keep-last by that time then id, everything last captured at or after the max-age cutoff, everything when neither is set, and the base closure. Production violated it as 10.2.0 shipped it: a republish kept its first capture time, all a pass ranked by (#5418). | Yes: `BackupRepublishHoldTests.A_republish_refreshes_the_ranking_so_retention_keeps_the_backup_a_capture_just_returned`, `BackupManifestRegistrationTests.Reconcile_records_the_latest_capture_for_retention_to_rank_by` and `BackupRetentionPlannerTests.A_republished_backup_ranks_by_its_latest_capture` (all red without the refresh), `BackupRetentionPlannerTests.Keep_last_keeps_the_newest_by_creation_time_then_ordinal_id`, `BackupRetentionPlannerTests.Max_age_keeps_a_backup_created_exactly_at_the_cutoff`, `BackupRetentionPlannerTests.A_backup_is_retained_when_either_knob_keeps_it`, `BackupRetentionPlannerTests.Max_age_keeps_a_backup_keep_last_does_not_when_both_knobs_are_set` and `BackupSchedulerGrainUnitTests.PruneAsync_with_both_knobs_keeps_a_backup_only_max_age_retains` (both red when max-age is applied only without keep-last), and `BackupRetentionTests.PruneAsync_keep_last_two_prunes_the_oldest_chain`. |
| `ExpiredEventuallyPruned` | A listed backup outside the policy that nothing builds on stops being listed, provided passes keep running and storage completes - including after a pass faulted. A backup a capture holds is exempt while it holds it: a capture is not fair, and a republish refreshes the backup's ranking anyway. | Yes: `BackupRetentionTests.PruneAsync_keep_last_two_prunes_the_oldest_chain` and `BackupSchedulerGrainUnitTests.A_faulted_retention_pass_does_not_wedge_the_scope` (red when the guard is not cleared). |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification (issue #2321)

TypeOK, RetainedRestorable, ListedRestorable, ListedInSink and SinkChainIntact
are state safety;
InFlightBaseSurvives and KeepPolicyHonoured are action safety;
ExpiredEventuallyPruned is temporal liveness. Every property is reached and
falsifiable. The TwoPasses variant re-checks every state and action property
with two passes; ExpiredEventuallyPruned is bounded-out there for the TLC
liveness budget and checked at one pass, where the guard makes a second pass
equivalent to a later first one.

Fairness is weak fairness on each pass's steps only: a pass, once invoked,
keeps taking its storage steps, and passes keep being invoked (the schedule
keeps firing), an environment premise. No fairness is placed on captures,
crashes or the clock, nor on the keep decision, which `PruneList` takes
deterministically from the state it reads. Both liveness mutations that wedge a
pass (`ExpiredEventuallyPrunedCrashWedgesPass`, `ExpiredEventuallyPrunedPassNeverEnds`)
and the one that keeps everything (`ExpiredEventuallyPrunedOneKnobUnbounded`)
leave that fairness intact, so the property fails on a protocol defect.

Every experiment is an anchored perturbation of an existing action or helper;
none adds an action. The republish is part of the checked design (`Start`'s
second branch and `Acquire`), so the #5418 experiments perturb its hold and its
refresh rather than adding a branch.

| Mutation | Classification |
|----------|----------------|
| TypeOkIdRunaway | Anchored action: Start. |
| ListedInSinkUngatedRepublish | Anchored action: Acquire, without the wait on the pass's reservation. Production as 10.2.0 shipped it (#5418). |
| ListedInSinkReserveAfterRowRemoval | Anchored action: Acquire, with the pass's reservation taken only at its row removal rather than at its doom decision. |
| RetainedRestorableArtifactNotWritten | Anchored action: WriteArt. |
| RetainedRestorableManifestNotWritten | Anchored action: WriteMan. |
| InFlightBaseSurvivesNoBaseRecheck | Anchored action: Check. Production before #5357. |
| RetainedRestorableCheckReadsSinkManifest | Anchored action: Check. |
| InFlightBaseSurvivesPublishesAbandoned | Anchored action: Register. |
| KeepPolicyHonouredStaleRank | Anchored action: Register, without the ranking refresh. Production as 10.2.0 shipped it (#5418). |
| ListedRestorableStampReregisters | Anchored actions: Register, and PruneList, whose doom no longer subtracts a member awaiting its stamp. Production before #5449: a set member is registered at capture and again by the stamp. A member awaiting its stamp is now also held from every pass, which alone would hide the double registration, so the experiment removes that hold too, as that production had neither. |
| ListedInSinkStampRegistersBeforeManifest | Anchored action: Stamp. |
| InFlightBaseSurvivesAbandonPublishes | Anchored action: Abandon. |
| InFlightBaseSurvivesCrashPublishesUnchecked | Anchored action: CaptureCrash. |
| KeepPolicyHonouredKeepsOldest | Anchored action: PruneList. |
| ListedRestorableListingBeforeGuard | Anchored action: PruneList. Production before #5412; the stale listing is over-approximated by every row ever registered. |
| KeepPolicyHonouredMaxAgeBoundaryExclusive | Anchored action: PruneList. |
| ExpiredEventuallyPrunedOneKnobUnbounded | Anchored action: PruneList. |
| ListedInSinkPassDoomsPublishing | Anchored action: PruneList, dooming an id a capture holds. |
| RetainedRestorableConcurrentPasses | Anchored action: PruneList, at two passes (`BOUNDS:`). |
| RetainedRestorableRowKeptDuringScan | Anchored action: RemoveRow. |
| RetainedRestorableNoDependentScan | Anchored action: Scan. Production before #5357. |
| RetainedRestorableBlockedNotTransitive | Anchored helper: Blocked, read by Scan. |
| RetainedRestorableRestoreContinuesDeleting | Anchored action: RestoreRow. |
| RetainedRestorableDeleteBeforeScan | Anchored action: DelMan. |
| SinkChainIntactBaseDeletedFirst | Anchored action: DelMan. |
| ExpiredEventuallyPrunedPassNeverEnds | Anchored action: DelArt. |
| ExpiredEventuallyPrunedCrashWedgesPass | Anchored action: PruneCrash. |
| KeepPolicyHonouredClockStepsBack | Anchored action: Tick. |

## Detectors

The detectors run the production code at three levels. `BackupRetentionPlannerTests`
pins every decision of the pure core. `BackupSchedulerGrainUnitTests` drives the
real `BackupSchedulerGrain` against in-memory stores that record every catalog
and sink call, so the fence's order (row, scan, restore, manifest, artifacts),
the one-pass guard and its release after a fault are asserted call by call.
`BackupRetentionConcurrencyTests` runs a real single-silo cluster whose sink
runs a retention pass from inside the increment's own manifest write, just
before it and just after it, so the pass meets the increment on each side of the
fence. A further concurrency test starts a full capture of the unchanged tree from
inside the pass's sink scan, where its republished id meets the pass, and another
runs a capture and a pass from inside a multi-tree set capture, between its first
member's capture and the set's membership stamp. `BackupRepublishHoldTests` runs a
pass from inside a republish's publish, from inside a set whose member republishes
an expired backup, and against a republish whose id it holds for longer than one
publish call may wait; `BackupMaintenanceHoldTests` runs a catalog rebuild and a
scrub against a removal and a republish; and the gate's routing of every publish
and delete path is pinned by a recording gate.
`BackupRetentionCoyoteTests` explores every interleaving of a pass, an
increment and a newer full capture through the production planner, with one
guard per side of the fence.

### Executed production negative controls

Each control edited the real code, ran the named detectors, saw an assertion
failure, and restored the source.

| Production perturbation | Detector that went red |
|-------------------------|------------------------|
| The increment publishes without the re-check (`MayPublishIncrement` bypassed). | `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup`. |
| The pass keeps nothing it finds built on (`DependedOn` bypassed). | `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on` and `BackupSchedulerGrainUnitTests.PruneAsync_keeps_and_restores_a_doomed_base_an_in_flight_increment_builds_on`. |
| No one-pass guard. | `BackupSchedulerGrainUnitTests.A_retention_pass_is_skipped_while_another_is_in_flight`. |
| The listing read before the one-pass guard (production before #5412). | `BackupSchedulerGrainUnitTests.A_retention_pass_plans_from_a_listing_taken_under_the_one_pass_guard`. |
| The grain deletes in listing order, bypassing the planner's tip-first order. | `BackupSchedulerGrainUnitTests.PruneAsync_deletes_a_doomed_chain_tip_first`. |
| A backup id unique per capture (production between #5434 and the backward-compatible fix of #5418). | `BackupIdentityTests.A_full_backup_id_is_the_10_2_0_payload_hash`, `BackupIdentityTests.An_increment_id_is_the_10_2_0_hash_of_its_base_and_payload_hash` and `CapturingClusterStampTests.CaptureAsync_identical_content_republishes_the_same_backup`. |
| No per-id hold: a publish and a pass act on one id at once (production as 10.2.0 shipped it). | `BackupRetentionConcurrencyTests.A_full_capture_of_an_unchanged_tree_during_a_pass_waits_for_the_pass_and_republishes_the_backup`, `BackupRepublishHoldTests.A_pass_never_dooms_a_backup_a_capture_is_republishing` and `BackupRepublishHoldTests.A_pass_never_dooms_a_set_member_awaiting_its_stamp`. |
| A busy publish is not retried across calls. | `BackupRepublishHoldTests.A_republish_that_meets_a_pass_retiring_its_id_waits_beyond_one_call_and_then_publishes`. |
| A republish keeps its first capture time as its ranking (production as 10.2.0 shipped it). | `BackupRepublishHoldTests.A_republish_refreshes_the_ranking_so_retention_keeps_the_backup_a_capture_just_returned`, `BackupManifestRegistrationTests.Reconcile_records_the_latest_capture_for_retention_to_rank_by` and `BackupRetentionPlannerTests.A_republished_backup_ranks_by_its_latest_capture`. |
| An identical increment withdraws the manifest of the one already published. | `BackupRepublishHoldTests.An_identical_increment_whose_base_row_is_gone_falls_back_without_withdrawing_the_published_one`. |
| The stamp, an unstamped member, the abandoned increment's withdrawal, a rebuild, a scrub and an operator delete each bypass the gate. | `BackupRetentionConcurrencyTests.A_pass_during_a_set_capture_does_not_leave_a_member_listed_over_a_deleted_artifact`, `BackupRetentionConcurrencyTests.A_set_that_fails_after_a_member_was_captured_lists_that_member_unstamped`, `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup`, `BackupMaintenanceHoldTests.A_rebuild_lists_each_manifest_under_its_hold_and_never_one_removed_while_it_walks`, `BackupMaintenanceHoldTests.A_scrub_keeps_an_orphan_row_a_capture_republished_after_the_scrub_found_it` and `LatticeBackupControlDeleteHoldTests.DeleteBackupAsync_removes_the_backup_under_its_per_id_hold`. |
| A set member registered at capture and again by the stamp (production before #5449). | `BackupRetentionConcurrencyTests.A_pass_during_a_set_capture_does_not_leave_a_member_listed_over_a_deleted_artifact`. |
| A failed set leaves its captured members unregistered. | `BackupRetentionConcurrencyTests.A_set_that_fails_after_a_member_was_captured_lists_that_member_unstamped`. |
| A discarded fenced attempt leaves its members unregistered. | `LatticeBackupSetCaptureHoldLossTests.A_discarded_attempt_lists_its_members_unstamped_beside_the_stamped_ones`. |
| The increment never registers its row. | `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`. |
| Max-age applied only when keep-last is unset. | `BackupRetentionPlannerTests.Max_age_keeps_a_backup_keep_last_does_not_when_both_knobs_are_set` and `BackupSchedulerGrainUnitTests.PruneAsync_with_both_knobs_keeps_a_backup_only_max_age_retains`. |
| Max-age ignored. | `BackupSchedulerGrainUnitTests.RunScheduledCycleAsync_retention_max_age_keeps_recent_manifests`. |

## Defect mutations whose fixes have landed

InFlightBaseSurvivesNoBaseRecheck and RetainedRestorableNoDependentScan
reproduce production before the fix of #5357 (the capture side and the pass side
respectively), and RetainedRestorableConcurrentPasses the concurrent-pass hazard
the same fix closes. ListedRestorableListingBeforeGuard reproduces the fix as
first written, whose listing preceded its guard (#5412).
ListedInSinkUngatedRepublish reproduces 10.2.0's republish with nothing to keep a
pass off its id, and KeepPolicyHonouredStaleRank its republish keeping the first
capture time as its ranking (#5418); the fix keeps 10.2.0's content-addressed
ids. ListedRestorableStampReregisters reproduces a set member registered at
capture and again by the set's membership stamp (#5449). All four fixes land
with this module; every reproduction stands as a regression check, and each has
an executed production analogue above.

## Deliberate abstraction gaps

- **One artifact per capture.** Each manifest references only its own artifact,
  as production writes it: an artifact id is unique per capture (a fresh GUID in
  its name). The pass's retained-artifact set, which keeps an artifact a retained
  manifest also references, is therefore defensive and is pinned by its unit test
  rather than by the model. The model keeps one artifact per id: a republish
  rewrites the id's manifest over a fresh artifact, and production leaves the
  artifact the old manifest named in the sink unreferenced, as 10.2.0 did. It is
  not reclaimed, because a restore may still be streaming it: deleting it under
  the hold once the republish registers would fail that restore, which no hold
  covers. A scheduled full capture of an unchanged tree therefore leaves one
  unreferenced artifact per cycle, as 10.2.0's did.
  Production symbols: `BackupRetentionPlanner.RetainedArtifacts`, `BackupPublisher.PublishAsync`.
- **The clock never steps back.** `KeepPolicyHonoured` judges a deletion by the
  clock at the time of the delete, so it depends on the clock being monotonic
  (`KeepPolicyHonouredClockStepsBack` shows it). Production reads the wall clock,
  which can step back; a pass then deletes what it judged expired at its own
  reading, even if a later reading would call it young.
  No production symbol: the gap is a premise about the environment's clock.
- **One cluster's catalog and sink.** The fence relies on the catalog and the
  sink observing every completed write: a point read after a completed remove,
  and a sink scan after a completed manifest write. A lattice tree in one
  cluster and Azure Blob Storage both give that. A sink shared by several
  clusters through replication does not, and is outside the model.
  Production symbols: `InClusterLatticeBackupSink`, `BackupSinkSharingEnforcement`.
- **A single scheduler activation.** The one-pass guard and the per-id holds are
  activation-local, so they rely on Orleans running one activation of the scope's
  scheduler grain. A duplicate activation after a membership failure could run a
  second pass, or admit a publish its twin's pass holds, and is outside the model
  (`RetainedRestorableConcurrentPasses` and `ListedInSinkUngatedRepublish` show
  what that costs).
  Production symbols: `BackupSchedulerGrain`.
- **One scope's ids.** The model has one scope. A full backup's content address
  names no scope, so identical content in two scopes would share an id; the
  second scope takes a scope-qualified id instead, decided under a per-id claim
  (#5620), and each scope's ids are then its own, which is what lets one scope's
  holds serialise every publish of an id. The claim is outside the model.
  Production symbols: `BackupPublisher.ResolveFullIdAsync`, `BackupIdClaimGrain`, `BackupIdentity.ScopeQualifiedFull`.
- **A bounded wait per publish call.** `Acquire` waits as long as the pass holds
  the id. Production waits at most `LatticeBackupOptions.PublishHoldWait` per
  call and returns busy, and the publication gate retries across calls for up to
  `LatticeBackupOptions.PublishHoldTimeout`; that is a liveness bound the safety
  properties do not ask about, pinned by its own detector.
  Production symbols: `SchedulerBackupPublicationGate`, `BackupSchedulerGrain.TryHoldAsync`, `LatticeBackupOptions.PublishHoldWait`, `LatticeBackupOptions.PublishHoldTimeout`.
- **Schedule cadence and reminder ticks.** The model has no clock-driven
  schedule: a capture may start at any time in any slot, and a pass may follow
  any capture. Production's reminder tick now dispatches its cycle (capture,
  then retention) onto an interleaved grain timer and returns, and a tick that
  fires while a reminder-driven cycle is still running starts nothing, except a
  full tick during an incremental cycle, which runs one full cycle when that
  cycle ends. That
  only removes interleavings the model already explores - two scheduled cycles
  overlapping each other - while a capture through the backup API or another
  silo still overlaps a pass, as the capture slots model; the one-pass guard is
  unchanged. Cadence, tick coalescing and the load a schedule puts on the silo
  are liveness and resource questions the safety properties do not ask, and are
  pinned by the scheduler's overlap tests instead.
  Production symbols: `BackupSchedulerGrain.ReceiveReminder`, `BackupSchedulerGrain.RunScheduledCycleAsync`.
- **An atomic catalog listing.** `PruneList` reads the whole catalog in one step.
  Production streams it; a row registered during the stream may be missed, which
  is the same as a listing taken before that registration, and a missed row is
  never deleted.
  Production symbols: `BackupSchedulerGrain.ListScopeAsync`.
- **Operator delete and catalog repair.** An operator's explicit delete removes a
  named backup regardless of what builds on it, so it is outside the retention
  policy by definition, and it breaks every increment built on what it deletes,
  in flight or published. A catalog rebuild re-registers every sink manifest and
  a scrub removes rows the sink cannot resolve. The model runs none of these as
  actions; each takes the same per-id hold as a capture or a pass, and
  `BackupIdClaim` checks a retire of an id against publishes in two scopes. A
  scrub retires its id, so a pass never dooms it meanwhile and a publish of it
  waits, and re-probes under the hold, keeping a row a capture made resolvable
  again. An operator delete removes the row and reserves the id by a lease of
  its own in one short grain call, deletes the artifacts outside the grain, so a
  large delete never outlasts the grain response timeout (#5628), and ends in the
  grain, under the hold, deleting the sink manifest there only while no row lists
  the id and the manifest is still its scope's (#5631); a retry of an interrupted
  delete finds the sink manifest and finishes (#5334). While any delete's lease
  holds, no pass dooms the id and no publish of it proceeds; `BackupIdClaim`
  models two overlapping deletes. A rebuild registers through the publish path, re-reading the
  manifest and listing it only while it is still in the sink. A delete therefore
  no longer races a pass on the same id, a rebuild no longer re-lists a row a pass
  or a delete has emptied, and a member a rebuild lists before its stamp is still
  held from every pass. A pass interrupted after removing rows still leaves
  backups that only a rebuild lists again.
  Production symbols: `LatticeBackupControl.DeleteBackupAsync`, `LatticeBackupCatalogRebuildService`, `LatticeBackupCatalogScrubService`, `BackupSchedulerGrain.RetireAsync`, `BackupSchedulerGrain.BeginDeleteAsync`, `BackupSchedulerGrain.EndDeleteAsync`, `BackupPublisher.DeleteArtifactsAsync`, `BackupPublisher.FinishDeleteAsync`, `LatticeBackupOptions.DeleteLease`.
- **A set member's hold outlives a lost capture by a lease.** The model's
  `CaptureCrash` resets a set member awaiting its stamp, which takes it out of
  `Busy` at once. Production cannot see a capturing silo die: the member's hold
  is released by its stamp, or by its unstamped registration when the set fails,
  and otherwise lapses when its lease runs out (#5629). Until then the backup it
  republished is exempt from retention, which `ExpiredEventuallyPruned`'s
  `Busy` exemption admits; the lease bounds that exemption where the model makes
  it immediate.
  Production symbols: `LatticeBackupOptions.PendingStampLease`, `BackupSchedulerGrain.PublishAsync`.
- **Mixed-version silos.** The model assumes every silo runs this design. During
  a rolling upgrade from 10.2.0, a capture, delete, scrub or rebuild on an
  upgraded silo fails closed when the scope's scheduler grain is active on an old
  silo, which does not know the publish and delete calls; captures and passes on
  old silos run without the per-id hold and rank by first capture time, so the
  #5418 race and the pruning of a just-republished backup stay reachable until
  the roll completes; and an old silo that rewrites a row drops
  `BackupManifest.LastCapturedAtUtc`. The upgrade note recommends pausing
  retention and operator deletes for the roll.
  Production symbols: `ILatticeBackupSchedulerGrain`, `BackupManifest.LastCapturedAtUtc`.
- **A crashed increment's leftover sink manifest.** An increment that crashes
  after writing its sink manifest, or between withdrawing its manifest and its
  artifact, leaves a sink manifest no catalog row lists. Every later pass that
  dooms its base finds it in the scan and restores the base's row, so the base and
  its chain stay listed, outside the policy, for as long as the leftover stays in
  the sink. This is the safe choice - the leftover is a restorable increment a
  catalog rebuild would list - and `ExpiredEventuallyPruned` states it rather than
  hides it: its `Depended` premise counts sink manifests. Nothing removes the
  leftover on its own; a catalog rebuild lists it, after which the policy applies
  to it like any other backup, or an operator deletes it. A full capture that is
  a multi-tree set member leaves the same kind of leftover if its silo is lost
  before the set's membership stamp: its sink manifest and artifact stay unlisted
  until a catalog rebuild lists them, for as long as the whole set took rather
  than one capture's write-to-register gap. The model covers it (`CaptureCrash` at
  the stamp phase keeps every property).
  Production symbols: `BackupRetentionPlanner.DependedOn`, `LatticeBackupCatalogRebuildService`, `LatticeBackupCaptureService.StampSetMembershipAsync`.
- **Inventory and metrics.** The pass records each pruned manifest in the
  inventory registry and its reclaimed bytes as a metric; neither affects what is
  restorable.
  Production symbols: `BackupInventoryRegistry.RecordPruned`, `LatticeBackupMetrics.RecordRetention`.
- **Fallback full captures and other bases.** An increment that falls back to a
  full capture for another reason (another cluster's chain, a trimmed WAL, a range
  delete, a legacy base) is a full capture to this model, which starts it as a new
  capture. What an increment holds is `BackupIncremental`'s question.
  No production symbol: the behaviour belongs to `BackupIncremental`, mapped in that module's own refinement note.

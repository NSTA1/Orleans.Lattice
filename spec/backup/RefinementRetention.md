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
clock advances. Each backup id is one capture's manifest with its own artifact. A
backup is restorable while its manifest resolves (catalog row first, then the sink
manifest, as the restore engine resolves it), its artifact is in the sink, and its
whole base chain is restorable. The model checks the fix of the defect it found
(#5357): the two-sided fence between a pass and an increment, the transitive keep
of what an increment outside the pass builds on, the tip-first delete order, and
the activation-local guard that runs one pass at a time.

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
behind the other's scan.

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
- a pass is skipped while another is in flight on the activation.

Whichever side moves second sees the other: either the pass's scan finds the
increment's sink manifest, or the increment finds its base's row gone.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `now` | The wall clock | The wall clock (UtcNow), read by `BackupSchedulerGrain.PruneCoreAsync` for the keep set and by `LatticeBackupCaptureService.CaptureIncrementalAsync` for `BackupManifest.CreatedAtUtc`. |
| `pol` | The scope's retention policy | `LatticeBackupScheduleOptions.RetentionKeepLast` and `LatticeBackupScheduleOptions.RetentionMaxAge`, with `LatticeBackupScheduleOptions.RetentionEnabled` set. |
| `next`, `meta` | Each capture's manifest: kind, base and creation time | `BackupManifest.Kind`, `BackupManifest.BaseBackupId`, `BackupManifest.CreatedAtUtc`. |
| `arts` | Artifacts in the sink | `ILatticeBackupSink.WriteArtifactAsync`, `ILatticeBackupSink.DeleteArtifactAsync`; one per capture (`BackupContentDescriptor.ArtifactId`). |
| `snk` | Manifests in the sink | `ILatticeBackupSink.WriteManifestAsync`, `ILatticeBackupSink.DeleteManifestAsync`, `ILatticeBackupSink.ListManifestsAsync`. |
| `cat` | Catalog rows | `ILatticeBackupCatalogStore.RegisterAsync`, `ILatticeBackupCatalogStore.RemoveAsync`, `ILatticeBackupCatalogStore.GetAsync`, `ILatticeBackupCatalogStore.ListAsync`. |
| `cap` | Each in-flight capture's progress | The activation-local progress of `LatticeBackupCaptureService.CaptureIncrementalAsync` and of a full capture (`LatticeBackupCaptureService.CaptureAsync`). |
| `prPh`, `prDoomed`, `prGone`, `prBlocked`, `prManGone` | A retention pass's progress | The locals of `BackupSchedulerGrain.PruneCoreAsync`, and the guard `BackupSchedulerGrain._pruneInFlight`. |
| `crashes` | The crash budget | No production state: a bound on the checked instance. |
| `pub` | Ghost: ids ever registered | No production state: it lets `SinkChainIntact` tell a published increment from a crashed capture's leftover. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Tick` | The clock advances | The wall clock read by `BackupSchedulerGrain.PruneCoreAsync`. **Environment argument:** the clock advances at any time by any amount the bound allows. The model's clock never steps back; production's wall clock can, which is the one premise stronger than production (see the gaps). | Yes: `BackupRetentionPlannerTests.Max_age_keeps_a_backup_created_exactly_at_the_cutoff` and `BackupSchedulerGrainUnitTests.RunScheduledCycleAsync_retention_max_age_keeps_recent_manifests`. |
| `Start(c,k,b)` | A capture starts; an increment names a base whose sink manifest it can read | `BackupSchedulerGrain.TriggerIncrementalAsync` (base = the newest catalog row, `BackupSchedulerGrain.FindLatestForScopeAsync`), `BackupOperationService.StartIncrementalCaptureAsync` (base supplied by the caller), and the base read in `LatticeBackupCaptureService.CaptureIncrementalAsync`. **Environment argument:** any free slot starts any kind of capture at any time, and an increment may name any base with a sink manifest - a superset of the scheduler's choice and exactly the API's. | Yes: `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_throws_when_the_base_backup_is_missing` and `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`. |
| `WriteArt(c)` | The capture streams its artifact | `ILatticeBackupSink.WriteArtifactAsync` in `LatticeBackupCaptureService.CaptureIncrementalAsync`. | Yes: `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_captures_only_the_delta_committed_after_the_base` and `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`, which probes every listed chain's artifacts. |
| `WriteMan(c)` | The capture writes its sink manifest, before its check | `ILatticeBackupSink.WriteManifestAsync` in `LatticeBackupCaptureService.CaptureIncrementalAsync`. | Yes: `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`, whose pass finds the written manifest. |
| `Check(c)` | The increment re-reads its base's catalog row | `ILatticeBackupCatalogStore.GetAsync` and `BackupRetentionPlanner.MayPublishIncrement` in `LatticeBackupCaptureService.CaptureIncrementalAsync`. | Yes: `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup` (red without the re-check) and `BackupRetentionPlannerTests.An_increment_publishes_only_while_its_base_row_is_present`. |
| `Register(c)` | The capture publishes its catalog row | `ILatticeBackupCatalogStore.RegisterAsync` in `LatticeBackupCaptureService.CaptureIncrementalAsync`. | Yes: `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on` and `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_captures_only_the_delta_committed_after_the_base`. |
| `Abandon(c)` | An increment whose base is being pruned withdraws its sink manifest and falls back to a full capture | The fallback at the end of `LatticeBackupCaptureService.CaptureIncrementalAsync`: `ILatticeBackupSink.DeleteManifestAsync`, `ILatticeBackupSink.DeleteArtifactAsync`, then `LatticeBackupCaptureService.CaptureTreeAsync`. The model starts the fallback full capture as a new `Start`. | Yes: `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup`, which asserts the result is a full backup and no sink manifest names the pruned base. |
| `CaptureCrash(c)` | A capture crashes at any step; what it wrote stays | A faulted or lost capture. **Environment argument:** a crash may strike any capture at any step, up to the bound. | Yes: `BackupSchedulerGrainUnitTests.RunScheduledCycleAsync_rethrows_when_capture_service_throws`. |
| `PruneList(p)` | One listing and plan: the keep set, its base closure and the doomed rest, under the one-pass guard | `BackupSchedulerGrain.ListScopeAsync`, `BackupRetentionPlanner.ComputeRetained`, `BackupRetentionPlanner.Doomed`, guarded by `BackupSchedulerGrain._pruneInFlight` in `BackupSchedulerGrain.PruneCoreAsync`. | Yes: `BackupRetentionTests.PruneAsync_keep_last_two_prunes_the_oldest_chain`, `BackupRetentionTests.PruneAsync_keep_last_one_preserves_the_base_of_a_retained_increment`, `BackupRetentionPlannerTests.Keep_last_keeps_the_newest_by_creation_time_then_ordinal_id` and `BackupSchedulerGrainUnitTests.A_retention_pass_is_skipped_while_another_is_in_flight` (red without the guard). |
| `RemoveRow(p,m)` | The pass removes a doomed catalog row, before its scan | `ILatticeBackupCatalogStore.RemoveAsync` in `BackupSchedulerGrain.PruneUnderGuardAsync`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts`. |
| `Scan(p)` | One sink scan for what an increment outside the pass builds on, transitively | `ILatticeBackupSink.ListManifestsAsync` and `BackupRetentionPlanner.DependedOn`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_keeps_and_restores_a_doomed_base_an_in_flight_increment_builds_on` (red without the scan), `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on` (red without the scan), `BackupRetentionPlannerTests.The_base_chain_of_a_kept_manifest_is_kept_transitively` and `BackupRetentionPlannerTests.An_increment_the_pass_is_itself_pruning_does_not_keep_its_base`. |
| `RestoreRow(p,m)` | The pass restores a kept manifest's row and drops it from its plan | `ILatticeBackupCatalogStore.RegisterAsync` in `BackupSchedulerGrain.PruneUnderGuardAsync`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_keeps_and_restores_a_doomed_base_an_in_flight_increment_builds_on` and `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on`. |
| `DelMan(p,m)` | The pass deletes a doomed sink manifest, tip first and after its scan | `ILatticeBackupSink.DeleteManifestAsync` over `BackupRetentionPlanner.TipFirst`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts` and `BackupRetentionPlannerTests.Deletion_runs_tip_first`. |
| `DelArt(p,m)` | The pass deletes the doomed manifest's unshared artifact | `ILatticeBackupSink.DeleteArtifactAsync`, skipping `BackupRetentionPlanner.RetainedArtifacts`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts` and `BackupRetentionPlannerTests.An_artifact_a_retained_manifest_references_is_retained`. |
| `PruneCrash(p)` | A pass crashes at any step; its plan is lost and its deletes stay | A faulted pass or a lost activation; `BackupSchedulerGrain._pruneInFlight` is cleared in a `finally` and is never persisted. **Environment argument:** a crash may strike a pass at any step, up to the bound. | Yes: `BackupSchedulerGrainUnitTests.A_faulted_retention_pass_does_not_wedge_the_scope`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `RetainedRestorable` | Every backup the retention policy keeps from the catalog, with its whole base chain, is restorable. Production violated it before #5357 was fixed. | Yes: `BackupRetentionConcurrencyTests.A_pass_after_the_increment_wrote_its_manifest_keeps_the_base_the_increment_builds_on` (red without the scan), `BackupRetentionCoyoteTests.Production_retention_never_leaves_a_listed_backup_unrestorable`, `BackupRetentionCoyoteTests.Without_the_dependent_scan_a_pass_deletes_a_base_an_increment_published_over` and `BackupRetentionTests.PruneAsync_keep_last_one_preserves_the_base_of_a_retained_increment`. |
| `SinkChainIntact` | A published increment still in the sink keeps a restorable base, so an interrupted pass never leaves one a catalog rebuild would list unrestorable. | Yes: `BackupRetentionPlannerTests.Deletion_runs_tip_first` and `BackupRetentionTests.PruneAsync_keep_last_two_prunes_the_oldest_chain`. |
| `InFlightBaseSurvives` | An increment is published only while its base is restorable: a pass never prunes the base an in-flight increment chose before it completes, or the increment does not publish. Production violated it before #5357 was fixed. | Yes: `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup` (red without the re-check) and `BackupRetentionCoyoteTests.Without_the_base_recheck_an_increment_is_published_over_a_pruned_base`. |
| `KeepPolicyHonoured` | A pass deletes only what the policy does not keep: the newest keep-last by creation time then id, everything created at or after the max-age cutoff, everything when neither is set, and the base closure. | Yes: `BackupRetentionPlannerTests.Keep_last_keeps_the_newest_by_creation_time_then_ordinal_id`, `BackupRetentionPlannerTests.Max_age_keeps_a_backup_created_exactly_at_the_cutoff`, `BackupRetentionPlannerTests.A_backup_is_retained_when_either_knob_keeps_it` and `BackupRetentionTests.PruneAsync_keep_last_two_prunes_the_oldest_chain`. |
| `ExpiredEventuallyPruned` | A listed backup outside the policy that nothing builds on stops being listed, provided passes keep running and storage completes - including after a pass faulted. | Yes: `BackupRetentionTests.PruneAsync_keep_last_two_prunes_the_oldest_chain` and `BackupSchedulerGrainUnitTests.A_faulted_retention_pass_does_not_wedge_the_scope` (red when the guard is not cleared). |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification (issue #2321)

TypeOK, RetainedRestorable and SinkChainIntact are state safety;
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
none adds an action.

| Mutation | Classification |
|----------|----------------|
| TypeOkIdRunaway | Anchored action: Start. |
| RetainedRestorableArtifactNotWritten | Anchored action: WriteArt. |
| RetainedRestorableManifestNotWritten | Anchored action: WriteMan. |
| InFlightBaseSurvivesNoBaseRecheck | Anchored action: Check. Production before #5357. |
| RetainedRestorableCheckReadsSinkManifest | Anchored action: Check. |
| InFlightBaseSurvivesPublishesAbandoned | Anchored action: Register. |
| InFlightBaseSurvivesAbandonPublishes | Anchored action: Abandon. |
| InFlightBaseSurvivesCrashPublishesUnchecked | Anchored action: CaptureCrash. |
| KeepPolicyHonouredKeepsOldest | Anchored action: PruneList. |
| KeepPolicyHonouredMaxAgeBoundaryExclusive | Anchored action: PruneList. |
| ExpiredEventuallyPrunedOneKnobUnbounded | Anchored action: PruneList. |
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
fence. `BackupRetentionCoyoteTests` explores every interleaving of a pass, an
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

## Defect mutations whose fixes have landed

InFlightBaseSurvivesNoBaseRecheck and RetainedRestorableNoDependentScan
reproduce production before the fix of #5357 (the capture side and the pass side
respectively), and RetainedRestorableConcurrentPasses the concurrent-pass hazard
the same fix closes. The fix lands with this module; both reproductions stand as
regression checks, and each has an executed production analogue above.

## Deliberate abstraction gaps

- **One artifact per capture.** Each manifest references only its own artifact,
  as production writes it: an artifact id is unique per capture (a fresh GUID in
  its name). The pass's retained-artifact set, which keeps an artifact a retained
  manifest also references, is therefore defensive and is pinned by its unit test
  rather than by the model.
  Production symbols: `BackupRetentionPlanner.RetainedArtifacts`.
- **Content-addressed ids.** Each capture in the model gets a fresh id.
  Production derives a backup id from its content, so two full captures of an
  unchanged tree, or an identical retry of an increment, share an id and the
  second overwrites the first's sink manifest and catalog row. A pass that listed
  the earlier version can then delete the newer one. The model does not reach
  this; a capture that republishes an id is outside it.
  Production symbols: `BackupManifest.Id`, `IncrementalDeltaCollector.BackupId`.
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
- **A single scheduler activation.** The one-pass guard is activation-local, so
  it relies on Orleans running one activation of the scope's scheduler grain. A
  duplicate activation after a membership failure could run a second pass and is
  outside the model (`RetainedRestorableConcurrentPasses` shows what that costs).
  Production symbols: `BackupSchedulerGrain`.
- **An atomic catalog listing.** `PruneList` reads the whole catalog in one step.
  Production streams it; a row registered during the stream may be missed, which
  is the same as a listing taken before that registration, and a missed row is
  never deleted.
  Production symbols: `BackupSchedulerGrain.ListScopeAsync`.
- **Operator delete and catalog repair.** An operator's explicit delete removes a
  named backup regardless of what builds on it, so it is outside the retention
  policy by definition; it removes the catalog row first, the order the fixed pass
  uses, so it does not reopen the fence. A catalog rebuild re-registers every sink
  manifest and a scrub removes rows the sink cannot resolve; a pass interrupted
  after removing rows leaves backups that only a rebuild lists again, and the
  model does not run either repair.
  Production symbols: `LatticeBackupControl.DeleteBackupAsync`, `LatticeBackupCatalogRebuildService`, `LatticeBackupCatalogScrubService`.
- **Inventory and metrics.** The pass records each pruned manifest in the
  inventory registry and its reclaimed bytes as a metric; neither affects what is
  restorable.
  Production symbols: `BackupInventoryRegistry.RecordPruned`, `LatticeBackupMetrics.RecordRetention`.
- **Fallback full captures and other bases.** An increment that falls back to a
  full capture for another reason (another cluster's chain, a trimmed WAL, a range
  delete, a legacy base) is a full capture to this model, which starts it as a new
  capture. What an increment holds is `BackupIncremental`'s question.
  No production symbol: the behaviour belongs to `BackupIncremental`, mapped in that module's own refinement note.

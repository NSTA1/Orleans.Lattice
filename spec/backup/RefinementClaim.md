# Refinement note: BackupIdClaim to code

This note maps [`BackupIdClaim.tla`](BackupIdClaim.tla) - which scope owns a full
backup's bare content-addressed id - to the production symbols that play each
role, and to the detector tests that would notice production deviating from it.
It is a documented mapping, not a machine-checked refinement proof.

## Model scope in one paragraph

A full backup's id is the hash of its payload, which names no scope, so identical
content captured in two scopes derives one bare id. Two scopes, A and B, capture
identical content: two captures in A, one in B. Scope A already owns the bare id.
A's retention pass dooms it once, holding A's per-id hold while it removes the
catalog row and then the sink manifest. Two operator deletes of A's backup
overlap, as a retry after a timeout or a double submission does: each removes the
row - or, the row gone, finds the sink manifest an earlier delete left - under
A's hold and takes a lease of its own; deletes the artifacts of the manifest
write it began from outside the grain, only while that lease is live; and ends
under A's hold, deleting the sink manifest only while no row lists the id and
the manifest is still A's, then dropping its own lease. A lease can run out at
any point - weakly fairly, so a lost delete's lease does run out - and a catalog
rebuild of scope A then re-lists A's sink manifest under A's hold, only while
every artifact it references is in the sink. A capture asks the id's claim grain whether its scope may publish under
the bare id; a refused capture takes its scope-qualified id, which is its scope's
alone and outside the model. An admitted capture waits for its own scope's hold,
writes the sink manifest, registers the catalog row and releases its claim. The
two scopes' holds are separate grains, so only the claim keeps them apart. The
model checks the fix of #5627: a claim is recorded for every admitted capture,
the owning scope's included, and released one at a time, so an id a scope is
still publishing through its own retire never becomes another scope's, and no
publish replaces another scope's backup under the bare id. It checks the fix of
#5631 too: no delete's end releases another delete's lease, no delete removes a
manifest a publish has listed or is about to list, and no delete in one scope
removes another scope's backup. And the fix of #5637: no rebuild lists a backup
a delete has half removed. The claim grain's claims are persisted (#5632), so
`pend` is durable state. The cross-scope rule itself is #5620's.

## The model is the fix of #5627

Before the fix, `BackupIdClaimGrain.TryClaimAsync` admitted a capture whose scope
already owned the bare id without recording a claim, and
`BackupIdClaimGrain.ReleaseAsync` cleared a scope's one claim whichever publish
released it. A capture in the owning scope resolved the bare id, then waited on
its scope's hold while that scope's retention pass or operator delete removed the
id; the id then had neither owner nor claim, a capture of identical content in
another scope took it and published, and the waiting capture published over it.
That other scope's caller held an id that listed and restored as the first
scope's. `NoForeignOverwriteOwnerUnclaimed` reproduces it, and
`NoForeignOverwriteReleaseClearsAll` the same window opened by one publish
releasing another's claim. The fix records one claim per admitted capture and
releases one at a time.

## The model is the fix of #5631

Before the fix, overlapping operator deletes of one id shared one lease in
`BackupSchedulerGrain._deleting`, which `BackupSchedulerGrain.EndDeleteAsync`
removed whichever delete ended, and each deleted the sink manifest outside the
grain, unconditionally, in `BackupPublisher.CompleteDeleteAsync`. Once the first
delete ended, a capture of identical content republished the id; the second
delete then deleted the republished manifest, and the catalog listed a backup the
sink no longer held. A single delete whose lease ran out reached the same state.
`ListedInSinkJoinedDeleteSharedLease` reproduces it, and
`DeleteFenceEndClearsEveryLease` the shared lease alone. The fix gives each delete
a lease of its own, keyed by a token, and moves the sink-manifest delete into
`BackupSchedulerGrain.EndDeleteAsync`, under the hold, through
`BackupPublisher.FinishDeleteAsync`, which keeps a manifest a row lists or another
scope wrote. `BackupPublisher.BeginDeleteAsync` leaves an id another scope has
since taken alone (`NoForeignDeleteBeginIgnoresScope`).

## The model is the fix of #5637

The fix of #5628 reordered an operator delete to row, artifacts, then sink
manifest, so a retry resumes an interrupted delete (#5334). Before the fix of
#5637, a catalog rebuild registered any sink manifest it found, and a delete's
lease was not renewed while it deleted artifacts. A delete whose caller was lost
part-way left a manifest whose artifacts were partly gone, and once the lease
ran out a rebuild listed a backup that could not be restored; a delete whose
artifact deletes outlasted its lease let a rebuild re-list the still-whole
backup, and then deleted the listed backup's artifacts.
`ListedRestorableDeleteBeforeFix` reproduces both, `ListedRestorableRebuildUnprobed`
and `ListedRestorableArtifactsAfterLapse` each half. The fix renews the lease in
the background while the artifacts are deleted, deletes no further artifact once a
renewal is refused (`SchedulerBackupPublicationGate.RetireAsync`), and registers
a rebuilt manifest only when `ILatticeBackupSink.ProbeAsync` resolves it
(`BackupPublisher.PublishAsync`).

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `row` | The scope of the bare id's catalog row | `ILatticeBackupCatalogStore.GetAsync` read by `BackupIdOwnership.OwnerScopeKeyAsync`, and `BackupManifest.Scope`. |
| `man` | The scope of the bare id's sink manifest | `ILatticeBackupSink.ReadManifestAsync` read by `BackupIdOwnership.OwnerScopeKeyAsync` when the id has no row. |
| `pend` | The claim grain's live claims on the bare id | `BackupIdClaimState.Claims`, one `BackupIdPendingClaim` per admitted capture, persisted by `BackupIdClaimGrain`. |
| `held` | Each scope's hold on the bare id | `BackupSchedulerGrain._publishing`, `BackupSchedulerGrain._retiring` and `BackupSchedulerGrain._deleting` on that scope's scheduler grain. |
| `cap` | Each capture's progress and resolved id | The activation-local progress of `BackupSchedulerGrain.PublishAsync`, with the id `BackupPublisher.ResolveFullIdAsync` returned: the bare id, or `BackupIdentity.ScopeQualifiedFull`. |
| `ret` | Scope A's retention pass over the bare id | The progress of a retention pass (`BackupSchedulerGrain.PruneCoreAsync`). |
| `del` | Each operator delete's progress | The progress of `SchedulerBackupPublicationGate.RetireAsync` for `BackupRetireKind.Delete`: between `BackupSchedulerGrain.BeginDeleteAsync` and `BackupSchedulerGrain.EndDeleteAsync`. |
| `lease` | Each operator delete's lease on the id | The entry for the delete's token in `BackupSchedulerGrain._deleting`, live until `LatticeBackupOptions.DeleteLease` runs out without a renewal (`BackupSchedulerGrain.RenewDeleteAsync`). |
| `art` | Whether every artifact the current sink manifest references is in the sink | `BackupSinkResolution.IsResolvable`, read by `ILatticeBackupSink.ProbeAsync`. |
| `gen`, `dgen` | Which manifest write the current sink manifest is, and which one each delete began from | The artifact ids of a manifest's `BackupManifest.ContentDescriptors`, unique per capture: a delete deletes the artifacts of the manifest it began from. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Resolve(c)` | One call to the claim grain: refused while another scope holds a claim or owns the id, otherwise one claim recorded and persisted, for the owning scope too | `BackupIdClaimGrain.TryClaimAsync`, reached from `BackupPublisher.ResolveFullIdAsync` in `BackupSchedulerGrain.PublishAsync`. | Yes: `BackupIdCollisionTests.Identical_content_in_two_scopes_lists_one_backup_in_each_scope`, `BackupIdCollisionTests.A_bare_id_claim_admits_one_scope_until_it_is_released`, `BackupIdCollisionTests.The_owning_scope_holds_a_claim_while_it_republishes_its_id` (red when the owner's claim is not recorded), `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire` and `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_when_the_claim_grain_is_lost` (red when the claims are not persisted). |
| `Acquire(c)` | The publish waits for its scope's hold on the id and for every live delete lease of its scope | `BackupSchedulerGrain.TryHoldAsync` in `BackupSchedulerGrain.PublishAsync`, which waits while `BackupSchedulerGrain.IsDeleting`. | Yes: `BackupRetentionConcurrencyTests.A_full_capture_of_an_unchanged_tree_during_a_pass_waits_for_the_pass_and_republishes_the_backup`, `BackupRepublishHoldTests.A_republish_that_meets_a_pass_retiring_its_id_waits_beyond_one_call_and_then_publishes` and `BackupDeleteOverlapTests.A_delete_overlapping_another_keeps_the_id_reserved_until_its_own_end`. |
| `PubMan(c)` | The publish writes the sink manifest | `ILatticeBackupSink.WriteManifestAsync` in `BackupPublisher.PublishAsync`. | Yes: `BackupIdCollisionTests.Identical_content_in_two_scopes_lists_one_backup_in_each_scope`, which probes each scope's backup from the sink. |
| `PubRow(c)` | The publish registers the row, releases its hold and releases one claim, clearing the claim grain's stored record with the last | `ILatticeBackupCatalogStore.RegisterAsync` in `BackupPublisher.PublishAsync`, `BackupSchedulerGrain.ReleaseHold`, then `BackupIdClaimGrain.ReleaseAsync` through `BackupPublisher.ReleaseClaimAsync`. | Yes: `BackupIdCollisionTests.A_scope_keeps_the_bare_id_until_its_last_claim_is_released` (red when a release drops every claim of the scope), `BackupIdCollisionTests.A_bare_id_claim_leaves_no_stored_record_once_released` (red when the record is never cleared) and `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire`. |
| `RetireStart` | Scope A's pass takes A's hold on the id, never while a publish or a live delete lease holds it | `BackupSchedulerGrain.ReserveRetiring` in `BackupSchedulerGrain.PruneUnderGuardAsync`, which never dooms an id a publish or a delete holds (`BackupSchedulerGrain.IsHeld`). | Yes: `BackupRepublishHoldTests.A_pass_never_dooms_a_backup_a_capture_is_republishing`. |
| `RetireRow` | The pass removes the catalog row | `ILatticeBackupCatalogStore.RemoveAsync` in `BackupSchedulerGrain.PruneUnderGuardAsync`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts`. |
| `RetireMan` | The pass deletes the sink manifest and lets the id go | `ILatticeBackupSink.DeleteManifestAsync` in `BackupSchedulerGrain.PruneUnderGuardAsync`, then `BackupSchedulerGrain.ReleaseRetiring`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts`. |
| `DelBegin(d)` | An operator delete takes A's hold, removes A's row - or finds the sink manifest an earlier delete left - and takes a lease of its own, under the caller's token; an id another scope now owns is left alone | `BackupSchedulerGrain.BeginDeleteAsync`, through `BackupSchedulerGrain.TryHoldAsync` and `BackupPublisher.BeginDeleteAsync`. | Yes: `LatticeBackupControlDeleteHoldTests.DeleteBackupAsync_removes_the_backup_under_its_per_id_hold`, `BackupDeleteHoldTests.A_delete_interrupted_after_its_row_was_removed_is_finished_by_a_retry`, `BackupDeleteLeaseTests.A_delete_start_retried_with_its_token_holds_one_lease` (red when a retried start takes a second lease) and `BackupDeleteOverlapTests.A_delete_of_an_id_another_scope_has_since_taken_leaves_that_scopes_backup` (red when the scope is not checked). |
| `DelEnd(d)` | The delete ends under A's hold: it deletes the sink manifest only while no row lists the id and the manifest is A's, then drops its own lease | `BackupSchedulerGrain.EndDeleteAsync`, through `BackupSchedulerGrain.TryHoldAsync` and `BackupPublisher.FinishDeleteAsync`, after `BackupPublisher.DeleteArtifactsAsync` outside the grain. | Yes: `BackupDeleteHoldTests.A_delete_whose_artifact_deletes_outlast_the_grain_response_timeout_completes`, `BackupDeleteOverlapTests.A_delete_overlapping_another_keeps_the_id_reserved_until_its_own_end` (red when one end drops every lease), `BackupDeleteOverlapTests.A_delete_whose_lease_ran_out_keeps_a_backup_a_capture_republished_meanwhile` (red when the row is not checked) and `BackupDeleteOverlapTests.An_overlapping_delete_never_deletes_the_manifest_another_scope_published_under_the_id` (red when the scope is not checked). |
| `DelArt(d)` | The delete deletes the artifacts of the manifest it began from, outside the grain, only while its lease is live | `BackupPublisher.DeleteArtifactsAsync` in `SchedulerBackupPublicationGate.RetireAsync`, which renews the lease in the background through `BackupSchedulerGrain.RenewDeleteAsync` and deletes no further artifact once a renewal is refused. | Yes: `BackupDeleteLeaseTests.A_delete_that_loses_its_lease_deletes_no_further_artifact` (red when a refused renewal does not stop the delete) and `BackupDeleteLeaseTests.A_delete_whose_artifact_deletes_outlast_its_lease_keeps_a_rebuild_waiting` (red when the lease is not renewed). |
| `DelLapse(d)` | A delete's lease runs out: its caller was lost, or could not renew it for a whole lease; only that delete's lease, and eventually | `BackupSchedulerGrain.IsDeleting` and `BackupSchedulerGrain.RenewDeleteAsync`, which expire each delete's lease on its own once `LatticeBackupOptions.DeleteLease` passes without a renewal. | Yes: `BackupDeleteLeaseTests.A_lost_deletes_lease_runs_out_and_a_capture_then_republishes` (red when a lease never runs out) and `BackupDeleteLeaseTests.One_deletes_lease_running_out_leaves_another_deletes_lease_in_force` (red when one lease running out drops every lease of the id). |
| `Rebuild` | A catalog rebuild re-lists scope A's sink manifest under A's hold, waiting on a live delete lease, only when every artifact it references is in the sink | `BackupPublisher.PublishAsync` for `BackupPublishMode.Rebuild`, through `BackupSchedulerGrain.PublishAsync` and `BackupSchedulerGrain.TryHoldAsync`, reached from `LatticeBackupCatalogRebuildService`. | Yes: `BackupDeleteLeaseTests.A_rebuild_after_an_interrupted_delete_leaves_the_half_deleted_backup_unlisted` (red without the probe) and `BackupDeleteLeaseTests.A_delete_whose_artifact_deletes_outlast_its_lease_keeps_a_rebuild_waiting`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `ListedRestorable` | Every listed backup restores: its manifest is in the sink with every artifact it references. A rebuild never re-lists a backup a delete has half removed. Production violated it before #5637. | Yes: `BackupDeleteLeaseTests.A_rebuild_after_an_interrupted_delete_leaves_the_half_deleted_backup_unlisted` and `BackupDeleteLeaseTests.A_delete_whose_artifact_deletes_outlast_its_lease_keeps_a_rebuild_waiting`. |
| `LeaseEventuallyEnds` | A delete whose caller is lost does not keep the id reserved, and captures of identical content out, for good. | Yes: `BackupDeleteLeaseTests.A_lost_deletes_lease_runs_out_and_a_capture_then_republishes`. |
| `ListedInSink` | A listed backup's manifest is in the sink: a retire or a delete never deletes the manifest of a row a publish registered meanwhile. | Yes: `BackupRetentionConcurrencyTests.A_full_capture_of_an_unchanged_tree_during_a_pass_waits_for_the_pass_and_republishes_the_backup`, `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire`, which probes every listed backup, and `BackupDeleteOverlapTests.A_delete_whose_lease_ran_out_keeps_a_backup_a_capture_republished_meanwhile`. |
| `ScopeAgreement` | The catalog row and the sink manifest under the bare id name one scope, so a restore targets the scope that published it. Production violated it before #5620. | Yes: `BackupIdCollisionTests.Identical_content_in_two_scopes_lists_one_backup_in_each_scope` (red without the claim) and `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire`. |
| `NoForeignOverwrite` | No publish replaces another scope's backup under the bare id. Production violated it before #5627. | Yes: `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire` (red when the owner's claim is not recorded), `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_when_the_claim_grain_is_lost` and `BackupIdCollisionTests.A_scope_keeps_the_bare_id_until_its_last_claim_is_released`. |
| `DeleteFence` | While an operator delete of an id is in progress and its lease is live, no publish of the id in its scope runs: one delete ending never releases another's reservation. Production violated it before #5631. | Yes: `BackupDeleteOverlapTests.A_delete_overlapping_another_keeps_the_id_reserved_until_its_own_end`. |
| `NoForeignDelete` | A delete in one scope never removes another scope's backup under the bare id. | Yes: `BackupDeleteOverlapTests.A_delete_of_an_id_another_scope_has_since_taken_leaves_that_scopes_backup` and `BackupDeleteOverlapTests.An_overlapping_delete_never_deletes_the_manifest_another_scope_published_under_the_id`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification (issue #2321)

TypeOK, ListedInSink, ListedRestorable, ScopeAgreement and DeleteFence are state
safety; NoForeignOverwrite and NoForeignDelete are action safety;
LeaseEventuallyEnds is liveness, under weak fairness on `DelLapse` alone: a lost
delete's lease runs out, and nothing else need ever happen. Every property is
reached and falsifiable. Whether a refused capture ever publishes is the
scope-qualified id's business, outside the model, and a claim's expiry is a gap
below.

| Mutation | Classification |
|----------|----------------|
| TypeOkClaimRunaway | Anchored action: Resolve. |
| NoForeignOverwriteOwnerUnclaimed | Anchored action: Resolve. Production before #5627. |
| ScopeAgreementIgnoresOwner | Anchored action: Resolve. Production before #5620. |
| ListedInSinkAcquireIgnoresRetire | Anchored action: Acquire. |
| ListedInSinkManifestNotWritten | Anchored action: PubMan. |
| NoForeignOverwriteReleaseClearsAll | Anchored action: PubRow. |
| ListedInSinkRetireIgnoresPublish | Anchored action: RetireStart. |
| ListedInSinkRetireKeepsRow | Anchored action: RetireRow. |
| ListedInSinkManifestBeforeRow | Anchored action: RetireMan. |
| NoForeignDeleteBeginIgnoresScope | Anchored action: DelBegin. |
| ListedInSinkJoinedDeleteSharedLease | Anchored action: DelEnd. Production before #5631. |
| DeleteFenceEndClearsEveryLease | Anchored action: DelEnd. Production before #5631, the lease half alone. |
| ListedInSinkEndIgnoresRow | Anchored action: DelEnd. |
| ListedInSinkEndIgnoresPublish | Anchored action: DelEnd. |
| NoForeignDeleteEndIgnoresScope | Anchored action: DelEnd. |
| ListedRestorableRebuildUnprobed | Anchored action: Rebuild. Production before #5637, its rebuild half. |
| ListedRestorableRebuildIgnoresDelete | Anchored action: Rebuild. |
| ScopeAgreementRebuildWrongScope | Anchored action: Rebuild. |
| ListedRestorableArtifactsAfterLapse | Anchored action: DelArt. Production before #5637, its lease half. |
| ListedRestorableDeleteBeforeFix | Anchored actions: DelArt and Rebuild. Production before #5637. |
| LeaseEventuallyEndsNeverLapses | Anchored action: DelLapse. |
| DeleteFenceAcquireIgnoresLease | Anchored action: Acquire. |

## Detectors

`BackupIdCollisionTests` drives the claim grain directly - one scope admitted at a
time, the owner's claim recorded, a scope's claims released one at a time - and
captures identical content in two scopes. `BackupIdClaimRaceTests` reproduces the
#5627 interleaving on a real single-silo cluster: scope A's retention pass retires
the bare id; with its row gone, a capture in A resolves the id and waits on the
pass's hold, and once the manifest is deleted a capture of identical content in
scope B runs; B must take its scope-qualified id, and each scope's backup must
list as its own. A second race test loses the claim grain's activation in the
same window (#5632). `BackupDeleteOverlapTests` overlaps two deletes of one id,
lets a delete's lease run out under a republish, and lets another scope take the
id between and after overlapping deletes. `BackupDeleteLeaseTests` interrupts a
delete and rebuilds the catalog, makes a delete's artifact deletes outlast its
lease under a rebuild, loses the scheduler's activation mid-delete, loses a
delete's caller, and retries a delete's start.

## Defect mutations whose fixes have landed

NoForeignOverwriteOwnerUnclaimed and NoForeignOverwriteReleaseClearsAll reproduce
production before the fix of #5627, ScopeAgreementIgnoresOwner production before
#5620, ListedInSinkJoinedDeleteSharedLease and DeleteFenceEndClearsEveryLease
production before #5631, and ListedRestorableDeleteBeforeFix,
ListedRestorableRebuildUnprobed and ListedRestorableArtifactsAfterLapse production
before #5637. Each fix lands with this module, and each reproduction stands as a
regression check with a production analogue among the detectors above.

## Deliberate abstraction gaps

- **One bare id, one pass, two deletes, deleters in one scope.** The model has
  one bare id, which scope A already owns, one retention pass and two overlapping
  operator deletes, all in scope A. More ids are independent: each has its own
  claim grain and its own holds. Two deletes are the least that overlap, which is
  where #5631 lay; a third delete holds a lease of its own and ends the same way.
  A second pass cannot overlap the first, which runs under the scope's pass guard.
  B never retires or rebuilds: its deletes, passes and rebuilds run on B's
  scheduler grain, under B's hold, symmetrically. A delete's artifact deletes are
  one step over one flag, `art`, for the manifest write it began from: every
  capture streams artifacts of its own, so they never touch a republished
  backup's.
  No production symbol: the bound is a property of the checked instance.
- **Scope-qualified ids.** A refused capture is done as far as the model goes.
  Its scope-qualified id names its scope, so no other scope derives it, and its
  publish is serialised against its own scope's retention by `BackupRetention`.
  Production symbols: `BackupIdentity.ScopeQualifiedFull`.
- **Claim expiry.** A claim lapses after `BackupIdClaimGrain.PendingClaimLifetime`
  if its publish never releases it, for example when its silo is lost. The model
  has no crash; a lapsed claim of a capture that is still running would reopen the
  #5627 window, which the lifetime - far beyond the longest publish call - makes
  unreachable in practice. The claim grain itself may lose its activation at any
  time; its claims are persisted (#5632), so `pend` survives that. A claim whose
  write fails is taken back and not granted, and the capture fails and is
  retried; a release whose write fails leaves the claim stored, so a later
  activation may restore it until it lapses, which only refuses other scopes for
  longer. The last claim released clears the grain's stored record.
  Production symbols: `BackupIdClaimGrain.PendingClaimLifetime`, `BackupIdClaimState`.
- **A lease renewal is not a step.** `DelArt` deletes artifacts only under a live
  lease, as if renewal and delete were one step. Production renews in the
  background every third of `LatticeBackupOptions.DeleteLease` and checks for a
  refused renewal before each artifact delete, so an artifact delete already in
  flight when the lease runs out - a caller unable to reach the grain for a whole
  lease - can still complete after a rebuild has re-listed the backup. That one
  window is not modelled.
  Production symbols: `SchedulerBackupPublicationGate.RetireAsync`, `BackupSchedulerGrain.RenewDeleteAsync`, `LatticeBackupOptions.DeleteLease`.
- **A delete's two grain calls are atomic steps.** `DelBegin` and `DelEnd` each
  run in one call that holds the scope's hold across its awaits, so no publish,
  pass or other delete of the id interleaves inside one; `EndDeleteAsync` renews
  its own lease before it waits for the hold. A delete whose artifact deletes
  fail, or whose renewal is refused, ends its lease at once with the manifest
  kept, which the model's `DelLapse` followed by no end covers, and a retry
  begins afresh from the sink manifest.
  Production symbols: `BackupSchedulerGrain.EndDeleteAsync`.
- **A claim per publish call.** The publication gate retries a busy publish call,
  and each call claims and releases afresh. Between two calls another scope may
  take an id that has been retired; the retried capture then takes its
  scope-qualified id, which is the documented collision rule, not an overwrite.
  The model's one claim per capture covers each call.
  Production symbols: `SchedulerBackupPublicationGate`.

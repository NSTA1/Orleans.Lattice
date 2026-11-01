# Refinement note: BackupIdClaim to code

This note maps [`BackupIdClaim.tla`](BackupIdClaim.tla) - which scope owns a full
backup's bare content-addressed id - to the production symbols that play each
role, and to the detector tests that would notice production deviating from it.
It is a documented mapping, not a machine-checked refinement proof.

## Model scope in one paragraph

A full backup's id is the hash of its payload, which names no scope, so identical
content captured in two scopes derives one bare id. Two scopes, A and B, capture
identical content: two captures in A, one in B. Scope A already owns the bare id,
and A retires it once - a retention pass that dooms it, or an operator delete -
holding A's per-id hold while it removes the catalog row and then the sink
manifest. A capture asks the id's claim grain whether its scope may publish under
the bare id; a refused capture takes its scope-qualified id, which is its scope's
alone and outside the model. An admitted capture waits for its own scope's hold,
writes the sink manifest, registers the catalog row and releases its claim. The
two scopes' holds are separate grains, so only the claim keeps them apart. The
model checks the fix of #5627: a claim is recorded for every admitted capture,
the owning scope's included, and released one at a time, so an id a scope is
still publishing through its own retire never becomes another scope's, and no
publish replaces another scope's backup under the bare id. The cross-scope rule
itself is #5620's.

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

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `row` | The scope of the bare id's catalog row | `ILatticeBackupCatalogStore.GetAsync` read by `BackupIdOwnership.OwnerScopeKeyAsync`, and `BackupManifest.Scope`. |
| `man` | The scope of the bare id's sink manifest | `ILatticeBackupSink.ReadManifestAsync` read by `BackupIdOwnership.OwnerScopeKeyAsync` when the id has no row. |
| `pend` | The claim grain's live claims on the bare id | `BackupIdClaimGrain._pending`, one entry per admitted capture. |
| `held` | Each scope's hold on the bare id | `BackupSchedulerGrain._publishing`, `BackupSchedulerGrain._retiring` and `BackupSchedulerGrain._deleting` on that scope's scheduler grain. |
| `cap` | Each capture's progress and resolved id | The activation-local progress of `BackupSchedulerGrain.PublishAsync`, with the id `BackupPublisher.ResolveFullIdAsync` returned: the bare id, or `BackupIdentity.ScopeQualifiedFull`. |
| `ret` | Scope A's retire of the bare id | The progress of a retention pass (`BackupSchedulerGrain.PruneCoreAsync`) or an operator delete (`BackupSchedulerGrain.BeginDeleteAsync`). |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Resolve(c)` | One call to the claim grain: refused while another scope holds a claim or owns the id, otherwise one claim recorded, for the owning scope too | `BackupIdClaimGrain.TryClaimAsync`, reached from `BackupPublisher.ResolveFullIdAsync` in `BackupSchedulerGrain.PublishAsync`. | Yes: `BackupIdCollisionTests.Identical_content_in_two_scopes_lists_one_backup_in_each_scope`, `BackupIdCollisionTests.A_bare_id_claim_admits_one_scope_until_it_is_released` and `BackupIdCollisionTests.The_owning_scope_holds_a_claim_while_it_republishes_its_id` (red when the owner's claim is not recorded). |
| `Acquire(c)` | The publish waits for its scope's hold on the id | `BackupSchedulerGrain.TryHoldAsync` in `BackupSchedulerGrain.PublishAsync`. | Yes: `BackupRetentionConcurrencyTests.A_full_capture_of_an_unchanged_tree_during_a_pass_waits_for_the_pass_and_republishes_the_backup` and `BackupRepublishHoldTests.A_republish_that_meets_a_pass_retiring_its_id_waits_beyond_one_call_and_then_publishes`. |
| `PubMan(c)` | The publish writes the sink manifest | `ILatticeBackupSink.WriteManifestAsync` in `BackupPublisher.PublishAsync`. | Yes: `BackupIdCollisionTests.Identical_content_in_two_scopes_lists_one_backup_in_each_scope`, which probes each scope's backup from the sink. |
| `PubRow(c)` | The publish registers the row, releases its hold and releases one claim | `ILatticeBackupCatalogStore.RegisterAsync` in `BackupPublisher.PublishAsync`, `BackupSchedulerGrain.ReleaseHold`, then `BackupIdClaimGrain.ReleaseAsync` through `BackupPublisher.ReleaseClaimAsync`. | Yes: `BackupIdCollisionTests.A_scope_keeps_the_bare_id_until_its_last_claim_is_released` (red when a release drops every claim of the scope) and `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire`. |
| `RetireStart` | Scope A's retire takes A's hold on the id, waiting for a publish | `BackupSchedulerGrain.ReserveRetiring` in `BackupSchedulerGrain.PruneUnderGuardAsync`, which never dooms an id a publish holds (`BackupSchedulerGrain.IsHeld`), and `BackupSchedulerGrain.TryHoldAsync` in `BackupSchedulerGrain.BeginDeleteAsync`. | Yes: `BackupRepublishHoldTests.A_pass_never_dooms_a_backup_a_capture_is_republishing` and `LatticeBackupControlDeleteHoldTests.DeleteBackupAsync_removes_the_backup_under_its_per_id_hold`. |
| `RetireRow` | The retire removes the catalog row | `ILatticeBackupCatalogStore.RemoveAsync` in `BackupSchedulerGrain.PruneUnderGuardAsync` and in `BackupPublisher.BeginDeleteAsync`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts` and `BackupDeleteHoldTests.A_delete_interrupted_after_its_row_was_removed_is_finished_by_a_retry`. |
| `RetireMan` | The retire deletes the sink manifest and lets the id go | `ILatticeBackupSink.DeleteManifestAsync` in `BackupSchedulerGrain.PruneUnderGuardAsync`, then `BackupSchedulerGrain.ReleaseRetiring`; for a delete, in `BackupPublisher.CompleteDeleteAsync`, then `BackupSchedulerGrain.EndDeleteAsync`. | Yes: `BackupSchedulerGrainUnitTests.PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts` and `BackupDeleteHoldTests.A_delete_whose_artifact_deletes_outlast_the_grain_response_timeout_completes`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `ListedInSink` | A listed backup's manifest is in the sink: a retire never deletes the manifest of a row a publish of its scope registered meanwhile. | Yes: `BackupRetentionConcurrencyTests.A_full_capture_of_an_unchanged_tree_during_a_pass_waits_for_the_pass_and_republishes_the_backup` and `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire`, which probes every listed backup. |
| `ScopeAgreement` | The catalog row and the sink manifest under the bare id name one scope, so a restore targets the scope that published it. Production violated it before #5620. | Yes: `BackupIdCollisionTests.Identical_content_in_two_scopes_lists_one_backup_in_each_scope` (red without the claim) and `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire`. |
| `NoForeignOverwrite` | No publish replaces another scope's backup under the bare id. Production violated it before #5627. | Yes: `BackupIdClaimRaceTests.Another_scope_cannot_take_an_id_its_owner_is_republishing_through_a_retire` (red when the owner's claim is not recorded) and `BackupIdCollisionTests.A_scope_keeps_the_bare_id_until_its_last_claim_is_released`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification (issue #2321)

TypeOK, ListedInSink and ScopeAgreement are state safety; NoForeignOverwrite is
action safety. Every property is reached and falsifiable. The module checks no
liveness: whether a refused capture ever publishes is the scope-qualified id's
business, outside the model, and a claim's expiry is a gap below.

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

## Detectors

`BackupIdCollisionTests` drives the claim grain directly - one scope admitted at a
time, the owner's claim recorded, a scope's claims released one at a time - and
captures identical content in two scopes. `BackupIdClaimRaceTests` reproduces the
#5627 interleaving on a real single-silo cluster: scope A's retention pass retires
the bare id; with its row gone, a capture in A resolves the id and waits on the
pass's hold, and once the manifest is deleted a capture of identical content in
scope B runs; B must take its scope-qualified id, and each scope's backup must
list as its own.

## Defect mutations whose fixes have landed

NoForeignOverwriteOwnerUnclaimed and NoForeignOverwriteReleaseClearsAll reproduce
production before the fix of #5627, and ScopeAgreementIgnoresOwner production
before #5620. Each fix lands with this module, and each reproduction stands as a
regression check with a production analogue among the detectors above.

## Deliberate abstraction gaps

- **One bare id, one retire.** The model has one bare id, which scope A already
  owns, and one retire. More ids are independent: each has its own claim grain.
  A second retire would only repeat the window the first opens.
  No production symbol: the bound is a property of the checked instance.
- **Scope-qualified ids.** A refused capture is done as far as the model goes.
  Its scope-qualified id names its scope, so no other scope derives it, and its
  publish is serialised against its own scope's retention by `BackupRetention`.
  Production symbols: `BackupIdentity.ScopeQualifiedFull`.
- **Claim expiry.** A claim lapses after `BackupIdClaimGrain.PendingClaimLifetime`
  if its publish never releases it, for example when its silo is lost. The model
  has no crash; a lapsed claim of a capture that is still running would reopen the
  #5627 window, which the lifetime - far beyond the longest publish call - makes
  unreachable in practice.
  Production symbols: `BackupIdClaimGrain.PendingClaimLifetime`.
- **A claim per publish call.** The publication gate retries a busy publish call,
  and each call claims and releases afresh. Between two calls another scope may
  take an id that has been retired; the retried capture then takes its
  scope-qualified id, which is the documented collision rule, not an overwrite.
  The model's one claim per capture covers each call.
  Production symbols: `SchedulerBackupPublicationGate`.

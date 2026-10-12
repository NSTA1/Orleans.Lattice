# MaterialisedViewRebind refinement

This is a documented abstraction, not a machine-checked refinement proof.
It checks the intended physical-identity cutover contract, including intermediate
states during a shadow rebuild. It does not equate eventual self-heal with an
atomic identity-consistent cutover.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `stamps`, `logs`, `source`, `txOrigin`, `outcome` | Independently stamped source writes and physically owned prepares | `LatticeMutation.Timestamp`, `ViewMaintainerGrain.HandleStagingEntry`. |
| `alias`, `moves` | Logical source alias and bounded topology move | `ILatticeRegistry.SwapAliasAsync`. |
| `bound`, `generation`, `view` | Durable physical binding and served generation | `ViewCheckpointState.BoundPhysicalTreeId`, `ViewCheckpointState.ActiveGeneration`. |
| `read`, `checkpoint`, `staged`, `stageOrigin` | Bound-log offsets and volatile prepared state | `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`. |
| `phase`, `candidate`, `floor`, `shadow` | Physical scan identity, protected capture and completed shadow | `ViewMaintainerGrain.BuildShadowAsync`, `ViewMaintainerGrain.CaptureResumeFloorsAsync`. |
| `held`, `trim` | Offset read holds and physically scoped cursor GC | `ViewMaintainerGrain.EnsureReadRegisteredAsync`, `LatticeWalGc.ReadOffsetConsumerFloorsAsync`. |
| `crashes`, `rebuilt` | Bounded failure and explicit-rebuild budgets | Modelling devices, not production fields. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write` | Append an ordinary write to the alias's current physical WAL | `ILattice.SetAsync`. | Yes: `MaterialisedViewIdentitySwapHealTests.View_tails_new_identity_after_swap`. |
| `Prepare` | Append a physically owned prepare | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `MaterialisedViewAtomicVisibilityTests.Prepared_but_uncommitted_batch_is_not_visible_in_the_view`. |
| `Decide` | Commit or abort the source prepare | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced`. |
| `SourceMove` | Copy committed state and change the source identity | `ILatticeRegistry.SwapAliasAsync`. | Yes: `MaterialisedViewIdentitySwapHealTests.View_heals_after_source_physical_identity_swap`. |
| `ReadOrdinary` | Drain only the bound physical log | `ViewMaintainerGrain.DrainAsync`. | Yes: `MaterialisedViewIdentitySwapHealTests.View_tails_new_identity_after_swap`. |
| `Stage` | Tag staging with the physical log that owns its offsets | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal` stages an unresolved old-log prepare, then changes identity through direct rebuild and notified drain. `ViewMaintainerPhysicalStagingTests.Drain_physical_staging_detector_rejects_restored_old_state_controls` restores old staging and rejects its public-output effect. |
| `Flush` | Resolve only staging from the current bound physical log | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal` delivers a new-log terminal with the old transaction id, observes no old payload, then requires exactly the new prepare to publish; `ViewMaintainerPhysicalStagingTests.Drain_physical_staging_detector_rejects_restored_old_state_controls` discriminates stale staging. |
| `Checkpoint` | Bank and hold the bound physical resume point | `ViewMaintainerGrain.PublishReadPositions`. | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point`. |
| `BeginRebuild` | Register a physical floor before capturing and scanning | `ViewMaintainerGrain.EnsureReadRegisteredAsync`, `ViewMaintainerGrain.CaptureResumeFloorsAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Rebuild_new_alias_log_is_held_before_registration_and_at_the_captured_floor_before_scanning` checks simultaneous old-bound and candidate floors; `ViewMaintainerCandidateGcTests.Rebuild_real_gc_preserves_both_logs_until_durable_publication` runs actual GC after registration becomes visible, at head capture and during the scan. |
| `Scan` | Read the same physical source as the captured floor | `ViewMaintainerGrain.BuildShadowAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Rebuild_source_moves_preserve_the_captured_tuple_or_reject_before_publication` moves the alias during the captured source scan and rejects before publication. |
| `Validate` | Reject a moved alias without relabelling the already captured scan; alias may still move while persistence awaits | `ViewMaintainerGrain.SwapToShadowAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Rebuild_source_moves_preserve_the_captured_tuple_or_reject_before_publication` distinguishes a scan-time move from a move while persistence awaits; the late-move fresh-alias cursor control fails. |
| `Install` | Commit generation, captured physical identity, floor and staging reset together | `ViewMaintainerGrain.SwapToShadowAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Rebuild_source_moves_preserve_the_captured_tuple_or_reject_before_publication`, `ViewMaintainerRebuildIdentityTests.Rebuild_failed_checkpoint_response_reactivation_uses_the_persisted_physical_tuple` establish the physical tuple; `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal` establishes staging isolation and unrelated new-log offsets, with `ViewMaintainerPhysicalStagingTests.Drain_physical_staging_detector_rejects_restored_old_state_controls` at staging, cursor and persistence boundaries. |
| `RejectStale` | Retry a scan whose physical source is no longer current | `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Rebuild_stale_scan_rejects_then_retry_publishes_the_retry_physical_tuple` rejects the captured stale scan before any flip, retains the old tuple, then publishes only the retry source and its floor; `ViewMaintainerSourceIdentityTests.Failed_heal_does_not_suppress_the_retry_re_resolve_within_the_backstop` covers failed automatic heal retry. |
| `ReleaseRetired` | Withdraw the retired physical read hold only after install | `ViewMaintainerGrain.WithdrawReadRegistrationAsync`. | Yes: `ViewMaintainerRebuildIdentityTests.Rebuild_retires_the_old_source_pin_only_after_the_captured_tuple_is_durable`, `ViewMaintainerRebuildIdentityTests.Rebuild_failed_checkpoint_response_reactivation_uses_the_persisted_physical_tuple`, and `ViewMaintainerCandidateGcTests.Rebuild_real_gc_preserves_both_logs_until_durable_publication` cover successful release, both uncertain checkpoint responses and persisted-tuple reactivation. |
| `CursorTrim` | Trim only within the physical read entitlement | `LatticeWalGc.ReadOffsetConsumerFloorsAsync`. | Yes: `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints`. |
| `Crash` | Republish the persisted physical floor and lose volatile staging/build | `ViewMaintainerGrain.PublishActivationReadPositions`. | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point`. |
| `Stutter` | No step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `StagingNamesBoundLog` | Retired physical staging never rides into the new bound generation | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal` covers direct and notified rebind with reused transaction ids; `ViewMaintainerPhysicalStagingTests.Drain_physical_staging_detector_rejects_restored_old_state_controls` fails specifically on old payload publication. |
| `PrepareNamesPhysicalLog` | A transaction's prepare offset belongs to its tagged physical WAL | Yes: `MaterialisedViewAtomicVisibilityTests.Prepared_but_uncommitted_batch_is_not_visible_in_the_view`. |
| `ResumeNamesBoundLog` | Durable offsets are absolute in the persisted bound log, never copied across identities | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point` covers activation; `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal` requires new-log reads from positions 1 and 2, independently of old offset 7. `ViewMaintainerPhysicalStagingTests.Drain_physical_staging_detector_rejects_restored_old_state_controls` rejects both relabelled old read positions and an old checkpoint injected at persistence. |
| `BindingHeld` | Persisted physical binding has a read hold throughout rebuild and recovery | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point`, `ViewMaintainerRebuildIdentityTests.Rebuild_rejected_candidates_never_replace_the_persisted_bound_log_entitlement`, `ViewMaintainerRebuildIdentityTests.Rebuild_failed_publication_then_another_candidate_preserves_the_original_bound_entitlement`, `ViewMaintainerRebuildIdentityTests.Rebuild_revisiting_original_bound_log_after_failed_publication_keeps_its_durable_read_floor`, and `ViewMaintainerCandidateGcTests.Rebuild_real_gc_preserves_both_logs_until_durable_publication` cover immutable durable identity through repeated candidates, role transitions, actual rebuild-time GC and failed-response recovery. |
| `CandidateHeld` | Captured physical scan floor stays protected until install or rejection | Yes: `ViewMaintainerRebuildIdentityTests.Rebuild_new_alias_log_is_held_before_registration_and_at_the_captured_floor_before_scanning` and `ViewMaintainerCandidateGcTests.Rebuild_real_gc_preserves_both_logs_until_durable_publication` cover registration, head capture, scan-appended suffix and publication. `ViewMaintainerCandidateGcTests.Rebuild_real_gc_candidate_hold_removal_control_trims_needed_entries` removes only the candidate offset answer and makes actual GC delete needed entries before capture or during the scan. |
| `CursorTrimEntitled` | GC cannot erase a bound or rebuilding reader's unconsumed suffix | Yes: `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints` covers the bound-reader path; `ViewMaintainerCandidateGcTests.Rebuild_real_gc_preserves_both_logs_until_durable_publication` trims an eligible old prefix while retaining both unread suffixes at registration, head capture, source scan and checkpoint write, with an independent forward HLC cursor. `ViewMaintainerCandidateGcTests.Rebuild_real_gc_candidate_hold_removal_control_trims_needed_entries` proves the candidate offset hold, not a local HLC pin or an unreadable-input fallback, prevents actual storage deletion. |
| `NoAbortedPrepare` | Neither scan nor flush exposes undecided/aborted state | Yes: `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced` exercises terminal flush; `MaterialisedViewAtomicVisibilityTests.Bounded_staging_buffer_forces_a_rebuild_and_the_view_still_converges` forces an overflow rebuild with pending source prepares and verifies their exclusion from the installed generation. |
| `InstallUsesCapturedIdentity` | Every generation flip uses one coherent captured physical scan and floor, not a newly resolved alias | Yes: `ViewMaintainerRebuildIdentityTests.Rebuild_source_moves_preserve_the_captured_tuple_or_reject_before_publication`, `ViewMaintainerRebuildIdentityTests.Rebuild_failed_checkpoint_response_reactivation_uses_the_persisted_physical_tuple` cover scan-time/late alias moves and both failed-before-commit and committed-lost-response restart. |
| `CutoverKeepsCommittedSnapshot` | Topology cutover copies committed source state before alias publication | Yes: `MaterialisedViewIdentitySwapHealTests.View_heals_after_source_physical_identity_swap`. |
| `BindingConverges` | After finite source moves, crash recovery and fair rebuild/drain converge to the current physical state | Yes: `MaterialisedViewIdentitySwapHealTests.View_retries_heal_when_rebuild_fails_then_converges`. |
| `RetiredPinsReleased` | Old physical registrations eventually release after a successful cutover | Yes: `MaterialisedViewIdentitySwapHealTests.View_reports_its_wal_cursor_under_the_bound_physical_identity_after_swap`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain constraint, not a production behaviour. |

## Composition contracts and limits

The source topology contract comes from TreeLifecycle: unresolved source
transactions are not silently copied as if committed. A source move here copies
committed state into a fresh physical identity with its own absolute WAL offsets.
Resize/reshard/restore copy, fencing, reservation and undo internals belong to that
companion. This model does not prove a physical id can be reused with the same
offsets after undo; a fresh identity is required for this bounded instance.

The scan source, resume floor and resulting generation form one identity tuple.
Installation is checked as an action property over every flip. Losing a stale
candidate must trigger retry, not rename its scan with a freshly resolved alias.
Both old and candidate read registrations may coexist while scanning; releasing
the old registration early is not justified by a later successful rebuild.

Issue #4891 identified independent alias resolution during scan, cursor
publication and the outer rebind assignment. Its candidate remediation carries
WalTreeId with the scan result, validates that candidate, publishes captured
binding/generation/offsets together, and releases the prior physical registration
after publication. The accepted physical correction and restart detectors establish scan-time rejection,
late-move tuple consistency, and failed-before-commit/committed-lost-response
recovery. Candidate-specific actual GC evidence now exercises real rebuild,
GC and in-memory WAL storage at registration, capture, scan and publication. Both immutable offset holds now coexist through registration,
capture, scan and failed publication, independently of the process-local HLC
cursor. The durable binding identity is explicitly preserved through snapshot
role transitions, and only successful publication retires its entitlement.
Physical staging and moving-log resume isolation now have accepted actual
drain/rebuild detectors with anchored staging, cursor and checkpoint controls.
The GC fixture controls grain transport and the registry census while forwarding
actual maintainer snapshots; it is not a full-cluster or registry-durability proof.
Its candidate-hold-removal controls retain every other GC input and establish
real deletion of the otherwise protected capture prefix or scan suffix.

Validation and persistence are separate model actions: an alias move between
them may install a coherent but no-longer-current captured source. The action
property requires the captured physical tuple, not equality to the latest alias
at every publication. Subsequent binding convergence heals that late move.
A lost successful publication response is represented by Install followed by
Crash; failure before commit is Crash while captured/scanned/validated. The
model does not certify ambiguous persistence semantics without the production
restart detector. Temporal convergence alone cannot certify intermediate safety.

GC here is cursor-entitled, not retention-TTL trimming. Source pending prepares
are also held by the source-side transaction/WAL contract. A fresh binding starts
at the retained prefix and scans current source state; per-partition fences,
terminal horizons, source confirmation and multi-key atomic visibility belong
to their focused companions. A scalar source revision is used here to isolate
physical identity, not to replace those companions' visibility obligations.

The transaction bound is also deliberate: this instance has one global prepare
and no resolved-transaction cache. It cannot exercise reuse of a transaction
identifier in a second physical log. Issue #4895 identified that production
resolved identifiers can survive cutover and suppress the new log's transaction
while its checkpoint advances. Existing staging-reset mutants do not detect
that cache-ownership failure. The accepted source correction scopes the resolved
set and eviction queue to the physical WAL actually tailed, retaining same-log
duplicates through rebuild and failed candidate responses. The discriminating
`ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_reuse_old_log_resolved_transaction_ids`
and its old-cache control establish production regression evidence. The separate
[two-log identity companion](../view-resolved-identity/README.md) checks that
cache-ownership obligation, while this model continues to exclude it. No
resolved-cache safety claim follows from this model's census.

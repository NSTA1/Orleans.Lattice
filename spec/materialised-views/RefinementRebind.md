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
| `Stage` | Tag staging with the physical log that owns its offsets | `ViewMaintainerGrain.HandleStagingEntry`. | Partial: `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch` covers same-log staging; physical-rebind staging isolation needs its new source detector. #4891 retains the pending composition contract. |
| `Flush` | Resolve only staging from the current bound physical log | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`. | Partial: `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced` covers decisions, not a mid-rebuild physical swap. #4891 retains the pending composition contract. |
| `Checkpoint` | Bank and hold the bound physical resume point | `ViewMaintainerGrain.PublishReadPositions`. | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point`. |
| `BeginRebuild` | Register a physical floor before capturing and scanning | `ViewMaintainerGrain.EnsureReadRegisteredAsync`, `ViewMaintainerGrain.CaptureResumeFloorsAsync`. | Partial: `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints` exercises only the current-bound floor, not candidate capture-before-scan. A candidate-hold-removal detector is required. |
| `Scan` | Read the same physical source as the captured floor | `ViewMaintainerGrain.BuildShadowAsync`. | Partial: `MaterialisedViewShadowSwapTests.Rebuild_never_exposes_empty_or_partial_view_to_a_concurrent_reader` covers generation publication; source moving during scan requires the new rebind detector. #4891 retains the pending composition contract. |
| `Validate` | Reject a moved alias without relabelling the already captured scan; alias may still move while persistence awaits | `ViewMaintainerGrain.SwapToShadowAsync`. | Partial: `ViewMaintainerSourceIdentityTests.Failed_heal_does_not_suppress_the_retry_re_resolve_within_the_backstop` proves retry after failure, not capture validation. #4891 has the scan/late-publication candidate detector, awaiting source acceptance. |
| `Install` | Commit generation, captured physical identity, floor and staging reset together | `ViewMaintainerGrain.SwapToShadowAsync`. | Partial: `MaterialisedViewIdentitySwapHealTests.View_tails_new_identity_after_swap` covers a completed move, not a move between scan and cutover. #4891 retains the pending composition contract. |
| `RejectStale` | Retry a scan whose physical source is no longer current | `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`. | Partial: `ViewMaintainerSourceIdentityTests.Failed_heal_does_not_suppress_the_retry_re_resolve_within_the_backstop` detects retry after failure, not a stale successful scan. #4891 retains the pending composition contract. |
| `ReleaseRetired` | Withdraw the retired physical read hold only after install | `ViewMaintainerGrain.WithdrawReadRegistrationAsync`. | Yes: `MaterialisedViewIdentitySwapHealTests.View_reports_its_wal_cursor_under_the_bound_physical_identity_after_swap`. |
| `CursorTrim` | Trim only within the physical read entitlement | `LatticeWalGc.ReadOffsetConsumerFloorsAsync`. | Yes: `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints`. |
| `Crash` | Republish the persisted physical floor and lose volatile staging/build | `ViewMaintainerGrain.PublishActivationReadPositions`. | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point`. |
| `Stutter` | No step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `StagingNamesBoundLog` | Retired physical staging never rides into the new bound generation | Partial: `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch` is the existing same-log detector; the moving-source detector is needed. #4891 retains the pending composition contract. |
| `PrepareNamesPhysicalLog` | A transaction's prepare offset belongs to its tagged physical WAL | Yes: `MaterialisedViewAtomicVisibilityTests.Prepared_but_uncommitted_batch_is_not_visible_in_the_view`. |
| `ResumeNamesBoundLog` | Durable offsets are absolute in the persisted bound log, never copied across identities | Partial: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point` covers activation; the moving-source detector is needed. #4891 retains the pending composition contract. |
| `BindingHeld` | Persisted physical binding has a read hold throughout rebuild and recovery | Yes: `ViewMaintainerReadPositionTests.A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point`. |
| `CandidateHeld` | Captured physical scan floor stays protected until install or rejection | Partial: `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints` proves the current-bound reader hold only; it never starts a rebuild or removes the candidate hold. Candidate GC entitlement needs a discriminating production detector. |
| `CursorTrimEntitled` | GC cannot erase a bound or rebuilding reader's unconsumed suffix | Yes: `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints`. |
| `NoAbortedPrepare` | Neither scan nor flush exposes undecided/aborted state | Yes: `MaterialisedViewAtomicVisibilityTests.Prepared_but_uncommitted_batch_is_not_visible_in_the_view`, `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced`. |
| `InstallUsesCapturedIdentity` | Every generation flip uses one coherent captured physical scan and floor, not a newly resolved alias | Partial: `MaterialisedViewShadowSwapTests.Rebuild_never_exposes_empty_or_partial_view_to_a_concurrent_reader` covers generation atomicity; the moving-source regression must cover identity consistency. #4891 retains the pending composition contract. |
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
after publication. The scan-time alias-move detector is red/green in the source
child. These rows remain partial until accepted source detectors also establish
late movement during publication and failure/restart identity consistency.

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

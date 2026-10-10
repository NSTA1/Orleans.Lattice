# Refinement note: tenant policy freshness to production

This note maps [`TenantIsolation.tla`](TenantIsolation.tla) to the Orleans.Lattice code
that plays each role. The model covers a registry write committing on one silo, the
epoch advance published through the epoch grain, each silo's compiled snapshot and
lease, the snapshot rebuild, silo crash and membership recovery, and epoch grain
re-activation. Like the other refinement notes it is a documented mapping, not a
refinement proof; the gates check its names, detectors and coverage, and none checks
its claims.

The guarantee the model checks is bounded on purpose. Once a write has returned after
its publication completed, no silo trusts a snapshot older than that write, and a
decision taken while a silo cannot know it is current confirms against the registry
or denies. It does not claim that a decision already taken is revoked, and it keeps
two windows open rather than assuming them away: a write whose publication fails
returns while peers may still trust their old snapshots until the background
re-publish lands, and a writer that crashes after committing leaves peers unaware
until membership declares it dead. The guarantee is conditional on no silo's clock
running slower than the epoch grain's by more than `TenantPolicyEpochLedger.Margin`
over one lease.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `regVer` | The registry version | The durable tenant registry after `CompiledTenantPolicySnapshotMaintainer.OnMutationAsync` is called for a committed write. |
| `coveredVer` | The newest write every silo is guaranteed to have observed | The write whose `TenantPolicyEpochLedger.AdvanceAsync` completed, or whose crashed writer membership declared dead. |
| `returnedVer` | The newest write returned to its caller | Completion of the registry write call that ran the mutation hook. |
| `wstate`, `wsilo` | The write in flight and the silo it committed on | The hook's progress through `CompiledTenantPolicySnapshotMaintainer.PublishAdvanceAsync` and `CompiledTenantPolicySnapshotMaintainer.EnsureAdvanceRetry`. |
| `owes` | The silo has an advance in flight or owed | The maintainer's in-flight and failed advance tracking, which `CompiledTenantPolicySnapshotMaintainer.IsSnapshotAuthoritative` consults. |
| `epoch` | The current cluster epoch | `TenantPolicyEpoch`: an incarnation drawn per grain activation and a version. |
| `graceOpen`, `graceWait` | The fresh-activation grace, and whether an advance waits for it | `TenantPolicyEpochLedger.ReleaseGrace` and the ledger's grace wait. |
| `lease` | A silo's lease as the grain records it and the silo trusts it | `TenantPolicyEpochLedger.Lease` and the silo's lease clock applied by `CompiledTenantPolicySnapshotMaintainer.ApplyLease`: `"live"` before the deadline, `"margin"` past it but inside `TenantPolicyEpochLedger.Margin`, `"old"` granted by a previous activation. |
| `pend` | The advance's per-silo obligation | The subscribers `TenantPolicyEpochLedger.AdvanceAsync` captured and waits on: `"tied"` unacknowledged, `"open"` renewed past, `"lapsed"` deadline plus margin passed. |
| `snapVer`, `stale`, `scanning`, `scanDirty` | The compiled snapshot, its out-of-date mark and the rebuild | `CompiledTenantPolicySnapshotMaintainer.RebuildNowAsync` and `TenantSnapshotCurrency.IsCurrent`. |
| `knows` | The silo has observed the current epoch | `TenantSnapshotCurrency.Observe` via `CompiledTenantPolicySnapshotMaintainer.ObserveEpoch`. |
| `alive`, `awaiting` | Process liveness, and a crash membership has not yet declared | The silo process, and the membership watch in `TenantPolicyEpochSubscription`. |
| `dec` | The last authorization decision | The outcome of `TenantGateEnforcer.EnforceAsync`. |
| `crashes`, `restarts`, `failures` | Environment budgets | Modelling devices only. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `CommitWrite(s)` | A registry write commits and the hook marks the silo out of date | `CompiledTenantPolicySnapshotMaintainer.OnMutationAsync` calls `CompiledTenantPolicySnapshotMaintainer.ScheduleRebuild` before publishing. | Yes: `TenantPolicyCrossSiloCurrencyTests.Committing_silo_is_not_authoritative_while_its_advance_is_in_flight`. |
| `StartAdvance` | The epoch grain bumps the version and captures the leases to push to | `TenantPolicyEpochGrain.AdvanceAsync` and `TenantPolicyEpochLedger.AdvanceAsync`. | Yes: `TenantPolicyEpochLedgerTests.AdvanceAsync_bumps_the_version_and_pushes_it_to_every_leased_subscriber` and `TenantPolicyEpochLedgerMarginTests.AdvanceAsync_pushes_to_a_lease_past_its_deadline_but_inside_the_margin`. |
| `Deliver(t)` | A silo receives the push, marks its snapshot out of date and acknowledges | `TenantPolicyEpochSubscription.OnEpochAdvancedAsync`. | Yes: `TenantPolicyEpochSubscriptionTests.OnEpochAdvancedAsync_marks_the_snapshot_out_of_date_before_acknowledging`. |
| `LeaseAges(t)` | A lease passes its deadline, then its deadline plus the margin | The ledger's deadline filter and the silo's lease clock. | Yes: `TenantPolicyEpochLedgerMarginTests.AdvanceAsync_does_not_complete_while_a_slower_silo_still_trusts_its_lease` and `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Lease_lapses_exactly_one_lease_after_the_request_was_sent`. |
| `CaptureAges(t)` | A captured deadline the silo renewed past passes | The ledger's wait on the captured deadline. | Yes: `TenantPolicyEpochLedgerTests.AdvanceAsync_keeps_a_subscriber_that_renewed_while_its_lease_was_waited_out`. |
| `CompleteWrite` | Every captured silo acknowledged or lapsed and the grace is over: the write returns | `TenantPolicyEpochLedger.AdvanceAsync` completes and `CompiledTenantPolicySnapshotMaintainer.OnMutationAsync` returns. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.OnMutationAsync_registry_write_publishes_one_advance_and_completes_after_it` and `TenantPolicyEpochLedgerTests.AdvanceAsync_waits_out_the_lease_of_a_subscriber_that_does_not_acknowledge`. |
| `PublishFails` | The advance faults; the durable write returns and a re-publish is owed | `CompiledTenantPolicySnapshotMaintainer.OnMutationAsync` swallows the failure and calls `CompiledTenantPolicySnapshotMaintainer.EnsureAdvanceRetry`. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it`. |
| `RetryAdvance` | The background loop re-publishes the owed advance | `CompiledTenantPolicySnapshotMaintainer.EnsureAdvanceRetry`. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it`. |
| `RetrySucceeds` | The re-publish completes and clears the debt | `CompiledTenantPolicySnapshotMaintainer.EnsureAdvanceRetry` after `TenantPolicyEpochLedger.AdvanceAsync`. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it`. |
| `RebuildStart(s)` | A scheduled rebuild scans the registry | `CompiledTenantPolicySnapshotMaintainer.RebuildNowAsync`. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.ObserveEpoch_newer_epoch_revokes_authority_until_the_rebuild_lands`. |
| `RebuildFinish(s)` | The rebuild publishes, current only if nothing invalidated it meanwhile | `CompiledTenantPolicySnapshotMaintainer.RebuildNowAsync` and `TenantSnapshotCurrency.IsCurrent`. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Epoch_observed_while_a_scan_runs_leaves_the_result_non_authoritative_until_the_follow_up`. |
| `Renew(s)` | The silo renews; the lease carries the current epoch | `TenantPolicyEpochGrain.LeaseAsync` and `CompiledTenantPolicySnapshotMaintainer.ApplyLease`. | Yes: `TenantPolicyCrossSiloCurrencyTests.Unreachable_peer_that_renews_during_the_wait_learns_the_new_epoch`. |
| `Decide(s, r)` | Trust an authoritative snapshot, else confirm against the registry, else deny | `TenantGateEnforcer.EnforceAsync`. | Yes: `TenantGateEnforcerSnapshotLagTests.EnforceAsync_registry_failure_while_the_snapshot_is_not_authoritative_denies` and `TenantGateEnforcerSnapshotLagTests.EnforceAsync_owned_tree_while_the_snapshot_is_not_authoritative_confirms_membership_with_one_registry_read`. |
| `SiloCrash(s)` | A silo process is lost with its snapshot and any write in flight | Loss of the silo process. | Yes: `TenantPolicyCrossSiloCurrencyTests.Cold_silo_whose_warm_up_fails_denies_tenant_owned_access`. |
| `DeclareDead(s)` | Membership declares the crashed silo dead; survivors invalidate | `TenantPolicyEpochSubscription` membership watch calls `CompiledTenantPolicySnapshotMaintainer.InvalidateClusterView`. | Yes: `TenantPolicyEpochSubscriptionTests.A_silo_declared_dead_invalidates_the_snapshot_but_the_baseline_does_not` and `CompiledTenantPolicySnapshotMaintainerCurrencyTests.InvalidateClusterView_revokes_authority_and_rebuilds`. |
| `Recover(s)` | A fresh process starts with no snapshot and no observed epoch | `TenantPolicyEpochSubscription.StartAsync`. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Built_but_never_leased_snapshot_is_not_authoritative` and `TenantPolicyEpochSubscriptionTests.StartAsync_leases_through_an_observer_reference_and_makes_the_snapshot_authoritative`. |
| `EpochRestart` | The epoch grain re-activates with a fresh incarnation and an empty lease table | `TenantPolicyEpochGrain` activation draws a new incarnation for `TenantPolicyEpochLedger`. | Yes: `TenantPolicyEpochTests.Supersedes_any_version_of_a_different_incarnation` and `TenantPolicyCrossSiloCurrencyTests.Restarted_epoch_grain_invalidates_a_silo_that_renews_against_it`. |
| `GraceElapse` | One lease plus the margin passes after activation | The ledger's grace wait. | Yes: `TenantPolicyEpochLedgerTests.AdvanceAsync_on_a_fresh_incarnation_waits_out_one_lease_plus_margin` and `TenantPolicyCrossSiloCurrencyTests.Restarted_epoch_grain_holds_the_write_open_until_every_old_lease_has_lapsed`. |
| `ReleaseGrace` | The grace ends early once every live silo has leased afresh | `TenantPolicyEpochGrain.EveryLiveSiloHasLeased` then `TenantPolicyEpochLedger.ReleaseGrace`. | Yes: `TenantSnapshotCurrencyTests.ReleaseGrace_lets_a_fresh_incarnations_advance_complete_before_one_lease` and `TenantSnapshotCurrencyTests.EveryLiveSiloHasLeased_is_true_only_when_every_non_dead_silo_leased`. |
| `Stutter` | Terminal self-loop so a finished run is not reported as a deadlock | Not applicable: a modelling device with no production step. | Not applicable |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `NoStaleAuthority` | Once a write has returned after its publication completed, no silo holds a snapshot that misses it authoritative. | Yes: `TenantPolicyCrossSiloCurrencyTests.Every_peer_silo_is_told_before_the_write_completes` and `TenantPolicyCrossSiloCurrencyTests.Unreachable_peer_holds_the_write_open_until_its_lease_lapses_and_then_denies`. |
| `WriterReadsOwnWrite` | The committing silo never trusts a snapshot that misses its own write. | Yes: `TenantPolicyCrossSiloCurrencyTests.Committing_silo_is_not_authoritative_while_its_advance_is_in_flight`. |
| `PublicationWindowTracked` | A write returns ahead of its coverage only through a failed publication still owed a re-publish, or a crashed writer. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it`. |
| `UnconfirmableDenies` | A decision trusts only an authoritative snapshot, confirms only with a reachable registry, and otherwise denies. | Yes: `TenantGateEnforcerSnapshotLagTests.EnforceAsync_registry_failure_while_the_snapshot_is_not_authoritative_denies`. |
| `EpochMonotonic` | Epochs never repeat: versions grow within an incarnation and a new activation never reuses one. | Yes: `TenantPolicyEpochLedgerTests.AdvanceAsync_successive_advances_produce_strictly_increasing_versions` and `TenantSnapshotCurrencyTests.Observe_newer_version_or_incarnation_supersedes_and_older_does_not`. |
| `EveryCommitEventuallyCovered` | Every committed write is eventually published to every silo, or covered by its crashed writer being declared dead. | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it` and `TenantPolicyCrossSiloCurrencyTests.Peer_silo_converges_and_regains_authority_once_its_rebuild_lands`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Defect mutations whose fixes have landed

| Mutation | Behaviour it reproduced | Issue | Detectors |
|----------|-------------------------|-------|-----------|
| `StartAdvanceDropsMarginLease` | The ledger captured only leases inside their recorded deadline, so an advance neither pushed to nor waited out a silo whose slower clock still trusted its lease. | #4806 | `TenantPolicyEpochLedgerMarginTests.AdvanceAsync_pushes_to_a_lease_past_its_deadline_but_inside_the_margin` and `TenantPolicyEpochLedgerMarginTests.AdvanceAsync_does_not_complete_while_a_slower_silo_still_trusts_its_lease`, proven red without the margin in the live filter. |

## Deliberate abstraction gaps

- **Bounded windows, not immediate revocation.** A write whose publication fails, and a
  writer that crashes after committing, leave peers trusting the old snapshot until the
  background re-publish lands or membership declares the writer dead. Decisions already
  taken are never revoked.
- **Clock drift.** Time is abstract. The grain's lease table and the silo's lease clock
  are merged into one value per silo, so a silo whose clock runs fast and lapses early is
  not modelled (it only reduces authority). The guarantee assumes drift within the margin.
- **Serialised writes, one write.** The bounds are one write, two silos and, per variant
  configuration, one crash, one re-activation or one failed publication; all three
  interleave on one silo in `TenantIsolation.Faults.cfg`.
- **Atomic steps.** Lease grant and application, the membership declaration and the
  rebuild's publish are single steps; a failing rebuild, silos not yet joined and the
  alias path are not modelled.
- **Boolean epoch knowledge.** Epochs only grow, so a silo's observation is modelled as
  whether it has seen the current epoch.
- **Crashed silo leases.** A crashed silo's captured lease is released at the crash; in
  production its recorded deadline passes later, which only lengthens the wait.
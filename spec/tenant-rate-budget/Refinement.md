# Refinement note: TenantRateBudget to production

This is a documented correspondence, not a machine-checked refinement proof.
The source is `src/lattice.tenancy/`; the detector fixtures directly exercise
its real coordinator, apportionment functions, limiter and token buckets.

## Variable mapping

| Spec variable | Role | Code counterpart |
|---------------|------|------------------|
| `live` | Membership input, not a replicated rate-lease ledger | `ILiveSiloCountProvider.GetLiveSiloCountAsync`, consumed by `TenantRateBudgetCoordinator.RunLeaseCycleAsync`. |
| `rate`, `changed` | A configured rate and the one-change exploration bound | `ITenantRateProvider.GetConfiguredRatesAsync` supplies the OpsPerSecond positional record member, which `TenantRateBudgetCoordinator.RunLeaseCycleAsync` reads from each yielded spec. The change flag is a modeling bound. |
| `grant` | One cycle's rate, count, admitted demand and allocated share captured before a delayed response | Locals inside `TenantRateBudgetCoordinator.RunLeaseCycleAsync`, calculated by `TenantBudgetApportionment.StaticEvenShare` or `TenantBudgetApportionment.DemandProportionalShare`. No durable grant record exists. |
| `phase`, `cycles`, `cancelled` | Outstanding asynchronous cycle, exploration bound and canceled response | `TenantRateBudgetCoordinator.RunLeaseCycleAsync` and its cancellation token, driven by `TenantRateBudgetCoordinatorHostedService.StartAsync`. |
| `age`, `now` | Cadence and monotonic timestamp | `TenantRateBudgetCoordinatorHostedService.ResolveCycleTimeout` and the timer in its loop; `SiloLocalTenantRateLimiter.TryAcquire` reads the shared time provider. |
| `configured`, `emission`, `tolerance` | Presence and fixed GCRA parameters of each silo's current bucket | `SiloLocalTenantRateLimiter.Configure`, `TenantTokenBucket.EmissionIntervalTicks`, `TenantTokenBucket.BurstToleranceTicks`. |
| `tat`, `demand` | The theoretical arrival time and admitted operations since the last reset | `TenantTokenBucket.TryAcquire` and `TenantTokenBucket.ReadAndResetDemand`. |
| `joined`, `left`, `restarted` | Bounded environment interventions | No production fields; membership changes the count input, and coordinator recreation reuses the silo-local limiter. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Join` | A second silo becomes available with a fresh local limiter; future snapshots include it | Membership input to `ILiveSiloCountProvider.GetLiveSiloCountAsync`; `SiloLocalTenantRateLimiter` initially contains no configured buckets. | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_membership_change_is_observed_only_on_the_next_cycle` and `SiloLocalTenantRateLimiterTests.TryAcquire_admits_a_tenant_with_no_configured_bucket`. |
| `Leave(s)` | A silo stops serving, its pending cycle terminates, and future snapshots exclude it | Membership input to `ILiveSiloCountProvider.GetLiveSiloCountAsync`; `TenantRateBudgetCoordinatorHostedService.StopAsync` cancels its loop. The model keeps the departed process's heap but never schedules its operations again. | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_membership_change_is_observed_only_on_the_next_cycle` and `TenantRateBudgetCoordinatorHostedServiceTests.A_cycle_that_never_completes_is_cancelled_and_the_loop_survives`. |
| `ChangeBudget(newRate)` | The provider changes independently of installed buckets | `ITenantRateProvider.GetConfiguredRatesAsync` is read only by `TenantRateBudgetCoordinator.RunLeaseCycleAsync`. | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_budget_change_is_deferred_until_refresh`. |
| `BeginLease(s,total)` | Snapshot membership, read/reset demand, and allocate using fallback or aggregate | `TenantRateBudgetCoordinator.RunLeaseCycleAsync`, `SiloLocalTenantRateLimiter.ReadAndResetDemand`, `TenantBudgetApportionment.DemandProportionalShare`. | Yes: `TenantRateBudgetCoordinatorTests.RunLeaseCycleAsync_demand_strategy_engages_when_a_cluster_total_is_supplied`, `TenantRateBudgetRefinementTests.Apportionment_matches_every_bounded_model_allocation_input` and `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_membership_change_is_observed_only_on_the_next_cycle`. |
| `Cancel(s)` | The cycle token is canceled without removing the enforcing bucket | The cancellation fences in `TenantRateBudgetCoordinator.RunLeaseCycleAsync`; timeout tokens originate in `TenantRateBudgetCoordinatorHostedService.ResolveCycleTimeout`. | Yes: `TenantRateBudgetCancellationTests.RunLeaseCycleAsync_cancelled_enumeration_does_not_prune_existing_buckets` and `TenantRateBudgetCancellationTests.RunLeaseCycleAsync_cancelled_delayed_exchange_does_not_replace_the_bucket`. |
| `Deliver(s)` | A delayed result installs changed parameters, preserves identical parameters, or is discarded after cancellation | `TenantRateBudgetCoordinator.RunLeaseCycleAsync` calls `SiloLocalTenantRateLimiter.Configure` only after its cancellation check. | Yes: `TenantRateBudgetCancellationTests.RunLeaseCycleAsync_cancelled_delayed_exchange_does_not_replace_the_bucket`, `SiloLocalTenantRateLimiterTests.Configure_preserves_bucket_state_when_the_parameters_are_unchanged` and `SiloLocalTenantRateLimiterTests.Configure_replaces_the_bucket_when_the_parameters_change`. |
| `Restart(s)` | Recreate a coordinator on the same silo, abandoning its pending cycle but retaining its singleton limiter | `TenantRateBudgetCoordinator` accepts the existing `SiloLocalTenantRateLimiter`; its constructor does not reset it. | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_recreated_coordinator_does_not_reset_unchanged_bucket`. |
| `Tick` | Advance monotonic time and reach the next cadence without expiring a bucket | `SiloLocalTenantRateLimiter.TryAcquire` reads time; `TenantRateBudgetCoordinatorHostedService.StartAsync` drives refresh. | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_failed_refresh_after_cadence_retains_the_previous_bucket` and `TenantTokenBucketTests.TryAcquire_replenishes_one_token_per_interval_over_logical_time`. |
| `Acquire(s)` | Successful CAS advances TAT and increments admitted demand | `TenantTokenBucket.TryAcquire`. | Yes: `TenantTokenBucketTests.TryAcquire_admits_a_bounded_burst_then_throttles`, `TenantTokenBucketTests.TryAcquire_does_not_accrue_credit_beyond_the_burst_while_idle` and `TenantTokenBucketTests.TryAcquire_is_correct_under_concurrent_callers`. |
| `Reject(s)` | The GCRA debt comparison refuses the operation without changing TAT or demand | `TenantTokenBucket.TryAcquire` rejects before its compare-exchange and demand increment. | Yes: `TenantTokenBucketTests.ReadAndResetDemand_does_not_count_refused_ops` and `TenantTokenBucketTests.TryAcquire_with_no_burst_admits_one_then_refuses_until_a_full_interval_elapses`. |
| `Stutter` | Permit arbitrary environment inactivity, including permanent idleness after exploration bounds | Modeling device, no production counterpart. | Not applicable. |

## Property mapping

| Spec property | Code-level claim | Detector |
|---------------|------------------|----------|
| `ShareBound` | Each floored allocation is positive and no larger than its captured positive rate; static fallback divides its captured count | Yes: `TenantRateBudgetRefinementTests.Apportionment_matches_every_bounded_model_allocation_input`, `TenantRateBudgetCoordinatorTests.RunLeaseCycleAsync_floors_a_sub_unit_share_to_one_op_per_second` and `TenantBudgetApportionmentTests.DemandProportionalShare_is_capped_at_the_cluster_rate`. |
| `DepartedSiloNotEnforcing` | A departure removes the silo from future count inputs; it is no longer an operation source | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_membership_change_is_observed_only_on_the_next_cycle`. |
| `CancelledGrantIgnored` | A response returned after observed cancellation does not replace parameters or reset debt/demand | Yes: `TenantRateBudgetCancellationTests.RunLeaseCycleAsync_cancelled_delayed_exchange_does_not_replace_the_bucket`. |
| `RefreshPreservesDebt` | Identical parameters preserve the bucket's arrival time | Yes: `SiloLocalTenantRateLimiterTests.Configure_preserves_bucket_state_when_the_parameters_are_unchanged`. |
| `LocalAdmissionBound` | One admitted operation advances from max(TAT, now) by one interval; admission respects burst tolerance and counts demand once | Yes: `TenantTokenBucketTests.TryAcquire_is_correct_under_concurrent_callers`, `TenantTokenBucketTests.TryAcquire_does_not_accrue_credit_beyond_the_burst_while_idle` and `TenantTokenBucketTests.ReadAndResetDemand_counts_admitted_ops_and_resets`. |
| `RejectedDemandUnchanged` | Rejected operations cannot inflate demand or consume a later admission | Yes: `TenantTokenBucketTests.ReadAndResetDemand_does_not_count_refused_ops` and `TenantTokenBucketTests.TryAcquire_with_no_burst_admits_one_then_refuses_until_a_full_interval_elapses`. |
| `CadenceRetainsEnforcement` | Cadence expiry alone does not make a configured tenant unthrottled; failed refresh keeps its prior parameters | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_failed_refresh_after_cadence_retains_the_previous_bucket`. |
| `RestartRetainsEnforcement` | Recreating the coordinator on an unchanged silo does not mint another burst | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_recreated_coordinator_does_not_reset_unchanged_bucket`. |
| `JoinStartsUnconfigured` | A joined silo has no installed bucket until bootstrap, and the count input incorporates it | Yes: `SiloLocalTenantRateLimiterTests.TryAcquire_admits_a_tenant_with_no_configured_bucket` and `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_membership_change_is_observed_only_on_the_next_cycle`. |
| `LeaseSnapshotExact` | A cycle allocates from its observed count and admitted-demand reset, not from an invented count | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_membership_change_is_observed_only_on_the_next_cycle` and `TenantRateBudgetCoordinatorTests.RunLeaseCycleAsync_demand_strategy_engages_when_a_cluster_total_is_supplied`. |
| `CancelFencesGrant` | Cancellation prevents bucket removal or application of a late result | Yes: `TenantRateBudgetCancellationTests.RunLeaseCycleAsync_cancelled_enumeration_does_not_prune_existing_buckets` and `TenantRateBudgetCancellationTests.RunLeaseCycleAsync_cancelled_delayed_exchange_does_not_replace_the_bucket`. |
| `BudgetChangeDeferred` | Changing the source spec does not directly modify local debt; a completed refresh applies it | Yes: `TenantRateBudgetRefinementTests.RunLeaseCycleAsync_budget_change_is_deferred_until_refresh`. |
| `LeaseSettles` | Once the collaborator completes or observes cancellation, the cycle completes or throws rather than remaining pending | Yes: `TenantRateBudgetCoordinatorHostedServiceTests.A_cycle_that_never_completes_is_cancelled_and_the_loop_survives`, `TenantRateBudgetCoordinatorTests.RunLeaseCycleAsync_static_even_configures_a_bucket_at_the_apportioned_share` and `TenantRateBudgetCancellationTests.RunLeaseCycleAsync_cancelled_delayed_exchange_does_not_replace_the_bucket`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-state exploration domains, including cycle/time/fault bounds, not a production behavior. |

## Scope and fairness

- `live` is an **environment membership input**, not a model of Orleans failure
  detection. Join/departure detectors inject changed counts into the real
  coordinator and check their allocation effects. No claim is made about
  discovery latency, transport failure detection or shutting down an
  uncooperative process. A dead process's heap need not be cleared.
- A restart here is coordinator recreation **within the same silo**, not a
  whole-silo crash. Whole-silo replacement starts a new limiter, like `Join`.
- The default aggregate is absent. `Share` corresponds exactly to static
  fallback and to demand strategy with reserve zero, a supported setting. The
  model explores supplied totals that may disagree with local demand; the
  production cap is retained. Other reserve fractions and the aggregate
  implementation are outside this module.
- `WF_vars(Deliver(s))` means a continuously pending, still-live silo's
  collaborator eventually returns or throws (including cooperative
  cancellation). This is not a proof of that environmental premise. A
  collaborator that never completes **and ignores cancellation** may occupy
  the production loop forever; the code does not use an independent
  `WaitAsync` timeout. `Cancel` alone deliberately does not complete the cycle.
  The liveness mutation changes `Deliver` while leaving this fairness intact.
- A cycle snapshots one tenant. Across multiple tenants, earlier synchronous
  updates may remain if a later tenant fails; the model does not promise
  transactional all-tenant rollback. Demand already read/reset by a failed
  exchange may be lost. These are not claims that the model establishes.
- Changed parameters deliberately install a fresh bucket with a new burst.
  The unchanged-generation GCRA bound is not claimed across those resets.
  CAS interleavings are abstracted to linearized acquisitions; the real
  concurrent-caller detector covers the compare-exchange implementation.
- The clock and arithmetic instance are finite and non-overflowing. Timestamp
  rollover, extreme burst percentages, unlimited-rate tenants, cleared rates,
  exponential retry timing and general timestamp quantization are not modeled.
  The README states the honest admission bounds instead of an exact cluster
  ceiling.

## Verification evidence

The final local TLC run checked the complete base graph with 383,624 distinct
states and no violations. Each anchored mutant was generated from that same
source, selected only its target property, and reported that target without a
deadlock. No mutant changes fairness or disables the deadlock check.
The temporal witness is scheduler-dependent, so its early-found state count is
not a stable model census.

| Mutation | Target | Witness-run distinct states |
|----------|--------|-----------------------------|
| AcquireIgnoresDebt | LocalAdmissionBound | 1,064 |
| BudgetChangeResetsDebt | BudgetChangeDeferred | 51 |
| CadenceExpiresBucket | CadenceRetainsEnforcement | 8 |
| CancelDropsBucket | CancelFencesGrant | 31 |
| CancelledGrantInstalled | CancelledGrantIgnored | 207 |
| DepartedSiloStillCounted | DepartedSiloNotEnforcing | 3 |
| JoinNotPublished | JoinStartsUnconfigured | 2 |
| LeaseNeverSettles | LeaseSettles | 41,312 |
| LeaseUsesWrongSiloCount | LeaseSnapshotExact | 5 |
| RefreshResetsDebt | RefreshPreservesDebt | 187 |
| RejectCountsDemand | RejectedDemandUnchanged | 268 |
| RestartMintsBurst | RestartRetainsEnforcement | 53 |
| ShareBoundOverallocated | ShareBound | 5 |
| TypeOKUndeclaredPhase | TypeOK | 5 |

The production cancellation regression run discovered both tests and failed
both before the cancellation fences were applied: no exception was raised
after the delayed response or after the canceled enumeration. Other cited
fixtures are direct production-seam detectors; TLC mutation kills establish
model non-vacuity, not a mechanical C# refinement proof.

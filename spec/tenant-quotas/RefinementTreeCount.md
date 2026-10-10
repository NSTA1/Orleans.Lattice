# Refinement note: TenantTreeCount

This maps the authoritative quota check and its documented check/register race.
It does not claim a reservation, a strict count ceiling, or an end-to-end
refinement of registry replication.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `limit`, `burst` | Declared cap and floored burst headroom | `TenantQuotas.MaxTreeCount`, `TenantQuotas.BurstPercent`, `TenantQuotaEvaluator.AdmitTreeCreate` |
| `phase`, `checked` | Outstanding creates and the count each decision saw | `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` |
| `owned` | Authoritatively registered trees | `LatticeRegistryGrain.RegisterAsync` and `LatticeRegistryGrain.GetAllTreeIdsAsync` supply the count callback |
| `observed`, `allow` | Count and result of a later fresh create-admission check | `TenantQuotaEvaluator.AdmitTreeCreate` |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `CheckCreate` | Read authoritative count, charge the proposed tree, allow/refuse | `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` and `TenantQuotaEvaluator.AdmitTreeCreate` | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_enforces_the_ceiling_for_a_cold_unmetered_tenant` and `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_refuses_when_the_live_count_is_at_the_ceiling`. |
| `Register` | A separately scheduled admitted create joins the owned count | `LatticeRegistryGrain.RegisterAsync` after `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` | Yes: `LatticeRegistryGrainTests.RegisterAsync_sets_key_in_registry_tree` detects an omitted registration write; `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses` separately detects the check/register split. |
| `Probe` | Later creates read the current count rather than a cached decision | `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `RegistrationAccounted` | A completed registration contributes to authoritative count | Yes: `LatticeRegistryGrainTests.RegisterAsync_sets_key_in_registry_tree` and `LatticeRegistryGrainTests.GetAllTreeIdsAsync_returns_keys_from_registry` reach the production registry write/read seams. |
| `TruthfulCreate` | The observed count plus this tree is checked against floored burst headroom | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_refuses_when_the_live_count_is_at_the_ceiling` and `TenantQuotaEvaluatorTests.Burst_headroom_admits_over_a_small_ceiling_that_is_not_a_multiple_of_100`. |
| `ProbeTruth` | Every later check applies the same count-plus-one predicate | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses`. |
| `AllCreatesFinish` | Given successful available count/store replies, create check and registration complete | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_admits_below_the_ceiling` exercises successful create admission; `LatticeRegistryGrainTests.RegisterAsync_available_registry_replies_complete_within_bound` invokes real registration with completed registry replies and a three-second `WaitAsync` bound, detecting stranded registration directly without a runner hang proxy. |
| `EventualCreateRefusal` | A later create refuses after earlier admitted requests have filled the cap | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses` uses real production admission/evaluator with the stated controlled-count assumption. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Production completion detector evidence

`LatticeRegistryGrainTests.RegisterAsync_available_registry_replies_complete_within_bound`
passed against the clean production grain in Release. It supplies completed
registry replies, bounds the actual registration task with a three-second
`WaitAsync`, and checks the registration write. The coordinating parent measured
two isolated production perturbations: omitting `Registry.SetAsync` failed with
`ReceivedCallsException` (expected one call, actual zero); replacing its await
with a never-completing task failed with `TimeoutException` at the detector's
three-second bound. Each arm restored and verified the exact source bytes in
`finally`. The post-restoration rebuilt control was still running when this
evidence was recorded; its earlier clean Release control passed.

All action and property rows have mutation coverage. Repeated real admission
probes keep every mutant deadlock-free. Completion fairness represents
availability; it is not evidence that a disconnected registry eventually
answers. The model has no strict `owned <= limit` property.

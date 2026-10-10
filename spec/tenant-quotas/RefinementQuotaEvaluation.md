# Refinement note: TenantQuotaEvaluation

The pure evaluator is synchronous and dependency-free. This is a finite
arithmetic model and documented mapping, not a compiler refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `quotas`, `burst` | Independent nullable caps and burst percentage | `TenantQuotas` |
| `usage` | Bytes, keys, memory and sampled owned-tree count | `LocalUsageSample` |
| `result` | Admit or first breached dimension | `TenantQuotaEvaluator.Admit` and `LatticeQuotaExceededException.Dimension` |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Evaluate` | Compare all bounded dimensions in stable order | `TenantQuotaEvaluator.Admit` | Yes: `TenantQuotaEvaluatorTests.The_first_breached_dimension_is_reported_in_stable_order`, `TenantQuotaEvaluatorTests.Usage_over_the_bytes_ceiling_refuses_with_the_tenant_and_dimension`, `TenantQuotaEvaluatorTests.Usage_over_the_keys_ceiling_refuses_on_the_keys_dimension`, `TenantQuotaEvaluatorTests.Usage_over_the_memory_ceiling_refuses_on_the_memory_dimension`, and `TenantQuotaEvaluatorTests.Usage_over_the_tree_count_ceiling_refuses_on_the_trees_dimension`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `TruthfulEvaluation` | Independent null ceilings, inclusive cap, deterministic first breach, floored burst | Yes: `TenantQuotaEvaluatorTests.A_null_dimension_ceiling_is_never_breached`, `TenantQuotaEvaluatorTests.Usage_at_the_ceiling_admits`, `TenantQuotaEvaluatorTests.The_first_breached_dimension_is_reported_in_stable_order`, and `TenantQuotaEvaluatorTests.Burst_headroom_rounds_a_fractional_allowance_down_without_losing_it`. |
| `EventuallyEvaluated` | Synchronous evaluation returns or throws, never waits for I/O | Yes: `TenantQuotaEvaluatorTests.Usage_at_the_ceiling_admits` and `TenantQuotaEvaluatorTests.Usage_over_the_bytes_ceiling_refuses_with_the_tenant_and_dimension` directly execute the production evaluator. Model weak fairness selects this always-available synchronous action. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Model-domain predicate. |

All properties and the evaluation action have falsifying mutations. The
sentinel `Unbounded` is not a real quota ceiling: its only role is representing
null within a homogeneous TLC value domain. Integer storage saturation is a
finite analogue of the production UInt128 burst intermediate clamped to
long.MaxValue. Exception fields and cancellation are not modelled here.

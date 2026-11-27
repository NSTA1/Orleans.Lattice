# Refinement note: TenantTreeCount

This maps the authoritative quota check, its documented check/register race,
and the tree-admin alias seam that can add a registry row. It does not claim a
reservation, a strict count ceiling, or an end-to-end refinement of registry
replication.

## Alias admission decision

`MaxTreeCount` bounds the tenant's registry rows, and the count it is checked
against is the number of rows under the tenant's prefix. An alias therefore
counts exactly when it adds a row:

- **An alias onto a new logical id counts, like a create.** When the logical id
  has no registry row, the alias write creates one, so `SetTreeAliasAsync` must
  pass the same tree-create admission `CreateTreeAsync` passes, for the composed
  logical id, after both authorizations and before the registry write.
- **A re-point of an existing logical id does not count.** When the logical id
  already has a row, the alias rewrites that row and the count does not change,
  so no admission is consulted and a tenant at its cap can still move an alias.
- **One counter.** Create and alias admission read the same authoritative count
  (`LatticeRegistryGrain.GetAllTreeIdsAsync` under the tenant prefix) and charge
  the same one-tree increment. Neither reserves, so concurrent creates and aliases
  race exactly as concurrent creates do: a tenant can overshoot by the number of
  admissions in flight, and the next fresh check refuses.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `limit`, `burst` | Declared cap and floored burst headroom | `TenantQuotas.MaxTreeCount`, `TenantQuotas.BurstPercent`, `TenantQuotaEvaluator.AdmitTreeCreate` |
| `phase`, `checked`, `decided` | Outstanding creates and aliases, the count each decision saw and the cap it read | `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` |
| `owned` | Authoritatively registered trees, including logical alias rows | `LatticeRegistryGrain.RegisterAsync` (or `RegisterAndGetEntryAsync`, which shares its registration write), `LatticeRegistryGrain.SwapAliasAsync` for a new logical id, and `LatticeRegistryGrain.GetAllTreeIdsAsync` supply the count callback |
| `repoint` | Outcome of an alias on an already registered logical id | `LatticeTreeAdmin.SetTreeAliasAsync` |
| `quotaSet` | Whether the single operator quota change has happened | `LatticeTenantAdmin.SetTenantQuotasAsync` |
| `observed`, `probed`, `allow` | Count, cap and result of a later fresh create-admission check | `TenantQuotaEvaluator.AdmitTreeCreate` |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `CheckCreate` | Read authoritative count, charge the proposed tree, allow/refuse | `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` and `TenantQuotaEvaluator.AdmitTreeCreate` | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_enforces_the_ceiling_for_a_cold_unmetered_tenant` and `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_refuses_when_the_live_count_is_at_the_ceiling`. |
| `CheckAlias` | An alias onto a logical id with no registry row reads the same count, charges the new id, allow/refuse | `LatticeTreeAdmin.SetTreeAliasAsync`, through `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` | Yes: `LatticeTreeAdminTenantQuotaTests.SetTreeAliasAsync_new_logical_id_refuses_when_the_counted_trees_reach_the_ceiling` and `LatticeTreeAdminTenantQuotaTests.SetTreeAliasAsync_new_logical_id_is_charged_for_the_composed_logical_id`; `LatticeTreeAdminTenantQuotaTests.SetTreeAliasAsync_when_authorization_denies_never_consults_admission` pins the authorize-before-charge order. |
| `Register` | A separately scheduled admitted create or alias joins the owned count | `LatticeRegistryGrain.RegisterAsync`, or `LatticeRegistryGrain.RegisterAndGetEntryAsync` (the tree-admin create path; same absent-row check and registration write in one registry turn), after `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync`; `LatticeRegistryGrain.SwapAliasAsync` writes the logical row of a new alias | Yes: `LatticeRegistryGrainTests.RegisterAsync_sets_key_in_registry_tree` and `LatticeRegistryGrainTests.RegisterAndGetEntryAsync_registers_a_new_tree_and_returns_the_seeded_entry` detect an omitted registration write; `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses` separately detects the check/register split. |
| `Purge` | A purged tree's registry row is removed, so it leaves the count | `TreeDeletionGrain`, through `LatticeRegistryGrain.UnregisterAsync` | Yes: `LatticeRegistryGrainTests.UnregisterAsync_deletes_key` detects a purge that leaves the row; `LatticeTreeAdminTenantQuotaTests.CreateTreeAsync_counts_only_the_tenants_own_trees_from_the_registry` shows the count is the live row set. |
| `Repoint` | An alias on a logical id that already has a row rewrites it without admission | `LatticeTreeAdmin.SetTreeAliasAsync`, `AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync` | Yes: `LatticeTreeAdminTenantQuotaTests.SetTreeAliasAsync_repointing_an_existing_logical_id_is_not_charged`. |
| `SetQuota` | An operator changes the tenant's cap; later checks read the new cap | `LatticeTenantAdmin.SetTenantQuotasAsync`, read back by `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_reads_the_current_quota_on_every_check`. |
| `Probe` | Later creates read the current count rather than a cached decision | `LatticeTenantAdmissionController.IsTreeCreateAdmittedAsync` | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `RegistrationAccounted` | A completed registration contributes to authoritative count, and a purge removes it | Yes: `LatticeRegistryGrainTests.RegisterAsync_sets_key_in_registry_tree`, `LatticeRegistryGrainTests.UnregisterAsync_deletes_key` and `LatticeRegistryGrainTests.GetAllTreeIdsAsync_returns_keys_from_registry` reach the production registry write/read seams. |
| `TruthfulCreate` | The observed count plus this tree is checked against floored burst headroom of the cap current at the check | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_refuses_when_the_live_count_is_at_the_ceiling` and `TenantQuotaEvaluatorTests.Burst_headroom_admits_over_a_small_ceiling_that_is_not_a_multiple_of_100`. |
| `TruthfulAlias` | An alias onto a new logical id is checked against the same count and headroom as a create | Yes: `LatticeTreeAdminTenantQuotaTests.SetTreeAliasAsync_new_logical_id_refuses_when_the_counted_trees_reach_the_ceiling` checks the alias against the same tenant count a create reads. |
| `RepointUncharged` | Re-pointing an existing logical id is never refused for the tree quota | Yes: `LatticeTreeAdminTenantQuotaTests.SetTreeAliasAsync_repointing_an_existing_logical_id_is_not_charged`. |
| `ProbeTruth` | Every later check applies the same count-plus-one predicate to the cap it read | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses` and `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_reads_the_current_quota_on_every_check`. |
| `AllCreatesFinish` | Given successful available count/store replies, create and alias checks and registration complete | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_admits_below_the_ceiling` exercises successful create admission; `LatticeRegistryGrainTests.RegisterAsync_available_registry_replies_complete_within_bound` invokes real registration with completed registry replies and a three-second `WaitAsync` bound, detecting stranded registration directly without a runner hang proxy. |
| `EventualCreateRefusal` | Once the registered count stays at or above the cap, later creates refuse | Yes: `LatticeTenantAdmissionControllerTests.IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses` uses real production admission/evaluator with the stated controlled-count assumption. |

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

All action and property rows have mutation coverage. `AliasSkipsAdmission` is
the facade's alias path before tree-count admission was added to it: it admits an
alias onto a new logical id with no count check, and `TruthfulAlias` refuses it. Repeated real admission probes
keep every mutant deadlock-free. Completion fairness represents availability; it
is not evidence that a disconnected registry eventually answers. The model has
no strict `owned <= limit` property. Purges and the quota change carry no
fairness: they may happen at any point or never.

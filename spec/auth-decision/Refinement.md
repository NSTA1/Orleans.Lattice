# Refinement note: AuthDecision

This maps the authorization decision - the policy store's writers, the compiled
snapshot, `PolicyEvaluator` and the `PolicyAccessGate` that wraps it, the
capability authorizers, and the cross-tree batch's authorization - to the
abstract decision `AuthDecision` checks. It is a design check over a bounded
instance, not a machine-checked refinement proof: each row names the production
seam a model step stands for and the tests that fail when that seam's guard is
removed.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `rules` | The rules the policy tree holds, operator and tenant layers together; the compiled snapshot is derived from it on every decision | `LatticeAuthorizationPolicyStore`, `CompiledPolicy.Compile`, `CompiledTenantPartition.Build` |
| `last` | The most recent write, delete or decision, with the deciding layer and rule (the explain trace) | `PolicyMatch`, `LatticeAccessDecision` |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `OperatorUpsert` | A direct (operator) rule write; a `tenant:` rule id off system origin is refused before anything is read | `LatticeAuthorizationPolicyStore.PutRuleAsync` | Yes: `LatticeAuthorizationPolicyStoreTenantRuleTests.PutRuleAsync_refuses_a_tenant_rule_id_off_system_origin_before_reading`. |
| `TenantFacadeUpsert` | The tenant policy facade confines a rule to its tenant's data trees or tenant-wide scope and the data-plane operation mask, then writes it under system origin | `LatticeTenantPolicyAdmin.PutRuleAsync`, `TenantRuleConfinement.CheckTenantRule` | Yes: `LatticeTenantPolicyAdminTests.PutRule_refuses_a_tree_outside_the_tenant_layer`, `LatticeTenantPolicyAdminTests.PutRule_refuses_operations_outside_the_data_plane_mask` and `LatticeAuthorizationPolicyStoreTenantRuleTests.D7_refuses_a_tenant_rule_outside_its_tenants_data_trees`. |
| `SystemUpsert` | A system-origin write (restore, replication apply) reaches the policy tree without the facade's confinement | `LatticeAuthorizationPolicyStore.PutRuleAsync` | Yes: `LatticeAuthorizationPolicyStoreTenantRuleTests.PutRuleAsync_admits_a_confined_tenant_rule_under_system_origin`; the escaped rule it can carry is dropped at compile time, detected by `CompiledTenantPartitionTests.Compile_drops_a_tenant_rule_that_escaped_the_store_confinement`. |
| `RuleDelete` | A rule removal: the operator surface cannot remove a `tenant:` rule, and the tenant facade removes only its own tenant's rules | `LatticeAuthorizationPolicyStore.RemoveRuleAsync`, `LatticeTenantPolicyAdmin.RemoveRuleAsync` | Yes: `LatticeAuthorizationPolicyStoreTenantRuleTests.RemoveRuleAsync_refuses_a_tenant_rule_id_off_system_origin_before_reading` and `LatticeTenantPolicyAdminTests.RemoveRule_never_removes_a_platform_rule_or_another_tenants_rule`. |
| `Evaluate` | A point or whole-scope request: the evaluator's two layers, then the gate's control-plane isolation, which admits a reserved, cluster-wide or tenant-capability request only on an explicit matched allow; the telemetry and app authorizers issue the capability requests over `LatticeScope.ClusterWideTreeId` and `LatticeScope.TenantCapability` | `PolicyAccessGate.AuthorizeAsync`, `PolicyEvaluator.Evaluate`, `TelemetryAccessAuthorizer.AuthorizeClusterTelemetryAsync`, `TelemetryAccessAuthorizer.AuthorizeTenantTelemetryAsync`, `AppInstallAuthorizer.AuthorizeAsync` | Yes: `PolicyAccessGateUnitTests.AuthorizeAsync_reserved_namespace_unmatched_non_bootstrap_is_denied_even_under_default_allow`, `ClusterWideTelemetryGrantTests.Unmatched_telemetry_on_the_sentinel_is_denied_under_default_effect_allow`, `TenantCapabilityTelemetryGrantTests.An_unmatched_caller_is_denied_tenant_telemetry_whatever_the_default_effect` and `TenantAdminCapabilityScopeTests.Under_default_allow_a_non_granted_subject_is_denied_platform_and_tenant`. |
| `EvaluateRange` | A range or scan decision: a key filter composing the layers per key on operator coverage | `PolicyEvaluator.LayeredEffect`, `PolicyEvaluator.Evaluate` | Yes: `TenantLayerRangeFilterPropertyTests.Range_filter_equals_per_key_evaluation_and_the_composition_oracle_for_every_key` and `TenantLayerCollectionTests.Operator_per_key_deny_carves_the_tenant_allow_in_the_filter`. |
| `BootstrapEvaluate` | The root of trust, checked before the engine: a configured bootstrap administrator is allowed everything, an anonymous caller never | `PolicyAccessGate.AuthorizeAsync` | Yes: `PolicyAccessGateUnitTests.AuthorizeAsync_an_anonymous_caller_is_never_a_bootstrap_administrator` and `PolicyAccessGateUnitTests.AuthorizeAsync_bootstrap_administrator_is_allowed_on_any_tree`. |
| `BatchEvaluate` | A cross-tree atomic batch authorizes every key of every leg before any participant is dispatched | `LatticeCrossTreeTxGrain.CommitAsync`, `LatticeAccessGateEnforcement.EnforceManyPointsAsync` | Yes: `LatticeCrossTreeTxGrainTests.CommitAsync_denied_leg_throws_and_dispatches_no_participant` and `LatticeCrossTreeTxGrainTests.CommitAsync_authorizes_every_write_key_of_every_leg_before_dispatch`. |
| `ToolCheck` | The MCP tool gate with no registered authorizer | `McpToolAuthorizationGate.IsAuthorizedAsync` | Yes: `McpToolAuthorizationGateTests.Fails_closed_when_no_authorizer_is_registered`. |
| `Settle` | Returns the model's recorder to idle between steps | None: a modelling device that keeps each decision taken from an idle state. | Not applicable: models no protocol step. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `OperatorFinal` | A matched operator-layer verdict (every rule outside `tenant:`, including `Tree:*` and `app:`), allow or deny, is the decision | Yes: `TenantLayerEvaluationTests.Operator_allow_is_final_over_a_tenant_wide_deny`, `TenantLayerEvaluationTests.Tenant_key_allow_under_an_operator_tree_deny_is_denied` and `TenantLayerEvaluationTests.Operator_app_rule_is_final_over_a_tenant_deny`. |
| `TenantLayerConfined` | The tenant layer runs only when no operator rule matches, only on a tenant data tree `t/{tenant}/{name}`, and only with the tree owner's rules; the compiled partition drops an escaped tenant rule | Yes: `TenantLayerEvaluationTests.Tenant_layer_never_applies_outside_the_tenants_own_data_trees`, `TenantLayerEvaluationTests.Tenant_wide_rule_of_another_tenant_does_not_reach_this_tenants_tree`, `CompiledTenantPartitionTests.TryGetTenantLayerTree_classifies_tenant_data_trees` and `CompiledTenantPartitionTests.Compile_drops_a_tenant_rule_that_escaped_the_store_confinement`. |
| `PerKeyCoverage` | A range filter admits a key an operator rule allows, or one the lower layers allow that no operator rule covers | Yes: `TenantLayerRangeFilterPropertyTests.Range_filter_equals_per_key_evaluation_and_the_composition_oracle_for_every_key` and `TenantLayerCollectionTests.Operator_per_key_deny_carves_the_tenant_allow_in_the_filter`. |
| `ScopelessFromClusterWideOnly` | `AppInstall` only from a tree-scope allow on `LatticeScope.ClusterWideTreeId`; `Telemetry` from that or from a platform allow over one tenant's capability scope; never from a key or prefix rule on the sentinel, a data-plane or `Admin` grant, another tenant's grant, or the tenant layer | Yes: `ClusterWideTelemetryGrantTests.A_key_scoped_rule_in_the_sentinel_bucket_does_not_confer_the_capability`, `ClusterWideTelemetryGrantTests.A_data_plane_all_trees_grant_does_not_confer_telemetry`, `TelemetryAccessAuthorizerTests.AuthorizeClusterTelemetryAsync_is_not_conferred_by_admin`, `AppRegistryTests.Every_transition_authorizes_AppInstall_over_the_cluster_wide_sentinel`, `TenantCapabilityTelemetryGrantTests.A_tenant_telemetry_grant_admits_its_subject_to_its_own_tenant_only`, `TenantCapabilityTelemetryGrantTests.A_tenant_telemetry_grant_never_confers_cluster_wide_telemetry`, `TenantCapabilityTelemetryGrantTests.A_cluster_wide_wildcard_telemetry_grant_does_not_reach_a_tenant_capability_scope`, `TenantCapabilityTelemetryGrantTests.The_admin_bit_on_a_tenant_capability_scope_confers_nothing_of_telemetry`, `TelemetryTenantGrantTests.AuthorizeTenantTelemetryAsync_refuses_a_grant_for_another_tenant` and `TelemetryTenantGrantTests.AuthorizeAsync_refuses_a_tenant_grant_while_another_tenant_is_active`. |
| `FailClosed` | Missing evidence denies: an unmatched control-plane request under either admin tier whatever the default effect, an all-trees grant on a control-plane id, a key-filtered allow where a whole-scope allow is required, and an absent MCP authorizer | Yes: `PolicyAccessGateUnitTests.AuthorizeAsync_reserved_namespace_unmatched_non_bootstrap_is_denied_even_under_default_allow`, `TenantAdminCapabilityScopeTests.All_trees_wildcard_grant_does_not_confer_delegated_tenant_administration`, `AllTreesGrantEvaluationTests.Evaluate_all_trees_tier_never_applies_to_a_reserved_tree`, `TelemetryAccessAuthorizerTests.AuthorizeClusterTelemetryAsync_refuses_a_key_filtered_allow`, `TelemetryTenantGrantTests.AuthorizeTenantTelemetryAsync_refuses_a_key_filtered_allow`, `AppRegistryTests.A_key_filtered_allow_does_not_authorize_AppInstall` and `McpToolAuthorizationGateTests.Fails_closed_when_no_authorizer_is_registered`. |
| `BootstrapRootOfTrust` | Bootstrap administrators are the one root of trust, and an anonymous caller is never one whatever the configured set holds | Yes: `PolicyAccessGateUnitTests.AuthorizeAsync_an_anonymous_caller_is_never_a_bootstrap_administrator`. |
| `BatchAllOrNothingAuthorization` | A batch over several (tree, key) pairs is allowed only when every pair is; one deny denies the batch before any intent is placed | Yes: `LatticeCrossTreeTxGrainTests.CommitAsync_denied_leg_throws_and_dispatches_no_participant` and `LatticeEnforcementIntegrationTests.Cross_tree_saga_aborts_wholesale_when_one_legs_tree_is_denied`. |
| `RuleAuthoringConfined` | The operator surface writes and removes only operator rules; the tenant facade writes only its own tenant's confined rules and removes only its own | Yes: `LatticeAuthorizationPolicyStoreTenantRuleTests.PutRuleAsync_refuses_a_tenant_rule_id_off_system_origin_before_reading`, `LatticeAuthorizationPolicyStoreTenantRuleTests.RemoveRuleAsync_refuses_a_tenant_rule_id_off_system_origin_before_reading`, `LatticeTenantPolicyAdminTests.PutRule_refuses_a_tree_outside_the_tenant_layer` and `LatticeTenantPolicyAdminTests.RemoveRule_never_removes_a_platform_rule_or_another_tenants_rule`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

The model abstracts the decision's inputs to one subject, two tenants, two keys
and one prefix, with at most three rules held at once.

- **Group subjects and templated scopes.** Every rule names one user, so group closure, `LatticeAuthOptions.UserRuleBeatsGroupRuleAtEqualScope` and subject-relative templates are not modelled; resolution within a tier is deny-overrides only.
  Production symbols: `ScopeTemplate`, `LatticeAuthOptions.UserRuleBeatsGroupRuleAtEqualScope`.
- **The uniform-range shortcut.** A collection over exactly a granted prefix returns a plain allow rather than a filter; the model always issues ranges as per-key filters, which the shortcut must agree with.
  Production symbols: `CompiledTree.TryResolveUniformRange`.
- **Tenant isolation composition.** The gate's tenant-isolation enforcer can only turn an allow into a deny; it is the cross-tenant grant module's concern and is outside the Allowed contract.
  Production symbols: `ITenantGateEnforcer`.
- **Snapshot lifecycle and the strict-consistency fence.** Decisions read a snapshot that is always current with the rule set; warm-up, rebuild and the policy-epoch fence on strict trees are not modelled.
  Production symbols: `CompiledPolicySnapshotMaintainer`, `LatticePolicyEpochFenceContext`.
- **Existence probes.** The any-grant probe mirrors enforcement for listings and is not modelled as a separate decision.
  Production symbols: `PolicyEvaluator.HasAnyGrant`.
- **A registered MCP authorizer.** Only the absent-authorizer path is modelled; what a registered authorizer decides is the MCP lock-step module's concern.
  Production symbols: `ILatticeApiMcpAuthorizer`.

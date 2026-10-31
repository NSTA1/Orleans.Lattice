# Refinement note: tenant-scoped schema facade

## Model scope in one paragraph

A caller reaches the schema facade (`ILatticeSchemaControl` and the compliance
operations facade) naming a tree. The facade resolves the caller's tenant and the
effective tree once through the single tenant-resolution seam: a caller whose
tenant assertion failed validation, or could not be resolved, is refused; the
default tenant keeps the bare name; a confined tenant composes an unqualified name
into its own namespace, passes another tenant's well-formed qualified name through
to the gate, and is refused a reserved add-on name or a malformed tenant name. The
schema authorizer then asks the access gate for the operation's capability over the
whole effective tree, `SchemaAdmin` for a mutation and `Read` for an inspection, and
the operation acts on exactly that tree. Every per-tree schema setting (policy,
version config, runtime catalog, dead letters, remediation status) is keyed by the
effective tree, so one tenant's settings never collide with another's, and tracked
schema operations are recorded under the caller's tenant and found by operation id
only there, and only over trees the caller may read. The tenancy half of the access
gate can lose the ability to confirm its policy, which denies. The bound is two
requests (a status read needs a start before it) and one confirmation loss; the
`ThreeRequests` variant re-checks every property with three requests.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `grant`, `cross` | The capability grants each caller holds, and whether the second tenant granted the first a crossing into its namespace. | `ILatticeAccessGate`, `TenantGateEnforcer`. |
| `confirm`, `faults` | Whether the tenancy layer can confirm its policy, and the confirmation losses left. | `TenantGateEnforcer`. |
| `count`, `req` | The requests sent, and the one in flight: caller, name, operation, the effective tree it resolved and its phase. | `LatticeSchemaControl`, `LatticeSchemaComplianceOperations`. |
| `policy`, `catalog` | Per effective tree, the callers whose policy write or runtime registration landed there. | `ILatticeSchemaAdmin.SetPolicyAsync`, `SchemaRuntimeRegistry.RegisterAsync`. |
| `ops` | Tracked schema operations, keyed by the tenant they were started under and the tree they act on. | `LatticeOperationRunner.StartAsync`, `SchemaOperationService`. |
| `disclosed`, `seen` | Schema settings a caller read, and the operation records a status read served it with their owning tenant. | `LatticeSchemaControl.GetRegistryAsync`, `LatticeOperationRunner.GetAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Receive(c, n, op)` | Environment: a caller sends a schema facade request naming a tree. | No production counterpart: the caller is the environment. | Not applicable. |
| `Resolve` | Resolve the caller's tenant and the effective tree once, before anything uses the name; fail closed on an unattributed caller and on a confined tenant's escape into a reserved or malformed namespace. A status read is addressed by operation id under the caller's tenant. | `LatticeTenantResolution.ResolveEffectiveTreeIdAsync`, `LatticeTenantResolution.ComposeEffectiveTreeId`, `LatticeSchemaControl.SetPolicyAsync`. | Yes: `LatticeSchemaControlTenantScopeTests.SetPolicyAsync_under_a_confined_tenant_authorizes_and_writes_the_composed_tree`, `LatticeSchemaControlTenantScopeTests.A_confined_tenants_reserved_name_is_refused_before_the_gate_and_the_admin`, `LatticeSchemaControlTenantScopeTests.An_unattributed_caller_is_refused_before_the_gate_and_the_admin`, `LatticeTenantResolutionTests.ComposeEffectiveTreeId_non_default_tenant_composes_a_scoped_id`. |
| `Authorize` | Ask the access gate for the operation's capability over the whole effective tree, failing closed. | `SchemaAccessAuthorizer.AuthorizeManageAsync`, `SchemaAccessAuthorizer.AuthorizeReadAsync`, `LatticeAccessGateEnforcement.EnforceWholeTreeAsync`. | Yes: `SchemaAccessAuthorizerTests.AuthorizeManage_issues_a_whole_tree_schema_admin_request`, `SchemaAccessAuthorizerTests.AuthorizeRead_issues_a_whole_tree_read_request`, `LatticeSchemaControlTenantScopeTests.Another_tenants_qualified_name_reaches_the_gate_unchanged_and_is_denied_without_its_grant`. |
| `Act` | Run the operation on the effective tree it authorized: set a policy, register a definition, start a tracked operation under the caller's tenant, read the catalog, or serve an operation's status from the caller's tenant over trees it may read. | `ILatticeSchemaAdmin.SetPolicyAsync`, `LatticeSchemaControl.RegisterSchemaAsync`, `LatticeSchemaControl.GetRegistryAsync`, `LatticeSchemaControl.StartRemediationAsync`, `LatticeSchemaControl.GetOperationStatusAsync`. | Yes: `LatticeSchemaControlTenantScopeTests.SetPolicyAsync_under_a_confined_tenant_authorizes_and_writes_the_composed_tree`, `LatticeSchemaControlTenantScopeTests.GetPolicyAsync_under_a_confined_tenant_reads_the_composed_tree_under_read_authority`, `LatticeSchemaOperationsIntegrationTests.An_operation_in_another_tenant_or_over_unreadable_trees_is_not_found`. |
| `Unconfirm` | Environment: the tenancy layer loses the ability to confirm its policy. | No production counterpart: a registry or lease failure is a fault. | Not applicable. |
| `Reconfirm` | Environment: the tenancy layer can confirm its policy again. | No production counterpart: recovery from a fault. | Not applicable. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `EffectsStayInTenant` | Every schema setting a caller changes, and every tracked operation it starts, lands on a tree in its own tenant's namespace or one whose owner granted the crossing (or, for the default tenant, a platform tree); an unattributed caller changes nothing. | Yes: `LatticeSchemaControlTenantScopeTests.SetPolicyAsync_under_a_confined_tenant_authorizes_and_writes_the_composed_tree`, `LatticeSchemaControlTenantScopeTests.Another_tenants_qualified_name_reaches_the_gate_unchanged_and_is_denied_without_its_grant`, `LatticeSchemaControlTenantScopeTests.The_compliance_scan_start_under_a_confined_tenant_authorizes_and_records_the_composed_tree`. |
| `DisclosureStaysInTenant` | A schema read is served only from a tree within the caller's tenant or a granted crossing. | Yes: `LatticeSchemaControlTenantScopeTests.GetPolicyAsync_under_a_confined_tenant_reads_the_composed_tree_under_read_authority`, `LatticeSchemaControlTenantScopeTests.GetRegistryAsync_under_a_confined_tenant_reads_the_composed_trees_catalog`. |
| `StatusSeesOwnTenantOnly` | A status, list or cancel by operation id finds only operations recorded under the caller's own tenant. | Yes: `LatticeSchemaOperationsIntegrationTests.An_operation_in_another_tenant_or_over_unreadable_trees_is_not_found`. |
| `ManageNeedsSchemaAdmin` | A policy, version-config, registration or operation start needs the `SchemaAdmin` capability over the whole tree; `Read` is not enough. | Yes: `LatticeSchemaControlTenantScopeTests.A_schema_mutation_with_only_read_authority_is_denied_and_writes_nothing`, `LatticeSchemaOperationsIntegrationTests.Starting_and_cancelling_need_schema_management_while_reading_needs_read`, `LatticeSchemaControlTests.SetPolicyAsync_denied_throws_and_does_not_touch_admin`, `SchemaAccessAuthorizerTests.AuthorizeManage_issues_a_whole_tree_schema_admin_request`. |
| `ReadNeedsRead` | An inspection, including an operation status, needs `Read` authority over the whole tree. | Yes: `LatticeSchemaControlTests.GetPolicyAsync_denied_throws_and_does_not_touch_admin`, `LatticeSchemaComplianceOperationsTests.An_operation_over_a_tree_the_caller_cannot_read_is_not_found_rather_than_forbidden`. |
| `ActsOnAuthorizedTree` | The facade resolves the name once and hands the gate and the operation the same effective id, so nothing lands under the bare name or a second id. | Yes: `LatticeSchemaControlTenantScopeTests.SetPolicyAsync_under_a_confined_tenant_authorizes_and_writes_the_composed_tree`, `LatticeSchemaControlTenantScopeTests.GetRegistryAsync_under_a_confined_tenant_reads_the_composed_trees_catalog`. |
| `UnattributableDenied` | A caller whose tenant cannot be attributed is refused before the gate and before any operation, never defaulted. | Yes: `LatticeSchemaControlTenantScopeTests.An_unattributed_caller_is_refused_before_the_gate_and_the_admin`, `LatticeTenantResolutionTests.ResolveEffectiveTreeIdAsync_async_denying_resolver_fails_closed`. |
| `EscapeRefusedAtResolution` | A confined tenant is refused a reserved add-on name or a malformed tenant name before the gate, which would admit a platform-owned tree on capability alone. | Yes: `LatticeSchemaControlTenantScopeTests.A_confined_tenants_reserved_name_is_refused_before_the_gate_and_the_admin`, `LatticeSchemaControlTenantScopeTests.A_confined_tenants_malformed_tenant_name_is_refused_before_the_gate_and_the_admin`. |
| `AuthorizedOnlyWhenConfirmed` | The schema authorizer delegates to the access gate unchanged, so a tenancy layer that cannot confirm its policy denies the schema operation too. | Yes: `TenantGateEnforcerSnapshotLagTests.EnforceAsync_registry_failure_while_the_snapshot_is_not_authoritative_denies`, `SchemaAccessAuthorizerTests.AuthorizeManage_deny_throws_with_schema_admin_operation_and_reason`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiments. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, EffectsStayInTenant, DisclosureStaysInTenant and StatusSeesOwnTenantOnly
are state safety. ManageNeedsSchemaAdmin, ReadNeedsRead, ActsOnAuthorizedTree,
UnattributableDenied, EscapeRefusedAtResolution and AuthorizedOnlyWhenConfirmed are
action safety. The module has no liveness property: a refused request is the safe
outcome, and every property here must hold while the tenancy layer cannot confirm
its policy, with no fairness at all. The base checks two requests and one
confirmation loss; the ThreeRequests variant re-checks every property with three
requests.

| Mutation | Classification |
|----------|----------------|
| TypeOKReceiveOverCounts | Anchored action: Receive. |
| TypeOKUnconfirmUnbounded | Anchored action: Unconfirm. |
| TypeOKReconfirmCountsFault | Anchored action: Reconfirm. |
| EffectsStayInTenantActsOnRequestedName | Anchored action: Act. |
| ActsOnAuthorizedTreeRegistersUnderBothIds | Anchored action: Act. |
| DisclosureStaysInTenantReadsRequestedName | Anchored action: Act. |
| StatusSeesOwnTenantOnlyIgnoresTenant | Anchored action: Act. |
| ReadNeedsReadStatusUnchecked | Anchored action: Act. |
| ManageNeedsSchemaAdminAcceptsRead | Anchored action: Authorize. |
| AuthorizedOnlyWhenConfirmedCapabilityOnly | Anchored action: Authorize. |
| UnattributableDeniedDefaultsTenant | Anchored action: Resolve. |
| EscapeRefusedAtResolutionPassesQualified | Anchored action: Resolve. |

## Detectors

`LatticeSchemaControlTenantScopeTests` builds the schema facade over a fixed tenant
resolver and a recording access gate, and pins the seam end to end: the tree the
gate is asked about and the tree the in-process admin receives are the same
composed id, an unattributed caller and a confined tenant's reserved or malformed
name are refused before either is reached, and another tenant's qualified name
reaches the gate unchanged. `LatticeSchemaOperationsIntegrationTests` and
`LatticeSchemaComplianceOperationsTests` pin the tenant scoping and read gating of
tracked operations, `SchemaAccessAuthorizerTests` the whole-tree capability
request, `LatticeTenantResolutionTests` the resolution seam every facade shares,
and `TenantGateEnforcerSnapshotLagTests` the tenancy gate's denial when it cannot
confirm its policy, which the schema authorizer inherits unchanged.

## Defect mutations whose fixes have landed

None: no mutation here stands for a defect production had.

## Deliberate abstraction gaps

- **The tenancy layer is relied on, not modelled.** Whether a tenancy snapshot is
  current, how a grant or a revocation propagates, and when the gate can confirm
  its policy are TenantIsolation's; here confirmation is one flag that can drop
  and return, and a grant is fixed for the whole behaviour.
  Production symbols: `TenantGateEnforcer`, `ITenantGateEnforcer`.
- **One name per shape, two tenants.** A tenant-local name, one other tenant's
  qualified name, one reserved add-on name and one malformed tenant name stand for
  every name; a capability grant applies to every tree a caller addresses, so the
  tenancy half of the gate is the only per-tree discriminator. Path-scoped and
  key-filtered grants are refused for schema operations, which are whole-tree.
  Production symbols: `LatticeAccessGateEnforcement.EnforceWholeTreeAsync`.
- **In-process admins perform no authorization.** `ILatticeSchemaAdmin`,
  `ILatticeSchemaVersionAdmin` and the in-process remediation and compliance
  admins trust their caller and act on the id they are given; only the facade
  resolves and authorizes. A host that hands a tenant's caller an in-process admin
  bypasses everything modelled here.
  Production symbols: `ILatticeSchemaAdmin`, `ILatticeSchemaVersionAdmin`.
- **Data-plane writes and the schema stages.** A tenant's ordinary writes reach
  the write stages under the same effective tree id, so the policy and version
  config they are judged by are that tree's; the stages themselves are
  SchemaStageOrdering's and the per-silo caches SchemaPolicyConfig's.
  Production symbols: `LatticeTenantResolution.ResolveEffectiveTreeIdAsync`.
- **Dead-letter preview exposure.** Which callers may read dead-letter previews
  through grants that span every tree is a disclosure question of the access
  gate's grant model, not of tenant resolution, and is not modelled.
  No production symbol: the gap states a bound of the model, not a production mechanism.
- **Operation-id namespaces are per tenant only.** Two tenants may each start an
  operation with the same id; the model has one id, and records are found by
  tenant and id together, so it does not distinguish them further.
  Production symbols: `LatticeOperationRunner.GetAsync`.

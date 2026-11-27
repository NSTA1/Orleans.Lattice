# Refinement note: McpToolAuthorization

This maps MCP tool authorization (issue #6052) onto its production seams.
Authorization is the F1 contract's abstract `Allowed(caller, op, scope)`
(`spec/auth-decision/`, contract version 1), declared per policy snapshot as
`AllowedUnder(p, caller, op, scope)` because the policy changes between a
session's advertisement and its calls. The cfg binds it to
`ContractAllowedUnder`, the contract's minimal grant-set definition with one
grant set per snapshot. Each grant set satisfies C1-C6 by construction: no
bootstrap administrator, no control-kind allow, and `Telemetry` only on the
cluster-wide id or, as C3 allows since #5582, on one tenant's capability
scope.

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `policy` | The authorization policy snapshot every gate reads | The rule store the access gate and `AuthAdminMcpPermissionResolver.ResolveAsync` read; the registered `ILatticeApiMcpAuthorizer` may also follow it |
| `appEnabled` | Whether the installed app's install is enabled | The app registry projection `AppMcpToolSource.IsInvocationPermittedAsync` makes current before it decides |
| `authz` | Which coarse authorizer is registered | `DenyAllMcpAuthorizer` (the default), no registration, or a host's permissive `ILatticeApiMcpAuthorizer` |
| `session` | One MCP session: its caller, asserted active tenant, the resolver's carried operations, the snapshot it was built from, and its tool collection | `LatticeApiMcpSessionConfigurator.BuildSessionPlanAsync` builds `LatticeApiMcpSessionPlan`, whose tool collection serves both tools/list and tools/call |
| `obs` | What the last call returned and which checks decided it | No production state: the call's result, the gate verdict and the tool's own verdict the properties judge. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `OpenSession` | Resolve the caller and its access set, install the meta-tool for an authenticated caller, then add region discovery, each group tool past its per-tool minimum and the gate, and each app tool whose role the caller holds in its validated tenant | `LatticeApiMcpSessionConfigurator.BuildSessionPlanAsync`, `AuthAdminMcpPermissionResolver.ResolveAsync`, `AppMcpToolSource.GetPermittedToolsAsync`, `McpToolAuthorizationGate.IsAuthorizedAsync` | Yes: `LatticeApiMcpSessionConfiguratorTests.Unauthenticated_session_gets_no_tools_and_no_groups`, `LatticeApiMcpSessionConfiguratorTests.A_read_only_access_set_is_offered_the_read_tools_and_none_of_the_mutating_ones`, `LatticeApiMcpSessionConfiguratorTests.Deny_all_authorizer_hides_every_granted_group_tool` and `AppMcpToolSourceDiscoveryTests.A_caller_whose_tenant_resolution_is_denied_is_offered_no_app_tools`. |
| `CallMeta` | Serve lattice_capabilities from the session plan, ungated | `LatticeApiMcpSessionConfigurator.BuildSessionPlanAsync` | Yes: `LatticeApiMcpSessionConfiguratorTests.Authenticated_caller_with_no_grants_gets_only_the_meta_tool` and `LatticeApiMcpSessionConfiguratorTests.ConfigureAsync_leaves_an_anonymous_session_with_an_empty_tool_collection`. |
| `CallRegions` | Re-run the gate, then scope the region list to the validated active tenant | `AuthorizedMetaTool.InvokeAsync`, `LatticeApiMcpRegionCatalog.ListRegionsAsync` | Yes: `AuthorizedMetaToolTests.A_call_the_authorizer_now_refuses_is_denied_without_reaching_the_tool` and `LatticeApiMcpRegionCatalogTenantScopeTests.An_unvalidated_tenant_assertion_is_refused_and_falls_back_to_the_current_region`. |
| `CallGroupTool` | Re-run the gate, answer an argument error as a client error, then let the facade or the telemetry handler's cluster-wide capability check decide on the current policy | `CredentialStampingTool.InvokeAsync`, `TelemetryToolHandlers.QueryAsync`, `TelemetryAccessAuthorizer.AuthorizeClusterTelemetryAsync` | Yes: `CredentialStampingToolClientErrorTests.An_authorization_denial_still_throws_and_is_never_counted_as_a_client_error`, `TelemetryToolCapabilityGateTests.Query_without_the_telemetry_capability_is_refused` and `TelemetryToolCapabilityGateTests.A_tenant_capability_telemetry_grant_does_not_authorize_an_mcp_telemetry_tool`. |
| `CallAppTool` | Re-run the gate and the argument check, then re-run the advertisement decision against the current registry and policy | `CredentialStampingTool.InvokeAsync`, `AppMcpToolSource.IsInvocationPermittedAsync` | Yes: `AppMcpToolSourceInvocationTests.Disabling_the_app_after_advertisement_denies_the_invocation` and `AppMcpToolSourceInvocationTests.Leaving_the_bound_group_after_advertisement_denies_the_invocation`. |
| `ChangePolicy` | Environment: an operator edits the policy between a session's advertisement and its calls | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |
| `ToggleApp` | Environment: an operator disables or re-enables the app's install | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `LockStep` | The session's tool collection serves both tools/list and tools/call, and every invocation re-runs the gate and the tool's own check against the current snapshot, so advertisement is never a capability | Yes: `AppMcpToolSourceInvocationTests.Disabling_the_app_after_advertisement_denies_the_invocation`, `AppMcpToolSourceInvocationTests.Leaving_the_bound_group_after_advertisement_denies_the_invocation` and `AuthorizedMetaToolTests.A_call_the_authorizer_now_refuses_is_denied_without_reaching_the_tool`. |
| `OnlyMetaToolUngated` | lattice_capabilities is the only tool advertised or served without the authorizer | Yes: `LatticeApiMcpSessionConfiguratorTests.Region_discovery_tool_is_hidden_when_the_authorizer_denies_it_by_name`, `LatticeApiMcpSessionConfiguratorTests.Deny_all_authorizer_hides_every_granted_group_tool` and `AuthorizedMetaToolTests.A_call_the_authorizer_now_refuses_is_denied_without_reaching_the_tool`. |
| `PerToolMinimum` | A tool whose required operations the caller's granted operations do not meet is withheld, unconditionally (#4082), and so unreachable at tools/call | Yes: `LatticeApiMcpSessionConfiguratorTests.A_read_only_access_set_is_offered_the_read_tools_and_none_of_the_mutating_ones`, `LatticeApiMcpSessionConfiguratorTests.An_access_set_carrying_no_operations_is_offered_no_group_tool` and `DataToolGroupTests.Mutating_tools_require_a_mutating_operation`. |
| `ScopelessToolsClusterWide` | The MCP telemetry tools are advertised, carried and served only on a cluster-wide Telemetry grant; a tenant capability grant never reaches one | Yes: `AuthAdminMcpPermissionResolverTests.A_tenant_capability_telemetry_grant_does_not_advertise_the_mcp_telemetry_tools`, `AuthAdminMcpPermissionResolverTests.Telemetry_over_a_tree_scope_is_not_carried_into_the_granted_operations` and `TelemetryToolCapabilityGateTests.A_tenant_capability_telemetry_grant_does_not_authorize_an_mcp_telemetry_tool`. |
| `FailClosed` | No request context, no registered authorizer, the default deny-all authorizer and an anonymous session all deny | Yes: `McpToolAuthorizationGateTests.Fails_closed_when_no_http_context_is_available`, `McpToolAuthorizationGateTests.Fails_closed_when_no_authorizer_is_registered`, `LatticeApiMcpAuthorizerTests.DenyAllMcpAuthorizer_rejects_every_request` and `LatticeApiMcpSessionConfiguratorTests.Unauthenticated_session_gets_no_tools_and_no_groups`. |
| `ActiveTenantValidated` | An asserted active tenant is honoured only when it re-resolves to itself; otherwise the region list degrades to the current region, unannotated, and the tenant's app tools are not offered | Yes: `LatticeApiMcpRegionCatalogTenantScopeTests.An_unvalidated_tenant_assertion_is_refused_and_falls_back_to_the_current_region`, `LatticeApiMcpRegionCatalogTenantScopeTests.A_refused_assertion_never_echoes_the_asserted_tenant_back` and `AppMcpToolSourceInvocationTests.A_tool_advertised_to_one_tenant_is_denied_when_invoked_under_another`. |
| `DenialNotClientError` | A gate or tool refusal propagates as a denial; only argument faults are answered as client errors | Yes: `CredentialStampingToolClientErrorTests.An_authorization_denial_still_throws_and_is_never_counted_as_a_client_error` and `McpToolFaultTranslatorTests.Translate_permission_denied_stays_a_fail_closed_denial`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **Discovery reads grants, not denies.** The permission resolver admits a group on the presence of an allow and leaves deny rules to the call-time gate. The contract's grant-set definition has no deny, so the model never advertises a tool a deny then refuses.
  Production symbols: `AuthAdminMcpPermissionResolver.ResolveAsync`.
- **One tool per kind.** Each data tool stands for a family whose minimum is the same, and the telemetry tool for the four telemetry tools; the per-tool masks themselves are pinned by their group tests.
  Production symbols: `DataToolGroup.RequiredOperationsFor`, `LatticeApiMcpGroupCapabilityMap.RequiredOperations`.
- **Region routing and peer identity verification.** A call's region argument, its routing and the identity probe that omits a mismatched peer are not modelled; they decide where an authorized call runs, not whether it is authorized.
  Production symbols: `ILatticeApiMcpRegionRouter`, `ILatticeApiMcpRegionIdentityVerifier`.
- **Transient discovery faults.** A backend fault during discovery surfaces as a retryable error rather than a narrower tool set; the model's discovery always completes.
  Production symbols: `LatticeApiMcpDiscoveryUnavailableException`.
- **The capabilities payload is a session snapshot.** The meta-tool reports the caller's own grants as resolved when the session opened; it discloses nothing beyond them, so its staleness is not modelled.
  Production symbols: `LatticeApiMcpCapabilities`.
- **Unsupported and schema-tool deferral.** Tools a topology reports unsupported, and schema tools the probed capabilities do not support, are omitted at discovery; omission only narrows the collection.
  Production symbols: `ILatticeApiMcpUnsupportedToolSource`, `SchemaToolSupport.IsSupported`.
- **Region discovery re-checks only the authorizer at invocation.** `lattice_list_regions` is advertised only to a caller holding a facade grant, but its call re-runs only the coarse gate, so a session opened before every grant was revoked keeps receiving the topology. The model's call-time entitlement for this one tool omits the facade-grant condition until the code re-checks it.
  Production symbols: `AuthorizedMetaTool.InvokeAsync`, `LatticeApiMcpSessionConfigurator.BuildSessionPlanAsync`.

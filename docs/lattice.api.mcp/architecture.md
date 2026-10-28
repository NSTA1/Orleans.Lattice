---
agent_spec: "docs/agents/api/mcp.json"
---

# Architecture

`Orleans.Lattice.Api.Mcp` is a binding, not a second implementation of the cluster's behaviour. It hosts an MCP server on the official [`ModelContextProtocol`](https://www.nuget.org/packages/ModelContextProtocol) SDK's streamable-HTTP transport, decides per caller which tools exist, and turns each tool call into a call on an `Orleans.Lattice.Api.*` facade under the caller's own credential. Authorization of data is left to the facades' access gate; the binding adds gates that only ever narrow what reaches it.

This page names only public types; the internal collaborators are described by what they do. For the types themselves see [Public API](api.md), and for every knob see [Configuration](configuration.md).

## Composition

`AddLatticeMcp` registers the server with **no tools**, plus the seams every request passes through: the coarse authorizer (`DenyAllMcpAuthorizer` unless the host registers another `ILatticeApiMcpAuthorizer`), the credential bridge (`ILatticeApiMcpCredentialBridge`), the active-tenant bridge (`ILatticeApiMcpActiveTenantBridge`), the HTTP context accessor they read, the per-session discovery hook, the region router and catalog, an origin-validation startup filter, and the protected-resource-metadata challenge filter.

Each `AddXTools(...)` call then contributes one **tool group**: a record of which `LatticeApiMcpGroup` it serves and which tools it offers, determined by the module's flags. A destructive verb whose flag is off is not in the group's tool list, so it cannot be advertised or called. Companion packages contribute groups the same way - telemetry through `AddTelemetryTools`, repository context through `AddRepoContextTools` - and the installable-app surface adds app tools through its own source. The same groups are registered in both topologies; only the facade behind them differs.

## Request path

A request on the transport passes these stages in order:

1. **Origin check.** When `RequireAllowedOrigin` is `true`, an outermost middleware answers `403` for a request on the transport path whose `Origin` is not allowed, before authentication, routing or any tool runs. `MapLatticeMcp` repeats the same verdict on the mapped endpoint itself, so a path base or route-group prefix that moves the transport cannot bypass it.
2. **Endpoint authorization.** With `RequireAuthorization` at its `true` default, the endpoint requires an authenticated caller. When `ProtectedResourceMetadata` is set, the `401` bearer challenge on the transport path gains a `resource_metadata` hint, and the metadata document itself is served anonymously.
3. **Session discovery.** When a session initialises, the discovery hook resolves the caller through the credential bridge (fail-closed: an unauthenticated principal resolves to no credential), reads the caller's effective permissions through the auth facade as a trusted system-origin read, and builds the session's own tool collection. A group's tools are added only when the caller holds an Allow grant for an operation the group covers; inside a group, per-tool minimum operations withhold the tools the caller's grants cannot reach (for example the data group's mutating tools), and each tool is admitted by the coarse authorizer by name. The hook also installs the `lattice_capabilities` meta-tool, whose `LatticeApiMcpCapabilities` report is computed from the same view, and installs `lattice_list_regions` only when the caller holds some facade grant and the authorizer admits it. The session's instructions are written from that view as well. See [Permission-scoped discovery](security.md#permission-scoped-discovery).
4. **Invocation.** Every advertised tool is wrapped so that a call re-runs the same authorizer check that admitted it at discovery - the two enforcement points are one lock-step gate, and the gate denies when there is no `HttpContext` or no authorizer. The wrapper then stamps the bridged caller credential onto the ambient `LatticeCredentialContext` and the asserted active tenant onto the ambient `LatticeActiveTenantContext`, and invokes the tool's adapter.
5. **Facade call.** The adapter validates its arguments, calls the facade, and projects the answer. The facade's access gate makes the real per-tree / per-key / per-verb decision under the stamped credential; the binding never re-implements it. State, auth and tree-administration tools mostly return the facade's own records; the data tools return the compact records listed in [Public API](api.md#tool-result-and-argument-records).
6. **Error classification.** A failure the binding classifies as the caller's mistake - a missing, malformed or unknown argument, refused content, an unknown record - is answered as a tool error rather than thrown. Its text is sanitized once, at the invocation wrapper every call funnels through, before it is returned or logged, and it is counted on `orleans.lattice.api.mcp.tool.client_errors` (`LatticeApiMcpMetrics`). Any other fault, an authorization denial included, is thrown as a fault, never downgraded. See [Rejected calls are sanitised](security.md#rejected-calls-are-sanitised).

## Remote topology

`AddLatticeMcpRemote` keeps every stage above and swaps what sits behind the facade interfaces. For each group with a configured endpoint it registers an adapter that implements the facade interface over that group's `Orleans.Lattice.Api.*.Grpc` client, and adds the group's tool module with its destructive flag forwarded from the remote options. A group with no endpoint is not wired and is reported unavailable.

- **Credential forwarding.** Every outbound gRPC call passes through an interceptor that stamps the resolved credential as `{scheme} {token}` on `CredentialHeaderName`. It resolves, in order, the administrator credential for a system-origin introspection call, the ambient credential the tool stamped, the credential bridge, and otherwise nothing (anonymous). The same interceptor re-emits the ambient active tenant on `ActiveTenantHeaderName`. Enforcement therefore happens at the data owner, which authenticates the forwarded credential itself.
- **Administrator credential.** Remote discovery cannot use the in-silo system-origin bypass, so introspecting a non-administrator caller needs an administrator credential: the static `AdministratorCredential`, or the self-refreshing source `AddLatticeMcpManagedIdentityAdministrator` installs, which caches an `Azure.Core` token, refreshes it `RefreshSkew` before expiry, and forwards no administrator credential while acquisition is failing (the introspection then falls back to the caller's own credential).
- **Deferred tools.** Tools whose gRPC method is not yet bound are omitted from a remote session rather than advertised and failed. See [Deferred tools](remote.md#deferred-tools).

## Region routing

One router is the single source of truth for both discovery (`lattice_list_regions`, the per-group availability in `lattice_capabilities`) and per-call routing of the optional `region` argument, so the two cannot disagree. In-silo it holds one current region that serves exactly the registered groups in-process. Remote, it is built once at startup from `RegionId`, the top-level endpoints and each `LatticeApiMcpRemoteRegionOptions`; a peer serves only the groups it configures itself, and the current region can additionally serve telemetry from a co-located telemetry module. With `VerifyRegionIdentity`, a peer's reported cluster id is checked against its advertised `ClusterId` before a call is routed there, and a mismatched peer is omitted and refused. On a tenancy cluster the region catalog scopes discovery to a validated tenant's actionable regions; see [Tenant-scoped region discovery](security.md#3b-tenant-scoped-region-discovery).

## Seams

| Seam | Default | Replace it to |
|---|---|---|
| `ILatticeApiMcpAuthorizer` | `DenyAllMcpAuthorizer` | Admit tools at the coarse gate, for example `AllowAllMcpAuthorizer` or a name-based policy. |
| `ILatticeApiMcpCredentialBridge` | Reads the authenticated principal and the `CredentialHeaderName` token. | Source the caller credential elsewhere, for example a client certificate. |
| `ILatticeApiMcpActiveTenantBridge` | Reads the `ActiveTenantHeaderName` header. | Source the asserted tenant elsewhere, for example a principal claim. |

## Source map

- [Front-door registration and mapping](../../src/lattice.api.mcp/LatticeMcpServiceCollectionExtensions.cs)
- [Session discovery](../../src/lattice.api.mcp/Discovery/LatticeApiMcpSessionConfigurator.cs)
- [Security seams](../../src/lattice.api.mcp/Security/ILatticeApiMcpAuthorizer.cs)
- [Remote registration](../../src/lattice.api.mcp/Remote/LatticeMcpRemoteServiceCollectionExtensions.cs) and [credential forwarding](../../src/lattice.api.mcp/Remote/LatticeApiMcpCredentialForwardingInterceptor.cs)
- [Metrics](../../src/lattice.api.mcp/LatticeApiMcpMetrics.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Security](security.md), [Remote hosting](remote.md), [Tools](tools.md)
- [`Orleans.Lattice.Api.Mcp.Telemetry` architecture](../lattice.api.mcp.telemetry/architecture.md), [`Orleans.Lattice.Api.Mcp.RepoContext` architecture](../lattice.api.mcp.repocontext/architecture.md), [`Orleans.Lattice.Api.Mcp.Apps`](../lattice.api.mcp.apps/README.md)

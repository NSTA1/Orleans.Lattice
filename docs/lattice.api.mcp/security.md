# Security

The MCP server exposes read, write, and control facades to an AI agent, so it is built to **fail closed**: an unauthenticated or unauthorized session can enumerate nothing and call nothing until the host explicitly opts in. Three layers combine to deliver that posture.

## 1. Endpoint authorization

With `RequireAuthorization` at its `true` default, `MapLatticeMcp()` applies ASP.NET Core `RequireAuthorization()` to the mapped transport, so an anonymous request never reaches the MCP server at all. Set it to `false` only when an outer authentication boundary (a gateway, a reverse proxy, another middleware) already guarantees the caller is authenticated.

## 2. The coarse authorizer seam

Every inbound request passes through an `ILatticeApiMcpAuthorizer` before it reaches a facade. The binding registers `DenyAllMcpAuthorizer` by default (via `TryAdd`), so a host that maps the surface without configuring authorization rejects every call rather than exposing the cluster unauthenticated. A host opts in by registering a permissive or custom authorizer:

The authorizer is consulted at **both** enforcement points through one lock-step gate: tool **advertisement** (`tools/list`, in the per-session discovery configurator) and tool **invocation** (`tools/call`, in the credential-stamping tool). A tool the authorizer denies is therefore never advertised **and** cannot be invoked directly, so there is no discovery-versus-invocation gap. The gate is **fail-closed**: it denies when there is no `HttpContext` to authorize against or no authorizer registered, so the surface stays closed until a host deliberately opts an authorizer in. Only the `lattice_capabilities` meta-tool is exempt from the coarse gate.

```csharp verify
public sealed class ReadOnlyStateMcpAuthorizer : ILatticeApiMcpAuthorizer
{
    public Task<bool> IsAuthorizedAsync(
        LatticeApiMcpAuthorizationContext authorizationContext,
        CancellationToken cancellationToken)
    {
        // Let only the introspective state tools past the coarse gate; the
        // per-tree / per-key access gate still applies afterwards.
        var allowed = authorizationContext.ToolName
            .StartsWith("lattice_state_", System.StringComparison.Ordinal);
        return Task.FromResult(allowed);
    }
}
```

`AllowAllMcpAuthorizer` is the opt-in "defer everything to the access gate" implementation: it permits every request through the coarse gate and lets the per-tree / per-key enforcement on the gated `ILattice` surface make the real decision. Register it when the coarse gate adds no value beyond the subject-scoped decisions the access gate already makes.

## 3. The credential bridge

The coarse gate decides only whether a request may reach a facade; it does not decide what the caller may see once there. That is the job of the credential bridge. The default bridge reads the authenticated principal off the `HttpContext` and resolves a `LatticeCredential`; at each tool invocation the resolved credential is stamped onto the ambient `LatticeCredentialContext`, so every facade call the tool makes runs under the caller's subject and flows through the same per-tree / per-key access gate the gRPC bindings and the data path already use. The bridge is fail-closed: it resolves a credential only for an authenticated principal, and returns none (anonymous) otherwise. It is the public `ILatticeApiMcpCredentialBridge` seam, `TryAdd`-registered, so a host can substitute its own bridge.

The `CredentialHeaderName` and `CredentialScheme` options control which inbound header the token is read from and which scheme prefix is stripped before the remaining token is used as the credential.

The bridge resolves the caller's principal id from the durable object-id (`oid`) claim first, falling back to `sub` and then the identity name. For an Entra delegated (user) token `sub` is a pairwise (user, client-app) identifier that differs from the stable `oid` the silo auth model keys subjects on, so keying discovery on `oid` ensures the subject the tool list is filtered for is the same subject the access gate enforces on - grants authored once by `oid` apply consistently across every client app.

## 3a. The active-tenant bridge

On a cluster running the optional tenancy add-on, per-tenant capacity governance (write admission and quota enforcement) is scoped by the call's *active tenant*, a channel distinct from caller identity. A parallel bridge lifts the caller's asserted active tenant from a single inbound header - `lattice-active-tenant` by default, set by `ActiveTenantHeaderName` - and stamps it onto the ambient `LatticeActiveTenantContext` at each tool invocation, right beside the credential stamp. In-silo the stamped tenant flows to the grain on the Orleans request context; on a remote (split) head the credential-forwarding interceptor re-emits it as a gRPC metadata header, so both topologies reach the same silo-side enforcement.

Like the credential, the header carries only an *assertion*: the tenancy add-on's resolver re-validates it against the caller's authenticated subject membership downstream, and the access gate authorizes the operation before any per-tenant accounting is consulted, so a caller cannot escalate by asserting a tenant it is not a member of. The per-tenant admission controller is deliberately **not** part of that check - it runs strictly after authorization and is an accounting step, not an authorization one. The bridge performs no authorization of its own and is fail-closed - an absent, blank, or syntactically invalid header asserts no tenant. It is the public `ILatticeApiMcpActiveTenantBridge` seam, `TryAdd`-registered, so a host can substitute its own; setting `ActiveTenantHeaderName` empty disables header-based tenant selection. On a non-tenancy cluster the whole path is inert and allocation-free.

The stamp is applied at every facade tool invocation, beside the credential stamp, and additionally at the `lattice_list_regions` discovery tool, which stamps the tenant but no credential (it is a meta-tool that reads only routing configuration, never a facade). Both stamps run through the same `IHttpContextAccessor` and bridge, so the tenant a facade tool acts as is the same tenant discovery is scoped to.

## 3b. Tenant-scoped region discovery

`lattice_list_regions` projects the host's routing topology: each entry carries a region id, a cluster id, and the per-group gRPC endpoint of that region. On a cluster running the tenancy add-on that is operator information, so the tool answers differently depending on whether the call asserts a tenant.

| Caller | What `lattice_list_regions` returns |
|--------|-------------------------------------|
| No tenant asserted (an operator, or any caller on a non-tenancy cluster) | The full routing topology, unannotated and byte-for-byte as before tenant scoping existed. |
| The reserved default tenant | The same full topology - the default tenant *is* the pre-tenancy behaviour by definition. |
| A non-default tenant, standing resolved | The current region, plus only those peers in the tenant's **actionable set** (`allowed` union `resident`). Every entry is annotated with a `tenantScope` object. |
| A non-default tenant, standing unresolvable | The current region alone. Never a fallback to the full topology. |

The **actionable set** is the union of the tenant-facing region sets described in [the tenancy guide](../lattice.tenancy/README.md#the-region-sets): the regions the operator has authorized the tenant into, and the regions the tenant is currently resident in. A region outside it is neither usable by that caller (routing a call there is refused by the residency gate) nor modifiable by it (`lattice_tenant_set_residency` refuses anything outside the allowed set), so omitting it removes disclosure without removing capability.

The **current region is always advertised**, even when the tenant has no standing in it. The caller is already talking to it, so concealing it would break the caller's own session rather than conceal anything; its `tenantScope` reports the standing truthfully, which may be `isAllowed: false` with status `None`.

The `tenantScope` annotation is **additive and optional**: it is present only on a tenant-scoped answer. An operator answer, and every answer on a non-tenancy cluster, carries no `tenantScope` property at all.

```json
{
  "regionId": "eu-west",
  "clusterId": "cluster-eu",
  "isCurrent": false,
  "tenantScope": {
    "tenantId": "acme",
    "isAllowed": true,
    "status": "Online",
    "isResident": true
  }
}
```

Scoping is keyed on the **asserted active tenant**, not on the caller's role. An operator administering the platform sends no `lattice-active-tenant` header, so the operator path costs no extra authorization round trip - it simply never enters the scoping branch. This mirrors how `ITenantEnumerationFilter` scopes tree, tag-index, covered-tree, and view enumeration.

**Tenancy off is free.** The tenancy probe is a single ambient-context read - no service resolution, no allocation - and it is false whenever nothing stamped a tenant. A host with no tenancy add-on therefore keeps the original path and returns the router's frozen snapshot by reference, exactly as before.

`lattice_capabilities` is deliberately unchanged: it advertises only the **current** region's per-group endpoints, which the caller is already connected to, so it discloses no peer topology and needs no scoping.

## Permission-scoped discovery

Because the bridge resolves the caller's subject, the per-session discovery configurator can filter the advertised tool list to the caller's **effective permissions** before it is returned. A caller sees and can invoke only the tools its grants allow; an ungranted tool is never listed, so there is no "list then deny" gap. The `lattice_capabilities` meta-tool reports the same permission-scoped view.

## Least privilege by default

The server ships no tools. Each module is added explicitly, and within a module the destructive verbs (data writes, backup control, auth administration) stay hidden unless the host enables them. A minimal deployment exposes only the read tools it needs; a control deployment opts each destructive verb in deliberately.

## OAuth discovery is anonymous by design

When a host opts into OAuth 2.0 Protected Resource Metadata ([RFC 9728](https://www.rfc-editor.org/rfc/rfc9728)) by setting `ProtectedResourceMetadata` (see [Setup](setup.md#oauth-discovery-rfc-9728)), the metadata document at `/.well-known/oauth-protected-resource` is mapped with `AllowAnonymous()`, so it is served even when `RequireAuthorization` is `true` and the host installs a fail-closed fallback authorization policy. This does not weaken the posture: the document carries only public information a client needs to begin signing in - the resource identifier, the authorization server URLs, and the scopes to request - and never a token, a grant, or any cluster state. It must be anonymous because a client fetches it precisely because it was rejected with a `401`. The MCP transport itself stays default-denied; the only change to a `401` is an added `resource_metadata` hint pointing at that public document.

## Next

- [Tools](tools.md) - the modules, their opt-in flags, and the full catalogue.
- [Remote hosting](remote.md) - how the credential flows to a cluster over gRPC.

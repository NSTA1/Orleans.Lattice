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

## Permission-scoped discovery

Because the bridge resolves the caller's subject, the per-session discovery configurator can filter the advertised tool list to the caller's **effective permissions** before it is returned. A caller sees and can invoke only the tools its grants allow; an ungranted tool is never listed, so there is no "list then deny" gap. The `lattice_capabilities` meta-tool reports the same permission-scoped view.

## Least privilege by default

The server ships no tools. Each module is added explicitly, and within a module the destructive verbs (data writes, backup control, auth administration) stay hidden unless the host enables them. A minimal deployment exposes only the read tools it needs; a control deployment opts each destructive verb in deliberately.

## OAuth discovery is anonymous by design

When a host opts into OAuth 2.0 Protected Resource Metadata ([RFC 9728](https://www.rfc-editor.org/rfc/rfc9728)) by setting `ProtectedResourceMetadata` (see [Setup](setup.md#oauth-discovery-rfc-9728)), the metadata document at `/.well-known/oauth-protected-resource` is mapped with `AllowAnonymous()`, so it is served even when `RequireAuthorization` is `true` and the host installs a fail-closed fallback authorization policy. This does not weaken the posture: the document carries only public information a client needs to begin signing in - the resource identifier, the authorization server URLs, and the scopes to request - and never a token, a grant, or any cluster state. It must be anonymous because a client fetches it precisely because it was rejected with a `401`. The MCP transport itself stays default-denied; the only change to a `401` is an added `resource_metadata` hint pointing at that public document.

## Next

- [Tools](tools.md) - the modules, their opt-in flags, and the full catalogue.
- [Remote hosting](remote.md) - how the credential flows to a cluster over gRPC.

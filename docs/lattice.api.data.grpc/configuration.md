---
agent_spec: "docs/agents/api/data.json"
---

# Configuration

`AddLatticeDataApiGrpc(Action<LatticeDataApiGrpcOptions>? configure = null)` binds `LatticeDataApiGrpcOptions`, the only options type in this package. It configures the server side of the binding: whether the transport authorizer runs, and which request headers the default identity bridges read. The client has no options; its transport policy lives on the `GrpcChannel` or `CallInvoker` passed to `LatticeDataApiGrpcClient.Create`.

The data facade's own range-read and range-delete knobs are `LatticeApiDataOptions`, documented in the [data facade configuration](../lattice.api.data/configuration.md).

## `LatticeDataApiGrpcOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the authorization interceptor consults `ILatticeDataApiAuthorizer` before every data-API RPC. Default-deny: with the default `DenyAllDataApiAuthorizer` every call is refused with `PermissionDenied` until the host registers a permissive authorizer. Set `false` only when an outer boundary already guards the endpoint; the core access gate still authorizes each operation. The value is read per call, so a change through the options monitor applies to the next call. |
| `CredentialHeaderName` | `string` | `"authorization"` | Request-metadata header the default credential bridge reads. The lookup is case-insensitive (gRPC metadata keys are lower-case). An empty value disables the bridge, leaving every caller anonymous. |
| `CredentialScheme` | `string` | `"Bearer"` | Scheme stamped on the bridged credential. When the header value starts with this scheme (case-insensitive) followed by whitespace, the prefix is stripped; a bare scheme with no token reads as no credential. An empty value disables prefix stripping and stamps no scheme. |
| `ActiveTenantHeaderName` | `string` | `"lattice-active-tenant"` | Request-metadata header the default active-tenant bridge reads. A missing, blank or syntactically invalid value asserts no tenant. The assertion is re-validated against the caller's membership downstream. An empty value disables header-based tenant selection. |

No validator is registered for these options; the empty-string behaviours above are the defined outcomes of an empty value rather than a rejected configuration.

## Registration-time seams

`AddLatticeDataApiGrpc` registers each seam with `TryAdd`, so an implementation the host registered earlier is kept. Register a replacement before calling it.

| Seam | Registered default | Replace it to |
|---|---|---|
| `ILatticeDataApiAuthorizer` | `DenyAllDataApiAuthorizer` | Admit callers, for example with `AllowAllDataApiAuthorizer` on a trusted network or a custom implementation that inspects `LatticeDataApiAuthorizationContext`. |
| `ILatticeDataApiCredentialBridge` | header-based bridge using `CredentialHeaderName` and `CredentialScheme` | Source the caller identity elsewhere, such as a client certificate. |
| `ILatticeDataApiActiveTenantBridge` | header-based bridge using `ActiveTenantHeaderName` | Source the asserted tenant elsewhere, such as a principal claim. |

`AddLatticeDataApiGrpc` also appends the authorization interceptor to the host's gRPC pipeline - on every call, so call it once. The interceptor acts only on methods under the `orleans.lattice.api.data` service, so other gRPC services on the same host are unaffected.

## Source map

- [Options](../../src/lattice.api.data.grpc/LatticeDataApiGrpcOptions.cs)
- [Registration](../../src/lattice.api.data.grpc/LatticeDataApiGrpcServiceCollectionExtensions.cs)
- [Credential bridge seam](../../src/lattice.api.data.grpc/ILatticeDataApiCredentialBridge.cs)
- [Active-tenant bridge seam](../../src/lattice.api.data.grpc/ILatticeDataApiActiveTenantBridge.cs)
- [Authorizer seam](../../src/lattice.api.data.grpc/Security/ILatticeDataApiAuthorizer.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [Data facade configuration](../lattice.api.data/configuration.md)

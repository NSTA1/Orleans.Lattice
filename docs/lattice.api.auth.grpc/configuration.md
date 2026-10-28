---
agent_spec: "docs/agents/api/auth.json"
---

# Orleans.Lattice.Api.Auth.Grpc configuration

The binding has one options type, `LatticeAuthApiGrpcOptions`, bound through `AddLatticeAuthApiGrpc(configure)` on the host's `IServiceCollection`. When `configure` is omitted the options are still registered with their defaults. The options have no validator: a value is used as supplied, and the effect of an empty value is described per property below. Because the binding is the remote control plane for authorization itself, the defaults are fail-closed.

The facade's own option, `LatticeApiAuthOptions.MaxExplanationRules`, is configured on the silo through `AddLatticeAuthApi` and documented in the [facade configuration](../lattice.api.auth/configuration.md).

## `LatticeAuthApiGrpcOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the authorization interceptor consults the registered `ILatticeAuthApiAuthorizer` on every call to the auth-API service. With the default `DenyAllAuthApiAuthorizer` every call is rejected with `PermissionDenied` until the host registers a permissive authorizer or sets this to `false`. Turning it off does not open the surface: the facade's per-call administrator check still runs against the resolved caller. Read on every call, so a change through `IOptionsMonitor` takes effect without a restart. |
| `CredentialHeaderName` | `string` | `"authorization"` | The request header (gRPC metadata key) the default credential bridge reads for the caller's token. The lookup is lower-cased to match gRPC's metadata keys. `null` or empty disables the bridge, so every caller is anonymous and is denied by the facade. |
| `CredentialScheme` | `string` | `"Bearer"` | The scheme stamped on the bridged `LatticeCredential`, matched by a registered `ILatticeCredentialAuthenticator`. A leading scheme prefix on the header value, compared case-insensitively and followed by whitespace, is stripped; a header holding only the scheme is treated as no credential. `null` or empty strips nothing and stamps no scheme. |
| `ActiveTenantHeaderName` | `string?` | `"lattice-active-tenant"` | The request header carrying the caller's asserted active tenant. Honoured only by a `ListRules` call whose `AuthPageRequest.ActiveTenantOnly` is set; every other call is cluster-wide and ignores it. The facade re-validates the assertion against the caller's own membership, and an assertion the caller may not make fails the call with `PermissionDenied`. `null` or empty disables it, so a narrowed listing resolves the reserved default tenant. |

The default value of `ActiveTenantHeaderName` is the core constant `LatticeActiveTenantAssertion.DefaultHeaderName`.

## Choosing an authentication scheme

The credential bridge only carries the token; a credential authenticator registered on the silo validates it (see [Registering an authenticator](../lattice.membership/README.md#registering-an-authenticator)). Because the bridge always stamps `CredentialScheme`, a JWT authenticator that must serve these calls sets its `SchemeHint` to the same value. The Entra and OIDC authenticators fall through to their tenant or issuer match when the scheme does not match their hint, so they need no hint for this.

## Related

- [Binding guide](README.md) - including a [Quick Start](README.md#quick-start) registration.
- [Public API](api.md)
- [Architecture](architecture.md)
- [Facade configuration](../lattice.api.auth/configuration.md)

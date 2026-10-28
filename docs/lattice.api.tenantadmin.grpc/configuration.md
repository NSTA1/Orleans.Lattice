---
agent_spec: "docs/agents/api/tenantadmin.json"
---

# Orleans.Lattice.Api.TenantAdmin.Grpc configuration

The package has one public options type, `LatticeTenantAdminApiGrpcOptions`, which configures the server-side binding and is bound through `AddLatticeTenantAdminApiGrpc(configure)`. The two typed clients, `LatticeTenantAdminApiGrpcClient` and `LatticeTenantSelfServiceApiGrpcClient`, have no options of their own: their transport is the `CallInvoker` the caller supplies. The facades this binding serves have their own configuration, documented in the [facade configuration](../lattice.api.tenantadmin/configuration.md); delegated tenant access administration in particular is switched on by the tenancy engine, not by this package.

Tenant administration is a security surface. None of the settings below confers tenant or operator authority: the facades authorize every caller themselves, whatever the transport gate decided.

## `LatticeTenantAdminApiGrpcOptions`

| Property | Type | Default | Read | Meaning |
|---|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | On every call | Whether the authorization interceptor consults the registered `ILatticeTenantAdminApiAuthorizer` before an enforced RPC runs. With the default and the default-deny authorizer, every enforced call is refused. Set it to `false` only when an outer authentication boundary already guards the endpoint; the facades' own checks still run. `GetAuthScheme` and the three self-service RPCs are never sent to the authorizer, whatever this value is. |
| `CredentialHeaderName` | `string` | `"authorization"` | Once, when the default credential bridge first reads its options | The request header (gRPC metadata) the default credential bridge reads the caller's token from, on every call except `GetAuthScheme`. Matched case-insensitively. A `null` or empty name disables the bridge, so every caller is anonymous. |
| `CredentialScheme` | `string` | `"Bearer"` | Once, as above | The scheme stamped on the bridged credential, which a registered credential authenticator matches to resolve the caller's subject. A case-insensitive scheme prefix followed by whitespace is stripped from the header value; a bare scheme with no token leaves the caller anonymous. With a `null` or empty scheme nothing is stripped and the credential carries no scheme. |
| `ActiveTenantHeaderName` | `string` | `"lattice-active-tenant"` (`LatticeActiveTenantAssertion.DefaultHeaderName`) | Once, when the service first reads its options | The request header carrying the tenant the caller asserts it is acting as, stamped onto the call's ambient active-tenant scope. A `null` or empty name disables header-based tenant selection. An absent, blank or syntactically invalid value asserts no tenant, and the caller resolves the reserved `default` tenant. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` (get-only) | empty | On every `GetAuthScheme` call | The schemes the unauthenticated `GetAuthScheme` RPC advertises, in preference order. Add descriptors to the list inside the configure delegate. Each descriptor must carry only public configuration, never a secret, because the RPC answers callers that hold no credential. |

"Read" records how the binding consumes each value. `RequireAuthorization` and `AdvertisedAuthSchemes` are read through `IOptionsMonitor<LatticeTenantAdminApiGrpcOptions>.CurrentValue`, so an options source that reloads changes them for later calls. The header names and the scheme are read through `IOptions<LatticeTenantAdminApiGrpcOptions>.Value` by singletons, so they are fixed for the life of the process.

The binding registers no options validation. An empty header name is not rejected: it switches the corresponding header off, as the table describes.

The asserted tenant is a claim, not a grant. What it scopes on each RPC group - the self-service view, and which groups authorize on the named tenant instead - is described in the README's [per-tenant selection](README.md#per-tenant-selection).

### `AuthSchemeDescriptor`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `SchemeId` | `string` (`required`) | - | The stable scheme id a client matches to a login provider, for example `basic` or `entra`. |
| `DisplayName` | `string` | `""` | A human-readable name for the scheme. |
| `Parameters` | `IReadOnlyDictionary<string, string>` | empty | The public parameters a client needs to run the sign-in challenge, for example an authority, tenant, client id or audience. |

## Registration

`AddLatticeTenantAdminApiGrpc(this IServiceCollection services, Action<LatticeTenantAdminApiGrpcOptions>? configure = null)`:

- `configure` is optional. When supplied it is registered with `services.Configure(configure)`; when omitted the options are registered with their defaults.
- The default seams are registered with `TryAddSingleton`, so a host replaces one by registering its own implementation of the public interface **before** this call, or with `AddSingleton` afterwards (the last registration of a service wins):
  - `ILatticeTenantAdminApiAuthorizer` - default `DenyTenantAdminApiAuthorizer`. Registering a permissive authorizer, such as `AllowAllTenantAdminApiAuthorizer` or a host policy, is how the enforced RPCs are opened.
  - `ILatticeTenantAdminApiCredentialBridge` - default reads `CredentialHeaderName` and `CredentialScheme`. Replace it for another identity source, such as a client certificate.
  - `ILatticeTenantAdminApiAuthSchemeSource` - default advertises `AdvertisedAuthSchemes`.
- The authorization interceptor is appended to the global gRPC options on every call, so calling the method twice authorizes every call to this service twice. Call it once.
- It also calls `AddGrpc`, so the host does not need to.

`MapLatticeTenantAdminApiGrpc(this IEndpointRouteBuilder endpoints)` takes no configuration. It must run after `AddLatticeTenantAdminApiGrpc`, on a host whose service provider exposes the facades.

## Host prerequisites

The binding adapts the in-process facades, so the same host must register them: `AddLatticeTenantAdminApi` on the silo builder, which requires `AddLatticeTenancy` first (see the [facade README](../lattice.api.tenantadmin/README.md#registration)). The service requires `ILatticeTenantAdmin` and `ILatticeTenantSelfService`. `ILatticeTenantRegionAdmin`, `ILatticeTenantQuotaUsage`, `ILatticeTenantAccessAdmin`, `ILatticeTenantGrantAdmin`, `ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin` are optional: `AddLatticeTenantAdminApi` registers all of them, and a host that composes the binding without one still starts and answers that facade's RPCs `Unimplemented`.

Settings outside this package decide what the bridged credential and the asserted tenant achieve:

- Without the `Orleans.Lattice.Auth` add-on the core no-op access gate ignores the bridged credential and allows the facades' platform-operator check, so the transport authorizer is the only barrier. With the add-on registered, that check is evaluated on the control plane and refuses an unmatched caller whatever `LatticeAuthOptions.DefaultEffect` is set to. See [`Orleans.Lattice.Auth`](../lattice.auth/README.md).
- The tenancy engine the facades require re-validates every asserted tenant against the caller's membership, and its `LatticeTenancyOptions.DelegatedAccessAdministrationEnabled` (default `false`) decides whether the directory and policy RPCs are served: while it is off they refuse an authorized caller with `FailedPrecondition`, except `GetTenantAccessPosture`, which still answers. See the [tenancy configuration](../lattice.tenancy/configuration.md).

## Client transport

Both clients are configured entirely through the `CallInvoker` passed to `Create`:

- **Address, TLS, deadlines and retries** are set on the `GrpcChannel` or `CallInvoker`. The binding configures no listener, certificate or TLS policy of its own on either side: the server's endpoints and certificates come from the ASP.NET Core host. Because the default credential is a bearer token carried in request metadata, serve the endpoint over TLS outside a trusted network.
- **Credential and tenant headers** are not added by the clients. Attach the header the server reads (`CredentialHeaderName`, value prefixed with `CredentialScheme`) and, when acting as a tenant, the `ActiveTenantHeaderName` header, through the channel or a client interceptor. Without the credential header the caller is anonymous, and `ListAccessibleTenants` then returns an empty list rather than an error, so a missing header can look like a caller with no tenants.
- **Serialization**: the `IServiceProvider` passed to `Create` must have Orleans serialization registered (`AddSerializer()`) so the client's marshallers match the server's.

## Related

- [Package README](README.md)
- [API reference](api.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Api.TenantAdmin` configuration](../lattice.api.tenantadmin/configuration.md)

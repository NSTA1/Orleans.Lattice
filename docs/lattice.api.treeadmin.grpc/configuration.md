---
agent_spec: "docs/agents/api/treeadmin.json"
---

# Orleans.Lattice.Api.TreeAdmin.Grpc configuration

The package has one public options type, `LatticeTreeAdminApiGrpcOptions`, which configures the server-side binding and is bound through `AddLatticeTreeAdminApiGrpc(configure)`. The typed client, `LatticeTreeAdminApiGrpcClient`, has no options of its own: its transport is the `CallInvoker` the caller supplies. The facade this binding serves has its own (currently empty) options, documented in the [facade configuration](../lattice.api.treeadmin/configuration.md).

## `LatticeTreeAdminApiGrpcOptions`

| Property | Type | Default | Read | Meaning |
|---|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | On every call | Whether the authorization interceptor consults the registered `ILatticeTreeAdminApiAuthorizer` before a protected RPC runs. With the default and the default-deny authorizer, every protected call is refused. Set it to `false` only when an outer authentication boundary already guards the endpoint; the facade's own access gate still runs. |
| `CredentialHeaderName` | `string` | `"authorization"` | Once, when the default credential bridge first reads its options | The request header (gRPC metadata) the default credential bridge reads the caller's token from. Matched case-insensitively. A `null` or empty name disables the bridge, so every caller is anonymous. |
| `CredentialScheme` | `string` | `"Bearer"` | Once, as above | The scheme stamped on the bridged credential, which a registered credential authenticator matches to resolve the caller's subject. A case-insensitive scheme prefix followed by whitespace is stripped from the header value; a bare scheme with no token leaves the caller anonymous. With a `null` or empty scheme nothing is stripped and the credential carries no scheme. |
| `ActiveTenantHeaderName` | `string` | `"lattice-active-tenant"` (`LatticeActiveTenantAssertion.DefaultHeaderName`) | Once, when the service first reads its options | The request header carrying the tenant the caller asserts it is acting as, stamped onto the call's ambient active-tenant scope. A `null` or empty name disables header-based tenant selection. An absent, blank or syntactically invalid value asserts no tenant. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` (get-only) | empty | On every `GetAuthScheme` call | The schemes the unauthenticated `GetAuthScheme` RPC advertises, in preference order. Add descriptors to the list inside the configure delegate. Each descriptor must carry only public configuration, never a secret, because the RPC answers callers that hold no credential. |

"Read" records how the binding consumes each value. `RequireAuthorization` and `AdvertisedAuthSchemes` are read through `IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>.CurrentValue`, so an options source that reloads changes them for later calls. The header names and the scheme are read through `IOptions<LatticeTreeAdminApiGrpcOptions>.Value` by singletons, so they are fixed for the life of the process.

The binding registers no options validation. An empty header name is not rejected: it switches the corresponding header off, as the table describes.

### `AuthSchemeDescriptor`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `SchemeId` | `string` (`required`) | - | The stable scheme id a client matches to a login provider, for example `basic` or `entra`. |
| `DisplayName` | `string` | `""` | A human-readable name for the scheme. |
| `Parameters` | `IReadOnlyDictionary<string, string>` | empty | The public parameters a client needs to run the sign-in challenge, for example an authority, tenant, client id or audience. |

## Registration

`AddLatticeTreeAdminApiGrpc(this IServiceCollection services, Action<LatticeTreeAdminApiGrpcOptions>? configure = null)`:

- `configure` is optional. When supplied it is registered with `services.Configure(configure)`; when omitted the options are registered with their defaults.
- The default seams are registered with `TryAddSingleton`, so a host replaces one by registering its own implementation of the public interface **before** this call, or with `AddSingleton` afterwards (the last registration of a service wins):
  - `ILatticeTreeAdminApiAuthorizer` - default `DenyTreeAdminApiAuthorizer`. Registering a permissive authorizer, such as `AllowAllTreeAdminApiAuthorizer` or a host policy, is how the surface is opened.
  - `ILatticeTreeAdminApiCredentialBridge` - default reads `CredentialHeaderName` and `CredentialScheme`. Replace it for another identity source, such as a client certificate.
  - `ILatticeTreeAdminApiAuthSchemeSource` - default advertises `AdvertisedAuthSchemes`.
- The authorization interceptor is appended to the global gRPC options on every call, so calling the method twice authorizes every call to this service twice. Call it once.
- It also calls `AddGrpc`, so the host does not need to.

`MapLatticeTreeAdminApiGrpc(this IEndpointRouteBuilder endpoints)` takes no configuration. It must run after `AddLatticeTreeAdminApiGrpc`, on a host whose service provider exposes `ILatticeTreeAdmin`.

## Host prerequisites

The binding adapts the in-process facade, so the same host must register it: `AddLatticeTreeAdminApi` on the silo builder, which itself requires `AddLatticeSchemaApi` first (see the [facade README](../lattice.api.treeadmin/README.md)). `AddLatticeTreeAdminApi` also registers `ILatticeTreeAdminOperations`, `ILatticeStorageUsageOperations` and `ILatticeWalReclamation`; a host that composes the binding without one of them still starts, and answers that facade's RPCs `Unimplemented`.

Two settings outside this package decide what the bridged credential and the asserted tenant achieve:

- Without the `Orleans.Lattice.Auth` add-on the core access gate ignores the bridged credential and allows every call, so the transport authorizer is the only barrier. With the add-on registered, `LatticeAuthOptions.DefaultEffect` defaults to `Deny`. See [`Orleans.Lattice.Auth`](../lattice.auth/README.md).
- Without the tenancy add-on, an asserted tenant resolves to the reserved `default` tenant and changes nothing. With it, the assertion is re-validated against the caller's membership. See the README's [per-tenant selection](README.md#per-tenant-selection).

## Client transport

The client is configured entirely through the `CallInvoker` passed to `LatticeTreeAdminApiGrpcClient.Create`:

- **Address, TLS, deadlines and retries** are set on the `GrpcChannel` or `CallInvoker`. The binding configures no listener, certificate or TLS policy of its own on either side: the server's endpoints and certificates come from the ASP.NET Core host. Because the default credential is a bearer token carried in request metadata, serve the endpoint over TLS outside a trusted network.
- **Credential and tenant headers** are not added by the client. Attach the header the server reads (`CredentialHeaderName`, value prefixed with `CredentialScheme`) and, when acting as a tenant, the `ActiveTenantHeaderName` header, through the channel or a client interceptor.
- **Serialization**: the `IServiceProvider` passed to `Create` must have Orleans serialization registered (`AddSerializer()`) so the client's marshallers match the server's.

## Related

- [Package README](README.md)
- [API reference](api.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Api.TreeAdmin` configuration](../lattice.api.treeadmin/configuration.md)

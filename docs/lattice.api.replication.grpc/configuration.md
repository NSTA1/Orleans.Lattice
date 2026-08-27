# Orleans.Lattice.Api.Replication.Grpc configuration

The package has one public options type, `LatticeReplicationApiGrpcOptions`, bound through `AddLatticeReplicationApiGrpc(configure)` and resolvable via `IOptions<LatticeReplicationApiGrpcOptions>`.

## `LatticeReplicationApiGrpcOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the authorization interceptor enforces `ILatticeReplicationApiAuthorizer` on every inbound operation RPC. Default-deny: the binding fails closed unless a host registers a permissive authorizer or explicitly turns enforcement off. Set to `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `"authorization"` | The inbound request-header (gRPC metadata) name carrying the caller's credential token, bridged into the ambient Lattice credential so the access gate can resolve the caller's subject. Only consulted when auth-backed replication control is active (`Orleans.Lattice.Auth` is registered). |
| `CredentialScheme` | `string` | `"Bearer"` | The authentication scheme stamped on the bridged credential, matched by a registered `ILatticeCredentialAuthenticator`. A case-insensitive scheme prefix on the header value (for example `"Bearer "`) is stripped before the remaining token is used. |
| `ActiveTenantHeaderName` | `string` | `lattice-active-tenant` | The inbound request-header (gRPC metadata) name carrying the tenant the caller is acting as, lifted onto the ambient active-tenant scope for the duration of the call so replication verbs resolve tree names in that tenant's namespace. Set to an empty string to disable header-based tenant selection. Only consulted when the tenancy add-on is registered. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | empty | The auth schemes the endpoint advertises from its unauthenticated `GetAuthScheme` RPC, in preference order. Empty by default. Each descriptor must carry only public configuration - never a secret. |

## Transport policy lives on the caller

Address, TLS, retries, deadlines, and client credentials are not configured here: they belong to the caller's `GrpcChannel` / `CallInvoker`. This package configures only what the server-side binding needs to authorize and marshal.

## What is configured elsewhere

The control semantics - which trees may be enabled, the merge modes, the fail-closed access gate - are the facade's and engine's concern, configured on [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/configuration.md) and [`Orleans.Lattice.Replication`](../lattice.replication/configuration.md).

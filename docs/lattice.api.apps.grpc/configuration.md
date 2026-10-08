# Configuration

`AddLatticeAppsApiGrpc(options => ...)` configures transport behavior. The credential bridge reads the metadata credential header and forwards it to the facade call context. The active-tenant header is an assertion for the facade's tenant-resolution path, not an unchecked tenant override. `AdvertisedAuthSchemes` is a get-only list populated through the options object.

## `LatticeAppsApiGrpcOptions`

| Option | Type | Default | Mutability and effect |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Enforces the transport authorizer for app RPCs. Disable only behind an outer authentication boundary; facade authorization still applies. |
| `CredentialHeaderName` | `string` | `authorization` | Metadata key from which the credential bridge reads the credential. |
| `CredentialScheme` | `string` | `Bearer` | Optional prefix stripped from the credential and retained as its authentication scheme. |
| `ActiveTenantHeaderName` | `string` | `lattice-active-tenant` (`LatticeActiveTenantAssertion.DefaultHeaderName`) | Active-tenant assertion header. Null or empty disables it; facades validate any assertion. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` (get-only) | Empty list | Public sign-in descriptors returned by `GetAuthScheme`; never put credentials or user-specific data here. |

`GetAuthScheme` is exempt from transport authorization so a client can discover public sign-in scheme descriptors before presenting a credential. It does not return credential values.

## See also

- [Public API](api.md)
- [Architecture](architecture.md)
- [gRPC binding guide](README.md)
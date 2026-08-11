# Orleans.Lattice.Membership.Entra configuration

The package has one public options type, `LatticeEntraAuthenticatorOptions`, which configures a single Entra credential authenticator: the Entra authority it discovers OIDC metadata from, the tenant allow-list and audiences it accepts, and how it resolves overflowed group membership. It is bound per Entra application by the `AddEntraCredentialAuthenticator` registration extension, so a silo can trust several Entra apps at once alongside other issuers.

## `LatticeEntraAuthenticatorOptions`

Bind it through `AddEntraCredentialAuthenticator(configure)`.

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `DefaultIssuerTemplate` | `string` | `"https://login.microsoftonline.com/{tenantid}/v2.0"` | The default Entra v2.0 issuer template. `{tenantid}` is replaced with each token's tenant id when validating the issuer, so both single-tenant and multi-tenant tokens validate against one template. |
| `DefaultAuthorityHost` | `string` | `"https://login.microsoftonline.com"` | The default Entra login host used to derive the OIDC metadata address. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Authority` | `string` | `""` (empty) | The Entra authority the OIDC metadata is discovered from, for example `https://login.microsoftonline.com/common/v2.0` (multi-tenant) or `https://login.microsoftonline.com/{tenant-guid}/v2.0` (single-tenant). Must be set. When `MetadataAddress` is unset the discovery document address is derived from this value. |
| `MetadataAddress` | `string?` | `null` | The explicit OIDC discovery document address. When `null` it is derived from `Authority` by appending `/.well-known/openid-configuration`. |
| `IssuerTemplate` | `string` | `DefaultIssuerTemplate` | The issuer template validated against each token, with `{tenantid}` substituted by the token's tenant id. |
| `TenantIds` | `IList<string>` | empty list | The tenant ids (Entra `tid` values) this authenticator accepts. A single entry is single-tenant; several entries form a multi-tenant allow-list. A token whose `tid` is not in this set is not handled and resolution falls through to the next authenticator. Must contain at least one entry. Populate the collection in place. |
| `Audiences` | `IList<string>` | empty list | The audiences accepted (the token `aud` claim), typically the Entra application (client) id or its Application ID URI. Must contain at least one entry. Populate the collection in place. |
| `Algorithms` | `IList<string>` | `["RS256"]` | The token signature algorithms accepted (the JWT header `alg`), pinned via `ValidAlgorithms`. Defaults to `RS256`, the algorithm Entra issues v2.0 tokens with, so a token advertising any other algorithm is rejected (defense-in-depth against algorithm-confusion attacks). Clear and repopulate to accept a different set; empty disables pinning. Populate the collection in place. |
| `SchemeHint` | `string?` | `null` | Optional scheme hint. When set, a credential whose scheme equals this value selects this authenticator without the token being parsed. `null` selects solely by tenant / issuer. |
| `GroupResolutionMode` | `EntraGroupResolutionMode` | `TokenOnly` | How overflowed group membership is resolved. |
| `ValidateLifetime` | `bool` | `true` | Whether to validate the token lifetime (`exp` / `nbf`). |
| `ClockSkew` | `TimeSpan` | `5 minutes` | The permitted clock skew during lifetime validation. |
| `AutomaticRefreshInterval` | `TimeSpan` | `12 hours` | How often the discovered JWKS metadata is proactively refreshed. |
| `RefreshInterval` | `TimeSpan` | `5 minutes` | The minimum interval between forced JWKS refreshes. |

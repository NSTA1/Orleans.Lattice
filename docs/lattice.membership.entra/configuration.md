# Orleans.Lattice.Membership.Entra configuration

The package has one public options type, `LatticeEntraAuthenticatorOptions`, which configures a single Entra credential authenticator: the Entra authority it discovers OIDC metadata from, the tenant allow-list and audiences it accepts, and how it resolves overflowed group membership. It is bound per Entra application by the `AddEntraCredentialAuthenticator` registration extension, so a silo can trust several Entra apps at once alongside other issuers.

## `LatticeEntraAuthenticatorOptions`

Bind it through `AddEntraCredentialAuthenticator(configure)`. Each call registers one more authenticator; its options are validated when that authenticator is built, and a violation of any constraint in the tables below throws `OptionsValidationException` naming the option.

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `DefaultIssuerTemplate` | `string` | `"https://login.microsoftonline.com/{tenantid}/v2.0"` | The default Entra v2.0 issuer template. `{tenantid}` is replaced with each token's tenant id when validating the issuer, so both single-tenant and multi-tenant tokens validate against one template. |
| `DefaultAuthorityHost` | `string` | `"https://login.microsoftonline.com"` | The default Entra login host used to derive the OIDC metadata address. |
| `DefaultAlgorithm` | `string` | `"RS256"` | The default token signature algorithm Entra issues v2.0 tokens with (`SecurityAlgorithms.RsaSha256`). `Algorithms` is pre-populated with this single value. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Authority` | `string` | `""` (empty) | The Entra authority the OIDC metadata is discovered from, for example `https://login.microsoftonline.com/common/v2.0` (multi-tenant) or `https://login.microsoftonline.com/{tenant-guid}/v2.0` (single-tenant). Must be set. When `MetadataAddress` is unset the discovery document address is derived from this value. |
| `MetadataAddress` | `string?` | `null` | The explicit OIDC discovery document address. When `null` or blank it is derived from `Authority` (with any trailing `/` trimmed) by appending `/.well-known/openid-configuration`. |
| `IssuerTemplate` | `string` | `DefaultIssuerTemplate` | The issuer template validated against each token, with `{tenantid}` substituted by the token's tenant id. Must contain the `{tenantid}` placeholder. |
| `TenantIds` | `IList<string>` | empty list | The tenant ids (Entra `tid` values) this authenticator accepts, compared case-insensitively. A single entry is single-tenant; several entries form a multi-tenant allow-list. A token whose `tid` is not in this set is not handled on tenant grounds and resolution falls through to the next authenticator - **unless** `SchemeHint` is set and the credential's scheme matches it, in which case this authenticator claims the credential before the tenant check runs (see `SchemeHint`), so a scheme-tagged token from a disallowed tenant resolves to anonymous rather than falling through. Must contain at least one entry, and no null or empty entry. Populate the collection in place. |
| `Audiences` | `IList<string>` | empty list | The audiences accepted (the token `aud` claim), typically the Entra application (client) id or its Application ID URI. Must contain at least one entry, and no null or empty entry. Populate the collection in place. |
| `Algorithms` | `IList<string>` | `["RS256"]` | The token signature algorithms accepted (the JWT header `alg`), pinned via `ValidAlgorithms`. Defaults to `RS256`, the algorithm Entra issues v2.0 tokens with, so a token advertising any other algorithm is rejected (defense-in-depth against algorithm-confusion attacks). Clear and repopulate to accept a different set. Must contain at least one entry, and no null or empty entry: an empty list is refused by options validation when the authenticator is built rather than read as "accept any algorithm", and the authenticator independently denies every token should an empty list reach it by a path that bypasses options validation. Populate the collection in place. |
| `SchemeHint` | `string?` | `null` | Optional scheme hint. When set, a credential whose scheme equals this value selects this authenticator without the token being parsed. `null` selects solely by tenant / issuer. |
| `GroupResolutionMode` | `EntraGroupResolutionMode` | `TokenOnly` | How overflowed group membership is resolved. `TokenOnly` never makes an external lookup: the token-asserted groups and roles stand, and the directory merge upstream fills in the rest. `ResolveOnOverage` consults the registered `IEntraGroupResolver` when a token carries the overage marker in place of its `groups` claim, and falls back to the token-only behaviour when no resolver is registered. Must be a defined `EntraGroupResolutionMode` value. |
| `ValidateLifetime` | `bool` | `true` | Whether to validate the token lifetime (`exp` / `nbf`). |
| `ClockSkew` | `TimeSpan` | `5 minutes` | The permitted clock skew during lifetime validation. Must not be negative. |
| `AutomaticRefreshInterval` | `TimeSpan` | `12 hours` | How often the discovered JWKS metadata is proactively refreshed. Must be strictly positive. |
| `RefreshInterval` | `TimeSpan` | `5 minutes` | The minimum interval between forced JWKS refreshes. Must be strictly positive. |

### Methods

| Method | Returns | Meaning |
|---|---|---|
| `ResolveMetadataAddress()` | `string` | The OIDC discovery document address: `MetadataAddress` when it holds a non-blank value, otherwise `Authority` (trailing `/` trimmed) with `/.well-known/openid-configuration` appended; an empty string when both are blank, which the authenticator rejects at construction. |

## `EntraClaimNames`

The Entra v2.0 token claim names the package defines. The authenticator reads `oid`, `sub`, `tid`, `groups`, `roles`, and the `_claim_names` overage marker; every claim the token carries, including `scp`, `azp`, and `_claim_sources`, is also copied into the resolved subject's claim bag. The names are fixed - the Entra options expose no claim-type knobs - so use the constants instead of string literals when reading that claim bag, for example from a `LatticeMembershipOptions.ClaimToGroups` projection.

| Constant | Value | Meaning |
|---|---|---|
| `ObjectId` | `"oid"` | The immutable object id of the user or service principal: the subject id. |
| `Subject` | `"sub"` | The fallback subject id when `oid` is absent. |
| `TenantId` | `"tid"` | The tenant the token was issued for, checked against `TenantIds`. |
| `Groups` | `"groups"` | The security group object ids, asserted as groups. |
| `Roles` | `"roles"` | The application role values, also asserted as groups. |
| `Scope` | `"scp"` | The delegated permission scopes on a user token; carried in the claim bag only. |
| `AuthorizedParty` | `"azp"` | The authorized party (client id) on an app-only token; carried in the claim bag only. |
| `ClaimNames` | `"_claim_names"` | The overage marker; present without a `groups` claim, it signals groups overage. |
| `ClaimSources` | `"_claim_sources"` | The overage marker describing where an overflowed claim can be retrieved; carried in the claim bag only. |

# Orleans.Lattice.Membership.Oidc configuration

The package has one public options type, `LatticeOidcAuthenticatorOptions`, which configures a single generic OpenID Connect credential authenticator: the authority its discovery document is fetched from, the exact issuer and the audiences it accepts, and the claim names it reads the subject and group membership out of. It is bound per issuer by the `AddLatticeOidc` registration extension, so a silo can trust several OIDC providers at once alongside the Entra, basic, and anonymous authenticators.

## `LatticeOidcAuthenticatorOptions`

Bind it through `AddLatticeOidc(configure)`.

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Authority` | `string` | `""` (empty) | The OpenID Connect authority the discovery document is fetched from, for example `https://dev-123456.okta.com/oauth2/default` or `https://keycloak.example.com/realms/lattice`. Must be set. When `MetadataAddress` is unset the discovery document address is derived from this value. |
| `MetadataAddress` | `string?` | `null` | The explicit OIDC discovery document address. When `null` it is derived from `Authority` by appending `/.well-known/openid-configuration`. Set it explicitly for a provider that publishes its metadata somewhere other than the conventional path. |
| `Issuer` | `string` | `""` (empty) | The exact issuer (the token `iss` claim) this authenticator accepts. Must be set. Matching is ordinal and exact - there is no prefix, wildcard, or catch-all form - so a token from any other issuer is not handled and resolution falls through to the next authenticator. |
| `Audiences` | `IList<string>` | empty list | The audiences accepted (the token `aud` claim), typically the OAuth client id or an API identifier registered with the provider. Must contain at least one entry: audience validation is always enforced, so an empty list throws at construction rather than silently accepting every audience. Populate the collection in place. |
| `Algorithms` | `IList<string>` | empty list | The token signature algorithms accepted (the JWT header `alg`). Empty - the default - pins the algorithms the provider advertises in its discovery document's `id_token_signing_alg_values_supported`. Populate it to pin an explicit, narrower set. Pinning is always enforced: an empty list never means "accept any algorithm", and a provider that advertises no algorithms at all rejects every token. Populate the collection in place. |
| `SubjectClaimTypes` | `IList<string>` | `["sub"]` | The claim types, in priority order, read for the subject identifier. The first claim present on the validated token wins. Matched by exact claim name, not by JSON path - a dotted entry such as `realm_access.roles` matches nothing, because nested JSON objects are not flattened into dotted claim names. Populate the collection in place. |
| `GroupClaimTypes` | `IList<string>` | `["groups", "roles", "role"]` | The claim types read for group membership. Every value found across every listed claim type is asserted. Matched by exact claim name, not by JSON path (see `SubjectClaimTypes`); configure the provider to emit memberships as a top-level claim. Clear the collection to disable token-asserted groups entirely. Populate the collection in place. |
| `SchemeHint` | `string?` | `null` | Optional scheme hint. When set, a credential whose scheme equals this value (compared case-insensitively) selects this authenticator without the token being parsed. `null` selects solely by exact issuer match. A credential whose scheme is set but does *not* match still falls through to the issuer match rather than being declined outright, because the credential bridges stamp the scheme from operator configuration rather than from the caller - so a hint that did not match is not evidence the token belongs elsewhere. |
| `ValidateLifetime` | `bool` | `true` | Whether to validate the token lifetime (`exp` / `nbf`). |
| `ClockSkew` | `TimeSpan` | `5 minutes` | The permitted clock skew during lifetime validation. |
| `AutomaticRefreshInterval` | `TimeSpan` | `12 hours` | How often the discovered JWKS metadata is proactively refreshed. |
| `RefreshInterval` | `TimeSpan` | `5 minutes` | The minimum interval between forced JWKS refreshes. |

### Methods

| Method | Returns | Meaning |
|---|---|---|
| `ResolveMetadataAddress()` | `string` | The OIDC discovery document address: `MetadataAddress` when set, otherwise `Authority` with `/.well-known/openid-configuration` appended (any trailing `/` on the authority is trimmed first). Returns an empty string when neither value is set, which the authenticator rejects at construction. |

## `OidcClaimNames`

The standard OpenID Connect claim names the defaults are built from. Use them instead of string literals when overriding `SubjectClaimTypes` or `GroupClaimTypes`.

| Constant | Value | Meaning |
|---|---|---|
| `Subject` | `"sub"` | The subject identifier: the locally unique, never-reassigned identifier the provider asserts for the end user. |
| `Groups` | `"groups"` | The group memberships claim, the de facto convention across Okta, Auth0, Keycloak, and Ping. |
| `Roles` | `"roles"` | The plural roles claim, emitted by providers that model roles separately from groups. |
| `Role` | `"role"` | The singular role claim, emitted by providers that repeat a single-valued claim per role. |

## Validation

Options are validated when the authenticator is first resolved from the container - `AddLatticeOidc` registers a singleton factory and the factory validates before constructing - so an invalid configuration throws an `OptionsValidationException` with an aggregated message listing every violation at once. (Registration *ordering* is checked eagerly: calling `AddLatticeOidc` before `AddLatticeMembership` throws from `AddLatticeOidc` itself.) The rules are:

| Rule | Failure message contains |
|---|---|
| `Authority` must be set | `Authority` |
| `Issuer` must be set | `Issuer` |
| `Audiences` must contain at least one entry | `at least one audience` |
| No entry in `Audiences` may be blank | `null or empty audience` |
| `SubjectClaimTypes` must contain at least one entry, none blank | `SubjectClaimTypes` |
| No entry in `GroupClaimTypes` may be blank (an empty list is allowed) | `GroupClaimTypes` |
| No entry in `Algorithms` may be blank (an empty list is allowed and means "pin from discovery") | `Algorithms` |
| `AutomaticRefreshInterval` must be strictly positive | `AutomaticRefreshInterval` |
| `RefreshInterval` must be strictly positive | `RefreshInterval` |
| `ClockSkew` must not be negative | `ClockSkew` |

## Worked example

A single Okta issuer whose group claim is namespaced, pinned to `RS256` explicitly rather than to whatever the discovery document advertises:

```csharp verify
using Orleans.Lattice.Membership;
using Orleans.Lattice.Membership.Oidc;

siloBuilder.AddLatticeMembership();

siloBuilder.AddLatticeOidc(options =>
{
    options.Authority = "https://dev-123456.okta.com/oauth2/default";
    options.Issuer = "https://dev-123456.okta.com/oauth2/default";
    options.Audiences.Add("api://lattice");

    // Narrower than the discovery document: only RS256 is accepted, even if the
    // provider also advertises RS384 or ES256.
    options.Algorithms.Add("RS256");

    // This tenant emits membership under a namespaced claim.
    options.GroupClaimTypes.Clear();
    options.GroupClaimTypes.Add("https://example.com/claims/teams");

    // Tighten lifetime validation for a low-skew fleet.
    options.ClockSkew = TimeSpan.FromSeconds(30);
});
```

## See also

- [Package overview](README.md) - what the authenticator does and how it differs from the Entra sibling.
- [Membership configuration](../lattice.membership/configuration.md) - the base membership options this add-on plugs into.

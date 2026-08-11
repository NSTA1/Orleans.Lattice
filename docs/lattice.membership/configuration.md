# Orleans.Lattice.Membership configuration

The package has four public options types. `LatticeMembershipOptions` and `LatticeIdentityDirectoryOptions` are both bound by the `AddLatticeMembership` registration extension (the directory options via standard `services.Configure<LatticeIdentityDirectoryOptions>(...)`). `JwtAuthenticatorOptions` is bound per issuer by `AddLatticeJwtAuthenticator`, and `StaticIdentityDirectoryOptions` is bound by `AddStaticIdentityDirectory`.

## `LatticeMembershipOptions`

The token-vs-directory group merge policy, the per-silo resolution-cache lifetime, and the durable per-key history retention applied to the `sys-membership-*` trees. Bind it through `AddLatticeMembership(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `GroupMergeMode` | `SubjectGroupMergeMode` | `Union` | How the default subject mapper combines token-asserted and directory-derived groups. |
| `ResolutionCacheTtl` | `TimeSpan` | `5 minutes` | The maximum lifetime of a per-silo resolution-cache entry. A resolved subject is additionally never served past the inbound token's expiry, so the effective bound is the minimum of this value and the token's remaining validity. `TimeSpan.Zero` disables caching (every resolution re-validates). |
| `HistoryRetentionMode` | `HistoryRetentionMode` | `MetadataOnly` | The retention mode for the durable per-key history captured on the `sys-membership-*` trees. History is never disabled by default. |
| `HistoryRetentionWindow` | `TimeSpan?` | `null` | The age after which a membership history revision row expires, or `null` for no age bound. Must be strictly positive when supplied. |
| `EnableDurableHistoryView` | `bool` | `true` | Whether to create the durable per-key history materialised view over each `sys-membership-*` tree so membership changes remain auditable beyond the source write-ahead-log window. |
| `ClaimToGroups` | `Func<IReadOnlyDictionary<string, string>, IEnumerable<string>>?` | `null` | An optional projection from a principal's claims to additional group ids, applied by the default subject mapper. `null` adds no claim-derived groups. |

## `LatticeIdentityDirectoryOptions`

Provider-neutral bounds for the identity-directory seam: the default and maximum search page sizes, and whether a supplied id must resolve before a grant. Configured through `AddLatticeMembership` (`services.Configure<LatticeIdentityDirectoryOptions>(...)`).

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultPageSize` | `int` | `25` | The page size a provider applies when a directory search query requests none (its page size is `0`). Must be strictly positive and no greater than `MaxPageSize`. |
| `MaxPageSize` | `int` | `100` | The upper bound a provider clamps a requested search page size to. Must be strictly positive. |
| `ValidationRequired` | `bool` | `false` | Whether a supplied principal id must resolve to an existing directory principal before it may be granted access. `false` accepts ids without validation, matching the behaviour of the no-op null directory. |

## `JwtAuthenticatorOptions`

Configuration for a single JWT credential authenticator instance: the issuer it owns, the audiences and signing keys it trusts, and the claim types it maps into a principal. One authenticator is registered per issuer, so a silo can trust several identity providers at once. Bind it through `AddLatticeJwtAuthenticator(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Issuer` | `string` | `""` (empty) | The token issuer this authenticator owns (the JWT `iss` claim). Used both to validate the token and to select this authenticator when the credential's scheme / issuer hint matches. Must be set. |
| `SchemeHint` | `string?` | `null` | Optional scheme hint (for example `Bearer` or a short provider name). When set, a credential whose scheme equals this value selects this authenticator without the token being parsed. `null` selects solely by issuer. |
| `Audiences` | `IList<string>` | empty list | The audiences this authenticator accepts (the JWT `aud` claim). Must be non-empty when `ValidateAudience` is `true` (construction fails closed otherwise, so audience validation is never silently disabled). Populate the collection in place. |
| `SigningKeys` | `IList<SecurityKey>` | empty list | The signing keys trusted for token-signature validation. Ignored when an explicit `ValidationParameters` is supplied or a subclass overrides key resolution (for example via JWKS discovery). Populate the collection in place. |
| `Algorithms` | `IList<string>` | empty list | The token signature algorithms accepted (the JWT header `alg`), pinned via `ValidAlgorithms`. When empty, no algorithm allow-list is enforced; populate it (for example `["RS256"]`) to restrict acceptance as a defense-in-depth measure against algorithm-confusion attacks. Populate the collection in place. |
| `SubjectClaimTypes` | `IList<string>` | `["sub", "nameid"]` | The claim types consulted, in order, to resolve the subject id. The first present claim wins. Populate the collection in place. |
| `GroupClaimTypes` | `IList<string>` | `["groups", "roles", "role"]` | The claim types whose values are collected as token-asserted group ids. Populate the collection in place. |
| `ValidateAudience` | `bool` | `true` | Whether to validate the token audience. When `true`, at least one entry in `Audiences` is required or construction throws (audience validation is never silently disabled); set to `false` to accept any audience explicitly. |
| `ValidateLifetime` | `bool` | `true` | Whether to validate the token lifetime (`exp` / `nbf`). |
| `ClockSkew` | `TimeSpan` | `5 minutes` | The permitted clock skew during lifetime validation. |
| `ValidationParameters` | `TokenValidationParameters?` | `null` | An explicit validation-parameters override. When set it is used verbatim and the issuer / audience / signing-key fields above are ignored. Provided as an extension point for OIDC / JWKS discovery and signing-key rotation. |

## `StaticIdentityDirectoryOptions`

Configures the in-memory roster surfaced by the static identity directory: an explicitly-declared set of known principals for deployments with no queryable external directory. Bind it through `AddStaticIdentityDirectory(configure)`. Populate it via `AddUser` / `AddGroup`, or discover the deployed Basic user ids via `AddUsersFromEnvironment`.

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `DefaultEnvironmentVariablePrefix` | `string` | `"LATTICE_STATE_USER_"` | The default environment-variable prefix under which the reference Basic authorizer stores each user's credential, so `AddUsersFromEnvironment` discovers the same user set. User `alice` is provisioned as `LATTICE_STATE_USER_alice`. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Principals` | `IList<DirectoryPrincipal>` | empty list | The declared roster of known principals, in declaration order. The static directory takes an immutable snapshot at construction; later mutation has no effect on an already-built provider. When the same id is declared more than once the last entry wins. Populate via `AddUser` / `AddGroup` / `AddUsersFromEnvironment` rather than editing the list directly. |

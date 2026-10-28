---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Orleans.Lattice.Membership.Entra.Graph API reference

Every public type and member of `Orleans.Lattice.Membership.Entra.Graph`. The [package guide](README.md) explains the overage resolver, app-token management, the secret-less credential path and the identity directory; this page is the complete declaration reference. The Graph clients, the token provider and acquirer, and the group resolver are internal and are described in [Architecture](architecture.md).

## Registration

| Method | Signature | Purpose |
|---|---|---|
| `AddEntraGraphGroupResolver` | `ISiloBuilder AddEntraGraphGroupResolver(this ISiloBuilder builder, Action<LatticeEntraGraphOptions> configure)` | Registers one shared app-only Microsoft Graph client, the Graph-backed `IEntraGroupResolver` the Entra authenticator consults on overage, and `EntraGraphIdentityDirectory` as the active `ILatticeIdentityDirectory` (last registration wins). Throws `ArgumentNullException` for a null argument, `InvalidOperationException` unless `AddEntraCredentialAuthenticator(...)` was called first, and `OptionsValidationException` immediately, before registering anything, when the options are invalid. |

`LatticeEntraGraphServiceCollectionExtensions` declares the method. Every option is documented in [Configuration](configuration.md).

## Types

| Type | Role |
|---|---|
| `LatticeEntraGraphOptions` | The Graph connection: either a `Credential` (secret-less) or the `TenantId`, `ClientId` and `ClientSecret` triple, plus `Scopes`, `SecurityEnabledOnly`, `TokenRefreshSkew` and `DirectorySubjectIdSource`. `ResolveAuthority()` builds the MSAL authority from `AuthorityHost` and `TenantId`. |
| `EntraGraphIdentityDirectory` | The Graph-backed `ILatticeIdentityDirectory`, with `ProviderId` `"entra"` (its `EntraProviderId` constant). It has no public constructor; the registration builds it. Search and resolve degrade to an empty page and `null` when Graph denies the query. |
| `EntraDirectorySubjectIdSource` | Which Entra identifier the directory records as a principal id: `ObjectId` (default) or `UserPrincipalName`. Match it to the subject claim the authenticator resolves so validation and authentication agree. |

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, record positional members, and the protected extension points of unsealed public classes. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Membership.Entra.Graph.EntraDirectorySubjectIdSource`

[Source](../../src/lattice.membership.entra.graph/EntraDirectorySubjectIdSource.cs) (line 10).

`public enum EntraDirectorySubjectIdSource`

- `ObjectId`
- `UserPrincipalName`

### `Orleans.Lattice.Membership.Entra.Graph.EntraGraphIdentityDirectory`

[Source](../../src/lattice.membership.entra.graph/EntraGraphIdentityDirectory.cs) (line 21).

`public sealed class EntraGraphIdentityDirectory : ILatticeIdentityDirectory`

- `public const string EntraProviderId`
- `public string ProviderId { get; }`
- `public string DescribeEntry(DirectoryPrincipalKind? kind)`
- `public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default)`
- `public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Membership.Entra.Graph.LatticeEntraGraphOptions`

[Source](../../src/lattice.membership.entra.graph/LatticeEntraGraphOptions.cs) (line 20).

`public sealed class LatticeEntraGraphOptions`

- `public const string DefaultAuthorityHost`
- `public const string DefaultScope`
- `public string TenantId { get; set; }`
- `public string ClientId { get; set; }`
- `public string ClientSecret { get; set; }`
- `public TokenCredential? Credential { get; set; }`
- `public string AuthorityHost { get; set; }`
- `public IList<string> Scopes { get; }`
- `public bool SecurityEnabledOnly { get; set; }`
- `public TimeSpan TokenRefreshSkew { get; set; }`
- `public EntraDirectorySubjectIdSource DirectorySubjectIdSource { get; set; }`
- `public string ResolveAuthority()`

### `Orleans.Lattice.Membership.Entra.Graph.LatticeEntraGraphServiceCollectionExtensions`

[Source](../../src/lattice.membership.entra.graph/LatticeEntraGraphServiceCollectionExtensions.cs) (line 14).

`public static class LatticeEntraGraphServiceCollectionExtensions`

- `public static ISiloBuilder AddEntraGraphGroupResolver(this ISiloBuilder builder, Action<LatticeEntraGraphOptions> configure)`

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Membership.Entra` public API](../lattice.membership.entra/api.md) - the authenticator and the `IEntraGroupResolver` seam.
- [Identity-directory providers](../lattice.membership/identity-directory-providers.md) - the `ILatticeIdentityDirectory` seam.

---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Orleans.Lattice.Membership.Entra API reference

Every public type and member of `Orleans.Lattice.Membership.Entra`. The [package guide](README.md) explains tenant-aware validation, the Entra claim conventions and groups-overage handling, and the [setup guide](entra-setup.md) provisions an app registration end to end; this page is the complete declaration reference. Types not listed here are internal and are described by behaviour in [Architecture](architecture.md).

## Registration

| Method | Signature | Purpose |
|---|---|---|
| `AddEntraCredentialAuthenticator` | `ISiloBuilder AddEntraCredentialAuthenticator(this ISiloBuilder builder, Action<LatticeEntraAuthenticatorOptions> configure)` | Registers one more `EntraCredentialAuthenticator` as an `ILatticeCredentialAuthenticator`. Throws `ArgumentNullException` for a null argument and `InvalidOperationException` unless `AddLatticeMembership(...)` was called first. The options are built and validated when the authenticator is first constructed, and an invalid configuration throws `OptionsValidationException`. |

`LatticeEntraServiceCollectionExtensions` declares the method. Every option and the `EntraClaimNames` constants are documented in [Configuration](configuration.md).

## Types by role

| Type | Role |
|---|---|
| `EntraCredentialAuthenticator` | A subclass of the membership package's [`JwtCredentialAuthenticator`](../lattice.membership/api.md#extending-jwtcredentialauthenticator). It overrides `CanHandle` (scheme hint, or tenant allow-list plus templated issuer), and the protected `ResolveValidationParametersAsync` (OIDC discovery from the tenant authority, templated issuer validation, algorithm allow-list), `MapPrincipal` (`oid` then `sub` subject; `groups` and `roles` as asserted groups) and `EnrichPrincipalAsync` (groups overage). Its public constructor discovers metadata from the live authority; the class is unsealed so a host can specialise it further. |
| `LatticeEntraAuthenticatorOptions` | Per-authenticator configuration; `ResolveMetadataAddress()` returns `MetadataAddress` when set, otherwise `Authority` with `/.well-known/openid-configuration` appended, or an empty string when neither is set. |
| `EntraClaimNames` | The Entra v2.0 claim names the authenticator reads. |
| `EntraGroupResolutionMode` | `TokenOnly` (default; never consults a resolver) or `ResolveOnOverage`. |
| `IEntraGroupResolver` | The overage seam: `ResolveGroupsAsync(EntraGroupResolutionContext, CancellationToken)` returns the caller's groups, which are unioned with the token-asserted ones. [`Orleans.Lattice.Membership.Entra.Graph`](../lattice.membership.entra.graph/api.md) supplies the Microsoft Graph implementation. |
| `EntraGroupResolutionContext` | What a resolver receives: `SubjectId`, `TenantId` and any `TokenAssertedGroups`. |
| `TokenOnlyEntraGroupResolver` | A registrable resolver that returns the token-asserted groups, the same result as the dependency-free fallback. |

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, record positional members, and the protected extension points of unsealed public classes. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Membership.Entra.EntraClaimNames`

[Source](../../src/lattice.membership.entra/EntraClaimNames.cs) (line 8).

`public static class EntraClaimNames`

- `public const string ObjectId`
- `public const string TenantId`
- `public const string Groups`
- `public const string Roles`
- `public const string Scope`
- `public const string AuthorizedParty`
- `public const string Subject`
- `public const string ClaimNames`
- `public const string ClaimSources`

### `Orleans.Lattice.Membership.Entra.EntraCredentialAuthenticator`

[Source](../../src/lattice.membership.entra/EntraCredentialAuthenticator.cs) (line 20).

`public class EntraCredentialAuthenticator : JwtCredentialAuthenticator`

- `public EntraCredentialAuthenticator(LatticeEntraAuthenticatorOptions options, IEntraGroupResolver? groupResolver = null)`
- `public override bool CanHandle(in LatticeCredential credential)`
- `protected override ValueTask<LatticePrincipal?> EnrichPrincipalAsync(LatticePrincipal principal, LatticeCredential credential, CancellationToken cancellationToken)`
- `protected override ValueTask<TokenValidationParameters> ResolveValidationParametersAsync(LatticeCredential credential, CancellationToken cancellationToken)`
- `protected override LatticePrincipal? MapPrincipal(JsonWebToken token, ClaimsIdentity identity)`

### `Orleans.Lattice.Membership.Entra.EntraGroupResolutionContext`

[Source](../../src/lattice.membership.entra/EntraGroupResolutionContext.cs) (line 9).

`public sealed class EntraGroupResolutionContext`

- `public EntraGroupResolutionContext(string subjectId, string? tenantId = null, IReadOnlyCollection<string>? tokenAssertedGroups = null)`
- `public string SubjectId { get; }`
- `public string? TenantId { get; }`
- `public IReadOnlyCollection<string>? TokenAssertedGroups { get; }`

### `Orleans.Lattice.Membership.Entra.EntraGroupResolutionMode`

[Source](../../src/lattice.membership.entra/EntraGroupResolutionMode.cs) (line 8).

`public enum EntraGroupResolutionMode`

- `TokenOnly`
- `ResolveOnOverage`

### `Orleans.Lattice.Membership.Entra.IEntraGroupResolver`

[Source](../../src/lattice.membership.entra/IEntraGroupResolver.cs) (line 12).

`public interface IEntraGroupResolver`

- `ValueTask<IReadOnlyCollection<string>> ResolveGroupsAsync(EntraGroupResolutionContext context, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Membership.Entra.LatticeEntraAuthenticatorOptions`

[Source](../../src/lattice.membership.entra/LatticeEntraAuthenticatorOptions.cs) (line 12).

`public sealed class LatticeEntraAuthenticatorOptions`

- `public const string DefaultIssuerTemplate`
- `public const string DefaultAuthorityHost`
- `public string Authority { get; set; }`
- `public string? MetadataAddress { get; set; }`
- `public string IssuerTemplate { get; set; }`
- `public IList<string> TenantIds { get; }`
- `public IList<string> Audiences { get; }`
- `public const string DefaultAlgorithm`
- `public IList<string> Algorithms { get; }`
- `public string? SchemeHint { get; set; }`
- `public EntraGroupResolutionMode GroupResolutionMode { get; set; }`
- `public bool ValidateLifetime { get; set; }`
- `public TimeSpan ClockSkew { get; set; }`
- `public TimeSpan AutomaticRefreshInterval { get; set; }`
- `public TimeSpan RefreshInterval { get; set; }`
- `public string ResolveMetadataAddress()`

### `Orleans.Lattice.Membership.Entra.LatticeEntraServiceCollectionExtensions`

[Source](../../src/lattice.membership.entra/LatticeEntraServiceCollectionExtensions.cs) (line 11).

`public static class LatticeEntraServiceCollectionExtensions`

- `public static ISiloBuilder AddEntraCredentialAuthenticator(this ISiloBuilder builder, Action<LatticeEntraAuthenticatorOptions> configure)`

### `Orleans.Lattice.Membership.Entra.TokenOnlyEntraGroupResolver`

[Source](../../src/lattice.membership.entra/TokenOnlyEntraGroupResolver.cs) (line 13).

`public sealed class TokenOnlyEntraGroupResolver : IEntraGroupResolver`

- `public ValueTask<IReadOnlyCollection<string>> ResolveGroupsAsync(EntraGroupResolutionContext context, CancellationToken cancellationToken = default)`

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Membership` public API](../lattice.membership/api.md) - the authenticator seam and JWT base.
- [`Orleans.Lattice.Membership.Entra.Graph` public API](../lattice.membership.entra.graph/api.md) - the Graph-backed resolver and identity directory.

---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Orleans.Lattice.Membership.Oidc API reference

Every public type and member of `Orleans.Lattice.Membership.Oidc`. The [package guide](README.md) explains exact-issuer selection, algorithm pinning, the claim conventions and the security notes; this page is the complete declaration reference. The discovery-document source is internal and is described in [Architecture](architecture.md).

## Registration

| Method | Signature | Purpose |
|---|---|---|
| `AddLatticeOidc` | `ISiloBuilder AddLatticeOidc(this ISiloBuilder builder, Action<LatticeOidcAuthenticatorOptions> configure)` | Registers one more `OidcCredentialAuthenticator` as an `ILatticeCredentialAuthenticator`. Throws `ArgumentNullException` for a null argument and `InvalidOperationException` unless `AddLatticeMembership(...)` was called first. The options are built and validated when the authenticator is first constructed, and an invalid configuration throws `OptionsValidationException`. Call once per issuer. |

`LatticeOidcServiceCollectionExtensions` declares the method. Every option, the `OidcClaimNames` constants and the validation rules are documented in [Configuration](configuration.md).

## Types

| Type | Role |
|---|---|
| `OidcCredentialAuthenticator` | A subclass of the membership package's [`JwtCredentialAuthenticator`](../lattice.membership/api.md#extending-jwtcredentialauthenticator). It overrides `CanHandle` (scheme hint, or an exact ordinal match of the token's `iss` against `Issuer`), and the protected `ResolveValidationParametersAsync` (discovery-driven signing keys, exact issuer validation, algorithm pinning) and `MapPrincipal` (configured subject and group claim types). Its public constructor discovers metadata from the live provider and throws `ArgumentException` when no metadata address can be derived; the class is unsealed so a host can specialise it further. |
| `LatticeOidcAuthenticatorOptions` | Per-issuer configuration; `ResolveMetadataAddress()` returns `MetadataAddress` when set, otherwise `Authority` with `/.well-known/openid-configuration` appended, or an empty string when neither is set. |
| `OidcClaimNames` | The standard claim names used as defaults: `sub`, `groups`, `roles` and `role`. |

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, record positional members, and the protected extension points of unsealed public classes. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Membership.Oidc.LatticeOidcAuthenticatorOptions`

[Source](../../src/lattice.membership.oidc/LatticeOidcAuthenticatorOptions.cs) (line 11).

`public sealed class LatticeOidcAuthenticatorOptions`

- `public string Authority { get; set; }`
- `public string? MetadataAddress { get; set; }`
- `public string Issuer { get; set; }`
- `public IList<string> Audiences { get; }`
- `public IList<string> Algorithms { get; }`
- `public IList<string> SubjectClaimTypes { get; }`
- `public IList<string> GroupClaimTypes { get; }`
- `public string? SchemeHint { get; set; }`
- `public bool ValidateLifetime { get; set; }`
- `public TimeSpan ClockSkew { get; set; }`
- `public TimeSpan AutomaticRefreshInterval { get; set; }`
- `public TimeSpan RefreshInterval { get; set; }`
- `public string ResolveMetadataAddress()`

### `Orleans.Lattice.Membership.Oidc.LatticeOidcServiceCollectionExtensions`

[Source](../../src/lattice.membership.oidc/LatticeOidcServiceCollectionExtensions.cs) (line 11).

`public static class LatticeOidcServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeOidc(this ISiloBuilder builder, Action<LatticeOidcAuthenticatorOptions> configure)`

### `Orleans.Lattice.Membership.Oidc.OidcClaimNames`

[Source](../../src/lattice.membership.oidc/OidcClaimNames.cs) (line 15).

`public static class OidcClaimNames`

- `public const string Subject`
- `public const string Groups`
- `public const string Roles`
- `public const string Role`

### `Orleans.Lattice.Membership.Oidc.OidcCredentialAuthenticator`

[Source](../../src/lattice.membership.oidc/OidcCredentialAuthenticator.cs) (line 25).

`public class OidcCredentialAuthenticator : JwtCredentialAuthenticator`

- `public OidcCredentialAuthenticator(LatticeOidcAuthenticatorOptions options)`
- `public override bool CanHandle(in LatticeCredential credential)`
- `protected override ValueTask<TokenValidationParameters> ResolveValidationParametersAsync(LatticeCredential credential, CancellationToken cancellationToken)`
- `protected override LatticePrincipal? MapPrincipal(JsonWebToken token, ClaimsIdentity identity)`

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Membership` public API](../lattice.membership/api.md) - the authenticator seam and JWT base.
- [`Orleans.Lattice.Membership.Entra` public API](../lattice.membership.entra/api.md) - the Microsoft Entra ID sibling.

# Orleans.Lattice.Membership.Entra

Microsoft Entra ID (Azure AD) credential authenticator for [Orleans.Lattice.Membership](../lattice.membership/README.md).

## What is it?

`Orleans.Lattice.Membership.Entra` adds a Microsoft Entra ID authenticator that plugs into the membership credential-authenticator seam. It specializes the built-in JWT authenticator rather than reimplementing token validation, so it inherits the same issuer, audience, signing-key, and lifetime checks and only layers on the Entra-specific concerns. The Entra dependency stack stays out of the core `Orleans.Lattice.Membership` package.

## What it does

- **Tenant-aware OIDC metadata discovery and signing-key rotation.** Validation parameters are resolved from the Entra v2.0 authority's OpenID Connect metadata endpoint. The JSON Web Key Set is cached and refreshed on its own interval through a configuration manager, so signing keys rotate automatically without a metadata fetch on every call.
- **Single- and multi-tenant issuer validation.** The authenticator validates the token issuer against a configured tenant allow-list and the templated Entra v2.0 issuer. For multi-tenant applications it checks the token's tenant id against the allow-list and accepts the templated issuer for any allowed tenant. A token whose issuer or tenant is outside the allow-list is not handled by this authenticator, so resolution falls through to the next authenticator or anonymous.
- **Entra v2.0 claim conventions.** The subject is taken from the object id (`oid`), the tenant from `tid`, group membership from the `groups` claim, and application roles from `roles`. The delegated-versus-application distinction follows the presence of a scope (`scp`) claim.
- **Groups-overage handling.** When a token's group membership overflows and Entra emits the overage markers in place of the `groups` claim, full membership is resolved through a pluggable resolver abstraction. With no resolver registered the authenticator applies a documented, dependency-free token-only fallback and never throws.

## Transparent token freshness

Inbound user tokens are validated for freshness on every call. Expired tokens are rejected by the inherited lifetime validation and are never served from a cache past their expiry. Resolving overflowed group membership through Microsoft Graph is an opt-in concern handled by the separate `Orleans.Lattice.Membership.Entra.Graph` package, which keeps the Graph SDK dependency out of this package.

## Registration and ordering

The authenticator is registered on the silo builder after the base membership services. Registration guards this ordering and fails fast with a clear message when the base membership services are not present. When the add-on is not registered it has zero runtime cost.

## Reference

- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Azure CLI setup guide](entra-setup.md) - provision an app registration and wire the authenticator into a silo, end to end.
- [Membership documentation](../lattice.membership/README.md) - the base identity and authorization add-on this package extends.
- [Graph group resolver](../lattice.membership.entra.graph/README.md) - the opt-in Microsoft Graph-backed overflow resolver.

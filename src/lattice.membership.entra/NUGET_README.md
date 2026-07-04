# Orleans.Lattice.Membership.Entra

Microsoft Entra ID (Azure AD) credential authenticator for [`Orleans.Lattice.Membership`](https://github.com/NSTA1/Orleans.Lattice).

This package adds an Entra ID authenticator that plugs into the membership
authenticator seam. It specializes the built-in JWT authenticator rather than
reimplementing token validation, so it inherits the same issuer, audience,
signing-key, and lifetime checks and only layers on the Entra-specific concerns:

- Tenant-aware OIDC metadata discovery and JWKS signing-key rotation from the
  Entra v2.0 authority, cached and refreshed on its own interval.
- Single- and multi-tenant issuer validation against a configured tenant
  allow-list and the templated Entra v2.0 issuer.
- Entra v2.0 claim conventions: subject from `oid`, tenant from `tid`, groups
  from `groups`, and app roles from `roles`.
- Groups-overage handling through a pluggable resolver abstraction, with a
  dependency-free token-only fallback when no resolver is registered.

The Microsoft Graph dependency needed to resolve overflowed group membership is
isolated in the separate `Orleans.Lattice.Membership.Entra.Graph` package, so
this package stays free of the Graph SDK.

See the [Entra membership documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.membership.entra/README.md)
for configuration and the tracked feature index.

# Orleans.Lattice.Membership.Oidc

Generic OpenID Connect credential authenticator for [`Orleans.Lattice.Membership`](https://github.com/NSTA1/Orleans.Lattice).

This package adds a provider-agnostic OIDC authenticator that plugs into the
membership authenticator seam. It specializes the built-in JWT authenticator
rather than reimplementing token validation, so it inherits the same audience,
signing-key, and lifetime checks and only layers on the OIDC-specific concerns:

- Discovery-document-driven metadata and JWKS signing-key rotation from any
  conformant provider authority (Okta, Auth0, Keycloak, Ping, Google), cached
  and refreshed on its own interval.
- Exact ordinal issuer selection and validation. There is no prefix, wildcard,
  or catch-all form, so several issuers - and the Entra authenticator - can be
  registered on the same silo without ambiguity.
- Fail-closed signature-algorithm pinning. The accepted `alg` set comes from
  explicit configuration when supplied and from the discovery document's
  `id_token_signing_alg_values_supported` otherwise; an empty set rejects every
  token instead of accepting any.
- Standard OIDC claim conventions: subject from `sub`, groups from `groups`,
  `roles`, and `role`, all configurable per issuer.

This package is an additive sibling to `Orleans.Lattice.Membership.Entra`.
Neither depends on the other, and both can be registered together.

See the [OIDC membership documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.membership.oidc/README.md)
for configuration.

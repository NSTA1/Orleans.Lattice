---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Architecture

`Orleans.Lattice.Membership.Oidc` adds one provider-agnostic credential authenticator to the membership pipeline per configured issuer. It derives from the membership package's JWT authenticator, so token validation, the signature-algorithm pin and principal mapping run through the same base path, and it overrides only selection, the source of validation parameters, and claim mapping. Everything it knows about a provider comes from that provider's OpenID Connect discovery document. This page describes the steps by behaviour; the public types are listed in the [Public API](api.md).

## Registration

`AddLatticeOidc` refuses to run unless `AddLatticeMembership(...)` has been called first, and adds a factory for one `ILatticeCredentialAuthenticator`. The factory runs when the membership pipeline first resolves its authenticators: it applies the configure delegate to fresh options, validates them (throwing `OptionsValidationException` that lists every violation), and creates a discovery source honouring `AutomaticRefreshInterval` and `RefreshInterval`. The package has no dependency on [`Orleans.Lattice.Membership.Entra`](../lattice.membership.entra/architecture.md), and both can be registered on one silo.

## Selection

The membership pipeline asks each authenticator in turn whether it can handle the credential. This one answers yes when the credential's scheme equals `SchemeHint`, without reading the token. Otherwise it reads the token's `iss` claim and answers yes only on an exact ordinal match with `Issuer`; there is no prefix, wildcard or catch-all form, so two OIDC authenticators without a hint never compete for a token.

Unlike the base JWT authenticator, a non-empty scheme that does not match the hint does not end selection. The credential bridges stamp a scheme from operator configuration on nearly every request, so ending selection there would make an authenticator without a hint unselectable. Because the token is therefore read before authentication, a credential longer than the token handler's own maximum token size is declined without being parsed; validation would have rejected it anyway.

## Validation

Signing keys come from the discovery document through a configuration manager that caches them and refreshes them on its own schedule, so key rotation needs no restart. Audience validation is always on against `Audiences`. Issuer validation accepts only `Issuer` itself, not whatever issuer the discovery document advertises. Lifetime validation follows `ValidateLifetime` and `ClockSkew`.

The accepted signature algorithms are `Algorithms` when it is populated, otherwise the discovery document's `id_token_signing_alg_values_supported`. When neither names an algorithm, the base class's pin step denies every token rather than accepting any algorithm, which closes the algorithm-confusion gap.

## Mapping

A validated token's subject is the first present claim in `SubjectClaimTypes` (`sub` by default), falling back to the standard name-identifier claim. A missing or reserved subject maps to no principal, which the pipeline resolves as anonymous with no groups. Every value of the claims named in `GroupClaimTypes` (`groups`, `roles` and `role` by default) becomes an asserted group; claim types are matched by exact name, not as JSON paths. Every claim is copied into the claim bag, and the token's issuer is recorded on the principal.

The principal then continues through the membership pipeline's merge, claim filter and caching steps ([Resolving a subject](../lattice.membership/architecture.md#resolving-a-subject)), and the cache never serves it past the token's expiry.

## Source map

- [Registration](../../src/lattice.membership.oidc/LatticeOidcServiceCollectionExtensions.cs)
- [Authenticator](../../src/lattice.membership.oidc/OidcCredentialAuthenticator.cs)
- [Options validation](../../src/lattice.membership.oidc/LatticeOidcAuthenticatorOptionsValidator.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [`Orleans.Lattice.Membership` architecture](../lattice.membership/architecture.md)
- [`Orleans.Lattice.Membership.Entra` architecture](../lattice.membership.entra/architecture.md)

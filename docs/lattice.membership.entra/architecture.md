---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Architecture

`Orleans.Lattice.Membership.Entra` adds one credential authenticator to the membership pipeline. It derives from the membership package's JWT authenticator, so token validation, the signature-algorithm pin and principal mapping run through the same base path, and only the Entra-specific steps are overridden. This page describes those steps by behaviour; the public types are listed in the [Public API](api.md).

## Registration

`AddEntraCredentialAuthenticator` refuses to run unless `AddLatticeMembership(...)` has been called first. It records that an Entra authenticator is registered - the marker the [Graph package](../lattice.membership.entra.graph/architecture.md) checks for - and adds a factory for one `ILatticeCredentialAuthenticator`. The factory runs when the membership pipeline first resolves its authenticators. It applies the configure delegate to fresh options, validates them (throwing `OptionsValidationException` naming each violation), creates a discovery source honouring `AutomaticRefreshInterval` and `RefreshInterval`, and takes whatever `IEntraGroupResolver` is registered at that moment. Because resolution is deferred, a resolver registered after this call is still picked up. Each call registers one independent authenticator.

## Selection

The membership pipeline asks each authenticator in turn whether it can handle the credential. This one answers yes when the credential's scheme equals `SchemeHint`, without reading the token. Otherwise it reads the token's `tid` and `iss` claims and answers yes only when the tenant is in `TenantIds` and the issuer equals `IssuerTemplate` with that tenant substituted. Any other token is declined, so resolution falls through to the next authenticator.

## Validation

Validation parameters come from the authority's OpenID Connect discovery document, through a configuration manager that caches the signing keys and refreshes them on its own schedule, so key rotation needs no restart and no per-call metadata fetch. Audience validation is always on against `Audiences`; lifetime validation follows `ValidateLifetime` and `ClockSkew`. Issuer validation repeats the selection rule - an allowed tenant and the templated issuer - so a token claimed through the scheme hint is still refused when it comes from another tenant. The accepted algorithms are `Algorithms` (`RS256` by default); the base class's pin step denies every token when that list is empty, and the options validator refuses an empty list before the authenticator is built.

## Mapping and overage

A validated token's subject is its `oid` claim, or `sub` when `oid` is absent; a missing or reserved subject maps to no principal, which the pipeline resolves as anonymous. The `groups` and `roles` claims become the principal's asserted groups, and every claim is copied into its claim bag.

When `GroupResolutionMode` is `ResolveOnOverage` and the token reports a groups overage, the post-validation enrichment step asks the registered `IEntraGroupResolver` for the caller's groups, passing the subject, tenant and any groups still in the token, and unions the answer with the asserted groups. In `TokenOnly` mode, or with no resolver registered, the asserted groups stand and nothing is called. Because enrichment runs only after validation, a resolver can never be reached by an unvalidated token.

The resulting principal then continues through the membership pipeline's merge, claim filter and caching steps ([Resolving a subject](../lattice.membership/architecture.md#resolving-a-subject)), and the cache never serves it past the token's expiry.

## Source map

- [Registration](../../src/lattice.membership.entra/LatticeEntraServiceCollectionExtensions.cs)
- [Authenticator](../../src/lattice.membership.entra/EntraCredentialAuthenticator.cs)
- [Options validation](../../src/lattice.membership.entra/LatticeEntraAuthenticatorOptionsValidator.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Setup guide](entra-setup.md)
- [`Orleans.Lattice.Membership` architecture](../lattice.membership/architecture.md)
- [`Orleans.Lattice.Membership.Entra.Graph` architecture](../lattice.membership.entra.graph/architecture.md)

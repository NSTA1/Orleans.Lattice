---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Architecture

`Orleans.Lattice.Membership.Entra.Graph` plugs two Microsoft Graph-backed implementations into seams other packages declare: the Entra authenticator's `IEntraGroupResolver` and the membership package's `ILatticeIdentityDirectory`. Both share one app-only Graph client. This page describes the pipeline by behaviour; the public types are listed in the [Public API](api.md).

## Registration

`AddEntraGraphGroupResolver` refuses to run unless `AddEntraCredentialAuthenticator(...)` has been called first. It then applies the configure delegate and validates the options at once: exactly one authentication mode must be fully configured, `Scopes` must hold at least one non-empty scope, and `TokenRefreshSkew` must not be negative. Any violation throws `OptionsValidationException` before a service is registered. Three singletons follow:

1. **The Graph client.** With `Credential` set, the client is built directly from that `TokenCredential` and `Scopes`; no client secret is involved. Otherwise it is built over an MSAL confidential client for `ClientId`, `ClientSecret` and the authority from `ResolveAuthority()`, wrapped in the token cache described below.
2. **The group resolver**, registered as `IEntraGroupResolver`, using the shared client and `SecurityEnabledOnly`.
3. **The identity directory**, `EntraGraphIdentityDirectory`, registered as `ILatticeIdentityDirectory` so it replaces the membership package's no-op default. It reads the page-size bounds from `LatticeIdentityDirectoryOptions` and records principal ids per `DirectorySubjectIdSource`.

The [Entra authenticator](../lattice.membership.entra/architecture.md) picks up the resolver when it is first constructed, so the resolver is consulted only if that authenticator's `GroupResolutionMode` is `ResolveOnOverage`.

## App-only token management

On the client-secret path, a token provider sits between the Graph client and MSAL. It caches the app-only token and treats it as expired `TokenRefreshSkew` before its real expiry, so a call never carries a token that expires in flight. When the cache is cold or stale, concurrent callers share one in-flight acquisition, so a burst of lookups makes one request to the token endpoint. An expired token is re-acquired on the next request.

## Overage resolution

When the Entra authenticator meets a token whose groups claim overflowed, the resolver calls Graph's `getMemberGroups` for the caller, which returns every transitive group (and directory role) id regardless of the token's group cap, restricted to security groups when `SecurityEnabledOnly` is set. The ids are returned to the authenticator, which unions them with the token's asserted groups. A token or Graph failure on this path propagates through authentication rather than yielding an empty group set, so a caller is never authenticated with silently missing groups. Tokens that carry their groups inline never reach Graph.

## Identity directory

Search issues server-side searched (or browsed) and paged Graph queries over `/users` and `/groups`, honouring the requested principal kind - a search across both kinds pages through users first, then groups - and clamping the page size to `LatticeIdentityDirectoryOptions`. Resolve looks a principal up by exact id and returns `null` when Graph reports it not found. Each principal's id is shaped by `DirectorySubjectIdSource` - the object id by default - so a validated id matches the subject the authenticator resolves. When Graph denies a query (for example, the app lacks `User.Read.All` or `Group.Read.All`), search returns an empty page and resolve returns `null` instead of faulting; token-acquisition and non-Graph transport failures are not caught here. The directory is used only by administration and the Explorer, never during subject resolution ([Identity-directory providers](../lattice.membership/identity-directory-providers.md)).

## Source map

- [Registration](../../src/lattice.membership.entra.graph/LatticeEntraGraphServiceCollectionExtensions.cs)
- [Options validation](../../src/lattice.membership.entra.graph/LatticeEntraGraphOptionsValidator.cs)
- [Token cache](../../src/lattice.membership.entra.graph/EntraGraphTokenProvider.cs)
- [Group resolver](../../src/lattice.membership.entra.graph/GraphEntraGroupResolver.cs)
- [Identity directory](../../src/lattice.membership.entra.graph/EntraGraphIdentityDirectory.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [`Orleans.Lattice.Membership.Entra` architecture](../lattice.membership.entra/architecture.md)
- [`Orleans.Lattice.Membership` architecture](../lattice.membership/architecture.md)

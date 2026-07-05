# Orleans.Lattice.Membership.Entra.Graph

Microsoft Graph-backed group-overflow resolver for [Orleans.Lattice.Membership.Entra](../lattice.membership.entra/README.md).

## What is it?

`Orleans.Lattice.Membership.Entra.Graph` provides the Microsoft Graph-backed implementation of the Entra group resolver abstraction defined in `Orleans.Lattice.Membership.Entra`. It is a separate, opt-in package so that the Microsoft Graph SDK and the MSAL client library are pulled in only by applications that actually need to resolve overflowed group membership. Applications that never overflow their groups claim, or that are satisfied with the token-only fallback, never take this dependency.

## What it does

When an inbound Entra token's group membership overflows and the authenticator needs the caller's full transitive group set, it delegates to the registered group resolver. The Graph-backed resolver answers that request by calling the caller's transitive member-groups endpoint through Microsoft Graph and returning the resolved group ids. It is consulted only on the overflow path, so an application whose tokens carry their groups inline makes no Graph call.

## Transparent app-token management

The resolver authenticates to Graph with its own application-only token acquired through the MSAL confidential-client flow. Operators configure the tenant, client id, and client secret once; they never hand-manage or rotate a Graph token.

- The app-only token is cached and refreshed transparently. A token is proactively refreshed a configurable interval before its actual expiry, so a call never uses a token that expires mid-flight.
- A cold cache triggers exactly one token acquisition no matter how many lookups request a token at once. Concurrent callers share a single in-flight acquisition rather than stampeding the token endpoint.
- After a token expires it is re-acquired transparently on the next request, with no operator involvement.

The underlying MSAL confidential-client cache serves and renews the token, and the resolver layers a single-flight guard over it so the whole path stays allocation-light and free of duplicate network calls under load.

## Registration and ordering

The Graph resolver is registered on the silo builder after the Entra authenticator. Registration guards this ordering and fails fast with a clear message when the Entra authenticator has not been registered first. When the add-on is not registered it has zero runtime cost and the authenticator uses its token-only fallback.

## Reference

- [Entra authenticator](../lattice.membership.entra/README.md) - the authenticator that consumes this resolver.
- [Membership documentation](../lattice.membership/README.md) - the base identity and authorization add-on.

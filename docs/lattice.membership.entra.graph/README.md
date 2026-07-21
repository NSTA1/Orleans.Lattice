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

## Secret-less (managed-identity) authentication

For deployments that want no client secret to store, rotate, or leak, set `LatticeEntraGraphOptions.Credential` to any `Azure.Core` `TokenCredential` (for example `DefaultAzureCredential` or a `ManagedIdentityCredential` bound to a user-assigned managed identity with a federated credential on the app registration). When a credential is supplied, the shared app-only Graph client is built directly from it and no client secret is used; the tenant id, client id, and client secret are ignored.

The two modes are mutually exclusive and validated fail-closed: exactly one must be configured. Supplying neither a credential nor the full tenant/client/secret triple, or supplying both a credential and a client secret, is rejected at registration.

## Identity directory

The same registration also installs a Microsoft Graph-backed
`ILatticeIdentityDirectory` (`ProviderId` `"entra"`) - the provider-agnostic
identity source that the Explorer Access area searches and validates against when
an operator picks or creates a subject. It searches users and groups in the tenant
over the same app-only Graph token described above, requiring the `User.Read.All`
and `Group.Read.All` application permissions. When the token cannot be minted or a
Graph call is denied, it degrades cleanly rather than throwing: a search returns an
empty page and a resolve returns `null`, so the Access area keeps working without
an unhandled fault.

See [Identity-directory providers](../lattice.membership/identity-directory-providers.md)
for the seam, the static and custom alternatives, and the fail-closed create flow.

## Registration and ordering

The Graph resolver is registered on the silo builder after the Entra authenticator. Registration guards this ordering and fails fast with a clear message when the Entra authenticator has not been registered first. When the add-on is not registered it has zero runtime cost and the authenticator uses its token-only fallback.

## Reference

- [Entra authenticator](../lattice.membership.entra/README.md) - the authenticator that consumes this resolver.
- [Membership documentation](../lattice.membership/README.md) - the base identity and authorization add-on.

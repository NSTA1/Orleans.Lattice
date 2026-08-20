# Orleans.Lattice.Membership.Entra.Graph

Microsoft Graph-backed group-overflow resolver for [Orleans.Lattice.Membership.Entra](../lattice.membership.entra/README.md).

## What is it?

`Orleans.Lattice.Membership.Entra.Graph` provides the Microsoft Graph-backed implementation of the Entra group resolver abstraction defined in `Orleans.Lattice.Membership.Entra`. It is a separate, opt-in package so that the Microsoft Graph SDK and the MSAL client library are pulled in only by applications that actually need to resolve overflowed group membership. Applications that never overflow their groups claim, or that are satisfied with the token-only fallback, never take this dependency.

## What it does

When an inbound Entra token's group membership overflows and the authenticator needs the caller's full transitive group set, it delegates to the registered group resolver. The Graph-backed resolver answers that request by calling the caller's transitive member-groups endpoint through Microsoft Graph and returning the resolved group ids. It is consulted only on the overflow path, so an application whose tokens carry their groups inline makes no Graph call. Unlike the identity-directory search/resolve path (below), the overflow resolver does **not** degrade: a token-acquisition or Graph-call failure on this path propagates through authentication rather than resolving to an empty group set, so a caller whose overflow lookup fails is not silently authorized with no groups.

## Transparent app-token management

The resolver authenticates to Graph with its own application-only token acquired through the MSAL confidential-client flow. Operators configure the tenant, client id, and client secret once; they never hand-manage or rotate a Graph token.

- The app-only token is cached and refreshed transparently. A token is proactively refreshed a configurable interval before its actual expiry, so a call never uses a token that expires mid-flight.
- A cold cache triggers exactly one token acquisition no matter how many lookups request a token at once. Concurrent callers share a single in-flight acquisition rather than stampeding the token endpoint.
- After a token expires it is re-acquired transparently on the next request, with no operator involvement.

The underlying MSAL confidential-client cache serves and renews the token, and the resolver layers a single-flight guard over it so the whole path stays allocation-light and free of duplicate network calls under load.

## Secret-less (managed-identity) authentication

The resolver supports two mutually exclusive authentication modes, configured on `LatticeEntraGraphOptions`.

**Client-secret (confidential-client) path** - the default. Configure the tenant id, client id, and client secret; the app-only token is acquired and refreshed through the MSAL confidential-client cache described above:

```
siloBuilder.AddEntraGraphGroupResolver(options =>
{
    options.TenantId = tenantId;
    options.ClientId = clientId;
    options.ClientSecret = clientSecret; // e.g. injected from Key Vault
});
```

**Secret-less path** - for deployments that want no client secret to store, rotate, or leak. Set `LatticeEntraGraphOptions.Credential` to any `Azure.Core` `TokenCredential` (for example `DefaultAzureCredential` or a `ManagedIdentityCredential` bound to a user-assigned managed identity that carries a federated credential on the app registration). The shared app-only Graph client is then built directly from that credential and the configured scopes, and no client secret is acquired, cached, or refreshed; `TenantId` and `ClientId` are ignored, and `ClientSecret` must be left unset (supplying both a `Credential` and a `ClientSecret` fails validation as ambiguous):

```
siloBuilder.AddEntraGraphGroupResolver(options =>
{
    options.Credential = new DefaultAzureCredential();
});
```

`Microsoft.Graph` already provides `Azure.Core` transitively, so selecting the secret-less path adds no new package dependency (use `Azure.Identity` for a concrete credential such as `DefaultAzureCredential`).

The two modes are validated fail-closed at registration: exactly one must be configured. Supplying **neither** a credential nor the full tenant/client/secret triple is rejected, and supplying **both** a `Credential` and a `ClientSecret` is rejected as ambiguous rather than silently picking one.

## Identity directory

The same registration also installs a Microsoft Graph-backed
`ILatticeIdentityDirectory` (`ProviderId` `"entra"`) - the provider-agnostic
identity source that the Explorer Access area searches and validates against when
an operator picks or creates a subject. It searches users and groups in the tenant
over the same app-only Graph token described above, requiring the `User.Read.All`
and `Group.Read.All` application permissions. A Graph call that Graph itself
denies (an `ODataError`) degrades cleanly rather than throwing: a search returns
an empty page and a resolve returns `null`, so the Access area keeps working
without an unhandled fault. Token-acquisition failures and non-OData transport
failures are not caught on this path and currently propagate.

See [Identity-directory providers](../lattice.membership/identity-directory-providers.md)
for the seam, the static and custom alternatives, and the fail-closed create flow.

## Registration and ordering

The Graph resolver is registered on the silo builder after the Entra authenticator. Registration guards this ordering and fails fast with a clear message when the Entra authenticator has not been registered first. When the add-on is not registered it has zero runtime cost and the authenticator uses its token-only fallback.

## Reference

- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Entra authenticator](../lattice.membership.entra/README.md) - the authenticator that consumes this resolver.
- [Membership documentation](../lattice.membership/README.md) - the base identity and authorization add-on.

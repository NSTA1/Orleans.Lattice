# Orleans.Lattice.Membership.Entra.Graph

Microsoft Graph-backed group resolver for [`Orleans.Lattice.Membership.Entra`](https://github.com/NSTA1/Orleans.Lattice).

Entra ID tokens cap the number of group ids they carry. When a caller belongs to
more groups than the token can hold, Entra omits the `groups` claim and marks the
token as overflowed. This package resolves that overflow: it calls Microsoft
Graph to fetch the caller's full transitive group membership and hands it back to
the Entra authenticator through the `IEntraGroupResolver` seam.

The same registration also installs a Graph-backed `ILatticeIdentityDirectory`
(provider id `entra`) that searches and resolves the tenant's users and groups,
sharing the resolver's app-only Graph client.

The Microsoft Graph SDK and MSAL dependencies live here, isolated from the core
Entra authenticator package, so applications that never hit the overage case pay
for neither.

## Transparent token management

On the default client-secret path the resolver acquires its own app-only
Microsoft Graph access token through the MSAL confidential-client token cache
(`AcquireTokenForClient`), which caches the token and transparently refreshes it
before expiry; a secret-less `TokenCredential` (for example a managed identity)
can be supplied instead. Operators never hand-manage
or rotate a Graph token. Concurrent group lookups share a single in-flight token
acquisition rather than each triggering their own, so a cold cache does not
stampede the token endpoint.

Register it after the Entra authenticator, and set that authenticator's
`GroupResolutionMode` to `EntraGroupResolutionMode.ResolveOnOverage`: the
authenticator consults the resolver for the overage case only in that mode (its
`TokenOnly` default never makes an external lookup).

See the [Entra Graph documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.membership.entra.graph/README.md)
for configuration.

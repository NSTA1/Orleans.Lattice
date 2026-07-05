# Orleans.Lattice.Membership.Entra.Graph

Microsoft Graph-backed group resolver for [`Orleans.Lattice.Membership.Entra`](https://github.com/NSTA1/Orleans.Lattice).

Entra ID tokens cap the number of group ids they carry. When a caller belongs to
more groups than the token can hold, Entra omits the `groups` claim and marks the
token as overflowed. This package resolves that overflow: it calls Microsoft
Graph to fetch the caller's full transitive group membership and hands it back to
the Entra authenticator through the `IEntraGroupResolver` seam.

The Microsoft Graph SDK and MSAL dependencies live here, isolated from the core
Entra authenticator package, so applications that never hit the overage case pay
for neither.

## Transparent token management

The resolver acquires its own app-only Microsoft Graph access token through the
MSAL confidential-client token cache (`AcquireTokenForClient`), which caches the
token and transparently refreshes it before expiry. Operators never hand-manage
or rotate a Graph token. Concurrent group lookups share a single in-flight token
acquisition rather than each triggering their own, so a cold cache does not
stampede the token endpoint.

Register it after the Entra authenticator; the authenticator picks it up
automatically for the overage case.

See the [Entra Graph documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.membership.entra.graph/README.md)
for configuration.

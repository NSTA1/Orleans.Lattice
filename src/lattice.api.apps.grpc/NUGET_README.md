# Orleans.Lattice.Api.Apps.Grpc

Code-first gRPC binding for `ILatticeAppsControl`. One service dispatches by app
slug and exposes install, enable, disable, uninstall, list, describe, consent,
role re-binding, and capability operations. Role re-binding is served by a
registered `ILatticeAppRoleBindings` (or an `ILatticeAppsControl` that also
implements it) and answers `Unimplemented` on a host that serves neither. It references the abstractions package, not the
facade implementation. Three further services bind the app catalogue, workspace
and UI bridge contracts (`AddLatticeAppCatalogApiGrpc`,
`AddLatticeAppWorkspaceApiGrpc` and `AddLatticeAppBridgeApiGrpc`, each with its
`Map*` counterpart and its own client), behind the same default-deny interceptor.

Register Orleans serialization and an `ILatticeAppsControl` implementation in
the host, call `services.AddLatticeAppsApiGrpc()`, then
`app.MapLatticeAppsApiGrpc()`. Authorization defaults to deny. Implement and
register `ILatticeAppsApiAuthorizer` with the host's admission policy, including
when an outer authentication boundary protects the endpoint. The facade still
independently enforces its app-install permissions and validates all requested
capabilities.

The default credential bridge reads `authorization`, strips a `Bearer` prefix,
and scopes the token to the call. It does not authenticate the token.
`ActiveTenantHeaderName` defaults to `lattice-active-tenant`; this is an assertion
that the facade's tenancy seam must validate, never a grant of access.

Create a client with `LatticeAppsApiGrpcClient.Create(callInvoker,
serializerProvider)`. The client implements `ILatticeAppsControl` and
`ILatticeAppRoleBindings`; configure
TLS, deadlines, credentials, and routing on the supplied invoker. The client
does not own the invoker or channel.

`GetAuthSchemeAsync` is the only unauthenticated RPC. Configure
`AdvertisedAuthSchemes` with public sign-in parameters only, never secrets or
user-specific data. Missing descriptions and consent remain null across the
wire. Wire aliases and field ids are stable and additive-only.

Part of [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). See the
[Apps gRPC binding documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.apps.grpc/README.md)
for registration, clients and the wire contract.

# Orleans.Lattice.Api.Apps.Grpc

Code-first gRPC binding for `ILatticeAppsControl`. One service dispatches by app
slug and exposes install, enable, disable, uninstall, list, describe, consent,
and capability operations. It references the abstractions package, not the
facade implementation.

Register Orleans serialization and an `ILatticeAppsControl` implementation in
the host, call `services.AddLatticeAppsApiGrpc()`, then
`app.MapLatticeAppsApiGrpc()`. Authorization defaults to deny. Register an
`ILatticeAppsApiAuthorizer` to admit callers; the facade still independently
enforces its app-install permissions and validates all requested capabilities.
`AllowAllAppsApiAuthorizer` and `RequireAuthorization = false` are explicit
opt-ins for endpoints protected by an outer authentication boundary.

The default credential bridge reads `authorization`, strips a `Bearer` prefix,
and scopes the token to the call. It does not authenticate the token.
`ActiveTenantHeaderName` defaults to `lattice-active-tenant`; this is an assertion
that the facade's tenancy seam must validate, never a grant of access.

Create a client with `LatticeAppsApiGrpcClient.Create(callInvoker,
serializerProvider)`. The client implements `ILatticeAppsControl`; configure
TLS, deadlines, credentials, and routing on the supplied invoker. The client
does not own the invoker or channel.

`GetAuthSchemeAsync` is the only unauthenticated RPC. Configure
`AdvertisedAuthSchemes` with public sign-in parameters only, never secrets or
user-specific data. Missing descriptions and consent remain null across the
wire. Wire aliases and field ids are stable and additive-only.

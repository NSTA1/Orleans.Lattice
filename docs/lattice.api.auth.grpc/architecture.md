---
agent_spec: "docs/agents/api/auth.json"
---

# Architecture

The binding adapts the in-process [`ILatticeAuthAdmin`](../lattice.api.auth/architecture.md) facade to a code-first gRPC service. It adds a transport gate in front of the facade and an identity bridge into it, and leaves every authorization decision about membership and policy to the facade and the cluster's access gate. This page describes the pipeline by behaviour; the public surface is in the [Public API](api.md).

## Registration and routing

`AddLatticeAuthApiGrpc` registers, on the host's service collection, the method definitions, the service, the default-deny authorizer, the header credential bridge and the authorization interceptor. The method definitions are built from the Orleans serializers in the host's container, so every request and response record is marshalled with the Orleans binary serializer and no `.proto` file exists. `MapLatticeAuthApiGrpc` resolves the method definitions before mapping, so they are in place when ASP.NET Core binds the service, and maps one unary method per facade operation under the service name `orleans.lattice.api.auth`.

The interceptor is registered globally but acts only on calls whose routed method belongs to that service, so other gRPC services on the same host are unaffected.

## Call pipeline

Each call passes through these stages in order:

1. **Method classification.** The interceptor resolves the method from the routed endpoint's metadata rather than from the raw request path. Endpoint routing matches paths case-insensitively, so classifying the raw path could let a re-cased path reach the handler without passing the gate.
2. **Transport gate.** When `RequireAuthorization` is set, the interceptor describes the call as a `LatticeAuthApiAuthorizationContext` - the decoded `LatticeAuthApiOperation` (`Unknown` for an unrecognised method) and the id the request targets - and asks the registered `ILatticeAuthApiAuthorizer`. A `false` answer fails the call with `PermissionDenied` and logs a warning; cancellation while deciding fails it with `Cancelled`. With the default `DenyAllAuthApiAuthorizer` every call stops here.
3. **Identity bridge.** The service asks `ILatticeAuthApiCredentialBridge` for the caller's credential and, when there is one, places it on the ambient core credential context for the duration of the call. The default bridge reads the configured header, strips the configured scheme prefix and returns no credential for an absent or empty value, so the caller is anonymous. The bridge does not validate the token; a credential authenticator registered on the silo does, when the facade resolves the caller ([membership pipeline](../lattice.membership/architecture.md)).
4. **Active-tenant assertion.** Only for a `ListRules` call with `AuthPageRequest.ActiveTenantOnly` set, the service stamps the asserted tenant from the configured header onto the ambient context for the facade to validate.
5. **Facade call.** The service invokes the matching `ILatticeAuthAdmin` method. The facade's administrator check runs against the resolved subject whatever the transport gate decided, so a permissive authorizer or `RequireAuthorization = false` never admits an anonymous or non-administrator caller.
6. **Error mapping.** A facade denial becomes `PermissionDenied` with only the denied tree, operation, subject and reason in trailers; a refused tenant assertion becomes `PermissionDenied`; an `ArgumentException` becomes `InvalidArgument` with its message; cancellation becomes `Cancelled`; anything else is logged and returned as `Internal` with a generic message. See [Error mapping](README.md#error-mapping).

## Client

`LatticeAuthApiGrpcClient` builds the same method definitions from the caller's serializer provider and issues unary calls over the caller's `CallInvoker`. Because both sides derive their marshallers from the same Orleans serializers and record aliases, client and server stay wire-compatible by construction.

## Source map

- [Registration](../../src/lattice.api.auth.grpc/LatticeAuthApiGrpcServiceCollectionExtensions.cs)
- [Authorization interceptor](../../src/lattice.api.auth.grpc/Security/LatticeAuthApiGrpcAuthInterceptor.cs)
- [Default credential bridge](../../src/lattice.api.auth.grpc/HeaderLatticeAuthApiCredentialBridge.cs)
- [Service](../../src/lattice.api.auth.grpc/LatticeAuthApiGrpcService.cs)
- [Client](../../src/lattice.api.auth.grpc/LatticeAuthApiGrpcClient.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [`Orleans.Lattice.Api.Auth` architecture](../lattice.api.auth/architecture.md)
- [Security posture](../lattice.auth/security-posture.md)

---
agent_spec: "docs/agents/api/treeadmin.json"
---

# Orleans.Lattice.Api.TreeAdmin.Grpc architecture

This page describes how a remote call travels through the code-first gRPC binding to the tree-administration facade, and the seams it passes on the way. The gRPC service, the method definitions, the marshallers, the authorization interceptor, the default credential bridge and the options-backed auth-scheme source are internal, so they are described here by behaviour; the public client, options, seams and wire records are named. The facade's own behaviour - what each operation does and which authority it requires - is documented in the [facade architecture](../lattice.api.treeadmin/architecture.md) and [facade guide](../lattice.api.treeadmin/README.md).

## Request path

1. **Client.** A `LatticeTreeAdminApiGrpcClient` method checks its arguments, wraps them in the RPC's request record and sends a unary call through the caller's `CallInvoker`, passing only the caller's cancellation token. Headers, deadlines and TLS come from that invoker.
2. **Wire.** The request record is serialized with the Orleans binary serializer (see [Code-first binding](#code-first-binding)).
3. **Routing.** ASP.NET Core endpoint routing dispatches the call to the service mapped by `MapLatticeTreeAdminApiGrpc`.
4. **Transport gate.** The authorization interceptor decides whether the call may run at all (see [Transport gate](#transport-gate)).
5. **Service.** The service stamps the caller's credential and asserted tenant onto the ambient context for this call only, calls the one facade member the RPC maps to, and translates the outcome into a gRPC status (see [Facade gate](#facade-gate) and [Status mapping](#status-mapping)).
6. **Facade.** `ILatticeTreeAdmin`, `ILatticeTreeAdminOperations`, `ILatticeStorageUsageOperations` or `ILatticeWalReclamation` validates the request and authorizes it through the core access gate, exactly as for an in-process caller.

The response record travels back through the same marshalling, and the client returns the facade result, unwrapping the few RPCs whose response is an envelope (see [API reference](api.md#client)).

## Code-first binding

The binding defines its RPCs in C#, not in a `.proto`. A method-definition set builds one unary gRPC method per RPC under the service name `orleans.lattice.api.treeadmin`, each with a request and a response marshaller built from the Orleans `Serializer<T>` instances resolved from the service provider. The server-side service and the public client build the same set, so the two ends agree on every method name and message type by construction; the [API reference](api.md#service-and-rpcs) lists all 61 RPCs.

Each marshaller writes the encoded message straight into the gRPC stream's buffer writer, with no intermediate array. On the read side, a single-segment payload is deserialized in place; a multi-segment payload is copied into a pooled buffer that is returned after use.

Registration and mapping cooperate at startup. Registration provides the method-definition set as a singleton whose construction also publishes it to the static hook that `Grpc.AspNetCore` calls to bind the service's methods; `MapLatticeTreeAdminApiGrpc` resolves that singleton before mapping the service, so the hook never runs before the set exists.

Wire records carry stable `oitg.` aliases (see [Wire compatibility](api.md#wire-compatibility)). Contract evolution is additive: new members take new `[Id]` values, so a client built before a member existed reads the record and ignores it.

## Authorization

A protected call passes two independent gates. The transport gate is closed by default. The facade gate is closed by default once the `Orleans.Lattice.Auth` add-on is registered; without that add-on the core access gate allows every call, and the transport gate is the only barrier.

### Transport gate

The authorization interceptor is added to the host's global gRPC pipeline, but it acts only on calls routed to this service. It identifies the call by the method it was **routed to**, read from the matched endpoint, never by the raw request path: routing matches paths case-insensitively, so a check on the raw path would let a re-cased path reach the handler without passing the gate. Calls to other gRPC services on the host pass through untouched.

For a call to this service, the interceptor:

1. lets `GetAuthScheme` through without a check, so a client can learn how to sign in before it holds a credential;
2. lets every call through when `LatticeTreeAdminApiGrpcOptions.RequireAuthorization` is `false`;
3. otherwise describes the call as a `LatticeTreeAdminApiAuthorizationContext` - the routed `Operation` (a named `LatticeTreeAdminApiOperation`, or `Unknown` for every RPC the enum does not name), the `TargetId` decoded from the request, and the `ServerCallContext` - and asks the registered `ILatticeTreeAdminApiAuthorizer`;
4. when the request names a second tree the operation reaches - the destination of `SnapshotTree`, or the physical tree of `SetTreeAlias` - asks the authorizer again, under the same operation, with that tree as the target, unless it is blank or the same tree as the first;
5. refuses a denied call with `PermissionDenied`, logging a warning, before the service or facade is touched. If the authorizer itself is cancelled, the call ends `Cancelled`.

The default `DenyTreeAdminApiAuthorizer` refuses everything, so with the defaults no protected call succeeds until a host registers a permissive authorizer or turns enforcement off behind an outer boundary. Because unnamed RPCs arrive as `Unknown`, a per-operation policy that denies by default also refuses the RPCs the enum does not distinguish. The interceptor applies the same check to streaming call shapes, deciding before the first message is read, although every RPC this service defines is unary.

The README's [Authorization surface](README.md#authorization-surface) lists which RPCs carry which operation and which requests leave `TargetId` `null`.

### Facade gate

The service runs after the transport gate and is where the caller's identity enters the request:

- **Credential.** The registered `ILatticeTreeAdminApiCredentialBridge` lifts a `LatticeCredential` from the call. The default bridge reads the `CredentialHeaderName` header, strips a leading `CredentialScheme` prefix, and performs no validation - a registered credential authenticator validates the token. When the bridge returns a credential, the service opens a `LatticeCredentialContext` scope for the call; when it returns `null` the caller is anonymous, which auth-backed control refuses.
- **Asserted tenant.** The service reads the `ActiveTenantHeaderName` header through `LatticeActiveTenantAssertion` and, for a syntactically valid tenant id, opens a `LatticeActiveTenantContext` scope. An absent, blank or invalid value opens no scope. The value is only an assertion: the tenancy add-on re-validates it against the caller's membership, and an assertion the caller may not use is refused as `PermissionDenied`.

Both scopes are disposed when the call completes, so a credential or tenant never leaks into another call. Both are opened even when `RequireAuthorization` is `false`: switching the transport gate off does not switch off the facade gate. The two gates are complementary - the transport gate is a coarse edge control keyed by call metadata, operation and target, and the facade gate is the per-tree authorization the facade applies to every caller.

`GetAuthScheme` is served without either scope: it reads the registered `ILatticeTreeAdminApiAuthSchemeSource` and returns its advertisement, which must contain only public configuration.

## Optional facades

The service requires `ILatticeTreeAdmin`. It takes `ILatticeTreeAdminOperations`, `ILatticeStorageUsageOperations` and `ILatticeWalReclamation` as optional dependencies, so a host that registers the binding without one of them still starts, and the RPCs of the missing facade answer `Unimplemented`. `AddLatticeTreeAdminApi` registers all three, so a standard silo serves every RPC.

## Status mapping

The service translates every facade outcome into an explicit gRPC status at one boundary shared by all RPCs. A status the service raised itself, such as `Unimplemented`, passes through unchanged. Cancellation becomes `Cancelled`; authorization, tree-ownership and fail-closed tenant-resolution refusals become `PermissionDenied`; an unknown view or tag index becomes `NotFound`; precondition failures become `FailedPrecondition`; malformed arguments and out-of-order bulk-load chunks become `InvalidArgument`. Anything else is logged on the server and returned as `Internal` with a generic message, so exception text never reaches the client. The binding never issues `Unauthenticated`. The full exception-to-status table is in the README's [Status mapping](README.md#status-mapping).

## Source map

- [Registration and mapping](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminApiGrpcServiceCollectionExtensions.cs)
- [Options](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminApiGrpcOptions.cs)
- [Method definitions](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminGrpcMethods.cs) and [marshallers](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminGrpcMarshallers.cs)
- [Service and status mapping](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminGrpcService.cs)
- [Authorization interceptor](../../src/lattice.api.treeadmin.grpc/Security/LatticeTreeAdminApiGrpcAuthInterceptor.cs) and [routed-method resolution](../../src/lattice.api.treeadmin.grpc/Security/GrpcRoutedMethod.cs)
- [Default credential bridge](../../src/lattice.api.treeadmin.grpc/HeaderLatticeTreeAdminApiCredentialBridge.cs)
- [Typed client](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminApiGrpcClient.cs)

## Related

- [Package README](README.md)
- [API reference](api.md)
- [Configuration](configuration.md)
- [`Orleans.Lattice.Api.TreeAdmin` architecture](../lattice.api.treeadmin/architecture.md)

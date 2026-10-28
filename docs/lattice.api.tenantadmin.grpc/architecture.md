---
agent_spec: "docs/agents/api/tenantadmin.json"
---

# Orleans.Lattice.Api.TenantAdmin.Grpc architecture

This page describes how a remote call travels through the code-first gRPC binding to the tenant-administration facades, and the seams it passes on the way. The gRPC service, the method definitions, the marshallers, the authorization interceptor, the default credential bridge and the options-backed auth-scheme source are internal, so they are described here by behaviour; the public clients, options, seams and wire records are named.

The binding is a thin adapter. Tenant lifecycle, region residency, admin subjects, cross-tenant grants, quota usage, self-service and delegated tenant access all live in the facades, which are the single enforcement point for tenant and operator authority; see the [facade architecture](../lattice.api.tenantadmin/architecture.md) and [facade guide](../lattice.api.tenantadmin/README.md). The binding adds a coarse transport gate in front of them and never replaces, widens or downgrades their decisions.

## Request path

1. **Client.** A `LatticeTenantAdminApiGrpcClient` or `LatticeTenantSelfServiceApiGrpcClient` method checks its arguments for `null` and empty values, wraps them in the RPC's request record and sends a unary call through the caller's `CallInvoker`, passing only the caller's cancellation token. Headers, deadlines and TLS come from that invoker.
2. **Wire.** The request record is serialized with the Orleans binary serializer (see [Code-first binding](#code-first-binding)).
3. **Routing.** ASP.NET Core endpoint routing dispatches the call to the service mapped by `MapLatticeTenantAdminApiGrpc`.
4. **Transport gate.** The authorization interceptor decides whether an enforced call may run at all (see [Transport gate](#transport-gate)).
5. **Service.** The service stamps the caller's credential and asserted tenant onto the ambient context for this call only, calls the one facade member the RPC maps to, and translates the outcome into a gRPC status (see [Facade gate](#facade-gate) and [Status mapping](#status-mapping)).
6. **Facade.** The facade authorizes the caller - as a platform operator, or as an admin of the tenant the request names, depending on the verb - and only then reads or changes tenancy state.

The response record travels back through the same marshalling, and the client returns the facade result, unwrapping the few RPCs whose response is an envelope (see [API reference](api.md#clients)).

## Code-first binding

The binding defines its RPCs in C#, not in a `.proto`. A method-definition set builds one unary gRPC method per RPC under the service name `orleans.lattice.api.tenantadmin`, each with a request and a response marshaller built from the Orleans `Serializer<T>` instances resolved from the service provider. The server-side service and both public clients build the same set, so every end agrees on method names and message types by construction; the [API reference](api.md#service-and-rpcs) lists all 40 RPCs.

Each marshaller writes the encoded message straight into the gRPC stream's buffer writer, with no intermediate array. On the read side, a single-segment payload is deserialized in place; a multi-segment payload is copied into a pooled buffer that is returned after use.

Registration and mapping cooperate at startup. Registration provides the method-definition set as a singleton whose construction also publishes it to the static hook that `Grpc.AspNetCore` calls to bind the service's methods; `MapLatticeTenantAdminApiGrpc` resolves that singleton before mapping the service, so the hook never runs before the set exists.

Wire records this package defines carry stable `oitng.` aliases, and the facades' result records keep their `oitn.` aliases (see [Wire compatibility](api.md#wire-compatibility)). Contract evolution is additive: a member added to a record a request already carries - such as the delegated-access caps on `TenantQuotasDescriptor` - reads as `null` from a client that predates it.

## Authorization

An enforced call passes two independent gates: the transport gate in this package, then the facade's own authorization. The transport gate is closed by default. The facades always run their checks, but what the platform-operator check decides depends on the `Orleans.Lattice.Auth` add-on: without it the core no-op access gate allows every check, so the transport gate is the only barrier; with it, the operator check is evaluated on the control plane and refuses an unmatched caller whatever `LatticeAuthOptions.DefaultEffect` is set to.

### Transport gate

The authorization interceptor is added to the host's global gRPC pipeline, but it acts only on calls routed to this service. It identifies the call by the method it was **routed to**, read from the matched endpoint, never by the raw request path: routing matches paths case-insensitively, so a check on the raw path would let a re-cased path reach the handler without passing the gate. Calls to other gRPC services on the host pass through untouched.

For a call to this service, the interceptor:

1. lets `GetAuthScheme` through without a check, so a client can learn how to sign in before it holds a credential;
2. lets the three read-only self-service RPCs - `GetCurrentTenant`, `ListAccessibleTenants` and `GetTenant` - through without a check, so any read-capable caller, including an anonymous one, can reach them. Their scoping is enforced by `ILatticeTenantSelfService`, not here: an anonymous caller lists nothing, and a caller only ever sees tenants it is authorized for;
3. lets every other call through when `LatticeTenantAdminApiGrpcOptions.RequireAuthorization` is `false`;
4. otherwise describes the call as a `LatticeTenantAdminApiAuthorizationContext` - the routed `Operation` (one of the 36 named `LatticeTenantAdminApiOperation` values, or `Unknown` for an unrecognised method), the `TargetId`, and the `ServerCallContext` - and asks the registered `ILatticeTenantAdminApiAuthorizer`;
5. refuses a denied call with `PermissionDenied`, logging a warning, before the service or facade is touched. If the authorizer itself is cancelled, the call ends `Cancelled`.

`TargetId` is the tenant id the request names. A cross-tenant grant request names two tenants, and the interceptor reports the **granting** tenant for every grant mutation, including the grantee-side approve and reject. It is a coarse hint for a transport policy; the authority each grant transition actually requires is decided, fail-closed, by `ILatticeTenantGrantAdmin`.

The default `DenyTenantAdminApiAuthorizer` refuses everything, so with the defaults no enforced call succeeds until a host registers a permissive authorizer or turns enforcement off behind an outer boundary. Presenting a credential is not enough on its own. The interceptor applies the same check to streaming call shapes, deciding before the first message is read, although every RPC this service defines is unary.

### Facade gate

The service runs after the transport gate - and, for the exempt RPCs, without it - and is where the caller's identity enters the request:

- **Credential.** The registered `ILatticeTenantAdminApiCredentialBridge` lifts a `LatticeCredential` from the call. The default bridge reads the `CredentialHeaderName` header, strips a leading `CredentialScheme` prefix, and performs no validation - a registered credential authenticator validates the token. When the bridge returns a credential, the service opens a `LatticeCredentialContext` scope for the call; when it returns `null` the caller is anonymous.
- **Asserted tenant.** The service reads the `ActiveTenantHeaderName` header through `LatticeActiveTenantAssertion` and, for a syntactically valid tenant id, opens a `LatticeActiveTenantContext` scope. An absent, blank or invalid value opens no scope, and the caller resolves the reserved `default` tenant. The value is only an assertion: the tenancy engine re-validates it against the caller's membership, and an assertion the caller may not use is refused as `PermissionDenied`.

Both scopes are opened for every RPC except `GetAuthScheme`, the self-service RPCs included, and both are disposed when the call completes, so a credential or tenant never leaks into another call. Both are opened even when `RequireAuthorization` is `false`: switching the transport gate off does not switch off the facades' checks.

The lifecycle, region-residency, admin-subject, grant, quota-usage and delegated-access RPCs authorize on the caller's subject and the tenant the request names, not on the asserted active tenant. For the delegated-access RPCs the facades authorize the caller before they check the feature flag, the reserved `default` tenant or anything else, so a denied caller learns nothing; the binding adds no authorization path of its own for them. Names, trees and rule ids travel tenant-local and are composed into full ids by the facade, never accepted from the wire as full ids.

`GetAuthScheme` is served without either scope: it reads the registered `ILatticeTenantAdminApiAuthSchemeSource` and returns its advertisement, which must contain only public configuration.

## Optional facades

The service requires `ILatticeTenantAdmin` and `ILatticeTenantSelfService`. It takes `ILatticeTenantRegionAdmin`, `ILatticeTenantQuotaUsage`, `ILatticeTenantAccessAdmin`, `ILatticeTenantGrantAdmin`, `ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin` as optional dependencies, so a host that registers the binding without one of them still starts and serves every lifecycle and self-service RPC, and the RPCs of the missing facade answer `Unimplemented`. `AddLatticeTenantAdminApi` registers all of them.

## Status mapping

Each RPC group - lifecycle and quotas, quota usage, region residency, admin subjects, cross-tenant grants, self-service, and delegated tenant access - translates its facade's outcomes into explicit gRPC statuses at its own boundary, so a domain exception never falls through to an opaque fault. The groups share one vocabulary but not every arm: for example an `InvalidOperationException` is `FailedPrecondition` on the lifecycle, residency, subject, grant and delegated-access RPCs, but is not mapped on the quota-usage and self-service RPCs. A status the service raised itself, such as `Unimplemented`, passes through unchanged. Anything unmapped is logged on the server and returned as `Internal` with a generic message, so exception text never reaches the client.

On the delegated-access RPCs a reached cap (`LatticeQuotaExceededException`) becomes `ResourceExhausted`, with the breached dimension, when the exception names one, in the `lattice-quota-dimension` trailer and, when the limit is positive, the usage and cap in `lattice-quota-current` and `lattice-quota-limit`. Neither the tree nor the tenant is sent back.

The full exception-to-status table, with the groups each arm applies to, is in the README's [Status mapping](README.md#status-mapping).

## Source map

- [Registration and mapping](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminApiGrpcServiceCollectionExtensions.cs)
- [Options](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminApiGrpcOptions.cs)
- [Method definitions](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminGrpcMethods.cs) and [marshallers](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminGrpcMarshallers.cs)
- [Service and status mapping](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminGrpcService.cs), with the [delegated-access RPCs](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminGrpcService.TenantAccess.cs)
- [Authorization interceptor](../../src/lattice.api.tenantadmin.grpc/Security/LatticeTenantAdminApiGrpcAuthInterceptor.cs) and [routed-method resolution](../../src/lattice.api.tenantadmin.grpc/Security/GrpcRoutedMethod.cs)
- [Default credential bridge](../../src/lattice.api.tenantadmin.grpc/HeaderLatticeTenantAdminApiCredentialBridge.cs)
- [Typed client](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminApiGrpcClient.cs) and [self-service client](../../src/lattice.api.tenantadmin.grpc/LatticeTenantSelfServiceApiGrpcClient.cs)

## Related

- [Package README](README.md)
- [API reference](api.md)
- [Configuration](configuration.md)
- [`Orleans.Lattice.Api.TenantAdmin` architecture](../lattice.api.tenantadmin/architecture.md)
- [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md)

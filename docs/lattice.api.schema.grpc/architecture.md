# Orleans.Lattice.Api.Schema.Grpc architecture

This page describes the code-first gRPC binding and its two-layer, fail-closed authorization model. The gRPC service, method definitions, marshallers, interceptor, and default auth-scheme plumbing are internal and are described here by behaviour; the public client, options, and wire records are named.

## Code-first binding

The binding defines its RPCs in C# rather than a `.proto`. A method-definition singleton builds one gRPC `Method` per operation from the Orleans serializers resolved out of DI, under the service name `orleans.lattice.api.schema`. The server-side service and the public `LatticeSchemaApiGrpcClient` share those definitions, so the wire contract is identical on both ends by construction and there is no generated stub to keep in sync.

RPC request and response contracts are `[GenerateSerializer]` records marshalled with the Orleans binary serializer. Most RPCs wrap facade DTOs in transport records such as `SetPolicyRequest` and `GetPolicyResponse`; the dead-letter stream and capability probe carry shared schema DTOs directly. That is why the client's `Create` factory takes an `IServiceProvider` with `AddSerializer()` registered: the per-message marshallers are built from those serializers, so a client and server that share the Orleans serialization configuration cannot disagree on the wire format.

The operations map to two gRPC shapes: unary for policy, count, versioning, remediation, compliance, capability, and auth-scheme discovery; server-streaming for `StreamDeadLetters`. The streaming RPC is what lets a large dead-letter set move with bounded memory end to end - the facade streams, the service forwards each item as it arrives, and the client re-exposes it as an `IAsyncEnumerable<T>`.

## Two-layer authorization

Every protected call passes through two independent gates. The transport gate is closed by default; the facade gate is closed by default only once the `Orleans.Lattice.Auth` add-on is registered (see below).

### 1. Transport meta-authorizer

An authorization interceptor runs first, before the facade is touched. It treats `GetAuthScheme` as the only unauthenticated discovery call; every other schema-control RPC is protected when `RequireAuthorization` is true. The binding defaults to deny, so protected calls are rejected with `PermissionDenied` until the host configures authorization or sets `RequireAuthorization` to `false` behind a trusted boundary. The interceptor is registered globally but scopes its enforcement to the schema control-API service by service-name prefix, so other gRPC services on the same host are unaffected.

The interceptor decodes each protected call into a `LatticeSchemaApiAuthorizationContext` - the `LatticeSchemaApiOperation`, the governed tree id as `TargetId`, and the underlying `ServerCallContext` - and asks the registered `ILatticeSchemaApiAuthorizer` whether the call may run. An operation the interceptor does not recognise is presented to the authorizer as `Unknown` rather than being waved through, so a deny-by-default policy refuses a future or unmapped RPC. Separately, server faults the binding does not map are returned with safe gRPC status codes rather than leaking implementation details (see [Status mapping](#status-mapping)).

### 2. Facade scope authorization

Once past the transport gate, the service invokes the control facade. The facade then authorizes the operation's tree scope through the schema engine's internal authorization component, exactly as an in-process facade caller would. Reads require Read authority; mutations require SchemaAdmin authority. With the `Orleans.Lattice.Auth` add-on registered (its `LatticeAuthOptions.DefaultEffect` defaults to `Deny`), an anonymous or unauthorized caller is denied here even when the transport gate allowed the call; without it the core no-op access gate allows every call (see [Credential bridging](#credential-bridging)).

The two gates are complementary, not redundant: the transport gate is a coarse edge control keyed by headers, operation, and target, while the facade gate is the engine's own fine-grained, per-tree, fail-closed authorization. A deployment can run a permissive transport gate behind a trusted boundary and still get full per-tree enforcement from the facade, or tighten both.

## Credential bridging

The default credential path reads a single configurable header (`CredentialHeaderName`, default `authorization`), strips a case-insensitive scheme prefix (`CredentialScheme`, default `Bearer`), and lifts the remaining token onto the ambient Lattice credential for the registered credential authenticator to resolve into a subject. A host with a bespoke identity source - a client TLS certificate, a signed edge header, a pre-resolved principal - registers its own `ILatticeSchemaApiCredentialBridge`, and the built-in default steps aside. Returning no credential leaves the caller anonymous; the schema facade then denies protected operations when auth-backed schema control is active.

When the authorization add-on is not registered, the default bridge still reads and bridges the header, but the core no-op access gate ignores the credential, so the schema control API behaves exactly as it does without a credential layer.

## Auth-scheme discovery

`GetAuthScheme` is deliberately unauthenticated so a client can discover how to sign in before it holds a credential. The response is built from the host-configured `AdvertisedAuthSchemes` (empty by default, so a client falls back to manual or Basic selection). Because the advertisement is served without a credential, it must contain only public configuration - scheme ids and public parameters like an OIDC authority or client id - and never a secret or user-specific data.

## Status mapping

A denial from either gate reaches the client as a `PermissionDenied` `RpcException`, so a caller handles authorization failure uniformly regardless of which layer refused the call.

The binding also translates the facade's other failure shapes into stable gRPC status codes, so a client can branch on the code rather than parse a message:

| Facade outcome | gRPC `StatusCode` | Notes |
| --- | --- | --- |
| Authorization denied (transport gate or facade scope check), or a fail-closed tenant resolution (`LatticeTenantAccessDeniedException`) | `PermissionDenied` | The detail is safe to surface; never names a secret. |
| Entry not found (`KeyNotFoundException`) | `NotFound` | |
| Admission cap reached (`LatticeQuotaExceededException`) | `ResourceExhausted` | A remediation or version migration rebuilds the tree into a fresh destination one entry at a time, so it runs under the same per-tree admission caps (`LatticeOptions.MaxLiveKeys` / `MaxEstimatedBytes`) as any other write. Mapped ahead of the `InvalidOperationException` row below, which would otherwise shadow it. See [Quota refusals](#quota-refusals). |
| Precondition failure (`InvalidOperationException`) | `FailedPrecondition` | Most often a versioning operation when the host has not registered `AddLatticeSchemaVersioning(...)` (non-versioning schema operations may still be available); also an unversioned tree, a target version that does not advance, or a conflicting remediation already in flight. |
| Invalid argument (`ArgumentException`) | `InvalidArgument` | |
| Request cancelled | `Cancelled` | On the server-streaming `StreamDeadLetters` RPC a cancelled call simply ends the stream instead. |
| Any other fault | `Internal` | The detail is deliberately opaque; the real exception is logged server-side, not returned. |

The `FailedPrecondition` shape is the one an operator most often needs to act on for versioning: the endpoint is reachable, but the silo intentionally did not register the optional versioning add-on. The detail should be clear enough for an operator UI to explain which registration is missing.

### Quota refusals

A `ResourceExhausted` refusal carries the breached dimension - and, where the dimension has one, the observed value and the configured ceiling - as response trailers, so a client can branch without parsing the status message:

| Trailer | Value |
| --- | --- |
| `lattice-quota-dimension` | `keys` or `bytes` for the per-tree admission caps a remediation build can reach. |
| `lattice-quota-tree` | The tree whose admission quota was breached (the remediation destination, not the caller's source id). |
| `lattice-quota-current` | The observed value on the breached dimension. Omitted for a dimension with no numeric ceiling. |
| `lattice-quota-limit` | The configured ceiling on the breached dimension. Omitted for a dimension with no numeric ceiling. |

No tenant id is echoed back: it is a server-side attribution decision, and the dimension is what the caller needs to act on. The remedy is to reduce the source tree's footprint or raise the cap, then re-run the remediation - it resumes idempotently. See [`LatticeQuotaExceededException`](../lattice/api.md#admission-back-pressure---latticequotaexceededexception) for the full contract.

## Wire compatibility

Wire messages live under `Model/*` and use Orleans aliases prefixed `oisg.`. The facade's shared abstractions record (`LatticeSchemaCapabilities`) uses the `ois.` prefix, and the schema engine's records carried on the wire keep the engine's own `ols.` aliases. Contract evolution is additive-only: new fields use new `[Id(n)]` values, and aliases or field numbers are never renumbered. That lets a newer response decode under an older client while preserving the stable wire names.

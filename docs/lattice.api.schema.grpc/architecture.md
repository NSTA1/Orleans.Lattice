# Orleans.Lattice.Api.Schema.Grpc architecture

This page describes the code-first gRPC binding and its two-layer, fail-closed authorization model. The gRPC service, method definitions, marshallers, interceptor, and default auth-scheme plumbing are internal and are described here by behaviour; the public client, options, and wire records are named.

## Code-first binding

The binding defines its RPCs in C# rather than a `.proto`. A method-definition singleton builds one gRPC `Method` per operation from the Orleans serializers resolved out of DI, under the service name `orleans.lattice.api.schema`. The server-side service and the public `LatticeSchemaApiGrpcClient` share those definitions, so the wire contract is identical on both ends by construction and there is no generated stub to keep in sync.

RPC request and response contracts are `[GenerateSerializer]` records marshalled with the Orleans binary serializer. Most RPCs wrap facade DTOs in transport records such as `SetPolicyRequest` and `GetPolicyResponse`; the dead-letter stream and capability probe carry shared schema DTOs directly. That is why the client's `Create` factory takes an `IServiceProvider` with `AddSerializer()` registered: the per-message marshallers are built from those serializers, so a client and server that share the Orleans serialization configuration cannot disagree on the wire format.

The operations map to two gRPC shapes: unary for policy, count, versioning, remediation, compliance, capability, and auth-scheme discovery; server-streaming for `StreamDeadLetters`. The streaming RPC is what lets a large dead-letter set move with bounded memory end to end - the facade streams, the service forwards each item as it arrives, and the client re-exposes it as an `IAsyncEnumerable<T>`.

## Two-layer authorization

Every protected call passes through two independent, fail-closed gates.

### 1. Transport meta-authorizer

An authorization interceptor runs first, before the facade is touched. It treats `GetAuthScheme` as the only unauthenticated discovery call; every other schema-control RPC is protected when `RequireAuthorization` is true. The binding defaults to deny, so protected calls are rejected with `PermissionDenied` until the host configures authorization or sets `RequireAuthorization` to `false` behind a trusted boundary. The interceptor is registered globally but scopes its enforcement to the schema control-API service by service-name prefix, so other gRPC services on the same host are unaffected.

An operation the interceptor does not recognise is not waved through. Unknown or unmapped failures are translated to safe gRPC status codes rather than leaking implementation details.

### 2. Facade scope authorization

Once past the transport gate, the service invokes the control facade. The facade then authorizes the operation's tree scope through the schema engine's internal authorization component, exactly as an in-process facade caller would. Reads require Read authority; mutations require SchemaAdmin authority. An anonymous or unauthorized caller is denied here even when the transport gate allowed the call.

The two gates are complementary, not redundant: the transport gate is a coarse edge control keyed by headers, operation, and target, while the facade gate is the engine's own fine-grained, per-tree, fail-closed authorization. A deployment can run a permissive transport gate behind a trusted boundary and still get full per-tree enforcement from the facade, or tighten both.

## Credential bridging

The default credential path reads a single configurable header (`CredentialHeaderName`, default `authorization`), strips a case-insensitive scheme prefix (`CredentialScheme`, default `Bearer`), and lifts the remaining token onto the ambient Lattice credential for the registered credential authenticator to resolve into a subject. A host with a bespoke identity source - a client TLS certificate, a signed edge header, a pre-resolved principal - can provide that identity before the facade runs. Returning no credential leaves the caller anonymous; the schema facade then denies protected operations when auth-backed schema control is active.

When the authorization add-on is not registered, no header is read and the schema control API behaves exactly as it does without a credential layer - the zero-cost path the engine already provides.

## Auth-scheme discovery

`GetAuthScheme` is deliberately unauthenticated so a client can discover how to sign in before it holds a credential. The response is built from the host-configured `AdvertisedAuthSchemes` (empty by default, so a client falls back to manual or Basic selection). Because the advertisement is served without a credential, it must contain only public configuration - scheme ids and public parameters like an OIDC authority or client id - and never a secret or user-specific data.

## Status mapping

A denial from either gate reaches the client as a transport auth failure, either `PermissionDenied` or `Unauthenticated`, so a caller handles authorization failure uniformly regardless of which layer refused the call.

The binding also translates the facade's other failure shapes into stable gRPC status codes, so a client can branch on the code rather than parse a message:

| Facade outcome | gRPC `StatusCode` | Notes |
| --- | --- | --- |
| Authorization denied (transport gate or facade scope check) | `PermissionDenied` or `Unauthenticated` | The detail is safe to surface; never names a secret. |
| Versioning operation when versioning is not registered (`InvalidOperationException`) | `FailedPrecondition` | The host has not registered `AddLatticeSchemaVersioning(...)`; non-versioning schema operations may still be available. |
| Invalid argument (`ArgumentException`) | `InvalidArgument` | |
| Request cancelled | `Cancelled` | |
| Any other fault | `Internal` | The detail is deliberately opaque; the real exception is logged server-side, not returned. |

The `FailedPrecondition` shape is the one an operator most often needs to act on for versioning: the endpoint is reachable, but the silo intentionally did not register the optional versioning add-on. The detail should be clear enough for an operator UI to explain which registration is missing.

## Wire compatibility

Wire messages live under `Model/*` and use Orleans aliases prefixed `oisg.`. Shared facade and abstractions records use aliases prefixed `ois.`. Contract evolution is additive-only: new fields use new `[Id(n)]` values, and aliases or field numbers are never renumbered. That lets a newer response decode under an older client while preserving the stable wire names.

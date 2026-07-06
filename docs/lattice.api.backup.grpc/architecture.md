# Orleans.Lattice.Api.Backup.Grpc architecture

This page describes the code-first gRPC binding and its two-layer, fail-closed authorization model. The gRPC service, method definitions, marshallers, interceptor, and the default header credential bridge / options auth-scheme source are internal and are described here by behaviour; the public client, options, and seams are named.

## Code-first binding

The binding defines its RPCs in C# rather than a `.proto`. A method-definition singleton builds one gRPC `Method` per operation from the Orleans serializers resolved out of DI, under the service name `orleans.lattice.api.backup`. The server-side service and the public `LatticeBackupApiGrpcClient` share those definitions, so the wire contract is identical on both ends by construction and there is no generated stub to keep in sync.

Every message is a `[GenerateSerializer]` record marshalled with the Orleans binary serializer. That is why the client's `Create` factory takes an `IServiceProvider` with `AddSerializer()` registered: the per-message marshallers are built from those serializers, so a client and server that share the Orleans serialization configuration cannot disagree on the wire format.

The operations map to two gRPC shapes: unary for capture, list, describe, delete, restore, revert, and auth-scheme discovery; server-streaming for `StreamBackups` (whole-catalog drain) and `ExportArtifact` (chunk-wise artifact export). The two streaming RPCs are what let a large catalog or artifact move with bounded memory end to end - the facade streams, the service forwards each item as it arrives, and the client re-exposes it as an `IAsyncEnumerable<T>`.

## Two-layer authorization

Every call passes through two independent, fail-closed gates.

### 1. Transport meta-authorizer

An authorization interceptor runs first, before the facade is touched. It decodes the inbound call into a `LatticeBackupApiAuthorizationContext` - the `LatticeBackupApiOperation`, an optional `TargetId` (the backup id, or the target / scope tree id for a capture or restore not yet keyed by a backup id), and the underlying `ServerCallContext` for header and peer inspection - and asks the registered `ILatticeBackupApiAuthorizer` whether the call may run at all. It defaults to `DenyAllBackupApiAuthorizer`, so every call is rejected with `PermissionDenied` until the host registers a permissive authorizer (or the opt-in `AllowAllBackupApiAuthorizer`) or sets `RequireAuthorization` to `false`. The interceptor is registered globally but scopes its enforcement to the backup control-API service by service-name prefix, so other gRPC services on the same host are unaffected.

An operation the interceptor does not recognise is presented to the authorizer as `Unknown` rather than being waved through, so a deny-by-default policy refuses a future or unmapped RPC instead of having it masquerade as a benign catalog read.

### 2. Facade scope authorization

Once past the transport gate, the service stamps the caller identity onto the ambient Lattice credential context - via the `ILatticeBackupApiCredentialBridge`, whose built-in default reads the configured bearer-style header - and invokes the control facade. The facade then authorizes the operation's scope against the backup access gate, exactly as an in-process facade caller would. An anonymous caller (no resolvable credential) is denied here even when the transport gate allowed the call.

The two gates are complementary, not redundant: the transport gate is a coarse edge control keyed by headers, operation, and target, while the facade gate is the engine's own fine-grained, per-scope, fail-closed authorization. A deployment can run a permissive transport gate behind a trusted boundary and still get full per-scope enforcement from the facade, or tighten both.

## Credential bridging

The credential bridge is the identity seam. The default implementation reads a single configurable header (`CredentialHeaderName`, default `authorization`), strips a case-insensitive scheme prefix (`CredentialScheme`, default `Bearer`), and lifts the remaining token onto the ambient `LatticeCredential` for the registered credential authenticator to resolve into a subject. A host with a bespoke identity source - a client TLS certificate, a signed edge header, a pre-resolved principal - registers its own bridge before the binding's registration runs, and the built-in default steps aside. Returning `null` leaves the caller anonymous; when auth-backed backup control is active an anonymous caller is denied, so a missing or malformed credential header can never drive a destructive operation.

When the authorization add-on is not registered, no header is read and the backup control API behaves exactly as it does without a credential layer - the zero-cost path the engine already provides.

## Auth-scheme discovery

`GetAuthScheme` is deliberately unauthenticated so a client can discover how to sign in before it holds a credential. The response is built by the `ILatticeBackupApiAuthSchemeSource`; the default implementation projects the host-configured `AdvertisedAuthSchemes` (empty by default, so a client falls back to manual or Basic selection). Because the advertisement is served without a credential, an implementation must return only public configuration - scheme ids and public parameters like an OIDC authority or client id - and never a secret or user-specific data.

## Denial mapping

A denial from either gate reaches the client as a `PermissionDenied` `RpcException`, so a caller handles authorization failure uniformly regardless of which layer refused the call.

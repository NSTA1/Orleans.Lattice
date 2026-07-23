# Orleans.Lattice.Api.Replication.Grpc architecture

This binding adapts the transport-agnostic [`ILatticeReplicationControl`](../lattice.api.replication/README.md) facade onto gRPC without a hand-written `.proto`. It adds transport and authentication concerns and nothing else: the control semantics stay in the facade and the engine.

## Code-first binding

Each facade operation is one gRPC `Method<TRequest, TResponse>` built from C# definitions. The request and response messages are `[GenerateSerializer]` records marshalled with the Orleans binary serializer, so the same serializer closure that runs in the cluster marshals the wire messages. There is no generated stub and no schema drift between a `.proto` and the records: the records are the contract. The public `LatticeReplicationApiGrpcClient` wraps a caller-supplied `CallInvoker`; the service that dispatches to the facade is internal.

## Two-layer, fail-closed authorization

Two independent gates both default to deny.

1. **Transport meta-authorizer.** The interceptor consults `ILatticeReplicationApiAuthorizer` for every guarded RPC before the service runs. With `RequireAuthorization` on and no authorizer registered, `DenyAllReplicationApiAuthorizer` rejects every call with `PermissionDenied`. The interceptor maps the inbound method to a `LatticeReplicationApiOperation`; an unrecognized method maps to `Unknown`, which the default-deny posture never grants, so a new or malformed method can never fall through to an allow.
2. **Facade access gate.** The identity bridge resolves the caller's credential from the configured header and stamps the ambient Lattice credential; the facade then re-authorizes that resolved caller against its own fail-closed access gate for the `LatticeOperation.Replication` capability on the target tree. This is the same gate an in-process caller passes through, so the wire path is no weaker than the local one.

Neither gate replaces the other: the meta-authorizer decides whether a call may run at all; the access gate decides whether the resolved subject may act on the specific tree.

## Discoverable sign-in

`GetAuthScheme` is the only unauthenticated RPC. It returns the host-configured `AdvertisedAuthSchemes` so a client can discover how to authenticate before it holds a credential. Each advertised descriptor carries only public configuration; a secret is never advertised.

## Status mapping

The service maps facade outcomes to stable gRPC status codes so a client sees a predictable contract:

| Facade outcome | Status |
|---|---|
| `LatticeAuthorizationDeniedException` (interceptor or gate) | `PermissionDenied` |
| `LatticeReplicationPreconditionFailedException` | `FailedPrecondition` |
| `LatticeReplicationModeChangeRejectedException` | `FailedPrecondition` |
| `ArgumentException` (null / empty tree id, unrecognized mode) | `InvalidArgument` |
| `OperationCanceledException` | `Cancelled` |
| Any other exception | `Internal` (logged server-side; the client sees a non-leaking message) |

An already-thrown `RpcException` is rethrown unchanged so an inner status is preserved.

## See also

- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/architecture.md) - the facade's authorization and engine-delegation model.
- [runtime replication configuration](../lattice.replication/runtime-config.md) - the engine-side config tree and fail-closed resolution.
- [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/architecture.md) - the sibling gRPC binding this one mirrors.

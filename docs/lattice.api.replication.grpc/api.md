# Orleans.Lattice.Api.Replication.Grpc API reference

The package exposes a public typed client, two registration entry points, a public authorization seam, and a public options type. The service, marshallers, method definitions, and interceptor are internal.

## Registration

| Member | Signature | Purpose |
|---|---|---|
| `AddLatticeReplicationApiGrpc` | `IServiceCollection AddLatticeReplicationApiGrpc(this IServiceCollection services, Action<LatticeReplicationApiGrpcOptions>? configure = null)` | Registers the server-side binding and its authorization interceptor. |
| `MapLatticeReplicationApiGrpc` | `IEndpointRouteBuilder MapLatticeReplicationApiGrpc(this IEndpointRouteBuilder endpoints)` | Maps the gRPC service onto the ASP.NET Core endpoint routing. |

## Client

`LatticeReplicationApiGrpcClient` is the public typed client.

| Member | Signature |
|---|---|
| Create | `static LatticeReplicationApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` |
| Enable | `Task<ReplicationEnableResult> EnableReplicationAsync(string treeId, LatticeMergeMode mode, string? bootstrapSourceClusterId = null, CancellationToken cancellationToken = default)` |
| Disable | `Task<ReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)` |
| Get config | `Task<ReplicationConfigReport> GetReplicationConfigAsync(CancellationToken cancellationToken = default)` |
| Get auth scheme | `Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(AuthSchemeAdvertisementRequest request, CancellationToken cancellationToken = default)` |

The result and report types (`ReplicationEnableResult`, `ReplicationDisableResult`, `ReplicationConfigReport`, `ReplicationTreeConfigEntry`) are the shared facade model records documented in the [facade API reference](../lattice.api.replication/api.md#model-types).

## Authorization seam

| Member | Kind | Purpose |
|---|---|---|
| `ILatticeReplicationApiAuthorizer` | interface | The transport meta-authorizer the interceptor consults for every guarded RPC. A host implements it to decide whether a call may run at all. |
| `DenyAllReplicationApiAuthorizer` | class | The default-deny authorizer used when a host registers no authorizer and leaves `RequireAuthorization` on. Rejects every guarded RPC. |
| `LatticeReplicationApiOperation` | enum | The operation an inbound RPC maps to (`EnableReplication`, `DisableReplication`, `Unknown`). An unrecognized method maps to `Unknown`, which the default-deny posture never grants. |

## Options

`LatticeReplicationApiGrpcOptions` - see [Configuration](configuration.md).

## Status mapping

| Failure | gRPC status |
|---|---|
| Caller not authorized (interceptor or facade gate) | `PermissionDenied` |
| In-place mode change on an enabled tree; unmet enable precondition | `FailedPrecondition` |
| Malformed request (for example null or empty tree id, unrecognized mode) | `InvalidArgument` |
| Request cancelled | `Cancelled` |
| Any other fault | `Internal` (with a non-leaking message) |

## See also

- [Architecture](architecture.md) - the two-layer authorization model, the identity bridge, and the code-first binding.
- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/api.md) - the facade contract and model records.

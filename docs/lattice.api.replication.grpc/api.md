# Orleans.Lattice.Api.Replication.Grpc API reference

The package exposes public typed clients for the control and peer-status services, their registration entry points, public authorization, credential-bridge, and auth-scheme seams, the public wire message records, the binding's serialization-alias constants, and a public options type. The services, marshallers, method definitions, and interceptor are internal.

## Registration

| Member | Signature | Purpose |
|---|---|---|
| `AddLatticeReplicationApiGrpc` | `IServiceCollection AddLatticeReplicationApiGrpc(this IServiceCollection services, Action<LatticeReplicationApiGrpcOptions>? configure = null)` | Registers the server-side binding: the method definitions, the service, the default-deny authorizer, the header credential bridge, the options-backed auth-scheme source, and the authorization interceptor. |
| `MapLatticeReplicationApiGrpc` | `IEndpointRouteBuilder MapLatticeReplicationApiGrpc(this IEndpointRouteBuilder endpoints)` | Maps the gRPC service onto the ASP.NET Core endpoint routing. |
| `AddLatticeReplicationStatusApiGrpc` | `IServiceCollection AddLatticeReplicationStatusApiGrpc(this IServiceCollection services)` | Registers the read-only peer-status service on top of the control binding, reusing its default-deny authorizer, credential bridge, options, and authorization interceptor. Must be called after `AddLatticeReplicationApiGrpc`; calling it first throws `InvalidOperationException`. The host must also expose `ILatticeReplicationStatus` (via `AddLatticeReplicationStatusApi`) in the same service provider. Idempotent. |
| `MapLatticeReplicationStatusApiGrpc` | `IEndpointRouteBuilder MapLatticeReplicationStatusApiGrpc(this IEndpointRouteBuilder endpoints)` | Maps the peer-status service (the `GetPeerStatus` RPC). Requires `AddLatticeReplicationStatusApiGrpc` first. |

Each is an extension method on the public static `LatticeReplicationApiGrpcServiceCollectionExtensions` class. Call `AddLatticeReplicationApiGrpc` once: its other registrations are `TryAdd`-guarded, but each call appends the authorization interceptor to the gRPC pipeline again, so after two calls every guarded call is authorized twice.

## Client

`LatticeReplicationApiGrpcClient` is the public typed client.

| Member | Signature |
|---|---|
| Create | `static LatticeReplicationApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` |
| Enable | `Task<ReplicationEnableResult> EnableReplicationAsync(string treeId, LatticeMergeMode mode, string? bootstrapSourceClusterId = null, CancellationToken cancellationToken = default)` |
| Disable | `Task<ReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)` |
| Get config | `Task<ReplicationConfigReport> GetReplicationConfigAsync(CancellationToken cancellationToken = default)` |
| Get auth scheme | `Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(AuthSchemeAdvertisementRequest request, CancellationToken cancellationToken = default)` |
| Decommission peer | `Task<ReplicationDecommissionPeerResult> DecommissionPeerAsync(string peerClusterId, CancellationToken cancellationToken = default)` |

The result and report types (`ReplicationEnableResult`, `ReplicationDisableResult`, `ReplicationConfigReport`, `ReplicationTreeConfigEntry`) are the shared facade model records documented in the [facade API reference](../lattice.api.replication/api.md#model-types).

`LatticeReplicationStatusGrpcClient` is the public typed client for the peer-status service. It implements `ILatticeReplicationStatus`, so a caller can swap an in-process facade for it with no adapter.

| Member | Signature |
|---|---|
| Create | `static LatticeReplicationStatusGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` |
| Get peer status | `Task<ReplicationPeerStatusPage> GetPeerStatusAsync(ReplicationPeerStatusQuery query, CancellationToken cancellationToken = default)` |

The query, page, and entry records are the same facade model records, documented in the [facade API reference](../lattice.api.replication/api.md#model-types).

## Authorization seam

| Member | Kind | Purpose |
|---|---|---|
| `ILatticeReplicationApiAuthorizer` | interface | The transport meta-authorizer the interceptor consults for every guarded RPC. A host implements it to decide whether a call may run at all. |
| `DenyAllReplicationApiAuthorizer` | class | The default-deny authorizer used when a host registers no authorizer and leaves `RequireAuthorization` on. Rejects every guarded RPC. |
| `AllowAllReplicationApiAuthorizer` | class | Opt-in authorizer that permits every guarded RPC. Register it explicitly only when an outer trust boundary already guards the endpoint. |
| `LatticeReplicationApiOperation` | enum | The operation an inbound RPC maps to (`EnableReplication`, `DisableReplication`, `GetReplicationConfig`, `Unknown`, `GetPeerStatus`, `DecommissionPeer`). An unrecognized method maps to `Unknown`, which the default-deny posture never grants. |
| `LatticeReplicationApiAuthorizationContext` | readonly struct | What the authorizer receives: `Call` (the `ServerCallContext`), `Operation`, and `TargetId` - the tree id exactly as the enable / disable request or the peer-status query's tree filter supplied it, or the peer cluster id for a `DecommissionPeer` call, before the facade's tenant-scoped resolution; `null` for the config read, a peer-status read with no tree filter, and an `Unknown` operation. The exempt `GetAuthScheme` RPC never reaches the authorizer. |
| `ILatticeReplicationApiCredentialBridge` | interface | Resolves the caller credential from an inbound `ServerCallContext` into the ambient `LatticeCredential` the facade access gate authorizes. Runs after the transport authorizer; returning `null` leaves the caller anonymous, which auth-backed replication control denies. The default reads `CredentialHeaderName` / `CredentialScheme`. |
| `ILatticeReplicationApiAuthSchemeSource` | interface | Supplies the advertisement the unauthenticated `GetAuthScheme` RPC returns; it must carry only public configuration. |
| `GrpcReplicationTypeAliases` | static class | The binding's stable serialization aliases for its wire messages (prefix `oirg.`). |

## Options

`LatticeReplicationApiGrpcOptions` - see [Configuration](configuration.md).

## Wire messages

The request and response records the RPCs carry are public, `[GenerateSerializer]` / `[Immutable]` Orleans-serializable records aliased by `GrpcReplicationTypeAliases`. Fields are additive-only: a new `[Id(n)]` never renumbers an existing one. Each row lists its members in `[Id(n)]` order, starting at `0`.

| Record | RPC | Members |
|---|---|---|
| `ReplicationEnableRequestMessage` | `EnableReplication` request | `TreeId` (required), `Mode`, `BootstrapSourceClusterId` (optional; an empty value is treated as none) |
| `ReplicationEnableResponse` | `EnableReplication` response | `TreeId` (required), `Mode`, `AlreadyEnabled`, `BootstrapRequested` |
| `ReplicationDisableRequestMessage` | `DisableReplication` request | `TreeId` (required) |
| `ReplicationDisableResponse` | `DisableReplication` response | `TreeId` (required), `AlreadyDisabled` |
| `ReplicationGetConfigRequest` | `GetReplicationConfig` request | none |
| `ReplicationConfigResponse` | `GetReplicationConfig` response | `Trees` (`IReadOnlyList<ReplicationTreeConfigMessage>`) |
| `ReplicationTreeConfigMessage` | one entry of `ReplicationConfigResponse.Trees` | `TreeId` (required), `Enabled`, `HasMode`, `Mode` (meaningful only when `HasMode` is `true`), `Ambiguous`, `Source` (`ReplicationEnrollmentSource`) |
| `AuthSchemeAdvertisementRequest` | `GetAuthScheme` request | none |
| `AuthSchemeAdvertisement` | `GetAuthScheme` response | `Schemes` (`IReadOnlyList<AuthSchemeDescriptor>`) |
| `AuthSchemeDescriptor` | one advertised scheme | `SchemeId` (required), `DisplayName`, `Parameters` (`IReadOnlyDictionary<string, string>` of public configuration only) |
| `ReplicationDecommissionPeerRequestMessage` | `DecommissionPeer` request | `PeerClusterId` (required) |
| `ReplicationDecommissionPeerResponse` | `DecommissionPeer` response | `PeerClusterId` (required), `TreeCount`, `AlreadyDecommissioned` |

The typed client maps these onto the facade model records, so a caller of `LatticeReplicationApiGrpcClient` sees `ReplicationEnableResult`, `ReplicationDisableResult`, and `ReplicationConfigReport` rather than the wire records.

The peer-status service has no wire records of its own: its `GetPeerStatus` RPC carries the facade's `ReplicationPeerStatusQuery` as the request and `ReplicationPeerStatusPage` as the response, Orleans-serialized under the `oir.` aliases of `ApiReplicationTypeAliases` in `Orleans.Lattice.Api.Abstractions`.

## Status mapping

| Failure | gRPC status |
|---|---|
| Caller not authorized (interceptor or facade gate), or a fail-closed tenant resolution | `PermissionDenied` |
| In-place mode change on an enabled tree; unmet enable or disable precondition; `DecommissionPeer` called while the peer is still present in `ReplicationPeers` | `FailedPrecondition` |
| Malformed request (for example a null or empty tree id) | `InvalidArgument` |
| Request cancelled | `Cancelled` |
| Any other fault | `Internal` (with a non-leaking message) |

`GetPeerStatus` follows the same mapping, except that it has no `FailedPrecondition` outcome: a negative page size or a malformed continuation token is `InvalidArgument`.

## See also

- [Architecture](architecture.md) - the two-layer authorization model, the identity bridge, and the code-first binding.
- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/api.md) - the facade contract and model records.

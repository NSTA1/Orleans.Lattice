# Orleans.Lattice.Api.Replication.Grpc API reference

The package exposes a public typed client, two registration entry points, public authorization, credential-bridge, and auth-scheme seams, the public wire message records, the binding's serialization-alias constants, and a public options type. The service, marshallers, method definitions, and interceptor are internal.

## Registration

| Member | Signature | Purpose |
|---|---|---|
| `AddLatticeReplicationApiGrpc` | `IServiceCollection AddLatticeReplicationApiGrpc(this IServiceCollection services, Action<LatticeReplicationApiGrpcOptions>? configure = null)` | Registers the server-side binding: the method definitions, the service, the default-deny authorizer, the header credential bridge, the options-backed auth-scheme source, and the authorization interceptor. |
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
| `AllowAllReplicationApiAuthorizer` | class | Opt-in authorizer that permits every guarded RPC. Register it explicitly only when an outer trust boundary already guards the endpoint. |
| `LatticeReplicationApiOperation` | enum | The operation an inbound RPC maps to (`EnableReplication`, `DisableReplication`, `GetReplicationConfig`, `Unknown`). An unrecognized method maps to `Unknown`, which the default-deny posture never grants. |
| `LatticeReplicationApiAuthorizationContext` | readonly struct | What the authorizer receives: `Call` (the `ServerCallContext`), `Operation`, and `TargetId` - the tree id exactly as the enable / disable request supplied it, before the facade's tenant-scoped resolution, or `null` for the config read and an `Unknown` operation. The exempt `GetAuthScheme` RPC never reaches the authorizer. |
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

The typed client maps these onto the facade model records, so a caller of `LatticeReplicationApiGrpcClient` sees `ReplicationEnableResult`, `ReplicationDisableResult`, and `ReplicationConfigReport` rather than the wire records.

## Status mapping

| Failure | gRPC status |
|---|---|
| Caller not authorized (interceptor or facade gate), or a fail-closed tenant resolution | `PermissionDenied` |
| In-place mode change on an enabled tree; unmet enable precondition | `FailedPrecondition` |
| Malformed request (for example a null or empty tree id) | `InvalidArgument` |
| Request cancelled | `Cancelled` |
| Any other fault | `Internal` (with a non-leaking message) |

## See also

- [Architecture](architecture.md) - the two-layer authorization model, the identity bridge, and the code-first binding.
- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/api.md) - the facade contract and model records.

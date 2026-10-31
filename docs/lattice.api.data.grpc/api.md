---
agent_spec: "docs/agents/api/data.json"
---

# Public API

`Orleans.Lattice.Api.Data.Grpc` publishes the remote half of the data API: the `LatticeDataApiGrpcClient`, the Orleans-serialized wire records the RPCs carry, the transport authorization seam, the identity bridges, the server options, and the registration extensions. The gRPC service, its method definitions, marshallers and authorization interceptor are internal; they are described by behaviour in [Architecture](architecture.md). The facade contract the service projects is listed in the [data facade's public API](../lattice.api.data/api.md).

## Registration

| Method | Host | What it registers |
|---|---|---|
| `AddLatticeDataApiGrpc(this IServiceCollection services, Action<LatticeDataApiGrpcOptions>? configure = null)` | the web host's service collection | The options, the default-deny `ILatticeDataApiAuthorizer`, the header-based `ILatticeDataApiCredentialBridge` and `ILatticeDataApiActiveTenantBridge`, the authorization interceptor, and the service. Every seam is registered with `TryAdd`, so a host registration made first wins. |
| `MapLatticeDataApiGrpc(this IEndpointRouteBuilder endpoints)` | the endpoint route builder | Maps the `orleans.lattice.api.data` service routes. |

The silo must also register the facade itself with `AddLatticeDataApi()`; see the [data facade guide](../lattice.api.data/README.md). Option values are documented in [Configuration](configuration.md).

## Client

`LatticeDataApiGrpcClient.Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` builds a client over a caller-supplied call invoker (typically `channel.CreateCallInvoker()`), resolving the per-message Orleans serializers from `serializerProvider`, which must have Orleans serialization registered. The client has no public constructor and carries no transport policy: address, TLS, deadlines, retries and call credentials belong to the channel or invoker.

| Client method | RPC | Request | Response |
|---|---|---|---|
| `SetAsync` | `Set` | `DataSetRequest` | `DataSetResponse` |
| `DeleteAsync` | `Delete` | `DataDeleteRequest` | `DataDeleteResponse` |
| `SetManyAtomicAsync` | `SetManyAtomic` | `DataAtomicRequest` | `DataAtomicResponse` |
| `SetManyAtomicCrossTreeAsync` | `SetManyAtomicCrossTree` | `DataCrossTreeRequest` | `DataCrossTreeResponse` |
| `GetAsync` | `Get` | `DataGetRequest` | `DataReadResult` (facade DTO) |
| `ReadRangeAsync` | `ReadRange` | `DataRangeRequest` (facade DTO) | `DataRangePage` (facade DTO) |
| `DeleteRangeAsync` | `DeleteRange` | `DataRangeDeleteRequest` (facade DTO) | `DataRangeDeleteResult` (facade DTO) |
| `SetManyAsync` | `SetMany` | `DataSetManyRequest` | `DataSetManyResponse` |
| `CrdtWriteAsync` | `CrdtWrite` | `CrdtWriteRequest` | `CrdtWriteResponse` |
| `CrdtReadAsync` | `CrdtRead` | `CrdtReadRequest` | `CrdtReadResponse` |
| `ProbeCapabilitiesAsync` | `ProbeCapabilities` | `DataProbeRequest` | `LatticeDataCapabilities` (facade DTO) |

Every RPC is unary. A failed call surfaces as an `RpcException` whose status follows the binding's [status mapping](README.md#status-mapping).

## Seams

| Seam | Default | Purpose |
|---|---|---|
| `ILatticeDataApiAuthorizer` | `DenyAllDataApiAuthorizer` | Coarse transport admission for every data-API RPC, given a `LatticeDataApiAuthorizationContext`. `AllowAllDataApiAuthorizer` is the opt-in permissive implementation. |
| `ILatticeDataApiCredentialBridge` | header-based (internal) | Lifts the caller credential from request metadata onto the ambient Lattice credential. |
| `ILatticeDataApiActiveTenantBridge` | header-based (internal) | Lifts the caller's asserted active tenant from request metadata onto the ambient active-tenant scope. |

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Data facade public API](../lattice.api.data/api.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Data.Grpc.ILatticeDataApiActiveTenantBridge`

[Source](../../src/lattice.api.data.grpc/ILatticeDataApiActiveTenantBridge.cs) (line 41).

`public interface ILatticeDataApiActiveTenantBridge`

- `TenantId? Resolve(ServerCallContext context)`

### `Orleans.Lattice.Api.Data.Grpc.ILatticeDataApiCredentialBridge`

[Source](../../src/lattice.api.data.grpc/ILatticeDataApiCredentialBridge.cs) (line 37).

`public interface ILatticeDataApiCredentialBridge`

- `LatticeCredential? Resolve(ServerCallContext context)`

### `Orleans.Lattice.Api.Data.Grpc.LatticeDataApiGrpcClient`

[Source](../../src/lattice.api.data.grpc/LatticeDataApiGrpcClient.cs) (line 21).

`public sealed class LatticeDataApiGrpcClient`

- `public static LatticeDataApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public Task<DataSetResponse> SetAsync(DataSetRequest request, CancellationToken cancellationToken = default)`
- `public Task<DataDeleteResponse> DeleteAsync(DataDeleteRequest request, CancellationToken cancellationToken = default)`
- `public Task<DataAtomicResponse> SetManyAtomicAsync(DataAtomicRequest request, CancellationToken cancellationToken = default)`
- `public Task<DataCrossTreeResponse> SetManyAtomicCrossTreeAsync(DataCrossTreeRequest request, CancellationToken cancellationToken = default)`
- `public Task<DataReadResult> GetAsync(DataGetRequest request, CancellationToken cancellationToken = default)`
- `public Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)`
- `public Task<DataRangeDeleteResult> DeleteRangeAsync(DataRangeDeleteRequest request, CancellationToken cancellationToken = default)`
- `public Task<DataSetManyResponse> SetManyAsync(DataSetManyRequest request, CancellationToken cancellationToken = default)`
- `public Task<CrdtWriteResponse> CrdtWriteAsync(CrdtWriteRequest request, CancellationToken cancellationToken = default)`
- `public Task<CrdtReadResponse> CrdtReadAsync(CrdtReadRequest request, CancellationToken cancellationToken = default)`
- `public Task<LatticeDataCapabilities> ProbeCapabilitiesAsync(DataProbeRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Data.Grpc.LatticeDataApiGrpcOptions`

[Source](../../src/lattice.api.data.grpc/LatticeDataApiGrpcOptions.cs) (line 7).

`public sealed class LatticeDataApiGrpcOptions`

- `public bool RequireAuthorization`
- `public string CredentialHeaderName`
- `public string CredentialScheme`
- `public string ActiveTenantHeaderName`

### `Orleans.Lattice.Api.Data.Grpc.LatticeDataApiGrpcServiceCollectionExtensions`

[Source](../../src/lattice.api.data.grpc/LatticeDataApiGrpcServiceCollectionExtensions.cs) (line 32).

`public static class LatticeDataApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeDataApiGrpc(this IServiceCollection services, Action<LatticeDataApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeDataApiGrpc(this IEndpointRouteBuilder endpoints)`

### `Orleans.Lattice.Api.Data.Grpc.CrdtKind`

[Source](../../src/lattice.api.data.grpc/Model/CrdtKind.cs) (line 11).

`public enum CrdtKind`

- `PnCounter = 0`
- `OrSet = 1`
- `OrFlag = 2`
- `RwFlag = 3`
- `VersionVector = 4`
- `MvRegister = 5`
- `Sequence = 6`
- `OrMap = 7`
- `GCounter = 8`
- `GSet = 9`
- `RwSet = 10`
- `MaxRegister = 11`
- `MinRegister = 12`

### `Orleans.Lattice.Api.Data.Grpc.CrdtMapField`

[Source](../../src/lattice.api.data.grpc/Model/CrdtMapField.cs) (line 14).

`public sealed record CrdtMapField`

- `public required string Field`
- `public List<byte[]> Values`
- `public bool Equals(CrdtMapField? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Data.Grpc.CrdtReadRequest`

[Source](../../src/lattice.api.data.grpc/Model/CrdtReadRequest.cs) (line 12).

`public sealed record CrdtReadRequest`

- `public required string TreeId`
- `public required string Key`
- `public required CrdtKind Kind`

### `Orleans.Lattice.Api.Data.Grpc.CrdtReadResponse`

[Source](../../src/lattice.api.data.grpc/Model/CrdtReadResponse.cs) (line 16).

`public sealed record CrdtReadResponse`

- `public long CounterValue`
- `public bool FlagValue`
- `public List<byte[]> Elements`
- `public List<CrdtVectorEntry> Vector`
- `public List<CrdtMapField> Map`
- `public bool Equals(CrdtReadResponse? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Data.Grpc.CrdtVectorEntry`

[Source](../../src/lattice.api.data.grpc/Model/CrdtVectorEntry.cs) (line 10).

`public sealed record CrdtVectorEntry`

- `public required string ReplicaId`
- `public required string Clock`

### `Orleans.Lattice.Api.Data.Grpc.CrdtWriteOp`

[Source](../../src/lattice.api.data.grpc/Model/CrdtWriteOp.cs) (line 10).

`public enum CrdtWriteOp`

- `CounterIncrement = 0`
- `CounterDecrement = 1`
- `SetAdd = 2`
- `SetRemove = 3`
- `OrFlagEnable = 4`
- `OrFlagDisable = 5`
- `RwFlagEnable = 6`
- `RwFlagDisable = 7`
- `VersionVectorTick = 8`
- `RegisterSet = 9`
- `SequenceInsertAt = 10`
- `SequenceRemoveAt = 11`
- `MapSet = 12`
- `MapRemove = 13`
- `GCounterIncrement = 14`
- `GSetAdd = 15`
- `RwSetAdd = 16`
- `RwSetRemove = 17`
- `MaxRegisterSet = 18`
- `MinRegisterSet = 19`

### `Orleans.Lattice.Api.Data.Grpc.CrdtWriteRequest`

[Source](../../src/lattice.api.data.grpc/Model/CrdtWriteRequest.cs) (line 17).

`public sealed record CrdtWriteRequest`

- `public required string TreeId`
- `public required string Key`
- `public required CrdtWriteOp Op`
- `public string ReplicaId`
- `public long Amount`
- `public byte[] Element`
- `public string Field`
- `public int Index`
- `public bool Equals(CrdtWriteRequest? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Data.Grpc.CrdtWriteResponse`

[Source](../../src/lattice.api.data.grpc/Model/CrdtWriteResponse.cs) (line 12).

`public sealed record CrdtWriteResponse`

### `Orleans.Lattice.Api.Data.Grpc.DataAtomicRequest`

[Source](../../src/lattice.api.data.grpc/Model/DataAtomicRequest.cs) (line 17).

`public sealed record DataAtomicRequest`

- `public required string TreeId`
- `public DataAtomicBatch Batch`
- `public required string OperationId`

### `Orleans.Lattice.Api.Data.Grpc.DataAtomicResponse`

[Source](../../src/lattice.api.data.grpc/Model/DataAtomicResponse.cs) (line 12).

`public sealed record DataAtomicResponse`

### `Orleans.Lattice.Api.Data.Grpc.DataCrossTreeRequest`

[Source](../../src/lattice.api.data.grpc/Model/DataCrossTreeRequest.cs) (line 17).

`public sealed record DataCrossTreeRequest`

- `public List<DataTreeBatch> Batches`
- `public required string OperationId`

### `Orleans.Lattice.Api.Data.Grpc.DataCrossTreeResponse`

[Source](../../src/lattice.api.data.grpc/Model/DataCrossTreeResponse.cs) (line 14).

`public sealed record DataCrossTreeResponse`

- `public CrossTreeAtomicWriteOutcome Outcome`

### `Orleans.Lattice.Api.Data.Grpc.DataDeleteRequest`

[Source](../../src/lattice.api.data.grpc/Model/DataDeleteRequest.cs) (line 11).

`public sealed record DataDeleteRequest`

- `public required string TreeId`
- `public required string Key`

### `Orleans.Lattice.Api.Data.Grpc.DataDeleteResponse`

[Source](../../src/lattice.api.data.grpc/Model/DataDeleteResponse.cs) (line 12).

`public sealed record DataDeleteResponse`

- `public bool Removed`

### `Orleans.Lattice.Api.Data.Grpc.DataGetRequest`

[Source](../../src/lattice.api.data.grpc/Model/DataGetRequest.cs) (line 12).

`public sealed record DataGetRequest`

- `public required string TreeId`
- `public required string Key`

### `Orleans.Lattice.Api.Data.Grpc.DataProbeRequest`

[Source](../../src/lattice.api.data.grpc/Model/DataProbeRequest.cs) (line 12).

`public sealed record DataProbeRequest`

- `public required string TreeId`
- `public string? Key`

### `Orleans.Lattice.Api.Data.Grpc.DataSetManyRequest`

[Source](../../src/lattice.api.data.grpc/Model/DataSetManyRequest.cs) (line 16).

`public sealed record DataSetManyRequest`

- `public required string TreeId`
- `public List<DataEntry> Upserts`

### `Orleans.Lattice.Api.Data.Grpc.DataSetManyResponse`

[Source](../../src/lattice.api.data.grpc/Model/DataSetManyResponse.cs) (line 12).

`public sealed record DataSetManyResponse`

### `Orleans.Lattice.Api.Data.Grpc.DataSetRequest`

[Source](../../src/lattice.api.data.grpc/Model/DataSetRequest.cs) (line 10).

`public sealed record DataSetRequest`

- `public required string TreeId`
- `public required string Key`
- `public byte[] Value`
- `public bool Equals(DataSetRequest? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Data.Grpc.DataSetResponse`

[Source](../../src/lattice.api.data.grpc/Model/DataSetResponse.cs) (line 11).

`public sealed record DataSetResponse`

### `Orleans.Lattice.Api.Data.Grpc.GrpcDataTypeAliases`

[Source](../../src/lattice.api.data.grpc/Model/GrpcDataTypeAliases.cs) (line 15).

`public static class GrpcDataTypeAliases`

- `public const string DataSetRequest`
- `public const string DataSetResponse`
- `public const string DataDeleteRequest`
- `public const string DataDeleteResponse`
- `public const string DataAtomicRequest`
- `public const string DataAtomicResponse`
- `public const string DataCrossTreeRequest`
- `public const string DataCrossTreeResponse`
- `public const string DataGetRequest`
- `public const string DataSetManyRequest`
- `public const string DataSetManyResponse`
- `public const string CrdtKind`
- `public const string CrdtWriteOp`
- `public const string CrdtWriteRequest`
- `public const string CrdtWriteResponse`
- `public const string CrdtReadRequest`
- `public const string CrdtReadResponse`
- `public const string CrdtVectorEntry`
- `public const string CrdtMapField`
- `public const string DataProbeRequest`

### `Orleans.Lattice.Api.Data.Grpc.ILatticeDataApiAuthorizer`

[Source](../../src/lattice.api.data.grpc/Security/ILatticeDataApiAuthorizer.cs) (line 17).

`public interface ILatticeDataApiAuthorizer`

- `Task<bool> IsAuthorizedAsync(LatticeDataApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Data.Grpc.DenyAllDataApiAuthorizer`

[Source](../../src/lattice.api.data.grpc/Security/ILatticeDataApiAuthorizer.cs) (line 43).

`public sealed class DenyAllDataApiAuthorizer : ILatticeDataApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeDataApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Data.Grpc.AllowAllDataApiAuthorizer`

[Source](../../src/lattice.api.data.grpc/Security/ILatticeDataApiAuthorizer.cs) (line 59).

`public sealed class AllowAllDataApiAuthorizer : ILatticeDataApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeDataApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Data.Grpc.LatticeDataApiOperation`

[Source](../../src/lattice.api.data.grpc/Security/LatticeDataApiAuthorizationContext.cs) (line 10).

`public enum LatticeDataApiOperation`

- `SetPoint`
- `DeletePoint`
- `SetManyAtomic`
- `SetManyAtomicCrossTree`
- `GetPoint`
- `ReadRange`
- `DeleteRange`
- `Unknown`
- `SetMany`
- `CrdtWrite`
- `CrdtRead`
- `ProbeCapabilities`

### `Orleans.Lattice.Api.Data.Grpc.LatticeDataApiAuthorizationContext`

[Source](../../src/lattice.api.data.grpc/Security/LatticeDataApiAuthorizationContext.cs) (line 64).

`public readonly struct LatticeDataApiAuthorizationContext`

- `public LatticeDataApiAuthorizationContext(ServerCallContext call, LatticeDataApiOperation operation, string? targetTreeId)`
- `public ServerCallContext Call`
- `public LatticeDataApiOperation Operation`
- `public string? TargetTreeId`

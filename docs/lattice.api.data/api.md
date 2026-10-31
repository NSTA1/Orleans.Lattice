---
agent_spec: "docs/agents/api/data.json"
---

# Public API

`Orleans.Lattice.Api.Data` declares two public types of its own: the `LatticeApiDataOptions` options class and the `AddLatticeDataApi` registration extension. Registration installs the package's implementation of the shared `ILatticeDataApi` contract, whose interface and request/response records live in `Orleans.Lattice.Api.Abstractions` under the `Orleans.Lattice.Api.Data` namespace. Both inventories are listed below. The [package guide](README.md) explains each operation, how it routes onto `ILattice`, and the tenant-scoping and authorization behaviour; this page is the declaration reference.

The remote transport's surface - `LatticeDataApiGrpcClient`, the wire records, and the authorization and identity seams - belongs to the gRPC binding and is listed in the [gRPC binding's public API](../lattice.api.data.grpc/api.md).

## Registration

`AddLatticeDataApi(this ISiloBuilder builder, Action<LatticeApiDataOptions>? configure = null)` registers `ILatticeDataApi` as a silo singleton and binds `LatticeApiDataOptions`. It must be called after `AddLattice(...)`: called first, it throws `InvalidOperationException`. Repeat calls are idempotent for the facade registration and still apply any supplied configure delegate. See [Configuration](configuration.md) for the option values.

## Contract summary

`ILatticeDataApi` groups its methods as follows. Every `treeId` is a tenant-local name; see [Tenant-scoped tree names](README.md#tenant-scoped-tree-names).

| Group | Methods |
|---|---|
| Point and bulk writes | `SetAsync`, `DeleteAsync`, `DeleteRangeAsync`, `SetManyAsync` |
| Streamed bulk write | `SetManyStreamAsync` - writes an `IAsyncEnumerable<DataEntry>` non-atomically in batches of `DataSetManyStreamOptions.BatchSize` (256 by default), reporting each batch's `DataSetManyStreamBatch` outcome as it completes and returning the `DataSetManyStreamResult` totals; memory is bounded by the batch, never by the stream. An entry refused for its own sake - a key the caller may not write (`denied`), a value the schema policy rejects (`schema`), or an invalid write (`invalid`) - is reported in the batch's `Rejections` with its ordinal, key and reason, and every other entry still lands; any other fault ends the stream. A default interface member over `SetManyAsync` and `SetAsync`, so an existing implementer streams without change; the gRPC binding streams over one duplex call. Used by the Explorer's import (issue #5365). |
| Atomic batches | `SetManyAtomicAsync`, `SetManyAtomicCrossTreeAsync` |
| Reads | `GetAsync`, `ReadRangeAsync` |
| Capability probe | `ProbeCapabilitiesAsync` - whether the caller may write or delete at a key, or anywhere in a tree, answered from the access gate with no side effects. A default interface member: an implementer written before it existed keeps compiling and reports no write and no delete (fail-closed), so a management surface hides its write controls against it while real writes are still authorized on attempt. This package's facade, the gRPC binding and the MCP remote adapter answer the real probe. |
| Typed CRDT verbs | the PN-counter, OR-set, OR-flag, RW-flag, G-counter, G-set, RW-set, version-vector, multi-value register, max/min register, sequence and OR-map methods listed in [Typed CRDT facade verbs](README.md#typed-crdt-facade-verbs) |

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [gRPC binding public API](../lattice.api.data.grpc/api.md)
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Data.LatticeApiDataOptions`

[Source](../../src/lattice.api.data/LatticeApiDataOptions.cs) (line 17).

`public sealed class LatticeApiDataOptions`

- `public int DefaultRangePageSize`
- `public int MaxRangePageSize`
- `public int RangeDeleteStepSize`

### `Orleans.Lattice.Api.Data.LatticeApiDataServiceCollectionExtensions`

[Source](../../src/lattice.api.data/LatticeApiDataServiceCollectionExtensions.cs) (line 12).

`public static class LatticeApiDataServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeDataApi(this ISiloBuilder builder, Action<LatticeApiDataOptions>? configure = null)`

## Shared contract declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Data.ILatticeDataApi`

[Source](../../src/lattice.api.abstractions/Data/ILatticeDataApi.cs) (line 32).

`public interface ILatticeDataApi`

- `Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)`
- `Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task<DataRangeDeleteResult> DeleteRangeAsync(DataRangeDeleteRequest request, CancellationToken cancellationToken = default)`
- `Task SetManyAtomicAsync(string treeId, DataAtomicBatch batch, string operationId, CancellationToken cancellationToken = default)`
- `Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(IReadOnlyList<DataTreeBatch> batches, string operationId, CancellationToken cancellationToken = default)`
- `Task SetManyAsync(string treeId, IReadOnlyList<DataEntry> upserts, CancellationToken cancellationToken = default)`
- `Task<DataSetManyStreamResult> SetManyStreamAsync(string treeId, IAsyncEnumerable<DataEntry> upserts, IProgress<DataSetManyStreamBatch>? progress = null, DataSetManyStreamOptions? options = null, CancellationToken cancellationToken = default)` (default interface member; batches through `SetManyAsync`, isolating per-entry refusals through `SetAsync`)
- `Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)`
- `Task<LatticeDataCapabilities> ProbeCapabilitiesAsync(string treeId, string? key = null, CancellationToken cancellationToken = default)` (default interface member; reports no capability unless overridden)
- `Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)`
- `Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)`
- `Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)`
- `Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)`
- `Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)`
- `Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)`
- `Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task GCounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)`
- `Task<long> GCounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task GSetAddAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> GSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task RwSetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)`
- `Task RwSetRemoveAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> RwSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task MaxRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)`
- `Task<byte[]?> MaxRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task MinRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)`
- `Task<byte[]?> MinRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default)`
- `Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default)`
- `Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Data.DataApiTypeAliases`

[Source](../../src/lattice.api.abstractions/Data/Model/DataApiTypeAliases.cs) (line 16).

`public static class DataApiTypeAliases`

- `public const string DataEntry`
- `public const string DataAtomicBatch`
- `public const string DataTreeBatch`
- `public const string DataRangeRequest`
- `public const string DataRangePage`
- `public const string DataReadResult`
- `public const string DataRangeDeleteRequest`
- `public const string DataRangeDeleteResult`
- `public const string LatticeDataCapabilities`
- `public const string DataEntryRejection`
- `public const string DataSetManyStreamBatch`
- `public const string DataSetManyStreamResult`

### `Orleans.Lattice.Api.Data.DataAtomicBatch`

[Source](../../src/lattice.api.abstractions/Data/Model/DataAtomicBatch.cs) (line 19).

`public sealed record DataAtomicBatch`

- `public List<DataEntry> Upserts`
- `public List<string> DeleteKeys`

### `Orleans.Lattice.Api.Data.DataEntry`

[Source](../../src/lattice.api.abstractions/Data/Model/DataEntry.cs) (line 18).

`public sealed record DataEntry`

- `public required string Key`
- `public byte[] Value`
- `public LatticeMergeMode? MergeMode`
- `public bool Raw`
- `public bool Equals(DataEntry? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Data.DataRangeDeleteRequest`

[Source](../../src/lattice.api.abstractions/Data/Model/DataRangeDeleteRequest.cs) (line 20).

`public sealed record DataRangeDeleteRequest`

- `public required string TreeId`
- `public required string StartInclusive`
- `public required string EndExclusive`

### `Orleans.Lattice.Api.Data.DataRangeDeleteResult`

[Source](../../src/lattice.api.abstractions/Data/Model/DataRangeDeleteResult.cs) (line 15).

`public sealed record DataRangeDeleteResult`

- `public required string TreeId`
- `public required int DeletedCount`

### `Orleans.Lattice.Api.Data.DataRangePage`

[Source](../../src/lattice.api.abstractions/Data/Model/DataRangePage.cs) (line 12).

`public sealed record DataRangePage`

- `public required string TreeId`
- `public IReadOnlyList<DataEntry> Entries`
- `public string? ContinuationToken`
- `public LatticeRangeReadGateCoverage? GateCoverage`

### `Orleans.Lattice.Api.Data.DataRangeRequest`

[Source](../../src/lattice.api.abstractions/Data/Model/DataRangeRequest.cs) (line 19).

`public sealed record DataRangeRequest`

- `public required string TreeId`
- `public string? StartInclusive`
- `public string? EndExclusive`
- `public int PageSize`
- `public string? ContinuationToken`
- `public bool IncludeGateCoverage`

### `Orleans.Lattice.Api.Data.DataReadResult`

[Source](../../src/lattice.api.abstractions/Data/Model/DataReadResult.cs) (line 12).

`public sealed record DataReadResult`

- `public required string TreeId`
- `public required string Key`
- `public bool Found`
- `public byte[] Value`
- `public LatticeMergeMode? MergeMode`
- `public bool Raw`
- `public bool Equals(DataReadResult? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Data.DataTreeBatch`

[Source](../../src/lattice.api.abstractions/Data/Model/DataTreeBatch.cs) (line 19).

`public sealed record DataTreeBatch`

- `public required string TreeId`
- `public List<DataEntry> Upserts`
- `public List<string> DeleteKeys`

### `Orleans.Lattice.Api.Data.LatticeDataCapabilities`

[Source](../../src/lattice.api.abstractions/Data/Model/LatticeDataCapabilities.cs) (line 27).

`public sealed record LatticeDataCapabilities`

- `public required string TreeId`
- `public string? Key`
- `public bool CanWrite`
- `public bool CanDelete`

### `Orleans.Lattice.Api.Data.DataSetManyStreamOptions`

[Source](../../src/lattice.api.abstractions/Data/Model/DataSetManyStreamOptions.cs) (line 8).

`public sealed record DataSetManyStreamOptions`

- `public const int DefaultBatchSize`
- `public const int MaxBatchSize`
- `public static DataSetManyStreamOptions Default`
- `public int BatchSize`
- `public long FirstOrdinal`
- `public void Validate()`

### `Orleans.Lattice.Api.Data.DataSetManyStreamBatch`

[Source](../../src/lattice.api.abstractions/Data/Model/DataSetManyStreamBatch.cs) (line 11).

`public sealed record DataSetManyStreamBatch`

- `public required string TreeId`
- `public required long BatchIndex`
- `public required long FirstOrdinal`
- `public required int EntryCount`
- `public required int CommittedCount`
- `public IReadOnlyList<DataEntryRejection> Rejections`

### `Orleans.Lattice.Api.Data.DataEntryRejection`

[Source](../../src/lattice.api.abstractions/Data/Model/DataEntryRejection.cs) (line 11).

`public sealed record DataEntryRejection`

- `public const string DeniedCode`
- `public const string SchemaCode`
- `public const string InvalidCode`
- `public required long Ordinal`
- `public required string Key`
- `public required string Code`
- `public required string Reason`

### `Orleans.Lattice.Api.Data.DataSetManyStreamResult`

[Source](../../src/lattice.api.abstractions/Data/Model/DataSetManyStreamResult.cs) (line 11).

`public sealed record DataSetManyStreamResult`

- `public required string TreeId`
- `public required long EntryCount`
- `public required long CommittedCount`
- `public required long RejectedCount`
- `public required long BatchCount`
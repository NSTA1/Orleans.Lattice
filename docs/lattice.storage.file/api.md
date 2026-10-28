---
agent_spec: "docs/agents/deployment.yaml"
---

# Public API

`Orleans.Lattice.Storage.File` has three public types: the `FileWalStorageProvider` implementation of the core `IWalStorageProvider` seam, its `FileWalStorageOptions`, and the `AddFileWalStorage` registration extension. Every other type in the package - the per-partition segment file, the record framing, the read-pressure governor and the file-system abstraction - is internal and is described by behaviour in [Architecture](architecture.md). The [package guide](README.md) summarises each public type's role.

## Registration

`AddFileWalStorage(this ISiloBuilder builder, Action<FileWalStorageOptions> configure)` - `configure` is required:

- binds `FileWalStorageOptions` and registers its options validator, so invalid options fail when the options are first resolved (see [Validation](configuration.md#validation));
- installs `FileWalStorageProvider` as the silo's `IWalStorageProvider` through the core `AddWalStorage` seam, displacing the in-memory baseline whether `AddLattice` runs before or after it - the last provider registration wins, so a second `AddFileWalStorage` call keeps the second configuration;
- registers the same durable-WAL garbage-collection wiring the Azure Table provider installs - a pin-aware leaf reporter and the WAL GC - so the GC never trims past a leaf's durable checkpoint across a restart. These registrations are idempotent; a host that already supplied its own keeps it.

The provider registered this way is given a routing record reader, so filtered replay reads can skip records whose keys fall outside the filter without decoding them.

## `FileWalStorageProvider`

The public constructor `FileWalStorageProvider(IOptions<FileWalStorageOptions> options, Serializer<WalRecord> serializer)` builds a standalone provider - for tests or a custom composition - without the routing reader, so its filtered replay reads decode every record they examine. Prefer `AddFileWalStorage` in a silo.

Its public methods are the `IWalStorageProvider` contract, documented with the core seam in [WAL storage providers](../lattice/wal-storage-providers.md):

| Group | Methods |
|---|---|
| Append | `AppendBatchAsync`, `AppendEncodedBatchAsync` |
| Read | `ReadAsync`, `ReadFilteredAsync`, `ReadEncodedAsync` |
| Offsets | `GetHighestOffsetAsync`, `GetLowestOffsetAsync`, `GetTrimWatermarkAsync` |
| Retention and size | `TrimAsync`, `GetRetainedByteSizeAsync`, `GetPhysicalByteSizeAsync`, `EvaluateCompactionAsync` |
| Recovery | `ReconcileAsync` |

`Dispose()` releases the open partition files.

## `FileWalStorageOptions`

The options carry the root directory, the flush policy, the compaction thresholds and the read-page byte ceiling, with public `Default*` constants for the compaction and read-page defaults. Every property, default and validation rule is in [Configuration](configuration.md).

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Core WAL storage providers](../lattice/wal-storage-providers.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Storage.File.FileWalStorageOptions`

[Source](../../src/lattice.storage.file/FileWalStorageOptions.cs) (line 10).

`public sealed class FileWalStorageOptions`

- `public const double DefaultCompactionThreshold`
- `public const int DefaultCompactionMinimumDeadBytes`
- `public const long DefaultCompactionMaximumDeadBytes`
- `public const long DefaultMaxReadBatchBytes`
- `public string RootDirectory`
- `public bool FlushToDisk`
- `public double CompactionThreshold`
- `public int CompactionMinimumDeadBytes`
- `public long CompactionMaximumDeadBytes`
- `public long MaxReadBatchBytes`

### `Orleans.Lattice.Storage.File.FileWalStorageProvider`

[Source](../../src/lattice.storage.file/FileWalStorageProvider.cs) (line 51).

`public sealed class FileWalStorageProvider : IWalStorageProvider, IDisposable`

- `public FileWalStorageProvider(IOptions<FileWalStorageOptions> options, Serializer<WalRecord> serializer)`
- `public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)`
- `public Task AppendEncodedBatchAsync(string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries, ReadOnlyMemory<long> offsets, IWalRecordEncoder encoder, CancellationToken cancellationToken)`
- `public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)`
- `public IAsyncEnumerable<WalEntry> ReadFilteredAsync(string treeId, int shardIndex, long fromOffsetExclusive, long toOffsetInclusive, int maxEntries, WalKeyFilter filter, CancellationToken cancellationToken)`
- `public Task<WalShardEncodedPage> ReadEncodedAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, IWalRecordEncoder encoder, CancellationToken cancellationToken)`
- `public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)`
- `public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)`
- `public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)`
- `public Task<long?> GetTrimWatermarkAsync(string treeId, int shardIndex, CancellationToken cancellationToken)`
- `public Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)`
- `public Task<long> GetPhysicalByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)`
- `public Task EvaluateCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken)`
- `public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)`
- `public void Dispose()`

### `Orleans.Lattice.Storage.File.LatticeFileServiceCollectionExtensions`

[Source](../../src/lattice.storage.file/LatticeFileServiceCollectionExtensions.cs) (line 14).

`public static class LatticeFileServiceCollectionExtensions`

- `public static ISiloBuilder AddFileWalStorage(this ISiloBuilder builder, Action<FileWalStorageOptions> configure)`

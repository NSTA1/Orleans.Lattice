# Azure Table WAL Public API Reference

This document describes the public `Orleans.Lattice.Storage.AzureTable` surface in caller-visible terms. It does not name library-internal product types; implementation details are described by behaviour. For the core WAL contract, see [WAL Storage Providers](../lattice/wal-storage-providers.md).

## Setup

Install the package:

```shell
dotnet add package Orleans.Lattice.Storage.AzureTable
```

Import the namespace:

```csharp verify
using Orleans.Lattice.Storage.AzureTable;
```

Register the provider on an Orleans silo:

```csharp verify
using Orleans.Lattice.Storage.AzureTable;

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true";
});
```

## Registration and DI

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `LatticeAzureTableServiceCollectionExtensions` | static class | Registers the Azure Table WAL provider and default Zstandard compressor fallback. | `AddAzureTableWalStorage`, `DefaultCompressionLevel` |
| `AzureTableWalStorageOptions` | sealed class | Configures authentication, table name, Azure SDK client options, retry knobs, commit pipeline behaviour, saturation handling, and stored-payload compression. | See [Configuration](configuration.md). |
| `AzureTableWalStorageProvider` | sealed partial class | Durable Azure Table implementation of `IWalStorageProvider`. | Implements the public WAL provider contract and `IAsyncDisposable`; exposes `MaxEntriesPerBatch` and `FlushPhaseTwoAsync`, and two public constructors for direct construction - `(IOptions<AzureTableWalStorageOptions>, Serializer<WalRecord>, IWalSaturationSignal? = null)` and the compression-aware `(IOptions<AzureTableWalStorageOptions>, Serializer<WalRecord>, IWalSaturationSignal?, IEnumerable<ILatticeCompressor>?)` that the registration helper uses. |

`AddAzureTableWalStorage` layers on the core `AddWalStorage` seam. It displaces the in-memory default installed by `AddLattice`, regardless of registration order. If multiple WAL provider registrations are made, the last provider factory wins. It also calls the core `AddWalCursorRegistry` and `AddLatticeWalGc` registrations, so a durable WAL is paired with the cursor registry and WAL garbage collection that trim it.

The helper also registers a default `ZstdLatticeCompressor` fallback at `DefaultCompressionLevel` (3) so the default `AzureTableWalStorageOptions.Compression = LatticeCompression.Zstd` works without extra wiring. The fallback is added with `TryAddEnumerable`, so it is skipped only when an `ILatticeCompressor` registration whose implementation type is `ZstdLatticeCompressor` already exists - for example a `ZstdLatticeCompressor` instance at a different compression level pre-registered with `AddLatticeCompressor` before the helper runs. The provider accepts one compressor per algorithm tag and throws at construction when two are registered, so a different compressor type for the Zstandard tag cannot sit alongside the fallback; a compressor for a different algorithm is simply added.

## Provider contract

`AzureTableWalStorageProvider` implements the public `IWalStorageProvider` seam used by the core WAL grain and replication WAL:

| Contract area | Caller-visible behaviour |
|---|---|
| Append | Appends a batch of at most `MaxEntriesPerBatch` (100) entries for one `(tree, shard)` stream. Offsets must be non-negative and dense within the batch (`ArgumentException` otherwise). Each entry is stored in one binary property, so its stored payload - after compression - must fit the service's 64 KiB limit for a binary property; a larger entry is not split, and the service rejects the whole batch. Batches may arrive out of order, and the provider does not require a batch to start at the stored tail; one that overlaps an offset another batch already wrote is rejected with `InvalidOperationException` before anything is written. |
| Encoded append | Stores already encoded WAL payload bytes without forcing a second encode. |
| Read | Streams entries after a supplied offset, in offset order, up to the requested maximum. |
| Encoded read | Returns encoded pages for efficient WAL consumers that do not need to materialize every mutation. |
| Filtered read | Examines a bounded window of entries for a reader that owns a `WalKeyFilter` and yields only the entries the filter does not exclude, plus the last examined entry routing-only when it is excluded. When the provider is registered through `AddAzureTableWalStorage`, each row is classified from its payload's routing prefix, so an excluded row is never decoded in full. |
| Highest offset | Returns the shard's stored tail, raised by the contiguous run of already-durable batches this instance's completion worker has accepted, or the empty-log sentinel defined by the core contract. |
| Lowest offset | Returns the lowest retained offset for a shard after trim. |
| Retained bytes | Reports retained payload size for capacity and trimming decisions. |
| Trim | Removes retained entries at or below the supplied offset without moving the committed tail backward. |
| Reconcile | Repairs interrupted append state before normal operation relies on the stored tail. |
| Flush | Drains the commit completions outstanding at the moment of the call, across every shard the instance has appended to, so already-appended batches become readable. Rethrows a failed completion instead of swallowing it, and leaves it observable to the next append. A no-op when commit completions are synchronous. |
| Disposal | `DisposeAsync` is idempotent: it awaits every outstanding pipelined commit completion and swallows its fault (a configured `PipelinedPhaseTwoFaultHandler` has already observed it), then stops each shard's completion worker, faulting any queued completion not yet written with `ObjectDisposedException`. |

See [Architecture](architecture.md) for the storage and commit model, and [Core WAL](../lattice/wal.md) for how the core library uses the provider.

## Options type

`AzureTableWalStorageOptions` is the single configuration object supplied to `AddAzureTableWalStorage`.

| Area | Public members |
|---|---|
| Authentication | `ConnectionString`, `ServiceUri`, `TokenCredential`, `SharedKeyCredential`, `ServiceClient` |
| Table and client | `TableName`, `DefaultTableName`, `ConfigureClientOptions` |
| Azure SDK retry | `RetryMaxAttempts`, `RetryDelay`, `RetryMaxDelay`, `RetryNetworkTimeout`, `RetryMode` |
| Commit pipeline | `PipelinePhaseTwoCommits`, `EliminateCandidateRowOnHotPath`, `PipelinedPhaseTwoFaultHandler`, `PhaseTwoCoalescingWindow`, `PhaseTwoCommitTimeout`, `PhaseOneTransientRetryMaxAttempts`, `PhaseOneTransientRetryBaseDelay` |
| Saturation | `HonorSaturationSignal`, `SaturationShortCircuitCooldown` |
| Compression | `Compression`, `CompressionMinPayloadBytes` |
| Defaults | `DefaultPipelinePhaseTwoCommits`, `DefaultEliminateCandidateRowOnHotPath`, `DefaultPhaseTwoCoalescingWindow`, `DefaultPhaseTwoCommitTimeout`, `DefaultRetryNetworkTimeout`, `DefaultHonorSaturationSignal`, `DefaultSaturationShortCircuitCooldown`, `DefaultCompression`, `DefaultCompressionMinPayloadBytes`, `DefaultPhaseOneTransientRetryMaxAttempts`, `DefaultPhaseOneTransientRetryBaseDelay`, `DefaultPhaseOneTransientRetryMaxDelay` |

Exactly one authentication mode must be configured. See [Configuration](configuration.md) for defaults, validation, and examples.

## Azure SDK pipeline policies

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `RetryAttemptTrackingPolicy` | sealed class | Azure SDK `HttpPipelinePolicy` that records one metric event per retry attempt. | `Instance`, `Process`, `ProcessAsync` |
| `SaturationAwareRetryPolicy` | sealed class | Azure SDK `HttpPipelinePolicy` that short-circuits retry attempts while `IWalSaturationSignal` reports saturation. | Constructors, `Process`, `ProcessAsync` |

The provider attaches `RetryAttemptTrackingPolicy` when it constructs a `TableServiceClient`. It attaches `SaturationAwareRetryPolicy` only when `HonorSaturationSignal` is enabled and `IWalSaturationSignal` is available from DI. If a host supplies a pre-built `TableServiceClient`, the host owns the Azure SDK pipeline and may attach these policies itself.

## Related public surfaces

The Azure Table package intentionally reuses public core surfaces instead of defining its own WAL contract:

- `IWalStorageProvider` - provider seam implemented by `AzureTableWalStorageProvider`.
- `IWalSaturationSignal` - aggregate saturation state consulted by `SaturationAwareRetryPolicy`.
- `LatticeCompression`, `ILatticeCompressor`, `ZstdLatticeCompressor` - stored-payload compression surface.
- `LatticeMetrics` - provider retry, timeout, and capacity telemetry instruments.

For replication use, `AddAzureTableWalStorage` is commonly paired with `AddLatticeReplication`; the replication package then uses the same WAL provider seam described in [Replication WAL](../lattice.replication/wal.md).

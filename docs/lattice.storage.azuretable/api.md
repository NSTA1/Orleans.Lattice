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
| `AzureTableWalStorageProvider` | sealed partial class | Durable Azure Table implementation of `IWalStorageProvider`. | Implements the public WAL provider contract and `IAsyncDisposable`; exposes `MaxEntriesPerBatch`. |

`AddAzureTableWalStorage` layers on the core `AddWalStorage` seam. It displaces the in-memory default installed by `AddLattice`, regardless of registration order. If multiple WAL provider registrations are made, the last provider factory wins.

The helper also registers a default `ZstdLatticeCompressor` fallback at `DefaultCompressionLevel` so the default `AzureTableWalStorageOptions.Compression = LatticeCompression.Zstd` works without extra wiring. Hosts that need a different compressor can register their own `ILatticeCompressor` before calling the helper.

## Provider contract

`AzureTableWalStorageProvider` implements the public `IWalStorageProvider` seam used by the core WAL grain and replication WAL:

| Contract area | Caller-visible behaviour |
|---|---|
| Append | Appends a batch for one `(tree, shard)` stream. Accepted offsets must be dense and start at the next expected offset. |
| Encoded append | Stores already encoded WAL payload bytes without forcing a second encode. |
| Read | Streams entries after a supplied offset, in offset order, up to the requested maximum. |
| Encoded read | Returns encoded pages for efficient WAL consumers that do not need to materialize every mutation. |
| Highest offset | Returns the current committed tail for a shard, or the empty-log sentinel defined by the core contract. |
| Lowest offset | Returns the lowest retained offset for a shard after trim. |
| Retained bytes | Reports retained payload size for capacity and trimming decisions. |
| Trim | Removes retained entries below a trim watermark without moving the committed tail backward. |
| Reconcile | Repairs interrupted append state before normal operation relies on the stored tail. |
| Disposal | Releases provider-owned resources and observes pending background work according to configured fault handling. |

See [Architecture](architecture.md) for the storage and commit model, and [Core WAL](../lattice/wal.md) for how the core library uses the provider.

## Options type

`AzureTableWalStorageOptions` is the single configuration object supplied to `AddAzureTableWalStorage`.

| Area | Public members |
|---|---|
| Authentication | `ConnectionString`, `ServiceUri`, `TokenCredential`, `SharedKeyCredential`, `ServiceClient` |
| Table and client | `TableName`, `DefaultTableName`, `ConfigureClientOptions` |
| Azure SDK retry | `RetryMaxAttempts`, `RetryDelay`, `RetryMaxDelay`, `RetryNetworkTimeout`, `RetryMode` |
| Commit pipeline | `PipelinePhaseTwoCommits`, `EliminateCandidateRowOnHotPath`, `PipelinedPhaseTwoFaultHandler`, `PhaseTwoCoalescingWindow`, `PhaseTwoCommitTimeout` |
| Saturation | `HonorSaturationSignal`, `SaturationShortCircuitCooldown` |
| Compression | `Compression`, `CompressionMinPayloadBytes` |
| Defaults | `DefaultPipelinePhaseTwoCommits`, `DefaultEliminateCandidateRowOnHotPath`, `DefaultPhaseTwoCoalescingWindow`, `DefaultPhaseTwoCommitTimeout`, `DefaultRetryNetworkTimeout`, `DefaultHonorSaturationSignal`, `DefaultSaturationShortCircuitCooldown`, `DefaultCompression`, `DefaultCompressionMinPayloadBytes` |

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

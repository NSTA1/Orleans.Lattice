# Configuration

This document covers `AzureTableWalStorageOptions`, the public configuration surface for `Orleans.Lattice.Storage.AzureTable`. Register the provider with `AddAzureTableWalStorage`; see [API Reference](api.md) for public types and [Architecture](architecture.md) for behavioural details.

## Registering the provider

```csharp verify
using Orleans.Lattice.Storage.AzureTable;

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true";
    o.TableName = "OrleansLatticeWal";
});
```

Each registration must configure **exactly one** authentication mode. The connection-string form above is one option; the three registrations below show the other mutually exclusive alternatives - pick whichever one matches your deployment, not more than one:

```csharp verify
using Azure.Core;
using Azure.Data.Tables;
using Orleans.Lattice.Storage.AzureTable;

TokenCredential tokenCredential = null!;
TableSharedKeyCredential sharedKeyCredential = null!;
TableServiceClient serviceClient = null!;

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ServiceUri = new Uri("https://account.table.core.windows.net");
    o.TokenCredential = tokenCredential;
});

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ServiceUri = new Uri("https://account.table.core.windows.net");
    o.SharedKeyCredential = sharedKeyCredential;
});

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ServiceClient = serviceClient;
});
```

## Options Reference - `AzureTableWalStorageOptions`

### Authentication and client

| Option | Type | Default |
|---|---|---|
| [`ConnectionString`](#connectionstring) | `string?` | `null` |
| [`ServiceUri`](#serviceuri) | `Uri?` | `null` |
| [`TokenCredential`](#tokencredential) | `TokenCredential?` | `null` |
| [`SharedKeyCredential`](#sharedkeycredential) | `TableSharedKeyCredential?` | `null` |
| [`ServiceClient`](#serviceclient) | `TableServiceClient?` | `null` |
| [`TableName`](#tablename) | `string` | `"OrleansLatticeWal"` |
| [`ConfigureClientOptions`](#configureclientoptions) | `Action<TableClientOptions>?` | `null` |

### Retry options

| Option | Type | Default |
|---|---|---|
| [`RetryMaxAttempts`](#retrymaxattempts) | `int?` | `null` |
| [`RetryDelay`](#retrydelay) | `TimeSpan?` | `null` |
| [`RetryMaxDelay`](#retrymaxdelay) | `TimeSpan?` | `null` |
| [`RetryNetworkTimeout`](#retrynetworktimeout) | `TimeSpan?` | `10 s` |
| [`RetryMode`](#retrymode) | `RetryMode?` | `null` |

### Commit pipeline options

| Option | Type | Default |
|---|---|---|
| [`PipelinePhaseTwoCommits`](#pipelinephasetwocommits) | `bool` | `true` |
| [`EliminateCandidateRowOnHotPath`](#eliminatecandidaterowonhotpath) | `bool` | `true` |
| [`PipelinedPhaseTwoFaultHandler`](#pipelinedphasetwofaulthandler) | `Action<Exception>?` | `null` |
| [`PhaseTwoCoalescingWindow`](#phasetwocoalescingwindow) | `TimeSpan` | 5 ms |
| [`PhaseTwoCommitTimeout`](#phasetwocommittimeout) | `TimeSpan?` | 3 seconds |

### Saturation options

| Option | Type | Default |
|---|---|---|
| [`HonorSaturationSignal`](#honorsaturationsignal) | `bool` | `true` |
| [`SaturationShortCircuitCooldown`](#saturationshortcircuitcooldown) | `TimeSpan` | 2 seconds |

### Compression options

| Option | Type | Default |
|---|---|---|
| [`Compression`](#compression) | `LatticeCompression` | `Zstd` |
| [`CompressionMinPayloadBytes`](#compressionminpayloadbytes) | `int` | 256 |

## Option guidance

### `ConnectionString`

Storage account connection string. Use this for Azurite, development, or deployments that manage secrets outside the Azure identity stack. Mutually exclusive with `ServiceUri`, credentials, and `ServiceClient`.

### `ServiceUri`

Azure Table service endpoint. Pair it with exactly one credential: `TokenCredential` or `SharedKeyCredential`.

### `TokenCredential`

Azure identity credential used with `ServiceUri`. This is the preferred production shape for managed identity or workload identity.

### `SharedKeyCredential`

Shared-key credential used with `ServiceUri`. Mutually exclusive with `TokenCredential`.

### `ServiceClient`

Pre-built `TableServiceClient` supplied by the host. When set, the provider uses it verbatim. `ConfigureClientOptions`, retry knobs, and provider-attached Azure SDK policies do not modify the supplied client; the host owns its options, pipeline, and lifetime.

### `TableName`

Azure Table name used for WAL storage. Defaults to `AzureTableWalStorageOptions.DefaultTableName`. The table is created on first use. Use distinct table names when sharing one storage account across independent Lattice deployments.

### `ConfigureClientOptions`

Callback used only when the provider constructs a `TableServiceClient`. Retry knobs are applied before this callback, so callback changes have final say. Add custom policies with `AddPolicy` rather than replacing the whole retry setup unless you intentionally want to own it.

### `RetryMaxAttempts`

Overrides the Azure SDK retry count after the initial attempt. `null` leaves the SDK default. `0` disables retries. Must be non-negative.

### `RetryDelay`

Overrides the Azure SDK base retry delay. `null` leaves the SDK default. Must be non-negative and must not exceed `RetryMaxDelay` when both are set.

### `RetryMaxDelay`

Overrides the Azure SDK maximum retry delay. `null` leaves the SDK default. Must be non-negative and at least `RetryDelay` when both are set.

### `RetryNetworkTimeout`

Overrides the Azure SDK per-attempt network timeout. Defaults to `10 s` - a finite bound below the WAL flush budget so a stuck request surfaces a fault into the WAL shard's failure handler (which releases and recovers the slot) instead of being abandoned while the transport zombies on for the SDK's unbounded ~100 s default. Set to `null` to restore the SDK default; must be positive when set. Prevents one stuck request from occupying a WAL slot - and, under a sustained storage brown-out, accumulating into hundreds of zombie attempts that self-sustain the brown-out.

### `RetryMode`

Overrides the Azure SDK retry mode. `null` leaves the SDK default, usually exponential backoff.

```csharp verify
using Azure.Core;
using Orleans.Lattice.Storage.AzureTable;

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true";
    o.RetryMaxAttempts = 2;
    o.RetryDelay = TimeSpan.FromMilliseconds(50);
    o.RetryMaxDelay = TimeSpan.FromSeconds(1);
    o.RetryNetworkTimeout = TimeSpan.FromSeconds(5);
    o.RetryMode = RetryMode.Exponential;
});
```

### `PipelinePhaseTwoCommits`

When `true`, the provider can return from an append after the durable entry write and after observing the previous pending completion for the same shard. Commit completion still runs in strict offset order, and failures remain sticky to a later append or the configured fault handler. Set to `false` when you want every append to wait for its own completion before returning.

### `EliminateCandidateRowOnHotPath`

When `true`, the provider removes an extra recovery-marker write from the normal append path. Recovery still detects interrupted batches using stored batch metadata and the committed shard tail. Upgrade from `false` to `true` is safe because both recovery shapes are recognized. Before moving from `true` back to `false`, drain pending appends and let reconciliation complete on a `true` deployment.

### `PipelinedPhaseTwoFaultHandler`

Optional observer for pipelined completion faults on a shard that goes idle before a successor append can observe the fault. The delegate should be idempotent and observability-only. Exceptions thrown by the delegate are ignored.

```csharp verify
using Orleans.Lattice.Storage.AzureTable;

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true";
    o.PipelinedPhaseTwoFaultHandler = static ex => _ = ex.Message;
});
```

### `PhaseTwoCoalescingWindow`

Maximum wait after the first pending completion arrives so more completions can be coalesced into the same Azure Table transaction. Default is 5 ms. Must be non-negative. Use `TimeSpan.Zero` to commit as soon as work is available.

### `PhaseTwoCommitTimeout`

Per-commit deadline for the ordered completion transaction. Default is 3 seconds. Set to `null` to make completion unbounded, or set a positive `TimeSpan` tuned above healthy p99 and below the silo-level timeout.

```csharp verify
using Orleans.Lattice.Storage.AzureTable;

var options = new AzureTableWalStorageOptions
{
    ConnectionString = "UseDevelopmentStorage=true",
    PhaseTwoCommitTimeout = TimeSpan.FromSeconds(30),
};
```

### `HonorSaturationSignal`

When `true`, and when `IWalSaturationSignal` is registered, the provider attaches `SaturationAwareRetryPolicy` to clients it constructs. The first attempt reaches the network; retry attempts can short-circuit while aggregate WAL state is saturated. Set to `false` to leave Azure SDK retries unguarded.

### `SaturationShortCircuitCooldown`

Sticky window after the last saturated observation during which retry attempts continue to short-circuit. Default is 2 seconds. Must be non-negative. Set to `TimeSpan.Zero` to consult only the present aggregate state.

```csharp verify
using Orleans.Lattice.Storage.AzureTable;

var optionsWithCooldownOverride = new AzureTableWalStorageOptions
{
    ConnectionString = "UseDevelopmentStorage=true",
    HonorSaturationSignal = true,
    SaturationShortCircuitCooldown = TimeSpan.FromSeconds(5),
};
```

### `Compression`

Stored WAL payload compression algorithm. Default is `LatticeCompression.Zstd`; set `LatticeCompression.None` to store payloads verbatim. Rows are self-describing, so changing this option affects newly written rows while older rows continue to decode with their recorded tags.

### `CompressionMinPayloadBytes`

Minimum encoded payload size at which compression is attempted. Default is 256 bytes. Must be non-negative. Ignored when `Compression` is `LatticeCompression.None`.

```csharp verify
using Orleans.Lattice;
using Orleans.Lattice.Storage.AzureTable;

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true";
    o.Compression = LatticeCompression.None;
    o.CompressionMinPayloadBytes = 0;
});
```

## Validation

At first use the provider validates:

- Exactly one authentication mode is configured.
- `TableName` is non-empty.
- Credential modes are mutually exclusive.
- Retry values are within valid ranges.
- `RetryDelay` does not exceed `RetryMaxDelay` when both are set.
- `PhaseTwoCoalescingWindow` and `SaturationShortCircuitCooldown` are non-negative.
- `PhaseTwoCommitTimeout` is positive when set.
- `CompressionMinPayloadBytes` is non-negative.

## See also

- [Core WAL Storage Providers](../lattice/wal-storage-providers.md) - provider seam and placement.
- [WAL tuning](../lattice/wal-tuning.md) - batch sizing and provider saturation envelope.
- [WAL saturation signal](../lattice/wal-saturation-signal.md) - saturation classifier used by retry short-circuiting.

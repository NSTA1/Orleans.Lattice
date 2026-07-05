# Orleans.Lattice.Storage.AzureTable

Durable Azure Table Storage-backed WAL provider for [Orleans.Lattice](../../README.md). It plugs into the public `IWalStorageProvider` seam so replicated WAL entries can survive silo restarts, support retention windows, and run against Azure Table Storage or Azurite.

## What is it?

`Orleans.Lattice.Storage.AzureTable` is the optional production WAL backend for the core lattice and replication packages:

- **Durable WAL storage.** `AzureTableWalStorageProvider` stores per-tree, per-shard WAL batches in Azure Table Storage and implements the public `IWalStorageProvider` contract.
- **Atomic batch append.** Each append batch is made visible all-or-nothing and assigned dense, monotone offsets within its shard.
- **Restart recovery.** Activation-time reconciliation completes or removes interrupted batches so the shard tail remains contiguous after a crash.
- **Azure SDK integration.** `AzureTableWalStorageOptions` controls authentication, table selection, retry tuning, stored-payload compression, phase-two commit behaviour, and WAL saturation-aware retries.
- **Drop-in registration.** `AddAzureTableWalStorage` replaces the in-memory WAL backend installed by core lattice registration.

Core WAL semantics, provider selection, and placement are covered in [WAL Storage Providers](../lattice/wal-storage-providers.md). Replication WAL consumption is covered in [Replication WAL](../lattice.replication/wal.md).

## Core Properties

- **Per-shard ordering.** Offsets are dense and monotone within each `(tree, shard)` stream.
- **Batch atomicity.** A successful append is visible as a complete batch; a rejected append leaves no visible partial batch.
- **Crash recoverability.** Interrupted appends are reconciled before normal reads and writes rely on the stored tail.
- **Bounded backend shape.** Append batches respect Azure Table transaction limits; tune replication batch sizing with [WAL tuning](../lattice/wal-tuning.md).
- **Operational back-pressure.** Optional saturation-aware retry short-circuiting cooperates with the core [WAL saturation signal](../lattice/wal-saturation-signal.md).

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Azure Table WAL provider** | Durable `IWalStorageProvider` implementation for production WAL retention and restart recovery. | [Architecture](architecture.md) |
| **Authentication modes** | Connection string, service URI plus token credential, service URI plus shared key, or a pre-built `TableServiceClient`. | [Configuration](configuration.md) |
| **Atomic append pipeline** | Entry rows become visible only after ordered commit metadata advances the shard tail. | [Architecture](architecture.md) |
| **Phase-two pipelining** | Overlaps commit completion with later appends while preserving ordering and recovery semantics. | [Configuration](configuration.md#pipelinephasetwocommits) |
| **Hot-path commit reduction** | `EliminateCandidateRowOnHotPath` removes an extra write from the normal append path while keeping recovery safe. | [Configuration](configuration.md#eliminatecandidaterowonhotpath) |
| **Retry telemetry and tuning** | Retry attempt tracking plus nullable Azure SDK retry knobs separate transient retry storms from exhausted retries. | [Configuration](configuration.md#retry-options) |
| **Saturation-aware retries** | `SaturationAwareRetryPolicy` abandons Azure SDK retries while the silo reports saturated WAL pressure. | [Configuration](configuration.md#saturation-options) |
| **Stored payload compression** | `LatticeCompression.Zstd` is enabled by default for larger stored WAL payloads. | [Configuration](configuration.md#compression-options) |
| **Chaos coverage** | Azurite-backed chaos suite validates dense offsets and monotone reads under concurrent append load. | [Chaos Tests](chaos-tests.md) |

## Quick Start

Install the package and register it on the silo that owns the WAL:

```shell
dotnet add package Orleans.Lattice.Storage.AzureTable
```

```csharp verify
using Orleans.Lattice.Storage.AzureTable;

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = "UseDevelopmentStorage=true";
    o.TableName = "OrleansLatticeWal";
});
```

For production, configure exactly one authentication mode. For example, with a service URI and a host-supplied token credential:

```csharp verify
using Azure.Core;
using Orleans.Lattice.Storage.AzureTable;

TokenCredential credential = null!;

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ServiceUri = new Uri("https://account.table.core.windows.net");
    o.TokenCredential = credential;
});
```

## Reference

For day-to-day use and operations:

- [API Reference](api.md) - public types, registration helper, and extension policies.
- [Configuration](configuration.md) - every `AzureTableWalStorageOptions` knob, default, and validation rule.
- [Architecture](architecture.md) - storage layout, transactional batch contract, commit pipeline, and recovery behaviour.
- [Chaos Tests](chaos-tests.md) - the Azurite-backed chaos suite and what it proves.

Related package docs:

- [Core WAL Storage Providers](../lattice/wal-storage-providers.md) - core provider seam, in-memory default, provider catalogue, and WAL placement.
- [Core WAL](../lattice/wal.md) - single-cluster WAL commit and replay semantics.
- [WAL tuning](../lattice/wal-tuning.md) - batching, pending depth, shard count, and saturation envelope.
- [WAL saturation signal](../lattice/wal-saturation-signal.md) - classifier and observer model used by saturation-aware retries.
- [Replication WAL](../lattice.replication/wal.md) - how replication consumes retained WAL entries.
- [Replication package](../lattice.replication/README.md) - end-to-end cross-cluster replication overview.

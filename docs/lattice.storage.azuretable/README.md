# Orleans.Lattice.Storage.AzureTable

Durable Azure Table Storage-backed WAL provider for [Orleans.Lattice](../../README.md). It plugs into the public `IWalStorageProvider` seam so replicated WAL entries can survive silo restarts, support retention windows, and run against Azure Table Storage or Azurite.

## What is it?

`Orleans.Lattice.Storage.AzureTable` is the optional production WAL backend for the core lattice and replication packages:

- **Durable WAL storage.** `AzureTableWalStorageProvider` stores per-tree, per-partition WAL batches in Azure Table Storage and implements the public `IWalStorageProvider` contract.
- **Atomic batch append.** Each append batch is made visible all-or-nothing, and its caller-assigned offsets must be dense within the batch.
- **Restart recovery.** Activation-time reconciliation completes an interrupted batch that contiguously extends the stored tail and removes any other, so a crash never leaves a half-committed batch behind and never lowers the tail.
- **Azure SDK integration.** `AzureTableWalStorageOptions` controls authentication, table selection, retry tuning, stored-payload compression, phase-two commit behaviour, and WAL saturation-aware retries.
- **Drop-in registration.** `AddAzureTableWalStorage` replaces the in-memory WAL backend installed by core lattice registration, and wires the durable-WAL garbage-collection stack (WAL cursor registry, leaf reporter, and WAL GC) alongside it.

Core WAL semantics, provider selection, and placement are covered in [WAL Storage Providers](../lattice/wal-storage-providers.md). Replication WAL consumption is covered in [Replication WAL](../lattice.replication/wal.md).

## Core Properties

- **Per-partition ordering.** Offsets are stored verbatim and read back in ascending order within each tree's WAL partition stream.
- **Batch atomicity.** A successful append is visible as a complete batch; a rejected append leaves no visible partial batch.
- **Crash recoverability.** Interrupted appends are reconciled before normal reads and writes rely on the stored tail.
- **Bounded backend shape.** The provider refuses a batch of more than 100 entries, the Azure Table transaction limit, and each entry must fit one 64 KiB binary property after compression: an entry is never split, so a larger one is rejected by the service and fails its whole batch. Tune WAL batching and pending depth with [WAL tuning](../lattice/wal-tuning.md).
- **Operational back-pressure.** Optional saturation-aware retry short-circuiting cooperates with the core [WAL saturation signal](../lattice/wal-saturation-signal.md).

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Azure Table WAL provider** | Durable `IWalStorageProvider` implementation for production WAL retention and restart recovery. | [Architecture](architecture.md) |
| **Authentication modes** | Connection string, service URI plus token credential, service URI plus shared key, or a pre-built `TableServiceClient`. | [Configuration](configuration.md) |
| **Atomic append pipeline** | Entry rows become readable only once their batch's commit metadata is written, in ascending offset order within each commit. | [Architecture](architecture.md) |
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

The WAL is only half of a durable tree: each leaf's state row and its snapshots live in the grain storage provider `AddLattice` registers, and the WAL garbage collector trims entries once a snapshot there covers them, so a production deployment pairs this provider with a durable grain storage provider too. That grain storage provider must enforce ETags on write, as Orleans' Azure Table grain storage does; see [The grain storage provider must enforce ETags](../lattice/configuration.md#the-grain-storage-provider-must-enforce-etags).

## Reference

For day-to-day use and operations:

- [API Reference](api.md) - public types, registration helper, and extension policies.
- [Configuration](configuration.md) - every `AzureTableWalStorageOptions` knob, default, and validation rule.
- [Architecture](architecture.md) - storage layout, transactional batch contract, commit pipeline, and recovery behaviour.
- [Chaos Tests](chaos-tests.md) - the Azurite-backed chaos suite and what it proves.

Related package docs:

- [Core WAL Storage Providers](../lattice/wal-storage-providers.md) - core provider seam, in-memory default, provider catalogue, and WAL placement.
- [Core WAL](../lattice/wal.md) - single-cluster WAL commit and replay semantics.
- [WAL tuning](../lattice/wal-tuning.md) - batching, pending depth, WAL partition count, and saturation envelope.
- [WAL saturation signal](../lattice/wal-saturation-signal.md) - classifier and observer model used by saturation-aware retries.
- [Replication WAL](../lattice.replication/wal.md) - how replication consumes retained WAL entries.
- [Replication package](../lattice.replication/README.md) - end-to-end cross-cluster replication overview.

# Orleans.Lattice.Storage.AzureTable

Durable **Azure Table Storage** backend for the [Orleans.Lattice](https://www.nuget.org/packages/Orleans.Lattice) write-ahead log (`IWalStorageProvider`). Pluggable via the core `AddWalStorage(...)` seam, so single-cluster deployments can persist their commit log without taking a hard reference on the replication package.

## What it gives you

- **Durable commit log** - persists the canonical WAL to Azure Table Storage, giving crash-safe recovery across silo restarts and redeployments.
- **Transactional batch writes** - commits are applied as Azure Table entity-group transactions, preserving the WAL's all-or-nothing batch contract.
- **No replication dependency** - plugs into the core `AddWalStorage(...)` seam directly; you get durability without pulling in the cross-cluster stack.
- **Capacity-aware layout** - a documented partitioning scheme spreads load across partitions and stays within Azure Table's per-transaction and entity-size limits.

## Getting started

```csharp
siloBuilder
    .AddLattice((silo, storageName) => silo.AddMemoryGrainStorage(storageName))
    .AddAzureTableWalStorage(opts =>
    {
        opts.ConnectionString = "DefaultEndpointsProtocol=https;...";
    });
```

See the [storage guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.storage.azuretable/README.md) for the full storage layout, transactional batch contract, capacity planning, and operations guide. For the core WAL provider seam, see [WAL storage providers](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice/wal-storage-providers.md).

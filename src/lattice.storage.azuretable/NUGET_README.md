# Orleans.Lattice.Storage.AzureTable

Durable **Azure Table Storage** backend for the [Orleans.Lattice](https://www.nuget.org/packages/Orleans.Lattice) write-ahead log (`IWalStorageProvider`). Pluggable via the core `AddWalStorage(...)` seam, so single-cluster deployments can persist their commit log without taking a hard reference on the replication package.

## What it gives you

- **Durable commit log** - persists the canonical WAL to Azure Table Storage, giving crash-safe recovery across silo restarts and redeployments.
- **Transactional batch writes** - commits are applied as Azure Table entity-group transactions, preserving the WAL's all-or-nothing batch contract.
- **No replication dependency** - plugs into the core `AddWalStorage(...)` seam directly; you get durability without pulling in the cross-cluster stack.
- **Capacity-aware layout** - a documented partitioning scheme spreads load across partitions and keeps each append batch within Azure Table's 100-entity transaction limit. Each WAL entry is stored in a single binary property, which Azure Table limits to 64 KiB, so an entry larger than that after compression is rejected rather than split.

## Getting started

```csharp
var connectionString = "DefaultEndpointsProtocol=https;...";

siloBuilder
    .AddLattice((silo, storageName) =>
        silo.AddAzureTableGrainStorage(storageName, options =>
        {
            options.TableServiceClient = new TableServiceClient(connectionString);
        }))
    .AddAzureTableWalStorage(opts =>
    {
        opts.ConnectionString = connectionString;
    });
```

The WAL is only half of a durable tree. Each leaf's state row and its snapshots live in the grain storage provider `AddLattice` registers, and the WAL garbage collector trims entries once a snapshot there covers them, so that provider must be durable too - here Orleans Azure Table grain storage (`AddAzureTableGrainStorage`, from the `Microsoft.Orleans.Persistence.AzureStorage` package; `TableServiceClient` is from `Azure.Data.Tables`). In-memory grain storage would lose that state on restart, and with it any data whose WAL entries were already trimmed.

See the [storage guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.storage.azuretable/README.md) for the full storage layout, transactional batch contract, capacity planning, and operations guide. For the core WAL provider seam, see [WAL storage providers](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice/wal-storage-providers.md).

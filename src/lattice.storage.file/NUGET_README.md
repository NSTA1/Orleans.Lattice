# Orleans.Lattice.Storage.File

Durable **local disk** backend for the [Orleans.Lattice](https://www.nuget.org/packages/Orleans.Lattice) write-ahead log (`IWalStorageProvider`). Pluggable via the core `AddWalStorage(...)` seam, so a cloud-free, single-box deployment can persist its commit log to a mounted directory without an external storage account.

## What it gives you

- **Durable commit log** - persists the canonical WAL to a segmented, append-only log on disk, giving crash-safe recovery across silo restarts and redeployments.
- **All-or-nothing batches** - a batch append is flushed to physical disk (fsync) before the write completes, preserving the WAL's all-or-nothing durability contract.
- **No cloud dependency** - plugs into the core `AddWalStorage(...)` seam directly, so a laptop or a single container gets, for its commit log, the same observable durability guarantees as the Azure Table backend.
- **Self-compacting** - trimmed (garbage-collected) payload bytes are reclaimed by rewriting a shard's segment file once enough dead space accumulates, with tunable thresholds.

## Getting started

```csharp
siloBuilder
    // Not durable: replace this in-memory grain storage with a durable
    // grain storage provider for a durable deployment (see below).
    .AddLattice((silo, storageName) => silo.AddMemoryGrainStorage(storageName))
    .AddFileWalStorage(options =>
    {
        options.RootDirectory = "/data/wal";
    });
```

The WAL is only half of a durable tree. Each leaf's state row and its snapshots live in the grain storage provider `AddLattice` registers, and the WAL garbage collector trims entries once a snapshot there covers them, so the in-memory grain storage above loses that state on restart - and with it any data whose WAL entries were already trimmed. For a durable deployment register a durable grain storage provider instead: the [RepoContext container sample](https://github.com/NSTA1/Orleans.Lattice/blob/main/samples/RepoContextContainer/README.md) pairs this WAL with Orleans ADO.NET grain storage over a single SQLite file on the same volume.

The provider also wires the durable-WAL garbage-collection stack (WAL cursor registry, leaf reporter, and WAL GC). Each leaf's applied frontier is mirrored into durable pins the GC consults, so a restart that wipes the process-local cursor registry cannot let the GC trim entries a leaf still needs.

See the [storage guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.storage.file/README.md) for the on-disk layout, the durability contract, compaction tuning, and operations guidance. For the core WAL provider seam, see [WAL storage providers](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice/wal-storage-providers.md).

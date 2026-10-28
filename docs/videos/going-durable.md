---
agent_spec: "docs/agents/deployment.yaml"
---

# Going durable

Durable grain storage and a durable write-ahead log, in place of the in-memory defaults.

<!-- video:begin episode="going-durable" path="build" order="6" length="1:56" cut="8d6a82f0db6f" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/going-durable.html),
> or [download it](../../docs-site/media/going-durable-8d6a82f0db6f.mp4)
> (MP4, 1:56, 7.2 MB).

<!-- video:end -->

## Transcript

<!-- transcript:begin -->

### Going durable

TTL and soft delete showed how data goes away when you ask it to. Here, we stop it going away when a silo restarts.

This is Going durable: durable grain storage and a durable write-ahead log, in place of the in-memory defaults.

### Two halves, both in memory

Every episode so far has run on the defaults, and the defaults keep everything in memory.

A tree lives in two places. Grain storage holds its leaves and their snapshots, and the write-ahead log holds every committed write, in order. In memory, neither survives a restart, so going durable means replacing both.

### Durable grain storage

Start with grain storage. The callback you pass when you register Lattice chooses it, so swap the memory provider for a durable Orleans provider, here Azure Table storage.

That provider must enforce ETags, so that a write presenting a stale one fails. Orleans' own durable providers do, and each silo checks as it starts: by default, a provider that accepts a stale write stops the silo from starting.

### A durable log on disk

Then the write-ahead log. With no cloud dependency, the file provider keeps it on local disk: one call, and a root directory.

It suits a single container with a mounted volume. Each batch is all or nothing, so a crash before a batch is committed rolls it back.

### Or in Azure Table storage

For production deployments replicated across regions, the Azure Table provider keeps the log in a storage table instead. Either call replaces the in-memory log, whether it comes before or after Lattice is registered.

Either way, the log is only half of a durable tree. It is trimmed once a snapshot in grain storage covers it, so pair it with durable grain storage too, not the memory provider.

### Where next

Next on Build: Moving in from another store. The links are on this episode's page in the documentation.

<!-- transcript:end -->

## The code on screen

The episode starts from the in-memory defaults, registers Azure Table grain storage through `AddLattice`, then registers a durable write-ahead log on local disk or in Azure Table storage. The snippets are compiled with the rest of the documentation.

<!-- video-snippet: going-durable/memory-defaults -->
```csharp verify
siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
```

<!-- video-snippet: going-durable/grain-storage -->
```csharp verify
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;

var connectionString = "UseDevelopmentStorage=true";
siloBuilder.AddLattice((services, storageName) =>
{
    services.AddAzureTableGrainStorage(storageName, options =>
        options.TableServiceClient = new TableServiceClient(connectionString));
});
```

<!-- video-snippet: going-durable/file-wal -->
```csharp verify
using Orleans.Lattice.Storage.File;

siloBuilder.AddFileWalStorage(options =>
{
    options.RootDirectory = "/data/wal";
});
```

<!-- video-snippet: going-durable/azure-table-wal -->
```csharp verify
using Orleans.Lattice.Storage.AzureTable;

siloBuilder.AddAzureTableWalStorage(options =>
{
    options.ConnectionString = "UseDevelopmentStorage=true";
});
```

## Where next

<!-- where-next:begin -->

- **Next on Build:** [Moving in from another store](moving-in-from-another-store.md)
- **The pages it introduces:** [Configuration](../lattice/configuration.md), [WAL storage providers](../lattice/wal-storage-providers.md), [File WAL](../lattice.storage.file/README.md) and [Azure Table storage](../lattice.storage.azuretable/README.md)

<!-- where-next:end -->

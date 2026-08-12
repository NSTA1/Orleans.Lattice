# Orleans.Lattice.Storage.File

Durable, cloud-free **local disk** WAL provider for [Orleans.Lattice](../../README.md). It plugs into the public `IWalStorageProvider` seam so a single-cluster deployment can persist its commit log to a mounted directory - crash-safe across silo restarts - without an external storage account.

## What is it?

`Orleans.Lattice.Storage.File` is the optional on-disk WAL backend for the core lattice:

- **Durable WAL storage.** `FileWalStorageProvider` stores each per-tree, per-shard write-ahead log as a segmented, append-only file on the local filesystem and implements the public `IWalStorageProvider` contract.
- **All-or-nothing batch append.** Each batch is framed as a run of data records sealed by a single commit trailer and made durable with one write plus fsync; a crash before the trailer is durable rolls the whole batch back on recovery.
- **Restart recovery.** Activation-time reconciliation rolls every committed batch forward, discards a torn tail, and reclaims trimmed space, so the shard tail stays contiguous after a crash.
- **Drop-in registration.** `AddFileWalStorage` displaces the in-memory WAL backend installed by core lattice registration, and wires the durable-WAL garbage-collection stack alongside it.

It matches the observable durability guarantees of the [Azure Table Storage provider](../lattice.storage.azuretable/README.md) without any cloud dependency, which makes it the enabler for a single-container, "codebase memory in a box" deployment (see the [RepoContext MCP](../lattice.api.mcp.repocontext/README.md) package and its [container sample](../../samples/RepoContextContainer/README.md)).

Core WAL semantics, the provider seam, and placement are covered in [WAL Storage Providers](../lattice/wal-storage-providers.md).

## Core Properties

- **Per-shard ordering.** Offsets are stored verbatim and read back in ascending order within each `(tree, shard)` stream.
- **Batch atomicity.** A successful append is visible as a complete batch; a torn or uncommitted trailing batch leaves no visible partial state after recovery.
- **Crash recoverability.** Interrupted appends are reconciled before normal reads and writes rely on the stored tail.
- **Self-compacting.** Trimmed payload bytes are physically reclaimed by rewriting a shard's segment file once enough dead space accumulates.

## Quick Start

Install the package and register it on the silo that owns the WAL:

```shell
dotnet add package Orleans.Lattice.Storage.File
```

```csharp verify
using Orleans.Lattice.Storage.File;

siloBuilder.AddFileWalStorage(options =>
{
    options.RootDirectory = "/data/wal";
});
```

Register it alongside `AddLattice(...)` on the silo that owns the WAL. The registration order does not matter: `AddFileWalStorage` displaces the in-memory baseline whether it runs before or after `AddLattice`.

## Reference

- [Configuration](configuration.md) - every `FileWalStorageOptions` knob, default, and validation rule.
- [Architecture](architecture.md) - the on-disk layout, the append/commit framing, the durability contract, compaction, and recovery behaviour.

Related package docs:

- [Core WAL Storage Providers](../lattice/wal-storage-providers.md) - the core provider seam, in-memory default, and WAL placement.
- [Core WAL](../lattice/wal.md) - single-cluster WAL commit and replay semantics.
- [Azure Table WAL provider](../lattice.storage.azuretable/README.md) - the cloud-backed alternative with the same durability contract.

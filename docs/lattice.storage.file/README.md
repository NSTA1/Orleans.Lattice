---
agent_spec: "docs/agents/deployment.yaml"
---

# Orleans.Lattice.Storage.File

Durable, cloud-free **local disk** WAL provider for [Orleans.Lattice](../../README.md). It plugs into the public `IWalStorageProvider` seam so a single-cluster deployment can persist its commit log to a mounted directory - crash-safe across silo restarts - without an external storage account.

## What is it?

`Orleans.Lattice.Storage.File` is the optional on-disk WAL backend for the core lattice:

- **Durable WAL storage.** `FileWalStorageProvider` stores each WAL partition of each tree as a segmented, append-only file on the local filesystem and implements the public `IWalStorageProvider` contract.
- **All-or-nothing batch append.** Each batch is framed as a run of data records sealed by a single commit trailer and made durable with one write plus fsync; a crash before the trailer is durable rolls the whole batch back on recovery.
- **Restart recovery.** Activation-time reconciliation rolls every committed batch forward, discards a torn tail, and reclaims trimmed space, so after a crash the log ends cleanly at its last committed batch; an honest offset gap left by a failed append is preserved, never renumbered.
- **Drop-in registration.** `AddFileWalStorage` displaces the in-memory WAL backend installed by core lattice registration, and wires the durable-WAL garbage-collection stack alongside it.

As a WAL store it matches the observable durability guarantees of the [Azure Table Storage provider](../lattice.storage.azuretable/README.md) without any cloud dependency. Tree state - each leaf's state row and its snapshots - still lives in the grain storage provider `AddLattice` registers, so a durable deployment pairs it with a durable grain storage provider, which must enforce ETags on write (see [The grain storage provider must enforce ETags](../lattice/configuration.md#the-grain-storage-provider-must-enforce-etags)); that pairing makes it the enabler for a single-container, "codebase memory in a box" deployment (see the [RepoContext MCP](../lattice.api.mcp.repocontext/README.md) package and its [container sample](../../samples/RepoContextContainer/README.md), which uses Orleans ADO.NET grain storage over a single SQLite file).

Core WAL semantics, the provider seam, and placement are covered in [WAL Storage Providers](../lattice/wal-storage-providers.md).

## Core Properties

- **Per-partition ordering.** Offsets are stored verbatim and read back in ascending order within each tree's WAL partition stream.
- **Batch atomicity.** A successful append is visible as a complete batch; a torn or uncommitted trailing batch leaves no visible partial state after recovery.
- **Crash recoverability.** Interrupted appends are reconciled before normal reads and writes rely on the stored tail.
- **Self-compacting.** Trimmed payload bytes are physically reclaimed by rewriting a WAL partition's segment file once enough dead space accumulates.
- **Published to NuGet** as `Orleans.Lattice.Storage.File`, released under `lattice.storage.file-v<X.Y.Z>` tags.

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **File WAL provider** | Durable `IWalStorageProvider` implementation on local disk, one `wal.log` per tree/WAL partition. | [Architecture](architecture.md#on-disk-layout) |
| **All-or-nothing append** | Data records sealed by one commit trailer and, with `FlushToDisk` on (the default), fsynced before the append returns; a failed write or flush is rolled back rather than resurrected by recovery. | [Architecture](architecture.md#append-and-commit-framing) |
| **Crash recovery** | Reconciliation rolls committed batches forward, discards a torn tail, and keeps honest offset gaps. | [Architecture](architecture.md#crash-recovery) |
| **Compaction** | Ratio, absolute-ceiling, and reconciliation triggers return trimmed space to the filesystem. | [Configuration](configuration.md#options) |
| **Bounded reads** | `MaxReadBatchBytes` caps a read page, narrowed further under memory pressure. | [Configuration](configuration.md#options) |
| **Filtered replay reads** | Records outside a reader's `WalKeyFilter` are classified from a short prefix and skipped without a full decode. | [Architecture](architecture.md#filtered-replay-reads) |
| **Telemetry** | Compaction and recovery instruments on the core `orleans.lattice` meter. | [Architecture](architecture.md#telemetry) |

## Public surface

The table summarises each public type's role; the [Public API](api.md) reference lists every public member and signature.

| Type or member | Role |
|---|---|
| `FileWalStorageProvider` | Public `IWalStorageProvider` implementation (also `IDisposable`) that stores one `wal.log` per tree/WAL-partition stream under `FileWalStorageOptions.RootDirectory`. Its public constructor, `FileWalStorageProvider(IOptions<FileWalStorageOptions>, Serializer<WalRecord>)`, builds a provider without the routing reader, so its filtered replay reads decode every record they examine; `AddFileWalStorage` is the registration that supplies one. |
| `FileWalStorageOptions` | Public options type for the root directory, flush policy, compaction thresholds, and read-page byte ceiling, with public `Default*` constants for the compaction and read-page defaults. |
| `LatticeFileServiceCollectionExtensions.AddFileWalStorage` | Registration extension that installs the file WAL provider, its options validator, and the durable-WAL garbage-collection wiring on an `ISiloBuilder`. |

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

- [Public API](api.md) - every public type, member and signature, and what `AddFileWalStorage` registers.
- [Configuration](configuration.md) - every `FileWalStorageOptions` knob, default, and validation rule.
- [Architecture](architecture.md) - the on-disk layout, the append/commit framing, the durability contract, compaction, recovery behaviour, and the telemetry the provider emits.

Related package docs:

- [Core WAL Storage Providers](../lattice/wal-storage-providers.md) - the core provider seam, in-memory default, and WAL placement.
- [Core WAL](../lattice/wal.md) - single-cluster WAL commit and replay semantics.
- [Azure Table WAL provider](../lattice.storage.azuretable/README.md) - the cloud-backed alternative with the same durability contract.

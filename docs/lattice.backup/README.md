# Orleans.Lattice.Backup

Causally-consistent backup and restore for [Orleans.Lattice](../../README.md).

## What is it?

`Orleans.Lattice.Backup` captures a named, timestamped, point-in-time backup of a selected scope of a lattice tree and restores it later without losing a single bit of causal metadata. It builds on the core snapshot, write-ahead-log, and merge machinery and adds:

- **Full capture** - a scoped snapshot exported through the core zero-observable-writes cursor, with a self-describing manifest that records the consistency cut, the shard topology, a per-key shape and merge-mode map, per-origin provenance high-water marks, and an optional compression-dictionary reference.
- **Incremental capture** - a forward write-ahead-log differential layered on a base backup, resuming from the base backup's per-partition WAL offsets, and falling back to a full capture when the resume point has been trimmed off the WAL or a range delete surfaces in the delta window.
- **Restore** - a mode-faithful replay that reinstalls every entry's hybrid-logical-clock, version vector, origin cluster id, expiry, and tombstone flag exactly as captured, either in place (bulk-load into an empty tree, or last-writer-wins merge into existing data) or via an atomic shadow-cutover to a fresh tree. Every artifact is validated against its content digest before anything is applied, and a restore is idempotent under retry.
- **Scheduling and retention** - opt-in recurring full and incremental schedules per scope, and a chain-aware retention policy that never prunes the base chain of a retained increment.
- **A pluggable sink** - the storage surface a backup is written to and read from, defaulting to an in-cluster dogfooded tree, with a durable [Azure Blob Storage sink](../lattice.backup.azureblob/README.md) shipped as a sibling package.
- **A cross-tree causal fence** - an opt-in, shared-HLC fence for a multi-tree backup set so a cross-tree atomic write is never torn across the set boundary.

The package registers the storage and engine surface; the [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) facade and its [gRPC binding](../lattice.api.backup.grpc/README.md) add the remotely-drivable control plane.

## Core properties

- **Causally faithful.** A restore preserves the captured history verbatim: entries replay through the HLC-preserving last-writer-wins merge and bulk-load seams, so a restored tree converges identically to the source.
- **Point-in-time isolation.** A capture rides the core snapshot cursor for a stable read, inherits the core snapshot shedding and replay-budget behaviour, and fails fast when the in-scope size would exceed the replay budget.
- **Fail-closed authorization.** Every capture, restore, list, describe, and delete authorizes its scope before touching data, through the same access gate the data path uses, against a dedicated `Backup` (capture) or `Restore` (author / bulk-load) capability.
- **Idempotent by construction.** Artifacts are content-addressed (SHA-256), so a retried capture that produces identical bytes is stored once; registering the same manifest twice, or re-running the same restore, converges without duplication.
- **Opt-in and hidden.** Registering the package installs the storage surface but starts no scheduled work until an operator opts in. The catalog and store live in reserved `sys-backup-*` trees that inherit the core `sys-` catalog-hiding filter, so the backup surface is the sole enumeration point for backups.

## Features

| Feature | Surface | Summary |
|---|---|---|
| Full capture | `ILatticeBackupCaptureService.CaptureAsync` | Scoped point-in-time snapshot registered as a `BackupManifest`. |
| Backup set | `ILatticeBackupCaptureService.CaptureSetAsync` | One full backup per scope under a single set manifest, optionally cross-tree consistent. |
| Incremental capture | `ILatticeBackupIncrementalCaptureService.CaptureIncrementalAsync` | Forward-WAL differential layered on a base backup, with full-capture fallback. |
| Restore | `ILatticeBackupRestoreService.RestoreAsync` | Mode-faithful, validated, idempotent replay of a manifest chain. |
| Revert | `ILatticeBackupRestoreService.RevertRestoreAsync` | Undoes a shadow-cutover restore by swapping the registry alias back. |
| Trigger / schedule / prune | `ILatticeBackupScheduler` | On-demand triggers, recurring schedules, and chain-aware retention per scope. |
| Catalog | `ILatticeBackupCatalogStore` | Durable, introspectable index of manifests keyed by backup id. |
| Catalog rebuild / scrub | `ILatticeBackupControl.RebuildCatalogFromSinkAsync` / `ScrubCatalogAgainstSinkAsync` | Re-derive the catalog from the sink, or reconcile and prune rows whose sink payload is gone. |
| Cold restore | `ILatticeBackupControl.ColdRestoreAsync` | Restore into a fresh cluster from the sink alone, with no surviving catalog. |
| Health monitoring | `ILatticeBackupHealthService` / `ILatticeBackupControl` health ops | Periodic presence + content-hash verification of each backup's durable sink payload, gated on a durable sink. |
| Sink | `ILatticeBackupSink` | Pluggable content-addressed artifact + manifest storage. |
| Reserved-namespace guard | `LatticeBackupReservedTrees` | Lets an application validate its own tree ids against the reserved `sys-backup-*` namespace. |
| Observability | `BackupMetrics` / `LatticeBackupMetrics` | A dedicated `orleans.lattice.backup` meter for space, throughput, failures, and inventory. |

## Quick Start

Register the core lattice, then the backup package, on the silo. Backup must be added after the core registration.

```csharp verify
using Orleans.Lattice;
using Orleans.Lattice.Backup;

siloBuilder
    .AddLattice((silo, storageName) =>
    {
        // Configure the storage provider named by storageName.
    })
    .AddLatticeBackup(options =>
    {
        // Durable per-key history on the catalog tree is on by default;
        // widen the cross-tree-set fence drain budget if needed.
        options.CrossTreeFenceDrainTimeout = TimeSpan.FromSeconds(45);
    });
```

Capture and restore through the registered services:

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Backup;

IServiceProvider serviceProvider = null!;
var captureService = serviceProvider.GetRequiredService<ILatticeBackupCaptureService>();
var restoreService = serviceProvider.GetRequiredService<ILatticeBackupRestoreService>();

// Capture a full backup of a whole tree.
var capture = await captureService.CaptureAsync(
    new LatticeBackupCaptureRequest("nightly", BackupScopeSelector.WholeTree("orders")),
    cancellationToken);

// Restore it later into a fresh tree via an atomic shadow-cutover.
await restoreService.RestoreAsync(
    new LatticeRestoreRequest(
        capture.BackupId,
        targetTreeId: "orders",
        mode: LatticeRestoreMode.ShadowCutover),
    cancellationToken);
```

Enable a recurring schedule and retention for a scope (opt-in; everything is disabled by default):

```csharp verify
using Orleans.Lattice.Backup;

siloBuilder.ConfigureLatticeBackupSchedule("orders-scope-key", options =>
{
    options.FullBackupScheduleEnabled = true;
    options.FullBackupInterval = TimeSpan.FromHours(6);
    options.IncrementalBackupScheduleEnabled = true;
    options.RetentionEnabled = true;
    options.RetentionKeepLast = 30;
});
```

The scope key passed to `ConfigureLatticeBackupSchedule` is the value returned by `BackupScopeKey.For(scope)`.

## Reference

- [API reference](api.md) - every public type and member, by name, with signatures.
- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Architecture](architecture.md) - the capture, incremental, restore, scheduling, and sink pipelines and the core seams they attach to.
- [Disaster recovery](disaster-recovery.md) - the sink-is-truth model, catalog rebuild and scrub, cold restore into a fresh cluster, and periodic health monitoring.
- [Observability](observability.md) - the `orleans.lattice.backup` meter and its instruments.

## See also

- [`Orleans.Lattice.Backup.AzureBlob`](../lattice.backup.azureblob/README.md) - the durable Azure Blob Storage sink implementation.
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the transport-agnostic backup / restore control facade.
- [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/README.md) - the code-first gRPC binding and typed client for the control facade.

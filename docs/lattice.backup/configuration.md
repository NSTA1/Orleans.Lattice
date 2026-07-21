# Orleans.Lattice.Backup configuration

The package has two public options types: `LatticeBackupOptions` (catalog history and cross-tree-set fence behaviour) and `LatticeBackupScheduleOptions` (per-scope scheduling and retention). Both are bound through the `AddLatticeBackup` / `ConfigureLatticeBackupSchedule` registration extensions and validated at silo start.

## `LatticeBackupOptions`

Configures the durable per-key history retained on the reserved `sys-backup-catalog` tree so the record of backups catalogued and removed stays auditable, and the drain behaviour of a cross-tree-consistent backup-set fence. Bind it through `AddLatticeBackup(configure)` or `ConfigureLatticeBackup(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `HistoryRetentionMode` | `HistoryRetentionMode` | `MetadataOnly` | The retention mode for the durable per-key history captured on the catalog tree. History is never disabled by default. |
| `HistoryRetentionWindow` | `TimeSpan?` | `null` | The age after which a catalog history revision row expires, or `null` for no age bound. Must be strictly positive when supplied. |
| `EnableDurableHistoryView` | `bool` | `true` | Whether to create the durable per-key history materialised view over the catalog tree so catalog changes remain auditable beyond the source write-ahead-log window. |
| `EnableBackupCatalogIndexView` | `bool` | `true` | Whether to create the backup-catalog index materialised view over the catalog tree. The index re-keys each catalogued backup so the catalog listing can be filtered, ordered newest-first, and paged efficiently by scanning the index rather than the whole catalog. When disabled, the listing falls back to a full catalog scan. |
| `CrossTreeFenceDrainTimeout` | `TimeSpan` | `30s` | The maximum total wall-clock time a cross-tree-consistent backup-set fence waits for in-flight cross-tree atomic sagas to drain before it gives up and fails the capture. Must be strictly positive. Single-tree and non-flagged backups never consult it. |
| `CrossTreeFencePollInterval` | `TimeSpan` | `25ms` | The poll interval between successive in-flight observations while the fence waits for sagas to drain. Must be strictly positive. |
| `MaxCrossTreeFenceAttempts` | `int` | `5` | The maximum number of fence attempts a cross-tree-consistent capture makes before failing. Each attempt drains, captures, and re-observes; an attempt is retried when a cross-tree saga registers on the set during the capture window. Must be at least 1. |

`HistoryRetentionMode` is the core Lattice history-retention enum; `MetadataOnly` retains the per-key revision metadata without retaining every historical value.

## `LatticeBackupScheduleOptions`

Per-scope configuration for scheduled backup triggering and backup-chain retention. Every knob defaults to disabled: registering the backup package never starts capturing or pruning on its own. Configure the global default with `ConfigureLatticeBackupSchedule(configure)`, or a single scope with `ConfigureLatticeBackupSchedule(scopeKey, configure)` where `scopeKey` is `BackupScopeKey.For(scope)`. The scheduler resolves the per-scope instance by named options, so a schedule configured for a scope and the coordination that runs it always resolve the same instance.

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `MinimumInterval` | `TimeSpan` | `1 minute` | The smallest cadence a schedule reminder honours (the Orleans reminder minimum). A configured interval smaller than this is clamped up to it rather than rejected. |
| `DefaultFullBackupInterval` | `TimeSpan` | `1 day` | The default for `FullBackupInterval`. |
| `DefaultIncrementalBackupInterval` | `TimeSpan` | `1 hour` | The default for `IncrementalBackupInterval`. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `FullBackupScheduleEnabled` | `bool` | `false` | Whether a recurring full-backup schedule is enabled for the scope. |
| `FullBackupInterval` | `TimeSpan` | `DefaultFullBackupInterval` (1 day) | Cadence between scheduled full backups. Clamped up to `MinimumInterval` when the reminder is registered. |
| `IncrementalBackupScheduleEnabled` | `bool` | `false` | Whether a recurring incremental-backup schedule is enabled for the scope. |
| `IncrementalBackupInterval` | `TimeSpan` | `DefaultIncrementalBackupInterval` (1 hour) | Cadence between scheduled incremental backups. Clamped up to `MinimumInterval` when the reminder is registered. |
| `RetentionEnabled` | `bool` | `false` | Whether backup-chain retention is enabled. When enabled, retention runs after every scheduled capture and can be invoked on demand. |
| `RetentionKeepLast` | `int?` | `null` | Keep at most this many of the most recent backups, or `null` to not bound by count. Must be at least 1 when supplied. |
| `RetentionMaxAge` | `TimeSpan?` | `null` | Retain backups captured within this window, or `null` to not bound by age. Must be strictly positive when supplied. |

### Retention rule semantics

A backup is retained if it satisfies `RetentionKeepLast` **or** `RetentionMaxAge`; only a backup that fails every enabled rule is eligible for pruning. Regardless of either bound, the base chain of a retained increment is always preserved, so a restore chain is never left with a missing ancestor.

## Sink selection

The storage sink is selected through the `ILatticeBackupSink` seam, not a property on these options. `AddLatticeBackup` installs the default in-cluster sink (a reserved dogfooded tree). To use durable external storage, register the [Azure Blob Storage sink](../lattice.backup.azureblob/configuration.md), which replaces the sink registration outright.

## Reserved trees

The catalog and store live in reserved `sys-backup-*` trees that carry the core `sys-` prefix, so they self-register, stay durable and individually auditable, yet are hidden from the default cluster-state tree catalog. An application can validate its own tree ids against this namespace with `LatticeBackupReservedTrees` (see [api.md](api.md)).

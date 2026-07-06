# Backup observability

`Orleans.Lattice.Backup` publishes its telemetry on a single [.NET meter](https://learn.microsoft.com/dotnet/core/diagnostics/metrics) so an OpenTelemetry pipeline can subscribe once and receive every backup metric. Two public static classes own the surface: `BackupMetrics` (the meter and the cross-tree-fence instruments) and `LatticeBackupMetrics` (the capture, restore, retention, scheduler, and inventory instruments plus the tag / phase / reason constants and the emission helpers).

## The meter

Every instrument is published on one meter, named by `BackupMetrics.MeterName`:

```text
orleans.lattice.backup
```

`BackupMetrics.Meter` is the `Meter` instance itself, exposed publicly so an integration test or a custom exporter can subscribe by reference. All durations are reported in milliseconds as `double` and all sizes in bytes, matching the core meter's conventions.

## Tags

| Tag | Constant | Values |
|---|---|---|
| `scope` | `LatticeBackupMetrics.TagScope` | The backup scope key (see `BackupScopeKey`). |
| `phase` | `LatticeBackupMetrics.TagPhase` | The capture / restore phase a failure occurred in. |
| `reason` | `LatticeBackupMetrics.TagReason` | The classified failure reason. |
| `kind` | `LatticeBackupMetrics.TagKind` | The backup kind: `full` or `incremental`. |
| `tree_count` | `BackupMetrics.TagTreeCount` | The participating-tree count of a cross-tree-consistent backup set. |

### Phase values (`LatticeBackupMetrics`)

Capture phases: `PhaseSnapshotOpen` (`snapshot-open`), `PhaseExport` (`export`), `PhaseSinkWrite` (`sink-write`), `PhaseManifestCommit` (`manifest-commit`). Restore phases: `PhaseRead` (`read`), `PhaseVerify` (`verify`), `PhaseMerge` (`merge`).

### Reason values (`LatticeBackupMetrics`)

`ReasonPermissionDenied` (`permission-denied`), `ReasonSaturation` (`saturation`), `ReasonSinkIoError` (`sink-io-error`), `ReasonIntegrityMismatch` (`integrity-mismatch`), `ReasonCancellation` (`cancellation`), `ReasonUnknown` (`unknown`), `ReasonIncrementalFallback` (`incremental-fallback`).

## Instruments

### Space and size

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `Captures` | `orleans.lattice.backup.captures` | Counter | `kind` | Backups whose manifest was committed. |
| `BackupBytes` | `orleans.lattice.backup.bytes` | Histogram (By) | `kind` | Artifact bytes consumed per backup. |
| `BackupArtifacts` | `orleans.lattice.backup.artifacts` | Histogram | `kind` | Content artifacts written per backup. |
| `BackupEntries` | `orleans.lattice.backup.entries` | Histogram | `kind` | Entries captured per backup. |
| `EntriesProcessed` | `orleans.lattice.backup.entries_processed` | Counter | `kind` | Cumulative entries processed by captures. |
| `BytesProcessed` | `orleans.lattice.backup.bytes_processed` | Counter (By) | `kind` | Cumulative bytes processed by captures. |
| `RetentionBytesReclaimed` | `orleans.lattice.backup.retention.bytes_reclaimed` | Counter (By) | `scope` | Artifact bytes reclaimed by retention / deletion. |
| `RetentionPruned` | `orleans.lattice.backup.retention.pruned` | Counter | `scope` | Backups pruned by retention. |

### Throughput and latency

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `CaptureDuration` | `orleans.lattice.backup.capture.duration` | Histogram (ms) | `kind` | Full / incremental capture wall-clock duration. |
| `RestoreDuration` | `orleans.lattice.backup.restore.duration` | Histogram (ms) | - | Restore wall-clock duration. |
| `RestoreEntriesApplied` | `orleans.lattice.backup.restore.entries` | Counter | - | Cumulative entries applied by restores. |
| `IncrementalLagEntries` | `orleans.lattice.backup.incremental.lag_entries` | Histogram | - | Delta entries an incremental capture folded (entries behind the base cut). |
| `IncrementalLagAge` | `orleans.lattice.backup.incremental.lag_age` | Histogram (ms) | - | Age of the base cut an incremental layered on (time behind the live cut). |

### Failures

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `CaptureFailures` | `orleans.lattice.backup.capture.failures` | Counter | `kind`, `phase`, `reason` | Capture failures. |
| `RestoreFailures` | `orleans.lattice.backup.restore.failures` | Counter | `phase`, `reason` | Restore failures. |
| `CaptureRetries` | `orleans.lattice.backup.capture.retries` | Counter | `reason` | Capture retries / fallbacks (for example an incremental falling back to a full). |
| `SchedulerSkipped` | `orleans.lattice.backup.scheduler.skipped` | Counter | `scope` | Capture cycles skipped because one was already in flight for the scope. |
| `SchedulerOverruns` | `orleans.lattice.backup.scheduler.overruns` | Counter | `scope` | Scheduled cycles that fired while a capture was still in flight for the scope. |

### Inventory (observable gauges)

These gauges read from the in-memory inventory registry on scrape without touching storage.

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `InventoryCount` | `orleans.lattice.backup.inventory.count` | Observable gauge | - | Current tracked backup count. |
| `InventoryChainDepth` | `orleans.lattice.backup.inventory.chain_depth_max` | Observable gauge | - | Deepest fully-tracked base-backup chain. |
| `InventoryCatalogBytes` | `orleans.lattice.backup.catalog.bytes` | Observable gauge (By) | - | Cumulative artifact bytes across tracked backups. |
| `InventoryOldestAge` | `orleans.lattice.backup.inventory.oldest_age` | Observable gauge (s) | - | Age in seconds of the oldest tracked backup (0 when none). |
| `InventoryNewestAge` | `orleans.lattice.backup.inventory.newest_age` | Observable gauge (s) | - | Age in seconds of the newest tracked backup (0 when none). |
| `ScopeLastRunStatus` | `orleans.lattice.backup.scope.last_run_status` | Observable gauge | `scope` | Per-scope last-run outcome (0=none, 1=success, 2=failure). |
| `ScopeLastSuccessAge` | `orleans.lattice.backup.scope.last_success_age` | Observable gauge (s) | `scope` | Per-scope seconds since the last successful capture (-1 when never). |

### Cross-tree fence (`BackupMetrics`)

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `CrossTreeFenceSelections` | `orleans.lattice.backup.cross_tree_fence.selections` | Counter | `tree_count` | Cross-tree-consistent backup-set fences selected (one per successful multi-tree cross-tree set capture). |
| `CrossTreeFenceDrainedInFlight` | `orleans.lattice.backup.cross_tree_fence.drained_in_flight` | Counter | - | In-flight cross-tree sagas a fence waited to drain. |
| `CrossTreeFenceRetries` | `orleans.lattice.backup.cross_tree_fence.retries` | Counter | - | Fence retries forced by a cross-tree saga registering during the capture window. |
| `CrossTreeFenceDrainWaitMilliseconds` | `orleans.lattice.backup.cross_tree_fence.drain_wait` | Histogram (ms) | - | Wall-clock time a fence waited for in-flight cross-tree sagas to drain. |

## Emission helpers

`LatticeBackupMetrics` exposes public helpers the engine calls at the emission sites; a host or exporter does not normally call them, but they are part of the public surface:

- `KindTag(BackupKind kind)` - returns the cached `kind` tag for a backup kind.
- `RecordCaptureSuccess(BackupManifest manifest, double durationMs, long byteLength, int artifactCount, int entryCount)` - records the capture success-path instruments and updates the inventory registry.
- `RecordIncrementalLag(long deltaEntries, double baseCutAgeMs)` - records the incremental-lag instruments.
- `RecordRestoreSuccess(double durationMs, long entriesApplied)` - records the restore success-path instruments.
- `RecordRetention(string scopeKey, long bytesReclaimed, int prunedCount)` - records the bytes reclaimed and backups pruned by a retention pass (zero increments are skipped).
- `RecordSchedulerSkipped(string scopeKey)` / `RecordSchedulerOverrun(string scopeKey)` - record the per-scope overlap-guard and overrun tallies.
- `RecordCaptureRetry(string reason)` - records a capture retry / fallback with a classified reason.
- `EmitCaptureFailure(BackupKind kind, string phase, Exception exception)` / `EmitRestoreFailure(string phase, Exception exception)` - record a failure with the phase and a reason classified from the exception, and always return `false` so they can be used as the condition of an exception filter that records the metric without catching the exception.
- `MapReason(Exception exception)` - classifies an exception into a `reason` tag value.

## Zero cost when idle

The failure emitters and success recorders run only on the capture / restore path, and the observable gauges read an in-memory registry on scrape. When no capture, restore, or retention is running and nothing scrapes the meter, the package does no measurement work.

# Backup observability

`Orleans.Lattice.Backup` publishes its telemetry on a single [.NET meter](https://learn.microsoft.com/dotnet/core/diagnostics/metrics) so an OpenTelemetry pipeline can subscribe once and receive every backup metric. Two public static classes own the surface: `BackupMetrics` (the meter and the cross-tree-fence instruments) and `LatticeBackupMetrics` (the capture, restore, retention, and scheduler instruments, the tag / phase / reason constants, and the emission helpers; it also registers the inventory gauges, which are not exposed as public fields).

## The meter

Every instrument is published on one meter, named by `BackupMetrics.MeterName`:

```text
orleans.lattice.backup
```

`BackupMetrics.Meter` is the `Meter` instance itself, exposed publicly so an integration test or a custom exporter can subscribe by reference. Operation durations (capture, restore, incremental lag age, and fence drain wait) are reported in milliseconds as `double`, the inventory age gauges in seconds, and all sizes in bytes, matching the core meter's conventions.

## Tags

| Tag | Constant | Values |
|---|---|---|
| `scope` | `LatticeBackupMetrics.TagScope` | The backup scope key (see `BackupScopeKey`). |
| `phase` | `LatticeBackupMetrics.TagPhase` | The capture / restore phase a failure occurred in. |
| `reason` | `LatticeBackupMetrics.TagReason` | The classified failure reason. |
| `kind` | `LatticeBackupMetrics.TagKind` | The backup kind: `full` or `incremental`. |
| `tree_count` | `BackupMetrics.TagTreeCount` | The participating-tree count of a cross-tree-consistent backup set. |
| `tenant` | `LatticeTenantLabel.TagTenant` | Always the platform sentinel `_platform_`: a backup scope spans an operator-chosen set of trees, so no backup measurement is attributed to a single tenant. Carried by every instrument below in addition to the tags its row lists. |

### Phase values (`LatticeBackupMetrics`)

Capture phases: `PhaseSnapshotOpen` (`snapshot-open`), `PhaseExport` (`export`), `PhaseSinkWrite` (`sink-write`), `PhaseManifestCommit` (`manifest-commit`). Restore phases: `PhaseRead` (`read`), `PhaseVerify` (`verify`), `PhaseMerge` (`merge`). A full capture streams its artifact straight into the sink during `export`, so its `sink-write` phase covers only the manifest write. An incremental capture also tags a failure to read its base manifest from the sink `read`, and it has no separate `sink-write` phase: its manifest write is tagged `manifest-commit`.

### Reason values (`LatticeBackupMetrics`)

`ReasonPermissionDenied` (`permission-denied`), `ReasonSaturation` (`saturation`), `ReasonSinkIoError` (`sink-io-error`), `ReasonIntegrityMismatch` (`integrity-mismatch`), `ReasonCancellation` (`cancellation`), `ReasonUnknown` (`unknown`), `ReasonIncrementalFallback` (`incremental-fallback`).

The failure counters classify a fault by type with `MapReason`: `LatticeAuthorizationDeniedException` is `permission-denied`; `LatticeSaturatedException`, `LatticeCursorSnapshotExpiredException`, and `LatticeSnapshotReplayBudgetExceededException` are `saturation`; `LatticeRestoreValidationException` is `integrity-mismatch`; an `OperationCanceledException` is `cancellation`; any `IOException` is `sink-io-error`; and everything else is `unknown`. A sink fault that does not derive from `IOException` - an Azure SDK `RequestFailedException`, for example - therefore classifies as `unknown`, as do the tenancy refusals `LatticeBackupTenantIsolationException` and `LatticeTenantAccessDeniedException`. The classifier never returns `incremental-fallback`: that reason is recorded only on `capture.retries`, which carries no other.

## Instruments

The Kind column carries each instrument's declared unit in parentheses: `By`, `ms`, `s`, or a `{...}` annotation unit.

### Space and size

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `Captures` | `orleans.lattice.backup.captures` | Counter (`{backup}`) | `kind` | Backups whose manifest was committed. |
| `BackupBytes` | `orleans.lattice.backup.bytes` | Histogram (By) | `kind` | Artifact bytes consumed per backup. |
| `BackupArtifacts` | `orleans.lattice.backup.artifacts` | Histogram (`{artifact}`) | `kind` | Content artifacts written per backup. |
| `BackupEntries` | `orleans.lattice.backup.entries` | Histogram (`{entry}`) | `kind` | Entries captured per backup. |
| `EntriesProcessed` | `orleans.lattice.backup.entries_processed` | Counter (`{entry}`) | `kind` | Cumulative entries processed by captures. |
| `BytesProcessed` | `orleans.lattice.backup.bytes_processed` | Counter (By) | `kind` | Cumulative bytes processed by captures. |
| `RetentionBytesReclaimed` | `orleans.lattice.backup.retention.bytes_reclaimed` | Counter (By) | `scope` | Artifact bytes reclaimed by a retention pass (a backup deleted through the control facade is not counted). |
| `RetentionPruned` | `orleans.lattice.backup.retention.pruned` | Counter (`{backup}`) | `scope` | Backups pruned by retention. |

### Throughput and latency

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `CaptureDuration` | `orleans.lattice.backup.capture.duration` | Histogram (ms) | `kind` | Full / incremental capture wall-clock duration. |
| `RestoreDuration` | `orleans.lattice.backup.restore.duration` | Histogram (ms) | - | Restore wall-clock duration. Recorded, like `RestoreEntriesApplied` and `RestoreFailures`, only when `ILatticeBackupRestoreService.RestoreAsync` runs the restore locally - which also covers each member of a local set restore and a cold restore. A restore the coordinated-restore seam takes over (a replicated target) records none of the three, on any cluster. |
| `RestoreEntriesApplied` | `orleans.lattice.backup.restore.entries` | Counter (`{entry}`) | - | Cumulative entries applied by restores. |
| `IncrementalLagEntries` | `orleans.lattice.backup.incremental.lag_entries` | Histogram (`{entry}`) | - | Delta entries an incremental capture folded (entries behind the base cut). |
| `IncrementalLagAge` | `orleans.lattice.backup.incremental.lag_age` | Histogram (ms) | - | Age of the base cut an incremental layered on (time behind the live cut). |

### Failures

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `CaptureFailures` | `orleans.lattice.backup.capture.failures` | Counter (`{failure}`) | `kind`, `phase`, `reason` | Capture failures, tagged with the phase the fault surfaced in. A backup-set fence that cannot stabilise (`LatticeBackupCrossTreeFenceException`) is not recorded here. |
| `RestoreFailures` | `orleans.lattice.backup.restore.failures` | Counter (`{failure}`) | `phase`, `reason` | Restore failures. |
| `CaptureRetries` | `orleans.lattice.backup.capture.retries` | Counter (`{retry}`) | `reason` | Capture retries / fallbacks (for example an incremental falling back to a full). |
| `SchedulerSkipped` | `orleans.lattice.backup.scheduler.skipped` | Counter (`{run}`) | `scope` | Capture cycles skipped because one was already in flight for the scope. |
| `SchedulerOverruns` | `orleans.lattice.backup.scheduler.overruns` | Counter (`{run}`) | `scope` | Scheduled cycles that fired while a capture was still in flight for the scope. Such a cycle is then skipped, so it is counted in `scheduler.skipped` as well. |
| `SchedulerFailures` | `orleans.lattice.backup.scheduler.failures` | Counter (`{run}`) | `scope`, `reason` | Capture cycles run by the per-scope scheduler - scheduled cycles and `ILatticeBackupScheduler` triggers - that faulted, with the reason classified from the fault. A gate denial on a gated host is recorded as `permission-denied`, so a refused scope is a rising series rather than an absence of successes. |

### Inventory (observable gauges)

These gauges read from the in-memory inventory registry on scrape without touching storage. The registry is process-local and not seeded from the catalog: it tracks the backups this silo process has captured, less those retention has pruned, since it started, so it resets on restart and does not reflect a backup deleted through the control facade. For restart-durable counts use `ILatticeBackupControl.GetInventoryAsync`, which is derived from the catalog. The gauges are registered on the meter by `LatticeBackupMetrics` but are not exposed as public fields, so subscribe to them by instrument name.

| Name | Kind | Tags | Meaning |
|---|---|---|---|
| `orleans.lattice.backup.inventory.count` | Observable gauge (`{backup}`) | - | Current tracked backup count. |
| `orleans.lattice.backup.inventory.chain_depth_max` | Observable gauge (`{backup}`) | - | Deepest fully-tracked base-backup chain. |
| `orleans.lattice.backup.catalog.bytes` | Observable gauge (By) | - | Cumulative artifact bytes across tracked backups. |
| `orleans.lattice.backup.inventory.oldest_age` | Observable gauge (s) | - | Age in seconds of the oldest tracked backup (0 when none). |
| `orleans.lattice.backup.inventory.newest_age` | Observable gauge (s) | - | Age in seconds of the newest tracked backup (0 when none). |
| `orleans.lattice.backup.scope.last_run_status` | Observable gauge (`{status}`) | `scope` | Per-scope last-run outcome (0=scheduled with no completed cycle, 1=success, 2=failure, 3=denied). A scope's series appears on this silo when an enabled schedule is registered for it (0 until a cycle completes), when any capture of the scope succeeds (1), or when a cycle the per-scope scheduler runs faults (2) or is denied (3); a failed capture outside the scheduler does not update it. A series is never removed, so a cancelled schedule keeps its last value, and an absent series means this silo has neither scheduled nor captured the scope since it started. |
| `orleans.lattice.backup.scope.last_success_age` | Observable gauge (s) | `scope` | Per-scope seconds since the last successful capture (-1 when never). |

### Cross-tree fence (`BackupMetrics`)

| Instrument | Name | Kind | Tags | Meaning |
|---|---|---|---|---|
| `CrossTreeFenceSelections` | `orleans.lattice.backup.cross_tree_fence.selections` | Counter (`{fence}`) | `tree_count` | Cross-tree-consistent backup-set fences selected (one per successful multi-tree cross-tree set capture). |
| `CrossTreeFenceDrainedInFlight` | `orleans.lattice.backup.cross_tree_fence.drained_in_flight` | Counter (`{saga}`) | - | In-flight cross-tree sagas a fence waited to drain: the peak in-flight count each fence attempt's drain observed, summed across the capture's attempts and added once when the fence is selected (a zero is skipped). |
| `CrossTreeFenceRetries` | `orleans.lattice.backup.cross_tree_fence.retries` | Counter (`{retry}`) | - | Fence attempts discarded because a cross-tree saga registered on the set during the capture window, or was still in flight when the attempt re-observed. Counted for every discarded attempt, including a final one after which the capture fails. |
| `CrossTreeFenceDrainWaitMilliseconds` | `orleans.lattice.backup.cross_tree_fence.drain_wait` | Histogram (ms) | - | Wall-clock time a fence waited for in-flight cross-tree sagas to drain, summed across the capture's attempts and recorded once when the fence is selected. |

A fence that cannot stabilise - a drain that times out, or every attempt discarded - records only its discarded attempts on `retries`: no selection, drained-saga, or drain-wait measurement.

## Emission helpers

`LatticeBackupMetrics` exposes public helpers the engine calls at the emission sites; a host or exporter does not normally call them, but they are part of the public surface:

- `KindTag(BackupKind kind)` - returns the cached `kind` tag for a backup kind.
- `RecordCaptureSuccess(BackupManifest manifest, double durationMs, long byteLength, int artifactCount, int entryCount)` - records the capture success-path instruments and updates the inventory registry.
- `RecordIncrementalLag(long deltaEntries, double baseCutAgeMs)` - records the incremental-lag instruments.
- `RecordRestoreSuccess(double durationMs, long entriesApplied)` - records the restore success-path instruments.
- `RecordRetention(string scopeKey, long bytesReclaimed, int prunedCount)` - records the bytes reclaimed and backups pruned by a retention pass (zero increments are skipped).
- `RecordSchedulerSkipped(string scopeKey)` / `RecordSchedulerOverrun(string scopeKey)` - record the per-scope overlap-guard and overrun tallies.
- `RecordSchedulerFailure(string scopeKey, string reason)` - records a faulted capture cycle for the scope with a classified reason (a `Reason*` constant).
- `RecordCaptureRetry(string reason)` - records a capture retry / fallback with a classified reason.
- `EmitCaptureFailure(BackupKind kind, string phase, Exception exception)` / `EmitRestoreFailure(string phase, Exception exception)` - record a failure with the phase and a reason classified from the exception, and always return `false` so they can be used as the condition of an exception filter that records the metric without catching the exception.
- `MapReason(Exception exception)` - classifies an exception into a `reason` tag value.

## Zero cost when idle

The failure emitters and success recorders run only on the capture / restore path, and the observable gauges read an in-memory registry on scrape. When no capture, restore, or retention is running and nothing scrapes the meter, the package does no measurement work.

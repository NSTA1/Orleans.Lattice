using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Backup;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the
/// <c>Orleans.Lattice.Backup</c> package, published on the package's dedicated
/// <see cref="BackupMetrics.Meter"/> (named <c>orleans.lattice.backup</c>)
/// alongside the cross-tree-fence instruments, so an OpenTelemetry pipeline
/// subscribes once to the backup meter and receives every backup metric. Every
/// instrument name is prefixed <c>orleans.lattice.backup.*</c> to match the
/// meter, and (mirroring the core meter's convention) all durations are reported
/// in <em>milliseconds</em> as <c>double</c> and all sizes in bytes.
/// </summary>
/// <remarks>
/// Instruments fall into four families:
/// <list type="bullet">
///   <item><b>Space / size</b> - per-backup byte, artifact and entry sizes,
///   cumulative processed counters, cumulative catalog bytes (gauge), and bytes
///   reclaimed by retention.</item>
///   <item><b>Throughput / latency</b> - full-capture, incremental-capture and
///   restore duration histograms, cumulative entry / byte processed counters, and
///   incremental lag (entries and age behind the base cut).</item>
///   <item><b>Failures</b> - capture- and restore-failure counters tagged by
///   <see cref="TagPhase"/> and <see cref="TagReason"/>, a retry / fallback
///   counter, and scheduler skipped-run and overrun counters.</item>
///   <item><b>Inventory</b> - observable gauges over the in-memory
///   <see cref="BackupInventoryRegistry"/>: current backup count, max chain depth,
///   oldest / newest age, catalog bytes, and per-scope last-run status and
///   last-success age.</item>
/// </list>
/// </remarks>
public static class LatticeBackupMetrics
{
    /// <summary>Tag key for the backup scope key (see <see cref="BackupScopeKey"/>).</summary>
    public const string TagScope = "scope";

    /// <summary>Tag key for the operation phase a capture / restore failed in.</summary>
    public const string TagPhase = "phase";

    /// <summary>Tag key for the classified reason a capture / restore failed.</summary>
    public const string TagReason = "reason";

    /// <summary>Tag key for the backup kind (<c>full</c> or <c>incremental</c>).</summary>
    public const string TagKind = "kind";

    // --- Phase values ---------------------------------------------------------

    /// <summary><see cref="TagPhase"/> value: opening the point-in-time snapshot cursor (capture).</summary>
    public const string PhaseSnapshotOpen = "snapshot-open";

    /// <summary><see cref="TagPhase"/> value: streaming captured entries out of the source (capture).</summary>
    public const string PhaseExport = "export";

    /// <summary><see cref="TagPhase"/> value: writing an artifact or manifest to the sink (capture).</summary>
    public const string PhaseSinkWrite = "sink-write";

    /// <summary><see cref="TagPhase"/> value: committing the manifest to the catalog (capture).</summary>
    public const string PhaseManifestCommit = "manifest-commit";

    /// <summary><see cref="TagPhase"/> value: reading the manifest chain / artifacts (restore).</summary>
    public const string PhaseRead = "read";

    /// <summary><see cref="TagPhase"/> value: merging / bulk-loading entries into the target (restore).</summary>
    public const string PhaseMerge = "merge";

    /// <summary><see cref="TagPhase"/> value: verifying artifact integrity before applying (restore).</summary>
    public const string PhaseVerify = "verify";

    // --- Reason values --------------------------------------------------------

    /// <summary><see cref="TagReason"/> value: the caller lacked the backup / restore capability.</summary>
    public const string ReasonPermissionDenied = "permission-denied";

    /// <summary><see cref="TagReason"/> value: the source shed the request under saturation or exceeded a budget.</summary>
    public const string ReasonSaturation = "saturation";

    /// <summary><see cref="TagReason"/> value: an I/O error reading from or writing to the sink.</summary>
    public const string ReasonSinkIoError = "sink-io-error";

    /// <summary><see cref="TagReason"/> value: an artifact failed its content-digest integrity check.</summary>
    public const string ReasonIntegrityMismatch = "integrity-mismatch";

    /// <summary><see cref="TagReason"/> value: the operation was cancelled.</summary>
    public const string ReasonCancellation = "cancellation";

    /// <summary><see cref="TagReason"/> value: an unclassified fault.</summary>
    public const string ReasonUnknown = "unknown";

    /// <summary><see cref="TagReason"/> value: an incremental capture fell back to a full capture.</summary>
    public const string ReasonIncrementalFallback = "incremental-fallback";

    private static readonly KeyValuePair<string, object?> KindFullTag = new(TagKind, "full");
    private static readonly KeyValuePair<string, object?> KindIncrementalTag = new(TagKind, "incremental");

    // --- Space / size ---------------------------------------------------------

    /// <summary>Counter of backups whose manifest was committed, tagged with <see cref="TagKind"/>.</summary>
    public static readonly Counter<long> Captures =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.captures", unit: "{backup}",
            description: "Backups captured (manifest committed), tagged by kind.");

    /// <summary>Histogram of the total artifact bytes consumed per backup, tagged with <see cref="TagKind"/>.</summary>
    public static readonly Histogram<long> BackupBytes =
        BackupMetrics.Meter.CreateHistogram<long>("orleans.lattice.backup.bytes", unit: "By",
            description: "Artifact bytes consumed per backup, tagged by kind.");

    /// <summary>Histogram of the content-artifact count per backup, tagged with <see cref="TagKind"/>.</summary>
    public static readonly Histogram<long> BackupArtifacts =
        BackupMetrics.Meter.CreateHistogram<long>("orleans.lattice.backup.artifacts", unit: "{artifact}",
            description: "Content artifacts written per backup, tagged by kind.");

    /// <summary>Histogram of the entries captured per backup, tagged with <see cref="TagKind"/>.</summary>
    public static readonly Histogram<long> BackupEntries =
        BackupMetrics.Meter.CreateHistogram<long>("orleans.lattice.backup.entries", unit: "{entry}",
            description: "Entries captured per backup, tagged by kind.");

    /// <summary>Cumulative counter of entries processed by captures (so a rate is derivable), tagged with <see cref="TagKind"/>.</summary>
    public static readonly Counter<long> EntriesProcessed =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.entries_processed", unit: "{entry}",
            description: "Entries processed by capture operations, tagged by kind.");

    /// <summary>Cumulative counter of bytes processed by captures (so a rate is derivable), tagged with <see cref="TagKind"/>.</summary>
    public static readonly Counter<long> BytesProcessed =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.bytes_processed", unit: "By",
            description: "Bytes processed by capture operations, tagged by kind.");

    /// <summary>Counter of bytes reclaimed by retention / deletion, tagged with <see cref="TagScope"/>.</summary>
    public static readonly Counter<long> RetentionBytesReclaimed =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.retention.bytes_reclaimed", unit: "By",
            description: "Artifact bytes reclaimed by retention / deletion, tagged by scope.");

    /// <summary>Counter of backups pruned by retention, tagged with <see cref="TagScope"/>.</summary>
    public static readonly Counter<long> RetentionPruned =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.retention.pruned", unit: "{backup}",
            description: "Backups pruned by retention, tagged by scope.");

    // --- Throughput / latency -------------------------------------------------

    /// <summary>Histogram of capture durations in milliseconds, tagged with <see cref="TagKind"/>.</summary>
    public static readonly Histogram<double> CaptureDuration =
        BackupMetrics.Meter.CreateHistogram<double>("orleans.lattice.backup.capture.duration", unit: "ms",
            description: "Full / incremental capture wall-clock duration, tagged by kind.");

    /// <summary>Histogram of restore durations in milliseconds.</summary>
    public static readonly Histogram<double> RestoreDuration =
        BackupMetrics.Meter.CreateHistogram<double>("orleans.lattice.backup.restore.duration", unit: "ms",
            description: "Restore wall-clock duration.");

    /// <summary>Cumulative counter of entries applied by restores (so a rate is derivable).</summary>
    public static readonly Counter<long> RestoreEntriesApplied =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.restore.entries", unit: "{entry}",
            description: "Entries applied by restore operations.");

    /// <summary>Histogram of the entry count an incremental capture folded behind the base cut.</summary>
    public static readonly Histogram<long> IncrementalLagEntries =
        BackupMetrics.Meter.CreateHistogram<long>("orleans.lattice.backup.incremental.lag_entries", unit: "{entry}",
            description: "Delta entries an incremental capture folded (entries behind the base cut).");

    /// <summary>Histogram of the age, in milliseconds, of the base cut an incremental capture layered on.</summary>
    public static readonly Histogram<double> IncrementalLagAge =
        BackupMetrics.Meter.CreateHistogram<double>("orleans.lattice.backup.incremental.lag_age", unit: "ms",
            description: "Age of the base cut an incremental capture layered on (time behind the live cut).");

    // --- Failures -------------------------------------------------------------

    /// <summary>Counter of capture failures, tagged with <see cref="TagKind"/>, <see cref="TagPhase"/> and <see cref="TagReason"/>.</summary>
    public static readonly Counter<long> CaptureFailures =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.capture.failures", unit: "{failure}",
            description: "Capture failures, tagged by kind, phase and reason.");

    /// <summary>Counter of restore failures, tagged with <see cref="TagPhase"/> and <see cref="TagReason"/>.</summary>
    public static readonly Counter<long> RestoreFailures =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.restore.failures", unit: "{failure}",
            description: "Restore failures, tagged by phase and reason.");

    /// <summary>Counter of capture retries / fallbacks, tagged with <see cref="TagReason"/>.</summary>
    public static readonly Counter<long> CaptureRetries =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.capture.retries", unit: "{retry}",
            description: "Capture retries / fallbacks (e.g. an incremental falling back to a full), tagged by reason.");

    /// <summary>Counter of scheduled or on-demand cycles skipped by the per-scope overlap guard, tagged with <see cref="TagScope"/>.</summary>
    public static readonly Counter<long> SchedulerSkipped =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.scheduler.skipped", unit: "{run}",
            description: "Capture cycles skipped because one was already in flight for the scope, tagged by scope.");

    /// <summary>Counter of scheduled cycles that fired while one was still in flight, tagged with <see cref="TagScope"/>.</summary>
    public static readonly Counter<long> SchedulerOverruns =
        BackupMetrics.Meter.CreateCounter<long>("orleans.lattice.backup.scheduler.overruns", unit: "{run}",
            description: "Scheduled cycles that fired while a capture was still in flight for the scope, tagged by scope.");

    // --- Inventory (observable gauges) ---------------------------------------

    private static readonly BackupInventoryRegistry Registry = BackupInventoryRegistry.Instance;

#pragma warning disable IDE0052 // Held to keep the observable gauges registered on the meter for the process lifetime.
    private static readonly ObservableGauge<long> InventoryCount =
        BackupMetrics.Meter.CreateObservableGauge("orleans.lattice.backup.inventory.count",
            static () => LatticeTenantLabel.PlatformMeasurement(Registry.Snapshot().Count), unit: "{backup}",
            description: "Current tracked backup count.");

    private static readonly ObservableGauge<long> InventoryChainDepth =
        BackupMetrics.Meter.CreateObservableGauge("orleans.lattice.backup.inventory.chain_depth_max",
            static () => LatticeTenantLabel.PlatformMeasurement((long)Registry.Snapshot().MaxChainDepth), unit: "{backup}",
            description: "Deepest fully-tracked base-backup chain.");

    private static readonly ObservableGauge<long> InventoryCatalogBytes =
        BackupMetrics.Meter.CreateObservableGauge("orleans.lattice.backup.catalog.bytes",
            static () => LatticeTenantLabel.PlatformMeasurement(Registry.Snapshot().TotalBytes), unit: "By",
            description: "Cumulative artifact bytes across tracked backups.");

    private static readonly ObservableGauge<double> InventoryOldestAge =
        BackupMetrics.Meter.CreateObservableGauge("orleans.lattice.backup.inventory.oldest_age",
            static () => LatticeTenantLabel.PlatformMeasurement(AgeSeconds(Registry.Snapshot().OldestCreatedAtUtc)), unit: "s",
            description: "Age in seconds of the oldest tracked backup (0 when none).");

    private static readonly ObservableGauge<double> InventoryNewestAge =
        BackupMetrics.Meter.CreateObservableGauge("orleans.lattice.backup.inventory.newest_age",
            static () => LatticeTenantLabel.PlatformMeasurement(AgeSeconds(Registry.Snapshot().NewestCreatedAtUtc)), unit: "s",
            description: "Age in seconds of the newest tracked backup (0 when none).");

    private static readonly ObservableGauge<long> ScopeLastRunStatus =
        BackupMetrics.Meter.CreateObservableGauge("orleans.lattice.backup.scope.last_run_status",
            ObserveScopeLastRunStatus, unit: "{status}",
            description: "Per-scope last-run outcome (0=none, 1=success, 2=failure), tagged by scope.");

    private static readonly ObservableGauge<double> ScopeLastSuccessAge =
        BackupMetrics.Meter.CreateObservableGauge("orleans.lattice.backup.scope.last_success_age",
            ObserveScopeLastSuccessAge, unit: "s",
            description: "Per-scope seconds since the last successful capture (-1 when never), tagged by scope.");
#pragma warning restore IDE0052

    private static double AgeSeconds(DateTimeOffset? whenUtc) =>
        whenUtc is { } when ? Math.Max(0d, (DateTimeOffset.UtcNow - when).TotalSeconds) : 0d;

    private static IEnumerable<Measurement<long>> ObserveScopeLastRunStatus()
    {
        foreach (var pair in Registry.EnumerateScopes())
        {
            yield return new Measurement<long>(
                (long)pair.Value.LastRunOutcome,
                new KeyValuePair<string, object?>(TagScope, pair.Key),
                LatticeTenantLabel.Platform);
        }
    }

    private static IEnumerable<Measurement<double>> ObserveScopeLastSuccessAge()
    {
        foreach (var pair in Registry.EnumerateScopes())
        {
            var age = pair.Value.LastSuccessUtc is { } when
                ? Math.Max(0d, (DateTimeOffset.UtcNow - when).TotalSeconds)
                : -1d;
            yield return new Measurement<double>(
                age,
                new KeyValuePair<string, object?>(TagScope, pair.Key),
                LatticeTenantLabel.Platform);
        }
    }

    // --- Emission helpers -----------------------------------------------------

    /// <summary>Returns the cached <see cref="TagKind"/> tag for a backup kind.</summary>
    /// <param name="kind">The backup kind.</param>
    /// <returns>The corresponding cached kind tag.</returns>
    public static KeyValuePair<string, object?> KindTag(BackupKind kind) =>
        kind == BackupKind.Incremental ? KindIncrementalTag : KindFullTag;

    /// <summary>Records the success-path instruments for one captured backup and updates the inventory registry.</summary>
    /// <param name="manifest">The committed manifest. Must not be <c>null</c>.</param>
    /// <param name="durationMs">The capture wall-clock duration in milliseconds.</param>
    /// <param name="byteLength">The artifact bytes consumed.</param>
    /// <param name="artifactCount">The number of content artifacts written.</param>
    /// <param name="entryCount">The number of entries captured.</param>
    public static void RecordCaptureSuccess(
        BackupManifest manifest, double durationMs, long byteLength, int artifactCount, int entryCount)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        var kindTag = KindTag(manifest.Kind);
        // A backup scope spans an operator-chosen set of trees, so a capture is
        // not attributable to a single tenant: every backup instrument carries
        // the reserved platform sentinel as its derived tenant dimension.
        var tenantTag = LatticeTenantLabel.Platform;
        Captures.Add(1, kindTag, tenantTag);
        CaptureDuration.Record(durationMs, kindTag, tenantTag);
        BackupBytes.Record(byteLength, kindTag, tenantTag);
        BackupArtifacts.Record(artifactCount, kindTag, tenantTag);
        BackupEntries.Record(entryCount, kindTag, tenantTag);
        EntriesProcessed.Add(entryCount, kindTag, tenantTag);
        BytesProcessed.Add(byteLength, kindTag, tenantTag);
        BackupInventoryRegistry.Instance.RecordCaptureSuccess(manifest);
    }

    /// <summary>Records the incremental-lag instruments for one incremental capture.</summary>
    /// <param name="deltaEntries">The delta entry count folded (entries behind the base cut).</param>
    /// <param name="baseCutAgeMs">The age of the base cut in milliseconds.</param>
    public static void RecordIncrementalLag(long deltaEntries, double baseCutAgeMs)
    {
        IncrementalLagEntries.Record(deltaEntries, LatticeTenantLabel.Platform);
        IncrementalLagAge.Record(baseCutAgeMs, LatticeTenantLabel.Platform);
    }

    /// <summary>Records the success-path instruments for one restore.</summary>
    /// <param name="durationMs">The restore wall-clock duration in milliseconds.</param>
    /// <param name="entriesApplied">The number of entries applied.</param>
    public static void RecordRestoreSuccess(double durationMs, long entriesApplied)
    {
        RestoreDuration.Record(durationMs, LatticeTenantLabel.Platform);
        RestoreEntriesApplied.Add(entriesApplied, LatticeTenantLabel.Platform);
    }

    /// <summary>Records the bytes reclaimed and backups pruned by a retention pass for a scope.</summary>
    /// <param name="scopeKey">The scope key. Must not be <c>null</c> or empty.</param>
    /// <param name="bytesReclaimed">The bytes reclaimed. Zero increments are skipped.</param>
    /// <param name="prunedCount">The number of backups pruned. Zero increments are skipped.</param>
    public static void RecordRetention(string scopeKey, long bytesReclaimed, int prunedCount)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        var scopeTag = new KeyValuePair<string, object?>(TagScope, scopeKey);
        if (bytesReclaimed > 0)
        {
            RetentionBytesReclaimed.Add(bytesReclaimed, scopeTag, LatticeTenantLabel.Platform);
        }

        if (prunedCount > 0)
        {
            RetentionPruned.Add(prunedCount, scopeTag, LatticeTenantLabel.Platform);
        }
    }

    /// <summary>Records that the per-scope overlap guard skipped a capture cycle.</summary>
    /// <param name="scopeKey">The scope key. Must not be <c>null</c> or empty.</param>
    public static void RecordSchedulerSkipped(string scopeKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        SchedulerSkipped.Add(1, new KeyValuePair<string, object?>(TagScope, scopeKey), LatticeTenantLabel.Platform);
    }

    /// <summary>Records that a scheduled cycle fired while a capture was still in flight.</summary>
    /// <param name="scopeKey">The scope key. Must not be <c>null</c> or empty.</param>
    public static void RecordSchedulerOverrun(string scopeKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        SchedulerOverruns.Add(1, new KeyValuePair<string, object?>(TagScope, scopeKey), LatticeTenantLabel.Platform);
    }

    /// <summary>Records a capture retry / fallback with a classified reason.</summary>
    /// <param name="reason">The retry reason (a <c>Reason*</c> constant).</param>
    public static void RecordCaptureRetry(string reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(reason);
        CaptureRetries.Add(1, new KeyValuePair<string, object?>(TagReason, reason), LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Records a capture failure with the phase and a reason classified from
    /// <paramref name="exception"/>, and bumps the aggregate registry tally.
    /// Always returns <see langword="false"/> so it can be used as the condition
    /// of an exception filter that records the metric without catching the
    /// exception (preserving the original stack).
    /// </summary>
    /// <param name="kind">The backup kind being captured.</param>
    /// <param name="phase">The phase the capture failed in (a <c>Phase*</c> constant).</param>
    /// <param name="exception">The fault.</param>
    /// <returns><see langword="false"/> always.</returns>
    public static bool EmitCaptureFailure(BackupKind kind, string phase, Exception exception)
    {
        CaptureFailures.Add(
            1,
            new System.Diagnostics.TagList
            {
                KindTag(kind),
                new KeyValuePair<string, object?>(TagPhase, phase),
                new KeyValuePair<string, object?>(TagReason, MapReason(exception)),
                LatticeTenantLabel.Platform,
            });
        BackupInventoryRegistry.Instance.IncrementCaptureFailures();
        return false;
    }

    /// <summary>
    /// Records a restore failure with the phase and a reason classified from
    /// <paramref name="exception"/>, and bumps the aggregate registry tally.
    /// Always returns <see langword="false"/> so it can be used as an exception
    /// filter that records the metric without catching the exception.
    /// </summary>
    /// <param name="phase">The phase the restore failed in (a <c>Phase*</c> constant).</param>
    /// <param name="exception">The fault.</param>
    /// <returns><see langword="false"/> always.</returns>
    public static bool EmitRestoreFailure(string phase, Exception exception)
    {
        RestoreFailures.Add(1,
            new KeyValuePair<string, object?>(TagPhase, phase),
            new KeyValuePair<string, object?>(TagReason, MapReason(exception)),
            LatticeTenantLabel.Platform);
        BackupInventoryRegistry.Instance.IncrementRestoreFailures();
        return false;
    }

    /// <summary>Classifies an exception into a <c>Reason*</c> tag value.</summary>
    /// <param name="exception">The fault to classify.</param>
    /// <returns>The classified reason tag value.</returns>
    public static string MapReason(Exception exception) => exception switch
    {
        LatticeAuthorizationDeniedException => ReasonPermissionDenied,
        LatticeSaturatedException => ReasonSaturation,
        LatticeCursorSnapshotExpiredException => ReasonSaturation,
        LatticeSnapshotReplayBudgetExceededException => ReasonSaturation,
        LatticeRestoreValidationException => ReasonIntegrityMismatch,
        OperationCanceledException => ReasonCancellation,
        IOException => ReasonSinkIoError,
        _ => ReasonUnknown,
    };
}

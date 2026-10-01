using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Runs backup and restore as tracked long-running operations: the backup
/// engine's client of the shared <see cref="LatticeOperationRunner"/>. Each start
/// declares the kind's phases and maps the engine's typed result onto the
/// operation's result map; the engine reports progress through the ambient
/// <see cref="LatticeOperationProgress.Current"/> sink.
/// </summary>
/// <remarks>
/// Callers authorize before starting: this service starts exactly the work it is
/// given. The returned <see cref="LatticeOperationLaunch{TResult}.Completion"/>
/// carries the engine's own result and exception, which is what lets the
/// deprecated blocking verbs wrap a start without changing their behaviour.
/// </remarks>
internal sealed class BackupOperationService(
    LatticeOperationRunner runner,
    ILatticeBackupCaptureService capture,
    ILatticeBackupIncrementalCaptureService incremental,
    ILatticeBackupRestoreService restore,
    ILatticeBackupColdRestoreService coldRestore,
    ILatticeBackupHealthService health,
    ILatticeBackupHealthStore healthStore,
    ILatticeBackupCatalogRebuildService catalogRebuild,
    ILatticeBackupCatalogScrubService catalogScrub)
{
    /// <summary>The phases a full or incremental capture reports.</summary>
    internal static readonly IReadOnlyList<string> CapturePhases =
        [BackupOperationPhases.Capturing, BackupOperationPhases.Cataloguing];

    /// <summary>The phases a set capture reports.</summary>
    internal static readonly IReadOnlyList<string> SetCapturePhases =
        [BackupOperationPhases.CapturingMembers];

    /// <summary>The phases a restore reports.</summary>
    internal static readonly IReadOnlyList<string> RestorePhases =
        [BackupOperationPhases.Validating, BackupOperationPhases.Applying, BackupOperationPhases.Replaying];

    /// <summary>The phases a cold restore reports.</summary>
    internal static readonly IReadOnlyList<string> ColdRestorePhases =
    [
        BackupOperationPhases.Bootstrapping,
        BackupOperationPhases.Validating,
        BackupOperationPhases.Applying,
        BackupOperationPhases.Replaying,
        BackupOperationPhases.Cataloguing,
    ];

    /// <summary>The phases a health check reports.</summary>
    internal static readonly IReadOnlyList<string> HealthCheckPhases = [BackupOperationPhases.Verifying];

    /// <summary>The phases a catalog rebuild reports.</summary>
    internal static readonly IReadOnlyList<string> CatalogRebuildPhases = [BackupOperationPhases.RebuildingCatalog];

    /// <summary>The phases a catalog scrub reports; a scrub that does not prune never enters the second.</summary>
    internal static readonly IReadOnlyList<string> CatalogScrubPhases =
        [BackupOperationPhases.ScrubbingCatalog, BackupOperationPhases.PruningOrphans];

    /// <summary>The shared runner, for status, list and cancel.</summary>
    internal LatticeOperationRunner Runner => runner;

    /// <summary>Starts a full capture.</summary>
    internal Task<LatticeOperationLaunch<LatticeBackupCaptureResult>> StartCaptureAsync(
        string tenantId, string operationId, LatticeBackupCaptureRequest request, IReadOnlyList<BackupScopeSelector> scopes) =>
        runner.StartAsync(
            Start(tenantId, operationId, BackupOperationKinds.Capture, scopes, CapturePhases),
            (_, ct) => capture.CaptureAsync(request, ct),
            static r => LatticeOperationCompletion.Succeeded(r.BackupId, BackupOperationResults.ToResultMap(r)));

    /// <summary>Starts an incremental capture.</summary>
    internal Task<LatticeOperationLaunch<LatticeBackupCaptureResult>> StartIncrementalCaptureAsync(
        string tenantId, string operationId, LatticeBackupIncrementalCaptureRequest request, IReadOnlyList<BackupScopeSelector> scopes) =>
        runner.StartAsync(
            Start(tenantId, operationId, BackupOperationKinds.IncrementalCapture, scopes, CapturePhases),
            (_, ct) => incremental.CaptureIncrementalAsync(request, ct),
            static r => LatticeOperationCompletion.Succeeded(r.BackupId, BackupOperationResults.ToResultMap(r)));

    /// <summary>Starts a backup-set capture.</summary>
    internal Task<LatticeOperationLaunch<LatticeBackupSetCaptureResult>> StartSetCaptureAsync(
        string tenantId, string operationId, LatticeBackupSetCaptureRequest request, IReadOnlyList<BackupScopeSelector> scopes) =>
        runner.StartAsync(
            Start(tenantId, operationId, BackupOperationKinds.SetCapture, scopes, SetCapturePhases),
            (_, ct) => capture.CaptureSetAsync(request, ct),
            static r => LatticeOperationCompletion.Succeeded(r.SetManifest.SetId, BackupOperationResults.ToResultMap(r)));

    /// <summary>Starts a restore.</summary>
    internal Task<LatticeOperationLaunch<LatticeRestoreResult>> StartRestoreAsync(
        string tenantId, string operationId, LatticeRestoreRequest request, IReadOnlyList<BackupScopeSelector> scopes) =>
        runner.StartAsync(
            Start(tenantId, operationId, BackupOperationKinds.Restore, scopes, RestorePhases),
            (_, ct) => restore.RestoreAsync(request, ct),
            static r => LatticeOperationCompletion.Succeeded(r.BackupId, BackupOperationResults.ToResultMap(r)));

    /// <summary>Starts a cold restore.</summary>
    internal Task<LatticeOperationLaunch<LatticeRestoreResult>> StartColdRestoreAsync(
        string tenantId, string operationId, LatticeRestoreRequest request, IReadOnlyList<BackupScopeSelector> scopes) =>
        runner.StartAsync(
            Start(tenantId, operationId, BackupOperationKinds.ColdRestore, scopes, ColdRestorePhases),
            (_, ct) => coldRestore.ColdRestoreAsync(request, ct),
            static r => LatticeOperationCompletion.Succeeded(r.BackupId, BackupOperationResults.ToResultMap(r)));

    /// <summary>
    /// Starts a health check: verifies the backup against the sink and persists the
    /// fresh report as its latest health state, exactly as the blocking verb does.
    /// </summary>
    internal Task<LatticeOperationLaunch<BackupHealthReport>> StartHealthCheckAsync(
        string tenantId, string operationId, string backupId, IReadOnlyList<BackupScopeSelector> scopes) =>
        runner.StartAsync(
            Start(tenantId, operationId, BackupOperationKinds.HealthCheck, scopes, HealthCheckPhases),
            async (_, ct) =>
            {
                var report = await health.VerifyAsync(backupId, ct).ConfigureAwait(false);
                await healthStore.SetReportAsync(report, ct).ConfigureAwait(false);
                return report;
            },
            static r => LatticeOperationCompletion.Succeeded(r.BackupId, BackupOperationResults.ToResultMap(r)));

    /// <summary>Starts a catalog rebuild from the sink.</summary>
    internal Task<LatticeOperationLaunch<BackupCatalogRebuildReport>> StartCatalogRebuildAsync(
        string tenantId, string operationId, IReadOnlyList<BackupScopeSelector> scopes) =>
        runner.StartAsync(
            Start(tenantId, operationId, BackupOperationKinds.CatalogRebuild, scopes, CatalogRebuildPhases),
            (_, ct) => catalogRebuild.RebuildFromSinkAsync(ct),
            static r => LatticeOperationCompletion.Succeeded(null, BackupOperationResults.ToResultMap(r)));

    /// <summary>Starts a catalog scrub against the sink, pruning orphans when asked.</summary>
    internal Task<LatticeOperationLaunch<BackupCatalogScrubReport>> StartCatalogScrubAsync(
        string tenantId, string operationId, bool pruneOrphans, IReadOnlyList<BackupScopeSelector> scopes) =>
        runner.StartAsync(
            Start(tenantId, operationId, BackupOperationKinds.CatalogScrub, scopes, CatalogScrubPhases),
            (_, ct) => catalogScrub.ScrubAsync(pruneOrphans, ct),
            static r => LatticeOperationCompletion.Succeeded(null, BackupOperationResults.ToResultMap(r)));

    private static LatticeOperationStart Start(
        string tenantId,
        string operationId,
        string kind,
        IReadOnlyList<BackupScopeSelector> scopes,
        IReadOnlyList<string> phases) =>
        new()
        {
            TenantId = tenantId,
            OperationId = operationId,
            Kind = kind,
            TreeIds = BackupOperationScopes.TreeIds(scopes),
            Attributes = BackupOperationScopes.ToAttributes(scopes),
            Phases = phases,
        };
}

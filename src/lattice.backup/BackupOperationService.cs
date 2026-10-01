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
    ILatticeBackupColdRestoreService coldRestore)
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

using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// The <see cref="ILatticeBackupOperations"/> half of the scripted facade: a start
/// records the request and registers a running operation whose status a test then
/// moves by hand through <see cref="Statuses"/> (or <see cref="Move"/>), so a page
/// following it can be driven through every phase and terminal state without a
/// cluster. Every call is recorded in <see cref="FakeBackupControl.Calls"/>.
/// </summary>
internal sealed partial class FakeBackupControl : ILatticeBackupOperations
{
    private int _nextOperation;

    /// <summary>The cluster's operations by id; a test may add, replace or remove entries.</summary>
    public Dictionary<string, LatticeOperationStatus> Statuses { get; } = new(StringComparer.Ordinal);

    /// <summary>When set, every start fails with this exception instead of registering an operation.</summary>
    public Exception? StartFault { get; set; }

    /// <summary>When set, every start waits for this task before it registers its operation, so a test can hold a start open.</summary>
    public Task? StartGate { get; set; }

    /// <summary>When set, every status read fails with this exception.</summary>
    public Exception? StatusFault { get; set; }

    /// <summary>When set, the listing fails with this exception.</summary>
    public Exception? ListFault { get; set; }

    /// <summary>How many status reads have been made.</summary>
    public int StatusReads { get; private set; }

    /// <summary>Replaces an operation's status with <paramref name="change"/> applied to it.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="change">The change.</param>
    public void Move(string operationId, Func<LatticeOperationStatus, LatticeOperationStatus> change) =>
        Statuses[operationId] = change(Statuses[operationId]);

    /// <summary>A status for <paramref name="kind"/> over <paramref name="treeIds"/>, running in its first phase.</summary>
    /// <param name="operationId">The id.</param>
    /// <param name="kind">The kind.</param>
    /// <param name="treeIds">The trees.</param>
    /// <returns>The status.</returns>
    public static LatticeOperationStatus Running(string operationId, string kind, params string[] treeIds) => new()
    {
        OperationId = operationId,
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = treeIds },
        State = LatticeOperationState.Queued,
        Phase = "Queued",
        StartedAtUtc = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero),
    };

    /// <summary>Finishes operation <paramref name="operationId"/> as a succeeded capture of <paramref name="backupId"/>.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="backupId">The captured backup id.</param>
    public void SucceedCapture(string operationId, string backupId) =>
        Move(operationId, status => status with
        {
            State = LatticeOperationState.Succeeded,
            Phase = "Completed",
            FinishedAtUtc = status.StartedAtUtc.AddMinutes(1),
            ResultReference = backupId,
            Result = new Dictionary<string, string> { [BackupOperationResultKeys.BackupId] = backupId },
        });

    /// <summary>Finishes operation <paramref name="operationId"/> as a succeeded restore with <paramref name="restore"/>.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="restore">The restore's result.</param>
    public void SucceedRestore(string operationId, LatticeRestoreResult restore)
    {
        var result = new Dictionary<string, string>
        {
            [BackupOperationResultKeys.BackupId] = restore.BackupId,
            [BackupOperationResultKeys.TargetTreeId] = restore.TargetTreeId,
            [BackupOperationResultKeys.Mode] = restore.Mode.ToString(),
            [BackupOperationResultKeys.RestoreOperationId] = restore.OperationId,
            [BackupOperationResultKeys.ManifestChain] = string.Join(',', restore.ManifestChain),
            [BackupOperationResultKeys.EntriesApplied] = restore.EntriesApplied.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [BackupOperationResultKeys.DeadLetteredCrossTenant] = restore.DeadLetteredCrossTenant.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [BackupOperationResultKeys.DeadLetteredOverQuota] = restore.DeadLetteredOverQuota.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        if (restore.ShadowPhysicalTreeId is { } shadow)
        {
            result[BackupOperationResultKeys.ShadowPhysicalTreeId] = shadow;
        }

        if (restore.PreviousPhysicalTreeId is { } previous)
        {
            result[BackupOperationResultKeys.PreviousPhysicalTreeId] = previous;
        }

        Move(operationId, status => status with
        {
            State = LatticeOperationState.Succeeded,
            Phase = "Completed",
            FinishedAtUtc = status.StartedAtUtc.AddMinutes(1),
            ResultReference = restore.BackupId,
            Result = result,
        });
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartBackupAsync(LatticeBackupCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StartBackupAsync), request));
        return StartAsync(operationId, BackupOperationKinds.Capture, request.Scope.TreeId);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StartIncrementalBackupAsync), request));
        return StartAsync(operationId, BackupOperationKinds.IncrementalCapture, request.Scope.TreeId);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartBackupSetAsync(LatticeBackupSetCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StartBackupSetAsync), request));
        return StartAsync(operationId, BackupOperationKinds.SetCapture, [.. request.Scopes.Select(static scope => scope.TreeId)]);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartRestoreAsync(LatticeRestoreRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StartRestoreAsync), request));
        return StartAsync(operationId, BackupOperationKinds.Restore, request.TargetTreeId ?? "captured");
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartColdRestoreAsync(LatticeRestoreRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StartColdRestoreAsync), request));
        return StartAsync(operationId, BackupOperationKinds.ColdRestore, request.TargetTreeId ?? "captured");
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartBackupHealthCheckAsync(string backupId, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StartBackupHealthCheckAsync), backupId));
        return StartAsync(operationId, BackupOperationKinds.HealthCheck, "captured");
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartCatalogRebuildAsync(string? operationId = null, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StartCatalogRebuildAsync), null));
        return StartAsync(operationId, BackupOperationKinds.CatalogRebuild, "sys-backup-catalog");
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartCatalogScrubAsync(bool pruneOrphans = false, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StartCatalogScrubAsync), pruneOrphans));
        return StartAsync(operationId, BackupOperationKinds.CatalogScrub, "sys-backup-catalog");
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        StatusReads++;
        Calls.Add((nameof(GetOperationStatusAsync), operationId));
        return StatusFault is { } fault
            ? Task.FromException<LatticeOperationStatus?>(fault)
            : Task.FromResult(Statuses.GetValueOrDefault(operationId));
    }

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ListOperationsAsync), request));
        if (ListFault is { } fault)
        {
            return Task.FromException<LatticeOperationPage>(fault);
        }

        var page = Statuses.Values
            .OrderByDescending(static status => status.StartedAtUtc)
            .ThenBy(static status => status.OperationId, StringComparer.Ordinal)
            .Take(request.EffectivePageSize)
            .ToList();
        return Task.FromResult(new LatticeOperationPage { Operations = page });
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(CancelOperationAsync), operationId));
        if (!Statuses.TryGetValue(operationId, out var status))
        {
            return Task.FromResult<LatticeOperationStatus?>(null);
        }

        if (!status.IsTerminal)
        {
            status = status with { CancelRequested = true };
            Statuses[operationId] = status;
        }

        return Task.FromResult<LatticeOperationStatus?>(status);
    }

    private async Task<LatticeOperationHandle> StartAsync(string? operationId, string kind, params string[] treeIds)
    {
        if (StartGate is { } gate)
        {
            await gate;
        }

        if (StartFault is { } fault)
        {
            throw fault;
        }

        var id = operationId ?? "op-" + (++_nextOperation).ToString(System.Globalization.CultureInfo.InvariantCulture);
        var status = Running(id, kind, treeIds) with { StartedAtUtc = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero).AddMinutes(_nextOperation) };
        Statuses[id] = status;
        return new LatticeOperationHandle { OperationId = id, Kind = kind, Scope = status.Scope, Created = true };
    }
}

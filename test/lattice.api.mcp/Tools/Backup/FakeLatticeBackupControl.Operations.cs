using System.Globalization;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// The <see cref="ILatticeBackupOperations"/> half of the fake: each start runs the
/// fake capture or restore synchronously and records a succeeded operation, so the
/// start-then-poll tools can be driven deterministically. A start with an id that
/// is already recorded returns it with <c>Created</c> false, as the real facade does.
/// </summary>
internal sealed partial class FakeLatticeBackupControl : ILatticeBackupOperations
{
    private readonly Dictionary<string, LatticeOperationStatus> _operations = new(StringComparer.Ordinal);
    private int _nextOperation;

    /// <summary>Seeds an operation status the fake reports as-is.</summary>
    public void SeedOperation(LatticeOperationStatus status) => _operations[status.OperationId] = status;

    public async Task<LatticeOperationHandle> StartBackupAsync(
        LatticeBackupCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Gate();
        if (TryExisting(operationId, out var existing))
        {
            return existing;
        }

        var result = await CreateBackupAsync(request, cancellationToken);
        return Record(operationId, BackupOperationKinds.Capture, request.Scope.TreeId, result.BackupId,
            new Dictionary<string, string> { [BackupOperationResultKeys.BackupId] = result.BackupId });
    }

    public async Task<LatticeOperationHandle> StartIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Gate();
        if (TryExisting(operationId, out var existing))
        {
            return existing;
        }

        var result = await CreateIncrementalBackupAsync(request, cancellationToken);
        return Record(operationId, BackupOperationKinds.IncrementalCapture, request.Scope.TreeId, result.BackupId,
            new Dictionary<string, string> { [BackupOperationResultKeys.BackupId] = result.BackupId });
    }

    public Task<LatticeOperationHandle> StartBackupSetAsync(
        LatticeBackupSetCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Gate();
        LastSetRequest = request;
        return Task.FromResult(Record(operationId, BackupOperationKinds.SetCapture, request.Scopes[0].TreeId, "set-0",
            new Dictionary<string, string> { [BackupOperationResultKeys.SetId] = "set-0" }));
    }

    /// <summary>The last set-capture request started, for assertions.</summary>
    public LatticeBackupSetCaptureRequest? LastSetRequest { get; private set; }

    /// <summary>The last tracked-operation id a start was given, for assertions.</summary>
    public string? LastOperationId { get; private set; }

    public async Task<LatticeOperationHandle> StartRestoreAsync(
        LatticeRestoreRequest request, string? operationId = null, CancellationToken cancellationToken = default)
    {
        Gate();
        if (TryExisting(operationId, out var existing))
        {
            return existing;
        }

        var restore = await RestoreBackupAsync(request, cancellationToken);
        var result = new Dictionary<string, string>
        {
            [BackupOperationResultKeys.BackupId] = restore.BackupId,
            [BackupOperationResultKeys.TargetTreeId] = restore.TargetTreeId,
            [BackupOperationResultKeys.Mode] = restore.Mode.ToString(),
            [BackupOperationResultKeys.RestoreOperationId] = restore.OperationId,
            [BackupOperationResultKeys.ManifestChain] = string.Join(',', restore.ManifestChain),
            [BackupOperationResultKeys.EntriesApplied] = restore.EntriesApplied.ToString(CultureInfo.InvariantCulture),
        };
        if (restore.ShadowPhysicalTreeId is { } shadow)
        {
            result[BackupOperationResultKeys.ShadowPhysicalTreeId] = shadow;
        }

        if (restore.PreviousPhysicalTreeId is { } previous)
        {
            result[BackupOperationResultKeys.PreviousPhysicalTreeId] = previous;
        }

        return Record(operationId, BackupOperationKinds.Restore, restore.TargetTreeId, restore.BackupId, result);
    }

    public Task<LatticeOperationHandle> StartColdRestoreAsync(
        LatticeRestoreRequest request, string? operationId = null, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        Gate();
        return Task.FromResult(_operations.GetValueOrDefault(operationId));
    }

    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        Gate();
        var ordered = _operations.Values.OrderByDescending(o => o.StartedAtUtc).ThenBy(o => o.OperationId).ToList();
        var skip = request.PageToken is null ? 0 : int.Parse(request.PageToken, CultureInfo.InvariantCulture);
        var page = ordered.Skip(skip).Take(request.EffectivePageSize).ToList();
        var next = skip + page.Count < ordered.Count ? (skip + page.Count).ToString(CultureInfo.InvariantCulture) : null;
        return Task.FromResult(new LatticeOperationPage { Operations = page, NextPageToken = next });
    }

    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        Gate();
        if (!_operations.TryGetValue(operationId, out var status))
        {
            return Task.FromResult<LatticeOperationStatus?>(null);
        }

        if (!status.IsTerminal)
        {
            status = status with { CancelRequested = true };
            _operations[operationId] = status;
        }

        return Task.FromResult<LatticeOperationStatus?>(status);
    }

    private bool TryExisting(string? operationId, out LatticeOperationHandle handle)
    {
        LastOperationId = operationId;
        if (operationId is not null && _operations.TryGetValue(operationId, out var status))
        {
            handle = new LatticeOperationHandle
            {
                OperationId = status.OperationId,
                Kind = status.Kind,
                Scope = status.Scope,
                Created = false,
            };
            return true;
        }

        handle = null!;
        return false;
    }

    private LatticeOperationHandle Record(
        string? operationId, string kind, string treeId, string resultReference, IReadOnlyDictionary<string, string> result)
    {
        LastOperationId = operationId;
        var id = operationId ?? $"op-{_nextOperation++}";
        var scope = new LatticeOperationScope { TenantId = "default", TreeIds = [treeId] };
        _operations[id] = new LatticeOperationStatus
        {
            OperationId = id,
            Kind = kind,
            Scope = scope,
            State = LatticeOperationState.Succeeded,
            Phase = "Completed",
            StartedAtUtc = DateTimeOffset.UnixEpoch.AddMinutes(_nextOperation),
            FinishedAtUtc = DateTimeOffset.UnixEpoch.AddMinutes(_nextOperation),
            ResultReference = resultReference,
            Result = result,
        };
        return new LatticeOperationHandle { OperationId = id, Kind = kind, Scope = scope, Created = true };
    }
}

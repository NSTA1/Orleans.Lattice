using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The operations half of the scripted schema facade: starts record the request,
/// register a cluster status and stamp the tree's remediation report with the
/// operation id so a fresh circuit can resume through the report.
/// </summary>
internal sealed partial class FakeSchemaControl : ILatticeSchemaOperations
{
    private int _nextOperation;

    /// <summary>The cluster's schema operations by id; tests may replace or remove entries.</summary>
    public Dictionary<string, LatticeOperationStatus> OperationStatuses { get; } = new(StringComparer.Ordinal);

    /// <summary>When set, every operation start fails with this exception.</summary>
    public Exception? StartFault { get; set; }

    /// <summary>When set, operation starts wait for this task before registering.</summary>
    public Task? StartGate { get; set; }

    /// <summary>When set, operation status reads fail with this exception.</summary>
    public Exception? OperationStatusFault { get; set; }

    /// <summary>How many operation status reads have been made.</summary>
    public int OperationStatusReads { get; private set; }

    /// <summary>Replaces an operation's status with <paramref name="change"/> applied to it.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="change">The change.</param>
    public void MoveOperation(string operationId, Func<LatticeOperationStatus, LatticeOperationStatus> change) =>
        OperationStatuses[operationId] = change(OperationStatuses[operationId]);

    /// <summary>A running schema operation status for <paramref name="treeId"/>.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="kind">The schema operation kind.</param>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns>The status.</returns>
    public static LatticeOperationStatus RunningOperation(string operationId, string kind, string treeId) => new()
    {
        OperationId = operationId,
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [treeId] },
        State = LatticeOperationState.Running,
        Phase = kind == SchemaOperationKinds.AdvanceAndMigrate ? SchemaOperationPhases.Advance : SchemaOperationPhases.DryRun,
        PhaseIndex = 0,
        PhaseCount = kind == SchemaOperationKinds.AdvanceAndMigrate ? 4 : 3,
        StartedAtUtc = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero),
    };

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartRemediationAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        Record("StartRemediation", treeId);
        LastRemediation = (transform, targetPolicy);
        return StartOperationAsync(operationId, SchemaOperationKinds.Remediation, treeId);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartMigrationAsync(
        string treeId,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        RecordVersion("StartMigration", treeId);
        return StartOperationAsync(operationId, SchemaOperationKinds.Migration, treeId);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartAdvanceAndMigrateAsync(
        string treeId,
        uint newTargetVersion,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        RecordVersion("StartAdvanceAndMigrate", treeId);
        Advance(treeId, newTargetVersion);
        return StartOperationAsync(operationId, SchemaOperationKinds.AdvanceAndMigrate, treeId);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        OperationStatusReads++;
        Calls.Add(nameof(GetOperationStatusAsync) + ":" + operationId);
        return OperationStatusFault is { } fault
            ? Task.FromException<LatticeOperationStatus?>(fault)
            : Task.FromResult(OperationStatuses.GetValueOrDefault(operationId));
    }

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add(nameof(ListOperationsAsync) + ":schema");
        var page = OperationStatuses.Values
            .OrderByDescending(static status => status.StartedAtUtc)
            .ThenBy(static status => status.OperationId, StringComparer.Ordinal)
            .Take(request.EffectivePageSize)
            .ToList();
        return Task.FromResult(new LatticeOperationPage { Operations = page });
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        Calls.Add(nameof(CancelOperationAsync) + ":" + operationId);
        if (!OperationStatuses.TryGetValue(operationId, out var status))
        {
            return Task.FromResult<LatticeOperationStatus?>(null);
        }

        if (!status.IsTerminal && status.Phase != SchemaOperationPhases.Cutover)
        {
            status = status with { CancelRequested = true };
            OperationStatuses[operationId] = status;
        }

        return Task.FromResult<LatticeOperationStatus?>(status);
    }

    private async Task<LatticeOperationHandle> StartOperationAsync(string? operationId, string kind, string treeId)
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
        var status = RunningOperation(id, kind, treeId) with
        {
            StartedAtUtc = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero).AddMinutes(_nextOperation),
        };
        OperationStatuses[id] = status;
        Status[treeId] = LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.DryRun, 0, null, id);
        if (OperationGate is { } operationGate)
        {
            _ = CompleteFromGateAsync(treeId, id, operationGate.Task);
        }

        return new LatticeOperationHandle { OperationId = id, Kind = kind, Scope = status.Scope, Created = true };
    }

    private async Task CompleteFromGateAsync(string treeId, string operationId, Task<LatticeSchemaRemediationReport> task)
    {
        var report = await task;
        Status[treeId] = report;
        if (!OperationStatuses.TryGetValue(operationId, out var current))
        {
            return;
        }

        OperationStatuses[operationId] = current with
        {
            State = report.WasCancelled ? LatticeOperationState.Cancelled
                : report.DidAbort ? LatticeOperationState.Failed
                : LatticeOperationState.Succeeded,
            Phase = report.Phase == LatticeSchemaRemediationPhase.Cancelled ? SchemaOperationPhases.Build : report.Phase.ToString(),
            PhaseIndex = report.WasCancelled ? 1 : current.PhaseCount - 1,
            CompletedUnits = report.ScannedCount,
            TotalUnits = report.ScannedCount,
            UnitName = SchemaOperationPhases.ValuesUnit,
            FinishedAtUtc = current.StartedAtUtc.AddMinutes(1),
            FailureReason = report.DidAbort ? "Stopped at key '" + report.OffendingKey + "': reason " + report.Reason + " Nothing was cut over." : null,
            Result = report.DidAbort
                ? new Dictionary<string, string>
                {
                    [SchemaOperationResultKeys.Outcome] = SchemaOperationResultKeys.Aborted,
                    [SchemaOperationResultKeys.ValuesProcessed] = report.ScannedCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    [SchemaOperationResultKeys.OffendingKey] = report.OffendingKey ?? string.Empty,
                    [SchemaOperationResultKeys.Reason] = report.Reason ?? string.Empty,
                }
                : new Dictionary<string, string>
                {
                    [SchemaOperationResultKeys.Outcome] = report.WasCancelled ? SchemaOperationResultKeys.Cancelled : SchemaOperationResultKeys.Completed,
                    [SchemaOperationResultKeys.ValuesProcessed] = report.ScannedCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
                },
        };
    }
}

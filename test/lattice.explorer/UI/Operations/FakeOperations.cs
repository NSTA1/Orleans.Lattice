using System.Globalization;
using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

/// <summary>
/// A scripted <see cref="ILatticeOperations"/> for one operation kind: a start
/// registers a queued operation whose status a test then moves by hand with
/// <see cref="Move"/>, so a page following it can be driven through every phase
/// and terminal state without a cluster. Every call is recorded in
/// <see cref="Calls"/>.
/// </summary>
/// <param name="kind">The kind every started operation carries.</param>
internal abstract class FakeOperations(string kind) : ILatticeOperations
{
    private static readonly DateTimeOffset Epoch = new(2026, 10, 1, 9, 0, 0, TimeSpan.Zero);
    private int _nextOperation;

    /// <summary>The operations by id; a test may add, replace or remove entries.</summary>
    public Dictionary<string, LatticeOperationStatus> Statuses { get; } = new(StringComparer.Ordinal);

    /// <summary>Every call, as the verb name and its argument.</summary>
    public List<(string Verb, object? Argument)> Calls { get; } = [];

    /// <summary>When set, every start fails with this exception.</summary>
    public Exception? StartFault { get; set; }

    /// <summary>When set, every start waits for this task before it registers its operation.</summary>
    public Task? StartGate { get; set; }

    /// <summary>When set, every cancel fails with this exception.</summary>
    public Exception? CancelFault { get; set; }

    /// <summary>The id of the newest started operation.</summary>
    public string? Latest { get; private set; }

    /// <summary>How many times <paramref name="verb"/> was called.</summary>
    /// <param name="verb">The verb, such as <c>GetOperationStatusAsync</c>.</param>
    /// <returns>The count.</returns>
    public int CountOf(string verb) => Calls.Count(call => call.Verb == verb);

    /// <summary>A queued status of this fake's kind over <paramref name="treeIds"/>.</summary>
    /// <param name="operationId">The id.</param>
    /// <param name="treeIds">The trees.</param>
    /// <returns>The status.</returns>
    public LatticeOperationStatus Queued(string operationId, params string[] treeIds) => new()
    {
        OperationId = operationId,
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = treeIds },
        State = LatticeOperationState.Queued,
        Phase = "Queued",
        StartedAtUtc = Epoch,
    };

    /// <summary>Replaces an operation's status with <paramref name="change"/> applied to it.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="change">The change.</param>
    public void Move(string operationId, Func<LatticeOperationStatus, LatticeOperationStatus> change) =>
        Statuses[operationId] = change(Statuses[operationId]);

    /// <summary>Moves an operation to running in <paramref name="phase"/> with the given progress.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="phase">The phase.</param>
    /// <param name="completed">The units done.</param>
    /// <param name="total">The units in all, or <see langword="null"/>.</param>
    /// <param name="unit">The unit.</param>
    public void Progress(string operationId, string phase, long completed, long? total, string unit) =>
        Move(operationId, status => status with
        {
            State = LatticeOperationState.Running,
            Phase = phase,
            CompletedUnits = completed,
            TotalUnits = total,
            UnitName = unit,
        });

    /// <summary>Finishes an operation as succeeded with <paramref name="result"/>.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="result">The result map.</param>
    public void Succeed(string operationId, IReadOnlyDictionary<string, string> result) =>
        Move(operationId, status => status with
        {
            State = LatticeOperationState.Succeeded,
            Phase = "Completed",
            FinishedAtUtc = status.StartedAtUtc.AddMinutes(1),
            Result = result,
        });

    /// <summary>Finishes an operation as failed with <paramref name="reason"/>.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="reason">The failure reason.</param>
    public void Fail(string operationId, string reason) =>
        Move(operationId, status => status with
        {
            State = LatticeOperationState.Failed,
            Phase = "Failed",
            FinishedAtUtc = status.StartedAtUtc.AddMinutes(1),
            FailureReason = reason,
        });

    /// <summary>Finishes an operation as cancelled.</summary>
    /// <param name="operationId">The operation id.</param>
    public void Cancel(string operationId) =>
        Move(operationId, status => status with
        {
            State = LatticeOperationState.Cancelled,
            Phase = "Cancelled",
            FinishedAtUtc = status.StartedAtUtc.AddMinutes(1),
        });

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(GetOperationStatusAsync), operationId));
        return Task.FromResult(Statuses.GetValueOrDefault(operationId));
    }

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ListOperationsAsync), request));
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
        if (CancelFault is { } fault)
        {
            return Task.FromException<LatticeOperationStatus?>(fault);
        }

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

    /// <summary>Records a start of <paramref name="verb"/> and registers a queued operation over <paramref name="treeIds"/>.</summary>
    /// <param name="verb">The start verb's name.</param>
    /// <param name="operationId">The caller's id, or <see langword="null"/> to generate one.</param>
    /// <param name="treeIds">The trees.</param>
    /// <returns>The handle.</returns>
    protected async Task<LatticeOperationHandle> StartAsync(string verb, string? operationId, params string[] treeIds)
    {
        Calls.Add((verb, treeIds.Length == 1 ? treeIds[0] : null));
        if (StartGate is { } gate)
        {
            await gate;
        }

        if (StartFault is { } fault)
        {
            throw fault;
        }

        var id = operationId ?? "op-" + (++_nextOperation).ToString(CultureInfo.InvariantCulture);
        var status = Queued(id, treeIds) with { StartedAtUtc = Epoch.AddMinutes(_nextOperation) };
        Statuses[id] = status;
        Latest = id;
        return new LatticeOperationHandle { OperationId = id, Kind = kind, Scope = status.Scope, Created = true };
    }
}

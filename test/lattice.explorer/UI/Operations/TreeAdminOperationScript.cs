using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

/// <summary>
/// Scripts the accept-then-poll half of a substitute tree-administration facade
/// (#4124) - the substitute must have been created for both
/// <see cref="ILatticeTreeAdmin"/> and <see cref="ILatticeTreeAdminOperations"/>. Each
/// start records the id the page chose and returns a handle; each status read of an
/// operation returns the next status scripted for its kind (the last one repeats), so
/// a test steps an operation through its phases by advancing the clock. The caller's
/// operation listing is <see cref="Listed"/>, so a test can stand an operation up as
/// already running, as if it had been started in another tab.
/// </summary>
internal sealed class TreeAdminOperationScript
{
    private readonly Dictionary<string, LatticeOperationStatus[]> _scripts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Queue<LatticeOperationStatus>> _pending = new(StringComparer.Ordinal);
    private readonly Dictionary<string, LatticeOperationStatus> _current = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();

    /// <summary>Scripts the operations half of <paramref name="admin"/>.</summary>
    /// <param name="admin">A substitute for both tree-administration interfaces.</param>
    public TreeAdminOperationScript(ILatticeTreeAdmin admin)
    {
        Operations = (ILatticeTreeAdminOperations)admin;
        Operations.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => new LatticeOperationPage { Operations = Listed.ToList() });
        Operations.GetOperationStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Read(call.Arg<string>()));
        Operations.CancelOperationAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Cancel(call.Arg<string>()));
        Operations.StartViewRebuildAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Start(TreeAdminOperationKinds.ViewRebuild, call.ArgAt<string?>(1)));
        Operations.StartViewReconcileAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Start(TreeAdminOperationKinds.ViewReconcile, call.ArgAt<string?>(1)));
        Operations.StartTagIndexReconcileAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Start(TreeAdminOperationKinds.TagIndexReconcile, call.ArgAt<string?>(1)));
        Operations.StartWalMoveAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<TreeWalMoveOptions?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Start(TreeAdminOperationKinds.WalMove, call.ArgAt<string?>(4)));
        Operations.StartOrphanedLeavesAuditAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Start(TreeAdminOperationKinds.OrphanedLeavesAudit, call.ArgAt<string?>(1)));
        Operations.StartOrphanedLeavesRepairAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Start(TreeAdminOperationKinds.OrphanedLeavesRepair, call.ArgAt<string?>(1)));
    }

    /// <summary>The operations facade the page reaches.</summary>
    public ILatticeTreeAdminOperations Operations { get; }

    /// <summary>The caller's operations the listing returns, newest first.</summary>
    public List<LatticeOperationStatus> Listed { get; } = [];

    /// <summary>The ids of the operations the page started, in order.</summary>
    public List<string> Started { get; } = [];

    /// <summary>The ids the page asked to cancel.</summary>
    public List<string> Cancelled { get; } = [];

    /// <summary>A status of <paramref name="kind"/> for the scripts; the id is filled in when it is read.</summary>
    /// <param name="kind">The operation kind.</param>
    /// <param name="state">The state.</param>
    /// <param name="phase">The phase.</param>
    /// <param name="completed">The units completed in the phase.</param>
    /// <param name="total">The phase's total units, or <see langword="null"/>.</param>
    /// <param name="unit">The unit name, or <see langword="null"/>.</param>
    /// <param name="result">The result map of a finished operation.</param>
    /// <param name="failure">The failure reason.</param>
    /// <returns>The status.</returns>
    public static LatticeOperationStatus Status(
        string kind,
        LatticeOperationState state,
        string phase = "Scanning",
        long completed = 0,
        long? total = null,
        string? unit = null,
        IReadOnlyDictionary<string, string>? result = null,
        string? failure = null) => new()
        {
            OperationId = "scripted",
            Kind = kind,
            Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
            State = state,
            Phase = phase,
            PhaseIndex = 0,
            PhaseCount = 2,
            CompletedUnits = completed,
            TotalUnits = total,
            UnitName = unit,
            Result = result ?? new Dictionary<string, string>(),
            FailureReason = failure,
        };

    /// <summary>Scripts the statuses an operation of <paramref name="kind"/> reports, read by read; the last repeats.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="statuses">The statuses.</param>
    public void Script(string kind, params LatticeOperationStatus[] statuses)
    {
        lock (_gate)
        {
            _scripts[kind] = statuses;
        }
    }

    /// <summary>Stands up an operation already running on the cluster, listed and scripted.</summary>
    /// <param name="operationId">Its id.</param>
    /// <param name="statuses">The statuses it reports, read by read; the first is also what the listing shows.</param>
    public void Running(string operationId, params LatticeOperationStatus[] statuses)
    {
        lock (_gate)
        {
            _pending[operationId] = new Queue<LatticeOperationStatus>(statuses.Select(status => status with { OperationId = operationId }));
            Listed.Insert(0, statuses[0] with { OperationId = operationId });
        }
    }

    private LatticeOperationHandle Start(string kind, string? operationId)
    {
        var id = operationId ?? Guid.NewGuid().ToString("N");
        lock (_gate)
        {
            Started.Add(id);
            var script = _scripts.TryGetValue(kind, out var statuses) && statuses.Length > 0
                ? statuses
                : [Status(kind, LatticeOperationState.Running)];
            _pending[id] = new Queue<LatticeOperationStatus>(script.Select(status => status with { OperationId = id }));
        }

        return new LatticeOperationHandle
        {
            OperationId = id,
            Kind = kind,
            Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
            Created = true,
        };
    }

    private LatticeOperationStatus? Read(string operationId)
    {
        lock (_gate)
        {
            if (_pending.TryGetValue(operationId, out var queue) && queue.Count > 0)
            {
                var next = queue.Count > 1 ? queue.Dequeue() : queue.Peek();
                if (_current.TryGetValue(operationId, out var current) && current.CancelRequested)
                {
                    next = next with { CancelRequested = true };
                }

                _current[operationId] = next;
                return next;
            }

            return _current.GetValueOrDefault(operationId);
        }
    }

    private LatticeOperationStatus? Cancel(string operationId)
    {
        lock (_gate)
        {
            Cancelled.Add(operationId);
            if (_current.TryGetValue(operationId, out var current))
            {
                _current[operationId] = current with { CancelRequested = true };
                return _current[operationId];
            }

            return null;
        }
    }
}

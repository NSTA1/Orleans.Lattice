using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// An in-memory stand-in for one coordinated-operation tracking grain, so the
/// tree-administration operations can run on a real <see cref="LatticeOperationRunner"/>
/// without a cluster. Records every progress report and the terminal completion.
/// </summary>
internal sealed class FakeOperationGrain(string key) : ILatticeOperationGrain
{
    private readonly object _sync = new();

    /// <summary>The begin request the runner sent, or <see langword="null"/>.</summary>
    public LatticeOperationBeginRequest? BeginRequest { get; private set; }

    /// <summary>Every progress report written through.</summary>
    public List<LatticeOperationProgressReport> Reports { get; } = [];

    /// <summary>The recorded terminal outcome, or <see langword="null"/> while running.</summary>
    public LatticeOperationCompletion? Completion { get; private set; }

    /// <summary>The current record.</summary>
    public LatticeOperationRecord? Record { get; set; }

    /// <inheritdoc />
    public Task<LatticeOperationBeginResult> BeginAsync(LatticeOperationBeginRequest request)
    {
        var created = Record is null;
        BeginRequest ??= request;
        var (tenant, id) = LatticeOperationKey.Parse(key);
        Record ??= new LatticeOperationRecord
        {
            OperationId = id,
            Kind = request.Kind,
            TenantId = tenant,
            TreeIds = request.TreeIds,
            State = LatticeOperationState.Queued,
            Phase = LatticeOperationPhaseNames.Queued,
        };
        return Task.FromResult(new LatticeOperationBeginResult(created, Record));
    }

    /// <inheritdoc />
    public Task<bool> ReportAsync(LatticeOperationProgressReport report)
    {
        lock (_sync)
        {
            Reports.Add(report);
        }

        return Task.FromResult(Record?.CancelRequested ?? false);
    }

    /// <inheritdoc />
    public Task<bool> HeartbeatAsync() => Task.FromResult(false);

    /// <inheritdoc />
    public Task<LatticeOperationRecord?> CompleteAsync(LatticeOperationCompletion completion)
    {
        Completion ??= completion;
        if (Record is not null)
        {
            Record = Record with
            {
                State = completion.State,
                ResultReference = completion.ResultReference,
                Result = completion.Result,
                FailureReason = completion.FailureReason,
            };
        }

        return Task.FromResult(Record);
    }

    /// <inheritdoc />
    public Task<LatticeOperationRecord?> GetAsync() => Task.FromResult(Record);

    /// <inheritdoc />
    public Task<LatticeOperationRecord?> RequestCancelAsync()
    {
        if (Record is not null)
        {
            Record = Record with { CancelRequested = true };
        }

        return Task.FromResult(Record);
    }
}

using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// An in-memory stand-in for one coordinated-operation tracking grain that a
/// tracked grain call relays to: it records every progress report and can answer
/// a report in a given phase with the stop signal, as the real grain does once a
/// cancellation has been requested.
/// </summary>
internal sealed class RecordingOperationGrain : ILatticeOperationGrain
{
    private readonly object _sync = new();
    private readonly List<LatticeOperationProgressReport> _reports = [];

    /// <summary>When set, a report in this phase is answered with the stop signal.</summary>
    public string? StopOnPhase { get; set; }

    /// <summary>When set, every report is answered with the stop signal.</summary>
    public bool StopAlways { get; set; }

    /// <summary>A snapshot of every report written through, in order.</summary>
    public IReadOnlyList<LatticeOperationProgressReport> Reports
    {
        get
        {
            lock (_sync)
            {
                return _reports.ToArray();
            }
        }
    }

    /// <inheritdoc />
    public Task<LatticeOperationBeginResult> BeginAsync(LatticeOperationBeginRequest request) =>
        throw new NotSupportedException("A relay never begins an operation.");

    /// <inheritdoc />
    public Task<bool> ReportAsync(LatticeOperationProgressReport report)
    {
        lock (_sync)
        {
            _reports.Add(report);
        }

        return Task.FromResult(StopAlways || string.Equals(report.Phase, StopOnPhase, StringComparison.Ordinal));
    }

    /// <inheritdoc />
    public Task<bool> HeartbeatAsync() => Task.FromResult(false);

    /// <inheritdoc />
    public Task<LatticeOperationRecord?> CompleteAsync(LatticeOperationCompletion completion) =>
        Task.FromResult<LatticeOperationRecord?>(null);

    /// <inheritdoc />
    public Task<LatticeOperationRecord?> GetAsync() => Task.FromResult<LatticeOperationRecord?>(null);

    /// <inheritdoc />
    public Task<LatticeOperationRecord?> RequestCancelAsync() => Task.FromResult<LatticeOperationRecord?>(null);
}

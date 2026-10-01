namespace Orleans.Lattice.Operations;

/// <summary>
/// The <see cref="ILatticeOperationProgress"/> a <see cref="LatticeOperationRunner"/>
/// hands to one operation. Reports are <b>coalesced</b> in memory and written
/// through to the tracking grain when the phase, unit or total changes, when the
/// last unit completes, or after roughly one percent of the total (every 1000
/// units while the total is unknown). Because the latest report can sit unwritten,
/// the runner banks it with <see cref="BankProgressAsync"/> on every fault and
/// cancellation path before recording the terminal state.
/// </summary>
internal sealed class LatticeOperationProgressSink : ILatticeOperationProgress
{
    private const long UnknownTotalStep = 1000;

    private readonly ILatticeOperationGrain _grain;
    private readonly CancellationTokenSource _cancellation;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly Lock _sync = new();

    private LatticeOperationProgressReport? _pending;
    private LatticeOperationProgressReport? _written;

    /// <summary>Initializes a new sink.</summary>
    /// <param name="grain">The operation's tracking grain.</param>
    /// <param name="cancellation">Cancelled when the grain reports that the runner must stop.</param>
    public LatticeOperationProgressSink(ILatticeOperationGrain grain, CancellationTokenSource cancellation)
    {
        ArgumentNullException.ThrowIfNull(grain);
        ArgumentNullException.ThrowIfNull(cancellation);
        _grain = grain;
        _cancellation = cancellation;
    }

    /// <inheritdoc />
    public ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(phase);
        _cancellation.Token.ThrowIfCancellationRequested();

        bool writeThrough;
        lock (_sync)
        {
            if (_pending is { } pending
                && string.Equals(pending.Phase, phase, StringComparison.Ordinal)
                && completedUnits < pending.CompletedUnits)
            {
                // A concurrent reporter that observed an older count: never regress.
                completedUnits = pending.CompletedUnits;
            }

            var report = new LatticeOperationProgressReport(phase, completedUnits, totalUnits, unitName);
            _pending = report;
            writeThrough = ShouldWriteThrough(report, _written);
        }

        return writeThrough ? new ValueTask(FlushAsync()) : ValueTask.CompletedTask;
    }

    /// <summary>
    /// Writes the latest coalesced report through to the tracking grain. Called on
    /// every fault and cancellation path so progress made before the fault is never
    /// lost. A failure to bank is swallowed here on purpose: the caller is already
    /// propagating the operation's own fault, which must not be masked.
    /// </summary>
    /// <returns>A task that completes when the bank attempt finishes.</returns>
    public async Task BankProgressAsync()
    {
        try
        {
            await FlushAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Best effort by design; see the summary.
        }
    }

    /// <summary>Writes the latest coalesced report through, when it differs from the last one written.</summary>
    /// <returns>A task that completes when the write finishes.</returns>
    public async Task FlushAsync()
    {
        await _writeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            LatticeOperationProgressReport report;
            lock (_sync)
            {
                if (_pending is not { } pending || pending.Equals(_written))
                {
                    return;
                }

                report = pending;
            }

            var stop = await _grain.ReportAsync(report).ConfigureAwait(false);
            lock (_sync)
            {
                _written = report;
            }

            if (stop)
            {
                _cancellation.Cancel();
            }
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private static bool ShouldWriteThrough(LatticeOperationProgressReport report, LatticeOperationProgressReport? written)
    {
        if (written is not { } last
            || !string.Equals(last.Phase, report.Phase, StringComparison.Ordinal)
            || !string.Equals(last.UnitName, report.UnitName, StringComparison.Ordinal)
            || last.TotalUnits != report.TotalUnits)
        {
            return true;
        }

        if (report.TotalUnits is { } total)
        {
            return report.CompletedUnits >= total
                || report.CompletedUnits - last.CompletedUnits >= Math.Max(1, total / 100);
        }

        return report.CompletedUnits - last.CompletedUnits >= UnknownTotalStep;
    }
}

namespace Orleans.Lattice.Operations;

/// <summary>
/// The grain-side half of a <b>tracked grain call</b>: a progress sink and a
/// cancellation source opened over a <see cref="LatticeOperationTicket"/>, so a
/// grain method that does long work in one call reports progress straight to the
/// operation's tracking grain and stops when the operation is cancelled. While
/// open it is also the ambient <see cref="LatticeOperationProgress.Current"/>, so
/// code nested under the grain method reports without a signature change.
/// </summary>
/// <remarks>
/// <para>
/// Reports are coalesced exactly as the runner's own are (the relay wraps a
/// <see cref="LatticeOperationProgressSink"/>), so the grain method must bank the
/// pending report on every fault and cancellation path before rethrowing, in the
/// house idiom: a private <c>Bank...Async</c> helper awaited from the
/// <c>catch</c>, which calls <see cref="BankProgressAsync"/> and lets the fault
/// propagate. On success it calls <see cref="FlushAsync"/>.
/// </para>
/// <para>
/// Opened over a <see langword="null"/> ticket the relay reports nothing, its
/// <see cref="Progress"/> is <see langword="null"/> and its <see cref="Token"/>
/// follows the call's own token only, so an untracked call pays no grain hop.
/// </para>
/// </remarks>
internal sealed class LatticeOperationRelay : IDisposable
{
    private readonly CancellationTokenSource _cancellation;
    private readonly LatticeOperationProgressSink? _sink;
    private readonly LatticeOperationProgress.Scope _scope;
    private bool _disposed;

    private LatticeOperationRelay(CancellationTokenSource cancellation, LatticeOperationProgressSink? sink)
    {
        _cancellation = cancellation;
        _sink = sink;
        _scope = LatticeOperationProgress.Enter(sink);
    }

    /// <summary>The relayed progress sink, or <see langword="null"/> when the call is untracked.</summary>
    public ILatticeOperationProgress? Progress => _sink;

    /// <summary>
    /// Cancelled when the call's own token is, or when the tracking grain reports
    /// that the operation must stop (its cancellation was requested, or it is no
    /// longer running).
    /// </summary>
    public CancellationToken Token => _cancellation.Token;

    /// <summary>
    /// Opens a relay and makes its sink the ambient
    /// <see cref="LatticeOperationProgress.Current"/> until disposed.
    /// </summary>
    /// <param name="grainFactory">The grain factory. Must not be <c>null</c>.</param>
    /// <param name="ticket">The operation to report to, or <see langword="null"/> for an untracked call.</param>
    /// <param name="cancellationToken">The call's own cancellation token.</param>
    /// <returns>The open relay. Dispose it on the same call flow that opened it.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grainFactory"/> is <c>null</c>.</exception>
    public static LatticeOperationRelay Open(
        IGrainFactory grainFactory,
        LatticeOperationTicket? ticket,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var sink = ticket is null
            ? null
            : new LatticeOperationProgressSink(
                grainFactory.GetGrain<ILatticeOperationGrain>(ticket.OperationKey),
                cancellation);
        return new LatticeOperationRelay(cancellation, sink);
    }

    /// <summary>Writes the latest coalesced report through. Call on the success path.</summary>
    /// <returns>A task that completes when the write finishes.</returns>
    public Task FlushAsync() => _sink?.FlushAsync() ?? Task.CompletedTask;

    /// <summary>
    /// Banks the latest coalesced report on a fault or cancellation path. A failure
    /// to bank is swallowed so it cannot mask the fault the caller is propagating.
    /// </summary>
    /// <returns>A task that completes when the bank attempt finishes.</returns>
    public Task BankProgressAsync() => _sink?.BankProgressAsync() ?? Task.CompletedTask;

    /// <summary>Restores the previous ambient sink and releases the cancellation source.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _scope.Dispose();
        _cancellation.Dispose();
    }
}

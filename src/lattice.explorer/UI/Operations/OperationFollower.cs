using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;

namespace Orleans.Lattice.Explorer.UI.Operations;

/// <summary>
/// Follows one long-running operation's status from the cluster while its page is
/// open (#4122): one read at once, then a read every
/// <see cref="ClusterStatusPoller.Interval"/> on the circuit's clock (backing off
/// while reads fail) until the operation is terminal, disappears, or the page goes
/// away. Kind-agnostic: the page supplies the read, so any area that shows a
/// <see cref="LatticeOperationStatus"/> follows it the same way. Because the status
/// is the cluster's, leaving and returning - even from another tab or after a
/// reload - picks the operation up where it is.
/// </summary>
/// <param name="time">The clock reads are paced on.</param>
internal sealed class OperationFollower(TimeProvider time) : IDisposable
{
    private readonly ClusterStatusPoller _poller = new(time);
    private Func<CancellationToken, Task<LatticeOperationStatus?>>? _read;
    private int _generation;

    /// <summary>Raised when the status, the not-found flag or the last error changes; raised off the renderer.</summary>
    public event Action? Changed;

    /// <summary>The latest status read, or <see langword="null"/> before the first read or when not found.</summary>
    public LatticeOperationStatus? Status { get; private set; }

    /// <summary>Whether the last read found no such operation visible to the caller.</summary>
    public bool NotFound { get; private set; }

    /// <summary>The last read's failure, or <see langword="null"/> after a successful read.</summary>
    public Exception? LastError { get; private set; }

    /// <summary>Whether the follower is still reading.</summary>
    public bool IsFollowing => _poller.IsFollowing;

    /// <summary>
    /// Reads the status once and, while it is not terminal, keeps following it.
    /// Replaces any earlier follow: a read of the earlier follow that answers later
    /// is discarded.
    /// </summary>
    /// <param name="read">Reads the status; <see langword="null"/> means not found.</param>
    /// <param name="cancellationToken">Cancels the first read.</param>
    /// <returns>A task that completes after the first read.</returns>
    public async Task StartAsync(
        Func<CancellationToken, Task<LatticeOperationStatus?>> read,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(read);
        _poller.Stop();
        var generation = Interlocked.Increment(ref _generation);
        _read = read;
        Status = null;
        NotFound = false;
        LastError = null;

        var outcome = await ReadAsync(cancellationToken).ConfigureAwait(false);
        if (outcome != ClusterPollOutcome.Settled && Volatile.Read(ref _generation) == generation)
        {
            _poller.Follow(ReadAsync);
        }
    }

    /// <summary>Reads once more now, outside the polling cadence, for example after asking the operation to cancel.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>A task that completes after the read.</returns>
    public Task RefreshAsync(CancellationToken cancellationToken) =>
        _read is null ? Task.CompletedTask : ReadAsync(cancellationToken);

    /// <summary>Stops following.</summary>
    public void Stop() => _poller.Stop();

    /// <inheritdoc />
    public void Dispose()
    {
        Interlocked.Increment(ref _generation);
        _read = null;
        _poller.Dispose();
    }

    private async Task<ClusterPollOutcome> ReadAsync(CancellationToken cancellationToken)
    {
        var generation = Volatile.Read(ref _generation);
        var read = _read;
        if (read is null)
        {
            return ClusterPollOutcome.Settled;
        }

        LatticeOperationStatus? status;
        Exception? failure = null;
        try
        {
            status = await read(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            status = null;
            failure = exception;
        }

        // A later start, or disposal, superseded this read while it was out: its
        // answer belongs to an operation no longer followed, so it changes nothing.
        if (Volatile.Read(ref _generation) != generation)
        {
            return ClusterPollOutcome.Settled;
        }

        ClusterPollOutcome outcome;
        if (failure is not null)
        {
            LastError = failure;
            outcome = ClusterPollOutcome.Failed;
        }
        else
        {
            Status = status ?? Status;
            NotFound = status is null;
            LastError = null;
            outcome = status is null || status.IsTerminal ? ClusterPollOutcome.Settled : ClusterPollOutcome.Running;
        }

        Changed?.Invoke();
        return outcome;
    }
}

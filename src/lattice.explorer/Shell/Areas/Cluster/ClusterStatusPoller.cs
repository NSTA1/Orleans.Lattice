namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>
/// Follows a long-running operation's status while its page is open: it asks
/// again every <see cref="Interval"/> on the circuit's clock until the refresh
/// reports the operation has finished, the page stops it, or the page goes away.
/// Leaving and coming back resumes it, because the status is the cluster's.
/// </summary>
/// <param name="time">The clock the interval is measured on.</param>
internal sealed class ClusterStatusPoller(TimeProvider time) : IDisposable
{
    /// <summary>How long the poller waits between reads.</summary>
    public static readonly TimeSpan Interval = TimeSpan.FromSeconds(2);

    private CancellationTokenSource? _following;

    /// <summary>Whether the poller is following an operation.</summary>
    public bool IsFollowing => _following is not null;

    /// <summary>
    /// Starts following, replacing any earlier follow. <paramref name="refresh"/>
    /// reads the status once and returns whether the operation is still running.
    /// </summary>
    /// <param name="refresh">Reads the status; <see langword="false"/> ends the follow.</param>
    public void Follow(Func<CancellationToken, Task<bool>> refresh)
    {
        ArgumentNullException.ThrowIfNull(refresh);

        Stop();
        var following = new CancellationTokenSource();
        _following = following;
        _ = FollowAsync(refresh, following);
    }

    /// <summary>Stops following.</summary>
    public void Stop()
    {
        var following = _following;
        _following = null;
        if (following is not null)
        {
            following.Cancel();
            following.Dispose();
        }
    }

    /// <inheritdoc />
    public void Dispose() => Stop();

    private async Task FollowAsync(Func<CancellationToken, Task<bool>> refresh, CancellationTokenSource following)
    {
        var token = following.Token;
        try
        {
            while (true)
            {
                await Task.Delay(Interval, time, token).ConfigureAwait(false);
                if (!await refresh(token).ConfigureAwait(false))
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return;
        }

        if (ReferenceEquals(_following, following))
        {
            _following = null;
            following.Dispose();
        }
    }
}

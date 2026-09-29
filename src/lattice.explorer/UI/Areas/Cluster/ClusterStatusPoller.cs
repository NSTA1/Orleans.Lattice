namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// Follows a long-running operation's status while its page is open: it asks
/// again every <see cref="Interval"/> on the circuit's clock until the refresh
/// reports the operation has settled, the page stops it, or the page goes away.
/// A read that fails - a dropped connection, a cluster that did not answer -
/// does not end the follow: the poller backs off, doubling the wait up to
/// <see cref="MaximumInterval"/>, and returns to <see cref="Interval"/> as soon
/// as a read succeeds, so a page rides out a reconnect without hammering the
/// cluster. Leaving and coming back resumes it, because the status is the
/// cluster's.
/// </summary>
/// <param name="time">The clock the interval is measured on.</param>
internal sealed class ClusterStatusPoller(TimeProvider time) : IDisposable
{
    /// <summary>How long the poller waits between reads while they succeed.</summary>
    public static readonly TimeSpan Interval = TimeSpan.FromSeconds(2);

    /// <summary>The longest the poller waits between reads while they keep failing.</summary>
    public static readonly TimeSpan MaximumInterval = TimeSpan.FromSeconds(30);

    private CancellationTokenSource? _following;

    /// <summary>Whether the poller is following an operation.</summary>
    public bool IsFollowing => _following is not null;

    /// <summary>
    /// The wait before the next read given how many reads in a row have failed:
    /// <see cref="Interval"/> doubled once per failure, capped at
    /// <see cref="MaximumInterval"/>.
    /// </summary>
    /// <param name="consecutiveFailures">The reads in a row that have failed.</param>
    /// <returns>The wait.</returns>
    public static TimeSpan Delay(int consecutiveFailures)
    {
        var delay = Interval;
        for (var i = 0; i < consecutiveFailures && delay < MaximumInterval; i++)
        {
            delay += delay;
        }

        return delay < MaximumInterval ? delay : MaximumInterval;
    }

    /// <summary>
    /// Starts following, replacing any earlier follow. <paramref name="refresh"/>
    /// reads the status once and says whether the operation is still running,
    /// whether the read failed, or whether the operation has settled.
    /// </summary>
    /// <param name="refresh">Reads the status.</param>
    public void Follow(Func<CancellationToken, Task<ClusterPollOutcome>> refresh)
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

    private async Task FollowAsync(Func<CancellationToken, Task<ClusterPollOutcome>> refresh, CancellationTokenSource following)
    {
        var token = following.Token;
        var failures = 0;
        try
        {
            while (true)
            {
                await Task.Delay(Delay(failures), time, token).ConfigureAwait(false);
                var outcome = await refresh(token).ConfigureAwait(false);
                if (outcome == ClusterPollOutcome.Settled)
                {
                    break;
                }

                failures = outcome == ClusterPollOutcome.Failed ? failures + 1 : 0;
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

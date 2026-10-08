using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// Follows a tenant's regions while any of them is part-way along a residency
/// path, so the Regions page shows the drain the region's own silos complete, or
/// an automatic lifecycle transition, without a manual refresh. It reads again after
/// <see cref="Interval"/> on the circuit's clock; each read that brings no change,
/// or fails, doubles the wait up to <see cref="MaximumInterval"/>, and a read that
/// shows a change returns it to <see cref="Interval"/>. It stops when the read
/// reports every region steady, when the page stops it, or when the page goes
/// away.
/// </summary>
/// <remarks>
/// A drain completes within moments of being seen, so the first reads are close
/// together; a region waiting on an operator can wait for days, so a quiet follow
/// settles at one read every <see cref="MaximumInterval"/>, which bounds what an
/// open page costs the cluster. Cancellation is held in a
/// <see cref="ComponentLifetime"/>, which cancels and never disposes.
/// </remarks>
/// <param name="time">The clock the waits are measured on.</param>
internal sealed class TenancyRegionFollower(TimeProvider time) : IDisposable
{
    /// <summary>The wait before a read after a change, and before the first read.</summary>
    public static readonly TimeSpan Interval = TimeSpan.FromSeconds(2);

    /// <summary>The longest wait between reads while nothing changes or reads keep failing.</summary>
    public static readonly TimeSpan MaximumInterval = TimeSpan.FromSeconds(30);

    private readonly ComponentLifetime _follows = new();
    private CancellationToken? _following;

    /// <summary>Whether the follower is following a change.</summary>
    public bool IsFollowing => _following is not null;

    /// <summary>
    /// The wait before the next read given how many reads in a row brought no
    /// change: <see cref="Interval"/> doubled once per quiet read, capped at
    /// <see cref="MaximumInterval"/>.
    /// </summary>
    /// <param name="quietReads">The reads in a row that brought no change or failed.</param>
    /// <returns>The wait.</returns>
    public static TimeSpan Delay(int quietReads)
    {
        var delay = Interval;
        for (var i = 0; i < quietReads && delay < MaximumInterval; i++)
        {
            delay += delay;
        }

        return delay < MaximumInterval ? delay : MaximumInterval;
    }

    /// <summary>
    /// Starts following, replacing any earlier follow. <paramref name="read"/>
    /// reads the regions once and says whether any changed, nothing changed, or
    /// every region is steady.
    /// </summary>
    /// <param name="read">Reads the regions.</param>
    public void Follow(Func<CancellationToken, Task<TenancyRegionFollowOutcome>> read)
    {
        ArgumentNullException.ThrowIfNull(read);

        var following = _follows.Renew();
        _following = following.IsCancellationRequested ? null : following;
        _ = FollowAsync(read, following);
    }

    /// <summary>Stops following.</summary>
    public void Stop()
    {
        _following = null;
        _follows.Renew();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _following = null;
        _follows.Leave();
    }

    private async Task FollowAsync(Func<CancellationToken, Task<TenancyRegionFollowOutcome>> read, CancellationToken token)
    {
        var quiet = 0;
        try
        {
            while (true)
            {
                await Task.Delay(Delay(quiet), time, token).ConfigureAwait(false);
                var outcome = await read(token).ConfigureAwait(false);
                if (outcome == TenancyRegionFollowOutcome.Steady)
                {
                    break;
                }

                quiet = outcome == TenancyRegionFollowOutcome.Changed ? 0 : quiet + 1;
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return;
        }

        if (_following == token)
        {
            _following = null;
        }
    }
}

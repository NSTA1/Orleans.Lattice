using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// Follows a lifecycle change the cluster accepted but had not finished
/// (<see cref="AppLifecycleResult.Pending"/>, issue #5482): reads the catalogue until the run
/// settles, for up to <see cref="Limit"/>.
/// </summary>
/// <remarks>
/// The catalogue lists an app whose run is still in flight with its
/// <see cref="AppSummary.PendingState"/> set and never as <see cref="AppLifecycleState.Failed"/>,
/// so a listed Failed state is final and ends the follow at once. The result carries the last
/// state read: settled once the app has reached the target or failed, still pending otherwise.
/// </remarks>
internal static class AppLifecycleFollow
{
    /// <summary>How often a still-running change is read back.</summary>
    internal static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    /// <summary>How long a still-running change is followed before the page stops waiting.</summary>
    internal static readonly TimeSpan Limit = TimeSpan.FromMinutes(5);

    /// <summary>Follows <paramref name="accepted"/> until the app reaches <paramref name="target"/> or fails.</summary>
    /// <param name="control">The lifecycle facade to read the catalogue from.</param>
    /// <param name="accepted">The pending result the change returned.</param>
    /// <param name="target">The state the change moves the app to.</param>
    /// <param name="time">The clock the reads are paced on.</param>
    /// <param name="cancellationToken">Stops following.</param>
    /// <returns>The settled result, or a pending one when the limit passed first.</returns>
    public static async Task<AppLifecycleResult> FollowAsync(
        ILatticeAppsControl control,
        AppLifecycleResult accepted,
        AppLifecycleState target,
        TimeProvider time,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(accepted);
        ArgumentNullException.ThrowIfNull(time);

        var current = accepted;
        var started = time.GetTimestamp();
        while (time.GetElapsedTime(started) < Limit)
        {
            await Task.Delay(PollInterval, time, cancellationToken);
            var (state, pending) = await ReadStateAsync(control, accepted.Slug, cancellationToken);
            current = current with { State = state };
            if (pending is not null)
            {
                continue;
            }

            if (state == target)
            {
                return current with { Pending = false, Changed = true };
            }

            if (state == AppLifecycleState.Failed)
            {
                return current with { Pending = false };
            }
        }

        return current;
    }

    /// <summary>
    /// The state the catalogue lists <paramref name="slug"/> in, and the state a run in flight is
    /// moving it to; uninstalled and settled when it is not listed.
    /// </summary>
    private static async Task<(AppLifecycleState State, AppLifecycleState? Pending)> ReadStateAsync(
        ILatticeAppsControl control, string slug, CancellationToken cancellationToken)
    {
        var catalog = await control.ListAsync(cancellationToken);
        foreach (var app in catalog.Apps)
        {
            if (string.Equals(app.Slug, slug, StringComparison.Ordinal))
            {
                return (app.State, app.PendingState);
            }
        }

        return (AppLifecycleState.Uninstalled, null);
    }
}

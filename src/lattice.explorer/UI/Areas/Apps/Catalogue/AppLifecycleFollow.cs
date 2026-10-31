using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// Follows a lifecycle change the cluster accepted but had not finished
/// (<see cref="AppLifecycleResult.Pending"/>, issue #5482): reads the catalogue until the app
/// reaches the change's target state, for up to <see cref="Limit"/>.
/// </summary>
/// <remarks>
/// A run records an interim failed status while it works, so a <see cref="AppLifecycleState.Failed"/>
/// read is not final until the limit. The result then carries the last state read: still
/// pending when the app is in neither the target nor a failed state, settled otherwise.
/// </remarks>
internal static class AppLifecycleFollow
{
    /// <summary>How often a still-running change is read back.</summary>
    internal static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    /// <summary>How long a still-running change is followed before the page stops waiting.</summary>
    internal static readonly TimeSpan Limit = TimeSpan.FromMinutes(5);

    /// <summary>Follows <paramref name="accepted"/> until the app reaches <paramref name="target"/>.</summary>
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
            var state = await ReadStateAsync(control, accepted.Slug, cancellationToken);
            current = current with { State = state };
            if (state == target)
            {
                return current with { Pending = false, Changed = true };
            }
        }

        return current.State == AppLifecycleState.Failed ? current with { Pending = false } : current;
    }

    /// <summary>The state the catalogue lists <paramref name="slug"/> in; uninstalled when it is not listed.</summary>
    private static async Task<AppLifecycleState> ReadStateAsync(ILatticeAppsControl control, string slug, CancellationToken cancellationToken)
    {
        var catalog = await control.ListAsync(cancellationToken);
        foreach (var app in catalog.Apps)
        {
            if (string.Equals(app.Slug, slug, StringComparison.Ordinal))
            {
                return app.State;
            }
        }

        return AppLifecycleState.Uninstalled;
    }
}

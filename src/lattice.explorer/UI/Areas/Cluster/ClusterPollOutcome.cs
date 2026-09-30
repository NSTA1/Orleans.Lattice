namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// What one status read told a <see cref="ClusterStatusPoller"/>.
/// </summary>
internal enum ClusterPollOutcome
{
    /// <summary>The operation is still running: ask again after <see cref="ClusterStatusPoller.Interval"/>.</summary>
    Running,

    /// <summary>
    /// The read failed - the connection dropped, or the cluster did not answer -
    /// so the poller backs off before asking again, up to
    /// <see cref="ClusterStatusPoller.MaximumInterval"/>.
    /// </summary>
    Failed,

    /// <summary>The operation has settled, or the caller may no longer read it: stop following.</summary>
    Settled,
}

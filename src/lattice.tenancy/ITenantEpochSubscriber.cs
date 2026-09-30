namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A per-silo tenant-registry snapshot kept current across silos by the
/// tenant-policy epoch (issues #4030, #4051, #4052). The
/// <see cref="TenantPolicyEpochSubscription"/> fans every lease, pushed epoch,
/// membership invalidation and the start-up warm-up out to each registered
/// subscriber.
/// </summary>
internal interface ITenantEpochSubscriber
{
    /// <summary>Records a pushed cluster epoch, rebuilding when it supersedes the latest one seen.</summary>
    /// <param name="epoch">The pushed epoch.</param>
    void ObserveEpoch(TenantPolicyEpoch epoch);

    /// <summary>Applies a lease granted by the epoch grain.</summary>
    /// <param name="lease">The granted lease.</param>
    /// <param name="requestedAt">The <see cref="TimeProvider"/> timestamp taken before the lease was requested.</param>
    void ApplyLease(TenantPolicyEpochLease lease, long requestedAt);

    /// <summary>Treats the snapshot as out of date without a new epoch and rebuilds.</summary>
    void InvalidateClusterView();

    /// <summary>Builds the snapshot once if it has never been built.</summary>
    /// <param name="cancellationToken">Cancels the caller's wait.</param>
    Task EnsureWarmAsync(CancellationToken cancellationToken = default);
}

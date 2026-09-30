namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The cluster-wide authority on whether each silo's compiled tenant-policy
/// snapshot is current. A single activation, addressed by <see cref="Key"/>,
/// that leases every silo a bounded window of authority and, on every committed
/// tenant-registry write, advances the epoch and pushes it to every silo before
/// the write returns (issue #4030).
/// </summary>
[Alias(TenantTypeAliases.ITenantPolicyEpochGrain)]
internal interface ITenantPolicyEpochGrain : IGrainWithStringKey
{
    /// <summary>The fixed key of the single cluster-wide activation.</summary>
    const string Key = "sys-tenant-policy-epoch";

    /// <summary>
    /// Grants or renews <paramref name="observer"/>'s lease and returns the
    /// current epoch with the granted duration. The observer is subscribed to
    /// advances for as long as its lease is live.
    /// </summary>
    /// <param name="observer">The requesting silo's epoch observer.</param>
    /// <param name="silo">The requesting silo's address, used to tell when every live silo has leased from this activation.</param>
    /// <returns>The current epoch and the granted lease duration.</returns>
    Task<TenantPolicyEpochLease> LeaseAsync(ITenantPolicyEpochObserver observer, SiloAddress silo);

    /// <summary>
    /// Advances the epoch and returns only once every silo holding a live lease
    /// has either acknowledged the new epoch or had its lease expire, so no silo
    /// can go on answering from a snapshot that predates the write.
    /// </summary>
    /// <returns>The advanced epoch.</returns>
    Task<TenantPolicyEpoch> AdvanceAsync();
}

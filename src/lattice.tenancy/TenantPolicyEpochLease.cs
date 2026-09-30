namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The answer to a silo's lease request: the current cluster
/// <see cref="TenantPolicyEpoch"/> and how long the silo may treat a snapshot
/// built for it as authoritative without hearing from the epoch grain again.
/// </summary>
/// <remarks>
/// The silo measures <see cref="Duration"/> from the moment it <em>sent</em> the
/// request, never from when the answer arrived, so its local deadline always
/// falls before the deadline the grain recorded. The grain relies on that when it
/// waits out the lease of a silo that did not acknowledge an advance.
/// </remarks>
/// <param name="Epoch">The cluster epoch as of the grant.</param>
/// <param name="Duration">The lease duration the grain granted.</param>
[GenerateSerializer]
[Immutable]
[Alias(TenantTypeAliases.TenantPolicyEpochLease)]
internal readonly record struct TenantPolicyEpochLease(
    [property: Id(0)] TenantPolicyEpoch Epoch,
    [property: Id(1)] TimeSpan Duration);

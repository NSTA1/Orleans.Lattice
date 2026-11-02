namespace Orleans.Lattice.Apps;

/// <summary>
/// The answer to a silo's lease request (#5713): the current cluster <see cref="AppRegistryEpoch"/>
/// and how long the silo may treat a snapshot built for it as current without hearing from the
/// <see cref="IAppRegistryEpochGrain"/> again.
/// </summary>
/// <remarks>
/// The silo measures <see cref="Duration"/> from the moment it <em>sent</em> the request, never from
/// when the answer arrived, so its local deadline always falls before the deadline the grain recorded.
/// The grain relies on that when it waits out the lease of a silo that did not acknowledge an advance.
/// </remarks>
/// <param name="Epoch">The cluster epoch as of the grant.</param>
/// <param name="Duration">The lease duration the grain granted.</param>
[GenerateSerializer]
[Immutable]
[Alias(AppRegistryTypeAliases.AppRegistryEpochLease)]
internal readonly record struct AppRegistryEpochLease(
    [property: Id(0)] AppRegistryEpoch Epoch,
    [property: Id(1)] TimeSpan Duration);

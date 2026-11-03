namespace Orleans.Lattice.Apps;

/// <summary>
/// The cluster-wide version of the reserved <c>sys-app-registry</c> tree that every silo's compiled
/// registry snapshot is measured against (#5595). Held in memory by the
/// <see cref="IAppRegistryEpochGrain"/> and advanced after every committed registry write.
/// </summary>
/// <remarks>
/// <see cref="Incarnation"/> is minted afresh by every activation of the epoch grain, so a restarted
/// grain can never report a version a silo has already built a snapshot against: a silo treats any
/// change of incarnation as a registry change and rebuilds. A lost counter can therefore only ever
/// cause an unnecessary rebuild, never make a stale snapshot look current.
/// </remarks>
/// <param name="Incarnation">The identity of the epoch-grain activation that minted this epoch.</param>
/// <param name="Version">The monotonic version within <paramref name="Incarnation"/>.</param>
[GenerateSerializer]
[Immutable]
[Alias(AppRegistryTypeAliases.AppRegistryEpoch)]
internal readonly record struct AppRegistryEpoch(
    [property: Id(0)] Guid Incarnation,
    [property: Id(1)] long Version)
{
    /// <summary>
    /// <c>true</c> when this epoch supersedes <paramref name="known"/>: it comes from a different
    /// incarnation of the epoch grain, or from the same one at a strictly later version. An equal or
    /// older epoch within one incarnation never supersedes, so a late, out-of-order answer cannot move a
    /// silo backwards.
    /// </summary>
    /// <param name="known">The epoch the silo last observed.</param>
    /// <returns><c>true</c> when the silo must treat its snapshot as out of date.</returns>
    public bool Supersedes(AppRegistryEpoch known) =>
        Incarnation != known.Incarnation || Version > known.Version;
}

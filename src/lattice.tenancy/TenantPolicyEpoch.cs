namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The cluster-wide version of the tenant registry that the per-silo compiled
/// tenant-policy snapshots are measured against. Held in memory by the
/// <see cref="ITenantPolicyEpochGrain"/> and advanced on every committed write to
/// the <c>sys-tenant-registry</c> tree.
/// </summary>
/// <remarks>
/// <see cref="Incarnation"/> is minted afresh by every activation of the epoch
/// grain, so a restarted grain can never report a version a silo has already
/// seen: a silo treats any change of incarnation as a registry change and
/// rebuilds. That is what lets the epoch stay in memory without a storage
/// provider - a lost counter can only ever make a silo rebuild unnecessarily,
/// never make a stale silo look current.
/// </remarks>
/// <param name="Incarnation">The identity of the epoch-grain activation that minted this epoch.</param>
/// <param name="Version">The monotonic version within <paramref name="Incarnation"/>.</param>
[GenerateSerializer]
[Immutable]
[Alias(TenantTypeAliases.TenantPolicyEpoch)]
internal readonly record struct TenantPolicyEpoch(
    [property: Id(0)] Guid Incarnation,
    [property: Id(1)] long Version)
{
    /// <summary>
    /// <c>true</c> when this epoch supersedes <paramref name="known"/>: it comes
    /// from a different incarnation of the epoch grain, or from the same one at a
    /// strictly later version. An epoch equal to or older than
    /// <paramref name="known"/> within the same incarnation does not supersede it,
    /// so a late, out-of-order lease response can never move a silo backwards.
    /// </summary>
    /// <param name="known">The epoch the silo last observed.</param>
    /// <returns><c>true</c> when the silo must treat its snapshot as out of date.</returns>
    public bool Supersedes(TenantPolicyEpoch known) =>
        Incarnation != known.Incarnation || Version > known.Version;
}

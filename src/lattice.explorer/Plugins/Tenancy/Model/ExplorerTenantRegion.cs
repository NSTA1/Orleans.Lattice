namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// One region of a tenant's residency: the region id, where it sits in the
/// residency lifecycle, and whether an operator has authorized the tenant to be
/// resident there at all.
/// <para>
/// The two facts are a <b>two-set model</b> and a surface must keep them
/// legible: <see cref="IsAllowed"/> is the operator-authorized allowed set, and
/// <see cref="Status"/> reflects the tenant-admin-managed residency, which is
/// always a subset of it. A region can be allowed without being resident; a
/// region can be resident and draining after an operator revoked it.
/// </para>
/// </summary>
/// <param name="RegionId">The region id. Never <see langword="null"/> for a mapped value.</param>
/// <param name="Status">Where the region sits in the tenant's residency lifecycle.</param>
/// <param name="IsAllowed">
/// <see langword="true"/> when an operator has authorized the tenant to be
/// resident in this region. Residency can only be set within the allowed set.
/// </param>
public readonly record struct ExplorerTenantRegion(
    string RegionId,
    ExplorerTenantRegionLifecycle Status,
    bool IsAllowed)
{
    /// <summary>
    /// <see langword="true"/> when the tenant currently holds data in the
    /// region - it is provisioning, back-filling, online, or draining - as
    /// opposed to never provisioned or fully removed.
    /// </summary>
    public bool IsResident => Status
        is ExplorerTenantRegionLifecycle.Provisioning
        or ExplorerTenantRegionLifecycle.Backfilling
        or ExplorerTenantRegionLifecycle.Online
        or ExplorerTenantRegionLifecycle.Draining;
}

namespace Orleans.Lattice;

/// <summary>
/// One tenant's standing in one region: whether the region is in the tenant's
/// operator-authorized <b>allowed</b> set, and the tenant's per-region lifecycle
/// <see cref="Status"/>. Returned per region id from a
/// <see cref="TenantRegionVisibilityMap"/> so a discovery surface can both prune
/// the regions a tenant has no relationship with and annotate the ones it keeps.
/// </summary>
/// <remarks>
/// The two fields are independent: a region can be allowed but not yet resident
/// (the tenant may move into it), and a region can still carry a non-terminal
/// status while an operator revocation is in flight. <see cref="IsVisible"/> is
/// the union the discovery surface filters by - the actionable set.
/// </remarks>
public readonly record struct TenantRegionVisibility
{
    /// <summary>
    /// Initializes a tenant's standing in one region.
    /// </summary>
    /// <param name="isAllowed">Whether the region is in the tenant's operator-authorized allowed set.</param>
    /// <param name="status">The tenant's per-region lifecycle status.</param>
    public TenantRegionVisibility(bool isAllowed, TenantRegionResidencyStatus status)
    {
        IsAllowed = isAllowed;
        Status = status;
    }

    /// <summary>Whether the region is in the tenant's operator-authorized allowed set.</summary>
    public bool IsAllowed { get; }

    /// <summary>The tenant's per-region lifecycle status.</summary>
    public TenantRegionResidencyStatus Status { get; }

    /// <summary>
    /// Whether the tenant is <b>resident</b> in the region: its status is
    /// <see cref="TenantRegionResidencyStatus.Provisioning"/>,
    /// <see cref="TenantRegionResidencyStatus.Backfilling"/>, or
    /// <see cref="TenantRegionResidencyStatus.Online"/>.
    /// </summary>
    /// <remarks>
    /// <see cref="TenantRegionResidencyStatus.Draining"/> is deliberately excluded:
    /// the region is already leaving and has stopped serving. This mirrors the
    /// tenancy package's own residency predicate exactly, so the discovery surface
    /// and the last-resident-region guard classify a region the same way.
    /// </remarks>
    public bool IsResident => Status
        is TenantRegionResidencyStatus.Provisioning
        or TenantRegionResidencyStatus.Backfilling
        or TenantRegionResidencyStatus.Online;

    /// <summary>
    /// Whether the region is in the tenant's actionable set (<c>allowed</c> union
    /// <c>resident</c>): a region the tenant is in, or one it may move into.
    /// </summary>
    public bool IsVisible => IsAllowed || IsResident;
}

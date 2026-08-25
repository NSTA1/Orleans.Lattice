namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The single source of truth for the per-tenant region-residency lifecycle: the
/// legal <see cref="TenantRegionStatus"/> transitions, the classification of a
/// status as resident or online, and the last-resident-region guard. Both the
/// tenant-admin residency operations (which initiate an add or a remove) and the
/// internal backfill/drain promotion driver validate every transition here, so the
/// rules live in exactly one place.
/// </summary>
/// <remarks>
/// <para>
/// Add path: <see cref="TenantRegionStatus.None"/> /
/// <see cref="TenantRegionStatus.Removed"/> / <see cref="TenantRegionStatus.Offline"/>
/// -&gt; <see cref="TenantRegionStatus.Provisioning"/> -&gt;
/// <see cref="TenantRegionStatus.Backfilling"/> -&gt;
/// <see cref="TenantRegionStatus.Online"/>.
/// </para>
/// <para>
/// Remove path: <see cref="TenantRegionStatus.Provisioning"/> /
/// <see cref="TenantRegionStatus.Backfilling"/> / <see cref="TenantRegionStatus.Online"/>
/// -&gt; <see cref="TenantRegionStatus.Draining"/> -&gt;
/// <see cref="TenantRegionStatus.Offline"/> -&gt; <see cref="TenantRegionStatus.Removed"/>.
/// </para>
/// </remarks>
public static class TenantRegionLifecycle
{
    /// <summary>
    /// Returns <c>true</c> when <paramref name="status"/> counts as resident: the
    /// region is provisioned to hold the tenant's data (the residency set the last
    /// -resident-region guard protects). <see cref="TenantRegionStatus.Draining"/>
    /// is already leaving and is not counted.
    /// </summary>
    /// <param name="status">The region status to classify.</param>
    /// <returns><c>true</c> when the region is resident.</returns>
    public static bool IsResident(TenantRegionStatus status) =>
        status is TenantRegionStatus.Provisioning
            or TenantRegionStatus.Backfilling
            or TenantRegionStatus.Online;

    /// <summary>
    /// Returns <c>true</c> when <paramref name="status"/> is
    /// <see cref="TenantRegionStatus.Online"/>: the region serves the tenant's
    /// clients and accepts steady-state replicated writes.
    /// </summary>
    /// <param name="status">The region status to classify.</param>
    /// <returns><c>true</c> when the region is online.</returns>
    public static bool IsOnline(TenantRegionStatus status) =>
        status == TenantRegionStatus.Online;

    /// <summary>
    /// The status an add transitions <paramref name="current"/> to, or <c>null</c>
    /// when an add is not legal from the current status (the region is already
    /// resident and needs no add). An add is legal only from a non-resident status.
    /// </summary>
    /// <param name="current">The region's current status.</param>
    /// <returns><see cref="TenantRegionStatus.Provisioning"/> when an add begins; otherwise <c>null</c>.</returns>
    public static TenantRegionStatus? NextOnAdd(TenantRegionStatus current) =>
        IsResident(current) ? null : TenantRegionStatus.Provisioning;

    /// <summary>
    /// The status a remove transitions <paramref name="current"/> to, or <c>null</c>
    /// when a remove is not legal from the current status (the region is not
    /// resident, so there is nothing to drain). A remove is legal only from a
    /// resident status.
    /// </summary>
    /// <param name="current">The region's current status.</param>
    /// <returns><see cref="TenantRegionStatus.Draining"/> when a remove begins; otherwise <c>null</c>.</returns>
    public static TenantRegionStatus? NextOnRemove(TenantRegionStatus current) =>
        IsResident(current) ? TenantRegionStatus.Draining : null;

    /// <summary>
    /// Returns <c>true</c> when a forward promotion from <paramref name="from"/> to
    /// <paramref name="to"/> is one of the legal single-step lifecycle advances the
    /// backfill/drain driver may apply.
    /// </summary>
    /// <param name="from">The current status.</param>
    /// <param name="to">The candidate next status.</param>
    /// <returns><c>true</c> when the transition is a legal single step.</returns>
    public static bool IsLegalPromotion(TenantRegionStatus from, TenantRegionStatus to) =>
        (from, to) switch
        {
            (TenantRegionStatus.Provisioning, TenantRegionStatus.Backfilling) => true,
            (TenantRegionStatus.Backfilling, TenantRegionStatus.Online) => true,
            (TenantRegionStatus.Draining, TenantRegionStatus.Offline) => true,
            (TenantRegionStatus.Offline, TenantRegionStatus.Removed) => true,
            _ => false,
        };

    /// <summary>
    /// Computes the single legal forward promotion the backfill/drain driver applies
    /// from <paramref name="current"/>: <see cref="TenantRegionStatus.Provisioning"/>
    /// -&gt; <see cref="TenantRegionStatus.Backfilling"/> -&gt;
    /// <see cref="TenantRegionStatus.Online"/> on the add path, and
    /// <see cref="TenantRegionStatus.Draining"/> -&gt;
    /// <see cref="TenantRegionStatus.Offline"/> -&gt;
    /// <see cref="TenantRegionStatus.Removed"/> on the remove path. Returns
    /// <c>false</c> for a terminal or non-transitional status
    /// (<see cref="TenantRegionStatus.None"/>, <see cref="TenantRegionStatus.Online"/>,
    /// <see cref="TenantRegionStatus.Removed"/>), leaving <paramref name="next"/> equal
    /// to <paramref name="current"/> so the driver is an idempotent no-op there.
    /// </summary>
    /// <param name="current">The region's current status.</param>
    /// <param name="next">The promoted status when this returns <c>true</c>; otherwise <paramref name="current"/>.</param>
    /// <returns><c>true</c> when a forward promotion exists.</returns>
    public static bool TryNextPromotion(TenantRegionStatus current, out TenantRegionStatus next)
    {
        next = current switch
        {
            TenantRegionStatus.Provisioning => TenantRegionStatus.Backfilling,
            TenantRegionStatus.Backfilling => TenantRegionStatus.Online,
            TenantRegionStatus.Draining => TenantRegionStatus.Offline,
            TenantRegionStatus.Offline => TenantRegionStatus.Removed,
            _ => current,
        };

        return next != current;
    }
}

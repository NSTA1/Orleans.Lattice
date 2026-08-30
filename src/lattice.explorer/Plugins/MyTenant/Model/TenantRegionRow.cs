using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// One region row on the residency surface: what the cluster reports about the
/// region, what the caller has planned for it, and whether the plugin will let
/// that plan change.
/// </summary>
/// <remarks>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so a plan of any size projects into a reused array
/// and a render reads it without allocating.
/// </remarks>
/// <param name="RegionId">The region's id.</param>
/// <param name="Status">The region's lifecycle status as the cluster reports it.</param>
/// <param name="IsAllowed">
/// Whether the region is in the tenant's operator-authorized allowed set. Only a
/// platform operator can change this, so a tenant admin reads it as a boundary.
/// </param>
/// <param name="IsResident">Whether the tenant is currently resident in the region.</param>
/// <param name="IsPlannedResident">Whether the caller's pending plan keeps a residency here.</param>
/// <param name="Refusal">
/// Why toggling this row is refused, or <see cref="TenantResidencyRefusal.None"/>
/// when it is permitted.
/// </param>
public readonly record struct TenantRegionRow(
    string RegionId,
    ExplorerTenantRegionLifecycle Status,
    bool IsAllowed,
    bool IsResident,
    bool IsPlannedResident,
    TenantResidencyRefusal Refusal)
{
    /// <summary>Whether the caller may toggle this row's planned residency.</summary>
    public bool CanToggle => Refusal == TenantResidencyRefusal.None;

    /// <summary>
    /// Whether the plan differs from what the cluster currently holds for this
    /// region, so the surface can mark the row as pending.
    /// </summary>
    public bool IsChanged => IsPlannedResident != IsResident;
}

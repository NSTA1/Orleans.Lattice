using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// One region of a tenant's footprint as the regions surface renders it: the
/// region id, its residency lifecycle, whether an operator has authorized the
/// tenant to be resident there, and the operator's pending intent for it.
/// <para>
/// The two facts stay distinct because they are a two-set model:
/// <see cref="IsAllowed"/> is the operator-authorized allowed set, and
/// <see cref="Status"/> reflects the tenant-admin-managed residency, always a
/// subset of it. A region can be allowed without being resident, and a region
/// can be resident and draining after an operator revoked it.
/// </para>
/// </summary>
public sealed class TenantRegionRow
{
    /// <summary>Creates a row over <paramref name="region"/>, with its pending intent matching its current state.</summary>
    /// <param name="region">The region as the seam reported it.</param>
    public TenantRegionRow(ExplorerTenantRegion region)
    {
        Region = region;
        Allow = region.IsAllowed;
    }

    /// <summary>The region as the seam reported it.</summary>
    public ExplorerTenantRegion Region { get; }

    /// <summary>The region id.</summary>
    public string RegionId => Region.RegionId;

    /// <summary>Where the region sits in the tenant's residency lifecycle.</summary>
    public ExplorerTenantRegionLifecycle Status => Region.Status;

    /// <summary>Whether an operator has authorized the tenant to be resident here.</summary>
    public bool IsAllowed => Region.IsAllowed;

    /// <summary>
    /// Whether the tenant currently holds data in the region - provisioning,
    /// back-filling, online, or draining - as opposed to never provisioned or
    /// fully removed.
    /// </summary>
    public bool IsResident => Region.IsResident;

    /// <summary>
    /// The operator's pending intent for the allowed set, edited in the surface
    /// and sent as one complete desired set. Starts equal to
    /// <see cref="IsAllowed"/>.
    /// </summary>
    public bool Allow { get; set; }

    /// <summary>Whether the pending intent differs from what the cluster currently holds.</summary>
    public bool IsChanged => Allow != IsAllowed;

    /// <summary>
    /// Whether the pending intent would revoke a region the tenant is still
    /// resident in. The cluster refuses exactly this, so the surface warns
    /// before the call rather than translating the refusal afterwards.
    /// </summary>
    public bool WouldRevokeResident => IsAllowed && !Allow && IsResident;

    /// <summary>The residency lifecycle as a display label.</summary>
    public string StatusLabel => Status switch
    {
        ExplorerTenantRegionLifecycle.None => "Not provisioned",
        ExplorerTenantRegionLifecycle.Provisioning => "Provisioning",
        ExplorerTenantRegionLifecycle.Backfilling => "Back-filling",
        ExplorerTenantRegionLifecycle.Online => "Online",
        ExplorerTenantRegionLifecycle.Draining => "Draining",
        ExplorerTenantRegionLifecycle.Offline => "Offline",
        _ => "Removed",
    };

    /// <summary>The residency lifecycle's modifier class.</summary>
    public string StatusClass => Status switch
    {
        ExplorerTenantRegionLifecycle.Online => "is-online",
        ExplorerTenantRegionLifecycle.Provisioning or ExplorerTenantRegionLifecycle.Backfilling => "is-moving",
        ExplorerTenantRegionLifecycle.Draining => "is-draining",
        _ => "is-idle",
    };

    /// <summary>The operator-authorization state as a display label.</summary>
    public string AllowedLabel => IsAllowed ? "Allowed" : "Not allowed";
}

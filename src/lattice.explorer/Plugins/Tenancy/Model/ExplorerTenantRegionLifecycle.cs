namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// A tenant's per-region lifecycle state as the Explorer presents it: where a
/// region sits between never provisioned and fully removed. The display-layer
/// counterpart to the control API's region lifecycle status.
/// </summary>
public enum ExplorerTenantRegionLifecycle
{
    /// <summary>The region carries no residency for this tenant.</summary>
    None = 0,

    /// <summary>The region is being provisioned for the tenant.</summary>
    Provisioning = 1,

    /// <summary>The region is provisioned and is back-filling the tenant's data.</summary>
    Backfilling = 2,

    /// <summary>The region is fully resident and serving.</summary>
    Online = 3,

    /// <summary>The region is draining the tenant's data ahead of removal.</summary>
    Draining = 4,

    /// <summary>The region has drained and is no longer serving.</summary>
    Offline = 5,

    /// <summary>The region has been removed from the tenant's residency.</summary>
    Removed = 6,
}

namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// A tenant's lifecycle state as the Explorer presents it. The display-layer
/// counterpart to the control API's tenant lifecycle status, so the tenancy
/// plugins never name a wire type.
/// </summary>
public enum ExplorerTenantLifecycle
{
    /// <summary>The tenant is registered and its data plane admits operations.</summary>
    Active = 0,

    /// <summary>
    /// The tenant is suspended: its trees remain intact but its data-plane
    /// operations are refused until an operator resumes it.
    /// </summary>
    Suspended = 1,
}

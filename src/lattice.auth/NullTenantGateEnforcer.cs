namespace Orleans.Lattice.Auth;

/// <summary>
/// The default no-op <see cref="ITenantGateEnforcer"/>: the null seam a cluster
/// without the tenancy add-on runs with. <see cref="IsActive"/> is <c>false</c>
/// and <see cref="Enforce"/> always allows, so the auth gate short-circuits on
/// the <see cref="IsActive"/> read and never invokes it. Registered by
/// <c>AddLatticeAuth</c> via <c>TryAddSingleton</c> so it is present whenever the
/// gate is, and displaced by the tenancy add-on's active enforcer via
/// <c>Replace</c> when tenancy is enabled.
/// </summary>
internal sealed class NullTenantGateEnforcer : ITenantGateEnforcer
{
    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public LatticeAccessDecision Enforce(in LatticeAccessRequest request) =>
        LatticeAccessDecision.Allow();
}

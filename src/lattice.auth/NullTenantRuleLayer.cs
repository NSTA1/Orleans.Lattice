namespace Orleans.Lattice.Auth;

/// <summary>
/// The default inactive <see cref="ITenantRuleLayer"/>: the null seam a cluster
/// without the tenancy add-on runs with. <see cref="IsActive"/> is <c>false</c>,
/// so the authorization engine never builds a tenant partition nor enters the
/// tenant layer. Registered by <c>AddLatticeAuth</c> via <c>TryAddSingleton</c> and
/// displaced by the tenancy add-on via <c>Replace</c>.
/// </summary>
internal sealed class NullTenantRuleLayer : ITenantRuleLayer
{
    /// <inheritdoc />
    public bool IsActive => false;
}

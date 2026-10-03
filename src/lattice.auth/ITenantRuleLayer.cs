namespace Orleans.Lattice.Auth;

/// <summary>
/// The on/off seam for the <b>tenant layer</b> of authorization: the tenant-tier
/// rules (<see cref="LatticeTenantRuleIds"/>) a tenant's administrators author over
/// their own trees, evaluated beneath the operator layer only when no operator
/// rule matches. It is an extension seam owned by the tenancy add-on
/// (<c>Orleans.Lattice.Tenancy</c>), declared here so the authorization engine can
/// consult it <b>without</b> a project reference into tenancy: tenancy references
/// this package, never the reverse.
/// </summary>
/// <remarks>
/// <para>
/// The default registration is the internal null layer, whose
/// <see cref="IsActive"/> is <c>false</c>, registered by <c>AddLatticeAuth</c> via
/// <c>TryAddSingleton</c>. The tenancy add-on displaces it with <c>Replace</c> and
/// an implementation whose <see cref="IsActive"/> reads the delegated tenant
/// access administration flag. While <see cref="IsActive"/> is <c>false</c> the
/// policy snapshot builds no tenant partition and the evaluator never enters the
/// tenant layer, so a cluster without tenancy, or with the flag off, decides and
/// allocates exactly as it did before this seam existed.
/// </para>
/// <para>
/// The entry points the evaluator uses to run the tenant layer are internal to
/// the authorization engine; this public surface is only the on/off switch.
/// </para>
/// </remarks>
public interface ITenantRuleLayer
{
    /// <summary>
    /// <c>true</c> when the tenant layer is wired in and enabled; <c>false</c> for
    /// the null default. Read once per decision on the hot path, so an
    /// implementation must answer from in-memory state without allocating.
    /// </summary>
    bool IsActive { get; }
}

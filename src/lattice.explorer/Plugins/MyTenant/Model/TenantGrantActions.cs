namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The transitions the active tenant may drive on one cross-tenant grant, given
/// its side of the agreement and the grant's current state.
/// <para>
/// Advisory: the cluster re-enforces every one of them. What this set guarantees
/// is that the plugin never <em>offers</em> a transition the active tenant has
/// no standing to make, which is how a tenant admin is kept from touching
/// another tenant's half of the flow.
/// </para>
/// </summary>
[Flags]
public enum TenantGrantActions
{
    /// <summary>Nothing may be done to the grant from here.</summary>
    None = 0,

    /// <summary>
    /// The grant may be approved, which is what makes it authorize. Offered only
    /// to the grantee, and only while the grant is pending.
    /// </summary>
    Approve = 1,

    /// <summary>
    /// The grant may be declined, closing it terminally. Offered only to the
    /// grantee, and only while the grant is pending.
    /// </summary>
    Reject = 2,

    /// <summary>
    /// The grant may be withdrawn, closing it terminally. Offered to either
    /// party, and only while the grant is active, so neither side is trapped in
    /// an agreement it no longer wants.
    /// </summary>
    Revoke = 4,
}

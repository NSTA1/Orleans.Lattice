namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// Which side of a cross-tenant grant the caller's own tenant is on.
/// <para>
/// The direction is derived from the grant itself against the active tenant, and
/// is what decides which half of the two-step agreement the caller may drive:
/// the granting tenant offers and the grantee approves, and neither can do the
/// other's step.
/// </para>
/// </summary>
public enum TenantGrantDirection
{
    /// <summary>
    /// Neither party is the active tenant, so the grant is none of this
    /// caller's business and no action on it is offered.
    /// </summary>
    Unrelated = 0,

    /// <summary>
    /// The active tenant is the granter: it offered a scope of its own data to
    /// another tenant. It may withdraw a live grant, but it may never approve
    /// its own offer.
    /// </summary>
    Outbound = 1,

    /// <summary>
    /// The active tenant is the grantee: another tenant offered it access. This
    /// is the side that approves or rejects, which is the step that makes a
    /// grant live.
    /// </summary>
    Inbound = 2,
}

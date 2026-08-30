namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// One cross-tenant grant: which tenant offered which scope of its own data to
/// which other tenant, what the grant authorizes, and - load-bearing - what
/// state it is in.
/// <para>
/// <b>Only <see cref="ExplorerTenantGrantState.Active"/> authorizes anything.</b>
/// The two-step flow means a grant exists from the moment it is offered, so a
/// surface that renders a grant without its state presents a pending offer as
/// live access. Gate every "this tenant can reach that data" affordance on
/// <see cref="AuthorizesAccess"/>, never on the grant's mere presence.
/// </para>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so a grant list is one array rather than one
/// object per row.
/// </para>
/// </summary>
/// <param name="GrantId">The grant's stable id.</param>
/// <param name="GranterTenantId">The tenant that offered a scope of its own data.</param>
/// <param name="GranteeTenantId">The tenant the grant was offered to.</param>
/// <param name="Scope">The scope of the granting tenant's data the grant covers.</param>
/// <param name="Operations">The operations the grant authorizes once active.</param>
/// <param name="State">
/// The grant's lifecycle state. Only
/// <see cref="ExplorerTenantGrantState.Active"/> authorizes anything.
/// </param>
public readonly record struct ExplorerTenantGrant(
    string GrantId,
    string GranterTenantId,
    string GranteeTenantId,
    string Scope,
    ExplorerTenantGrantAccess Operations,
    ExplorerTenantGrantState State)
{
    /// <summary>
    /// <see langword="true"/> only when the grant is
    /// <see cref="ExplorerTenantGrantState.Active"/>, so it really does
    /// authorize the operations it names. A pending, rejected, or revoked grant
    /// authorizes nothing.
    /// </summary>
    public bool AuthorizesAccess => State == ExplorerTenantGrantState.Active;

    /// <summary>
    /// <see langword="true"/> when the grant is awaiting the grantee's answer,
    /// so it is actionable from the grantee's inbox and authorizes nothing yet.
    /// </summary>
    public bool IsAwaitingApproval => State == ExplorerTenantGrantState.Pending;

    /// <summary>
    /// <see langword="true"/> when the grant has reached a terminal state -
    /// rejected or revoked - so no transition remains and it authorizes
    /// nothing.
    /// </summary>
    public bool IsClosed => State
        is ExplorerTenantGrantState.Rejected
        or ExplorerTenantGrantState.Revoked;
}

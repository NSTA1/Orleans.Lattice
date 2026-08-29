using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// One grant as the sharing surface renders it: the grant itself, the side of
/// the agreement the active tenant is on, and the transitions that side may
/// drive from here.
/// <para>
/// The grant's <see cref="ExplorerTenantGrant.State"/> travels with every row and
/// is rendered explicitly wherever a grant appears, because it is load-bearing:
/// only <see cref="ExplorerTenantGrantState.Active"/> authorizes anything, and a
/// pending grant must never read as live access.
/// </para>
/// </summary>
/// <remarks>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so the two grant lists project into reused arrays
/// and a render reads them without allocating.
/// </remarks>
/// <param name="Grant">The grant as the cluster reported it.</param>
/// <param name="Direction">Which side of it the active tenant is on.</param>
/// <param name="Actions">The transitions the active tenant may drive.</param>
public readonly record struct TenantGrantRow(
    ExplorerTenantGrant Grant,
    TenantGrantDirection Direction,
    TenantGrantActions Actions)
{
    /// <summary>
    /// Projects <paramref name="grant"/> for <paramref name="activeTenantId"/>,
    /// resolving the direction and the permitted transitions through
    /// <see cref="TenantGrantScope"/>.
    /// </summary>
    /// <param name="activeTenantId">The caller's active tenant id, or <see langword="null"/>.</param>
    /// <param name="grant">The grant to project.</param>
    /// <returns>The projected row.</returns>
    public static TenantGrantRow For(string? activeTenantId, in ExplorerTenantGrant grant) => new(
        grant,
        TenantGrantScope.Direction(activeTenantId, grant),
        TenantGrantScope.Available(activeTenantId, grant));

    /// <summary>
    /// Whether this grant currently authorizes access. <see langword="true"/>
    /// only for an active grant.
    /// </summary>
    public bool AuthorizesAccess => Grant.AuthorizesAccess;

    /// <summary>
    /// Whether this grant is waiting on the grantee's decision, and therefore
    /// authorizes nothing yet.
    /// </summary>
    public bool IsAwaitingApproval => Grant.IsAwaitingApproval;

    /// <summary>
    /// Whether this row is the one the caller must act on: an inbound offer
    /// awaiting their approval. The inbox counts these and the surface makes
    /// them the primary call to action, because approving is the step that turns
    /// an offer into live access.
    /// </summary>
    public bool NeedsDecision =>
        Direction == TenantGrantDirection.Inbound && Actions.HasFlag(TenantGrantActions.Approve);

    /// <summary>Whether the approve control should be rendered for this row.</summary>
    public bool CanApprove => Actions.HasFlag(TenantGrantActions.Approve);

    /// <summary>Whether the reject control should be rendered for this row.</summary>
    public bool CanReject => Actions.HasFlag(TenantGrantActions.Reject);

    /// <summary>Whether the revoke control should be rendered for this row.</summary>
    public bool CanRevoke => Actions.HasFlag(TenantGrantActions.Revoke);

    /// <summary>
    /// The other tenant in the agreement: the grantee for an outbound grant and
    /// the granter for an inbound one. For an unrelated grant - which this
    /// surface never renders - it is the granter.
    /// </summary>
    public string CounterpartyTenantId =>
        Direction == TenantGrantDirection.Outbound ? Grant.GranteeTenantId : Grant.GranterTenantId;
}

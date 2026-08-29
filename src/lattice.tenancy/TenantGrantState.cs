namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The lifecycle state of a <see cref="CrossTenantGrant"/>. A cross-tenant grant
/// is an agreement between two tenants, so it is reached in two steps: the
/// granting tenant <em>offers</em> it (<see cref="Pending"/>, authorizing
/// nothing) and the grantee tenant <em>approves</em> it
/// (<see cref="Active"/>, the only state that authorizes anything). Either side
/// may walk away - the grantee by rejecting a pending offer
/// (<see cref="Rejected"/>), and either party by revoking an active grant
/// (<see cref="Revoked"/>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Only <see cref="Active"/> authorizes.</b>
/// <see cref="ITenantPolicyEngine.ResolveCrossTenantGrant"/> resolves every other
/// state to a denial. That is the load-bearing property of the whole two-step
/// flow: if a <see cref="Pending"/> grant authorized anything, the approval step
/// would be decorative and the granting tenant could widen another tenant's
/// access unilaterally.
/// </para>
/// <para>
/// <b>Why <see cref="Active"/> is the zero value.</b> The state is an additive
/// <c>[Id(4)]</c> on a type that already shipped
/// (<c>lattice.tenancy-v9.4.0</c>), so a grant persisted before this field
/// existed deserializes to the zero value. Before the state existed, every live
/// grant a host had written through <see cref="TenantRecord.AddGrant"/>
/// authorized on an operation and scope match, so <see cref="Active"/> is the
/// only zero value that leaves an upgraded cluster behaving exactly as it did:
/// choosing <see cref="Pending"/> would silently sever every cross-tenant
/// authorization a host had deliberately configured, with no diagnostic, and
/// would also make the already-public <see cref="CrossTenantGrant.Create(string, TenantGranteeKind, string, TenantGrantOperations)"/>
/// overload silently inert. The fail-closed guarantee that matters is enforced
/// where new grants are created instead: the two-step offer path always stamps
/// <see cref="Pending"/> explicitly, so nothing reached through the control
/// facade can become <see cref="Active"/> without the grantee's approval.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantGrantState)]
public enum TenantGrantState
{
    /// <summary>
    /// The grant is in force: the grantee has approved it, and it authorizes its
    /// operations on its scope. The zero value, so a grant persisted before the
    /// state field existed keeps authorizing exactly as it did.
    /// </summary>
    Active = 0,

    /// <summary>
    /// The granting tenant has offered the grant and the grantee has not yet
    /// answered. Authorizes nothing.
    /// </summary>
    Pending = 1,

    /// <summary>
    /// The grantee declined the offer. Terminal and authorizes nothing; a new
    /// agreement for the same grantee and scope requires a fresh offer.
    /// </summary>
    Rejected = 2,

    /// <summary>
    /// A previously active grant was withdrawn by one of the two parties.
    /// Terminal and authorizes nothing; a new agreement for the same grantee and
    /// scope requires a fresh offer.
    /// </summary>
    Revoked = 3,
}

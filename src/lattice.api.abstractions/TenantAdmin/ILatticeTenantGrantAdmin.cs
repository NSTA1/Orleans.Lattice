namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>cross-tenant grant administration</b> control facade:
/// one coherent, discoverable, fail-closed surface over the two-step agreement by
/// which one tenant exposes a scope of its data to another - offer, approve,
/// reject, revoke, and list. It is a sibling of
/// <see cref="ILatticeTenantAccessAdmin"/> and
/// <see cref="ILatticeTenantRegionAdmin"/>, added append-only so the tenant
/// lifecycle surface is unchanged. Every transport binding (gRPC, MCP) is a thin
/// adapter over this single surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why two steps.</b> A cross-tenant grant is an agreement between two
/// tenants, so both sides participate and neither acts unilaterally. The granting
/// tenant <em>offers</em> a grant, which is created
/// <see cref="TenantGrantLifecycleState.Pending"/> and authorizes <b>nothing</b>;
/// only when the grantee tenant <em>approves</em> it does it become
/// <see cref="TenantGrantLifecycleState.Active"/> and authorize anything. The
/// asymmetry is deliberate: offering costs the grantee nothing, so the granting
/// tenant may do it alone, but activating exposes the granting tenant's data to
/// the grantee, so it requires the grantee to opt in. Revocation is open to both
/// parties because neither should be trapped in an agreement.
/// </para>
/// <para>
/// <b>The authority required differs per operation</b>, and this is the whole
/// point of the surface. Each operation is authorized for a platform operator
/// (cluster-wide <see cref="Orleans.Lattice.LatticeOperation.Admin"/> on the
/// reserved auth policy tree) <b>or</b> a live admin subject of one specific
/// tenant:
/// </para>
/// <list type="table">
/// <listheader><term>Operation</term><description>Tenant whose admin may drive it</description></listheader>
/// <item><term><see cref="OfferGrantAsync"/></term><description>the <b>granting</b> tenant - it is offering its own data.</description></item>
/// <item><term><see cref="ApproveGrantAsync"/> / <see cref="RejectGrantAsync"/></term><description>the <b>grantee</b> tenant - it decides what lands in its view.</description></item>
/// <item><term><see cref="RevokeGrantAsync"/></term><description><b>either</b> party - neither is trapped.</description></item>
/// <item><term><see cref="ListGrantsAsync"/></term><description>the tenant whose grants are listed.</description></item>
/// </list>
/// <para>
/// An admin of one tenant therefore can never approve a grant offered to a
/// different tenant, nor offer a grant from one. This is the same two-tier rule
/// <see cref="ILatticeTenantRegionAdmin.SetResidencyAsync"/> and
/// <see cref="ILatticeTenantAccessAdmin"/> apply, and deliberately <em>not</em>
/// the operator-only tier that gates the <see cref="ILatticeTenantAdmin"/>
/// lifecycle mutations, so two tenants can agree without a platform operator in
/// the loop. Both tiers are independent of the data-plane <c>DefaultEffect</c>,
/// so an unmatched request always resolves to deny.
/// </para>
/// <para>
/// <b>Existence is never probeable.</b> A caller that holds neither operator
/// authority nor the required tenant's admin authority is told <i>denied</i>,
/// never <i>not found</i>, whether or not the tenant exists. A grant that has not
/// been offered and a granting tenant that is not registered are likewise
/// reported identically, as <see cref="TenantGrantNotFoundException"/>.
/// </para>
/// <para>
/// <b>Tenant-to-tenant only.</b> The two-step agreement needs a counterparty with
/// admins who can approve it, so this surface administers grants whose grantee is
/// a whole tenant - exactly the grants the tenancy engine's cross-tenant
/// resolution consumes. Grants issued to an individual subject have no
/// counterparty to approve them and are out of scope here.
/// </para>
/// <para>
/// <b>Idempotent and convergent.</b> Asking for the state a grant is already in
/// is a no-op reporting <see cref="TenantGrantChangeResult.Changed"/>
/// <see langword="false"/>, so a retried call over an unreliable transport is
/// safe. Every write is stamped through the tenant record's CRDT merge, and
/// concurrent transitions from the two parties converge on the more restrictive
/// outcome, so convergence can never widen access.
/// </para>
/// </remarks>
public interface ILatticeTenantGrantAdmin
{
    /// <summary>
    /// Lists a tenant's cross-tenant grants in both directions - those it issued
    /// and those offered to it (a <b>tenant-admin</b> action on that tenant).
    /// Read-only.
    /// </summary>
    /// <param name="tenantId">The tenant id whose grants to list. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tenant's issued and received grants, in every lifecycle state.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator and no tenant with that id is registered.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin subject of that tenant (also raised, rather than a not-found, when a non-operator names a tenant that does not exist).</exception>
    Task<TenantGrantReport> ListGrantsAsync(
        string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Offers a cross-tenant grant from <paramref name="granterTenantId"/> to
    /// <paramref name="granteeTenantId"/>, creating it
    /// <see cref="TenantGrantLifecycleState.Pending"/> (a <b>tenant-admin</b>
    /// action on the <b>granting</b> tenant). A pending grant authorizes
    /// <b>nothing</b> until the grantee approves it. Re-offering over an
    /// unanswered offer amends its terms; re-offering over a terminally closed
    /// grant starts a fresh agreement.
    /// </summary>
    /// <remarks>
    /// The grantee tenant is deliberately not required to exist: an offer to an
    /// unregistered tenant is inert, since only that tenant's own admins could
    /// ever approve it, and checking would turn the granting tenant's admins into
    /// a tenant-existence oracle.
    /// </remarks>
    /// <param name="granterTenantId">The tenant offering its data. Must be a valid, non-empty tenant id.</param>
    /// <param name="granteeTenantId">The tenant the grant is offered to. Must be a valid, non-empty tenant id, and different from <paramref name="granterTenantId"/>.</param>
    /// <param name="scope">The scope of the granting tenant's data the grant covers - a tree name or tree-name prefix. Must not be <c>null</c>, empty, or whitespace.</param>
    /// <param name="operations">The operations the grant will authorize once active. Must not be <see cref="TenantGrantAccess.None"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The change result, carrying the pending grant as committed.</returns>
    /// <exception cref="ArgumentException">A tenant id is <c>null</c>, empty, or not a valid tenant id; the two tenant ids are equal; <paramref name="scope"/> is <c>null</c>, empty, or whitespace; or <paramref name="operations"/> is <see cref="TenantGrantAccess.None"/>.</exception>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator and no granting tenant with that id is registered.</exception>
    /// <exception cref="ReservedTenantOperationException">Either tenant id is the reserved default tenant.</exception>
    /// <exception cref="TenantGrantTransitionException">A live, already-approved grant exists for the same grantee and scope; revoke it before offering new terms.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin subject of the <em>granting</em> tenant.</exception>
    Task<TenantGrantChangeResult> OfferGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantAccess operations,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Approves a pending cross-tenant grant, transitioning it to
    /// <see cref="TenantGrantLifecycleState.Active"/> so it begins to authorize (a
    /// <b>tenant-admin</b> action on the <b>grantee</b> tenant - the party whose
    /// view the granted data lands in). Idempotent on an already-active grant.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must be a valid, non-empty tenant id.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to, and whose admin authority this call requires. Must be a valid, non-empty tenant id.</param>
    /// <param name="scope">The scope the grant covers. Must not be <c>null</c>, empty, or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The change result, carrying the grant as committed.</returns>
    /// <exception cref="ArgumentException">A tenant id is <c>null</c>, empty, or not a valid tenant id; the two tenant ids are equal; or <paramref name="scope"/> is <c>null</c>, empty, or whitespace.</exception>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator and no grantee tenant with that id is registered.</exception>
    /// <exception cref="TenantGrantNotFoundException">No such grant has been offered (reported identically when the granting tenant is not registered, so existence cannot be probed).</exception>
    /// <exception cref="TenantGrantTransitionException">The grant is not pending - it was already rejected or revoked.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin subject of the <em>grantee</em> tenant.</exception>
    Task<TenantGrantChangeResult> ApproveGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Declines a pending cross-tenant grant, transitioning it to the terminal
    /// <see cref="TenantGrantLifecycleState.Rejected"/> (a <b>tenant-admin</b>
    /// action on the <b>grantee</b> tenant). Idempotent on an already-rejected
    /// grant.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must be a valid, non-empty tenant id.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to, and whose admin authority this call requires. Must be a valid, non-empty tenant id.</param>
    /// <param name="scope">The scope the grant covers. Must not be <c>null</c>, empty, or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The change result, carrying the grant as committed.</returns>
    /// <exception cref="ArgumentException">A tenant id is <c>null</c>, empty, or not a valid tenant id; the two tenant ids are equal; or <paramref name="scope"/> is <c>null</c>, empty, or whitespace.</exception>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator and no grantee tenant with that id is registered.</exception>
    /// <exception cref="TenantGrantNotFoundException">No such grant has been offered.</exception>
    /// <exception cref="TenantGrantTransitionException">The grant is not pending - it was already approved or revoked.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin subject of the <em>grantee</em> tenant.</exception>
    Task<TenantGrantChangeResult> RejectGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Withdraws an active cross-tenant grant, transitioning it to the terminal
    /// <see cref="TenantGrantLifecycleState.Revoked"/> (a <b>tenant-admin</b>
    /// action on <b>either</b> party, so neither is trapped in the agreement).
    /// Idempotent on an already-revoked grant. A pending offer is not revoked but
    /// declined, through <see cref="RejectGrantAsync"/>.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must be a valid, non-empty tenant id.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to. Must be a valid, non-empty tenant id.</param>
    /// <param name="scope">The scope the grant covers. Must not be <c>null</c>, empty, or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The change result, carrying the grant as committed.</returns>
    /// <exception cref="ArgumentException">A tenant id is <c>null</c>, empty, or not a valid tenant id; the two tenant ids are equal; or <paramref name="scope"/> is <c>null</c>, empty, or whitespace.</exception>
    /// <exception cref="TenantGrantNotFoundException">No such grant exists.</exception>
    /// <exception cref="TenantGrantTransitionException">The grant is not active - it is still pending, or was already rejected.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is a platform operator for neither tenant and an admin subject of neither party.</exception>
    Task<TenantGrantChangeResult> RevokeGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);
}

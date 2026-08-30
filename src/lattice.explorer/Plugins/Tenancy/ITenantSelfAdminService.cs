namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The tenancy operations a <b>tenant administrator</b> may perform on its own
/// tenant: its membership, its cross-tenant grants, its residency within the
/// allowed regions an operator authorized, and a <em>read</em> of its quota
/// consumption. Every method folds a server refusal or a transport failure into
/// a <see cref="TenantOperationResult"/> rather than throwing, so a panel stays
/// thin and always has something to render.
/// <para>
/// This is the narrow half of the tenancy operations seam, and the whole of what
/// the My Tenant plugin can reach (issue #1785). The operator-only operations -
/// authoring quota ceilings, widening the allowed region set, and the tenant
/// lifecycle - live on <see cref="ITenantAdminService"/>, which extends this
/// interface and is what the platform-operator Tenants plugin receives. A
/// surface handed this contract therefore cannot call one of them at all, so
/// its reach is readable from its own source without knowing what the cluster's
/// authorizer would have refused.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>Argument validation still throws.</b> An empty tenant id, subject id, or
/// scope is a defect in the caller, not a decision the cluster made, so it
/// raises <see cref="ArgumentException"/> instead of being disguised as a server
/// refusal. Only faults that came from the cluster - or from failing to reach it
/// - become results.
/// </para>
/// <para>
/// <b>Cancellation still propagates.</b> A cancellation the caller asked for
/// surfaces as <see cref="OperationCanceledException"/>, as it does everywhere
/// else in the Explorer, rather than being rendered as a failure.
/// </para>
/// </remarks>
public interface ITenantSelfAdminService
{
    /// <summary>
    /// Resolves the tenant the caller's own credential operates as. A caller with
    /// no tenant in context resolves to the reserved default tenant.
    /// </summary>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The caller's current tenant, or a classified refusal.</returns>
    Task<TenantOperationResult<ExplorerTenantSummary>> GetCurrentTenantAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the tenants the caller may access, ascending by id. A caller who
    /// administers nothing gets an empty list, not a refusal.
    /// </summary>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The accessible tenants, or a classified refusal.</returns>
    Task<TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>> ListAccessibleTenantsAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads one tenant's lifecycle state, per-region residency, and quota
    /// ceilings.
    /// </summary>
    /// <param name="tenantId">The tenant to inspect. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The tenant detail, or a refusal - including
    /// <see cref="TenantOperationStatus.NotFound"/>, which the cluster also
    /// returns for a tenant the caller may not see, so the call cannot probe for
    /// tenant existence.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantDetail>> GetTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a tenant's consumption against its ceilings, qualified by the scope
    /// the figures were read under. Deliberately read-only: authoring the
    /// ceilings is <see cref="ITenantAdminService.SetQuotasAsync"/>, an operator
    /// action this contract does not expose.
    /// </summary>
    /// <param name="tenantId">The tenant to report on. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The usage reading, or a classified refusal.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantQuotaUsage>> GetQuotaUsageAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces a tenant's residency set within its allowed regions. A
    /// tenant-admin action; residency must stay a subset of the allowed set,
    /// which only an operator may widen.
    /// </summary>
    /// <param name="tenantId">The tenant whose residency to author. Must not be <see langword="null"/> or empty.</param>
    /// <param name="residencyRegions">The complete desired residency set. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The residency-change outcome, or a refusal -
    /// <see cref="TenantOperationStatus.RegionNotAllowed"/> for a region outside
    /// the allowed set, and <see cref="TenantOperationStatus.LastRegion"/> when
    /// the change would leave the tenant resident nowhere.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="residencyRegions"/> is <see langword="null"/>.</exception>
    Task<TenantOperationResult<ExplorerTenantResidencyChange>> SetResidencyAsync(
        string tenantId,
        IReadOnlyCollection<string> residencyRegions,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a tenant's per-region status: one row per region that is either
    /// allowed or carries a residency, ordered by region id.
    /// </summary>
    /// <param name="tenantId">The tenant to report on. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The per-region rows, or a classified refusal.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>> GetRegionStatusAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>Lists the subjects holding tenant-admin authority over a tenant.</summary>
    /// <param name="tenantId">The tenant whose admin subjects to list. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The live admin-subject set, or a classified refusal.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantAdmins>> ListAdminSubjectsAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Grants a subject tenant-admin authority over a tenant. Idempotent:
    /// granting an existing member succeeds and reports no change.
    /// </summary>
    /// <param name="tenantId">The tenant to grant authority over. Must not be <see langword="null"/> or empty.</param>
    /// <param name="subjectId">The subject to grant. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The resulting admin-subject set, or a classified refusal.</returns>
    /// <exception cref="ArgumentException">Either id is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantAdminChange>> AddAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Revokes a subject's tenant-admin authority over a tenant. Idempotent on a
    /// non-member.
    /// </summary>
    /// <param name="tenantId">The tenant to revoke authority over. Must not be <see langword="null"/> or empty.</param>
    /// <param name="subjectId">The subject to revoke. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The resulting admin-subject set, or a refusal -
    /// <see cref="TenantOperationStatus.LastAdminSubject"/> when the removal
    /// would leave the tenant with nobody able to administer it.
    /// </returns>
    /// <exception cref="ArgumentException">Either id is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantAdminChange>> RemoveAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists a tenant's cross-tenant grants in both directions, in every
    /// lifecycle state. Read each grant's state: only an active one authorizes
    /// anything.
    /// </summary>
    /// <param name="tenantId">The tenant whose grants to list. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The issued and received grants, or a classified refusal.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantGrants>> ListGrantsAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Offers a cross-tenant grant, creating it <b>pending</b>. Requires admin
    /// authority on the <em>granting</em> tenant. The grant authorizes nothing
    /// until the grantee approves it.
    /// </summary>
    /// <param name="granterTenantId">The tenant offering a scope of its own data. Must not be <see langword="null"/> or empty.</param>
    /// <param name="granteeTenantId">The tenant the grant is offered to. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope the grant covers. Must not be <see langword="null"/> or empty.</param>
    /// <param name="operations">The operations the grant will authorize once active.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The pending grant as committed, or a refusal -
    /// <see cref="TenantOperationStatus.GrantTransitionRejected"/> when new terms
    /// are offered over a live grant.
    /// </returns>
    /// <exception cref="ArgumentException">A tenant id or the scope is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantGrantChange>> OfferGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        ExplorerTenantGrantAccess operations,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Approves a pending cross-tenant grant so it begins to authorize. Requires
    /// admin authority on the <em>grantee</em> tenant, so an admin of the
    /// granting tenant cannot approve its own offer.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must not be <see langword="null"/> or empty.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope the grant covers. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The grant as committed, or a refusal -
    /// <see cref="TenantOperationStatus.GrantNotFound"/> for an unoffered grant
    /// and <see cref="TenantOperationStatus.GrantTransitionRejected"/> for one
    /// that is not pending.
    /// </returns>
    /// <exception cref="ArgumentException">A tenant id or the scope is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantGrantChange>> ApproveGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Declines a pending cross-tenant grant, closing it terminally. Requires
    /// admin authority on the <em>grantee</em> tenant.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must not be <see langword="null"/> or empty.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope the grant covers. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The grant as committed, or a classified refusal.</returns>
    /// <exception cref="ArgumentException">A tenant id or the scope is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantGrantChange>> RejectGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Withdraws an active cross-tenant grant, closing it terminally. Requires
    /// admin authority on <em>either</em> party, so neither side is trapped in
    /// the agreement.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must not be <see langword="null"/> or empty.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope the grant covers. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The grant as committed, or a refusal -
    /// <see cref="TenantOperationStatus.GrantTransitionRejected"/> for a grant
    /// that is not active.
    /// </returns>
    /// <exception cref="ArgumentException">A tenant id or the scope is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantGrantChange>> RevokeGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);
}

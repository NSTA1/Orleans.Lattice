namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The tenancy operations a plugin may perform, expressed entirely in the
/// Explorer's own vocabulary. Every method folds a server refusal or a transport
/// failure into a <see cref="TenantOperationResult"/> rather than throwing, so a
/// panel stays thin and always has something to render.
/// <para>
/// This is the plugin-facing half of the seam. Nothing on it names a control-API
/// wire type: <see cref="ITenantAdminClient"/> speaks those, and this layer is
/// the only thing that ever calls it. That is the whole of D3 for tenancy -
/// widening what a tenancy plugin can reach is an edit here and nowhere else.
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
public interface ITenantAdminService
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
    /// Registers a new active tenant. Passing no admin subjects asks the cluster
    /// to seed the calling subject, so the creator can read the tenant back.
    /// </summary>
    /// <param name="tenantId">The tenant id to create. Must not be <see langword="null"/> or empty.</param>
    /// <param name="adminSubjects">The subjects to seed, or <see langword="null"/> to seed the caller.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The creation outcome, or a refusal -
    /// <see cref="TenantOperationStatus.AlreadyExists"/> when the tenant is
    /// already registered.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantCreation>> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Suspends a tenant, refusing its data-plane operations until it is resumed.
    /// Its trees remain intact.
    /// </summary>
    /// <param name="tenantId">The tenant to suspend. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The transition outcome, or a refusal -
    /// <see cref="TenantOperationStatus.ReservedTenant"/> for the default tenant,
    /// which can never be suspended.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantStatusChange>> SuspendTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>Resumes a suspended tenant, returning it to the active state.</summary>
    /// <param name="tenantId">The tenant to resume. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The transition outcome, or a classified refusal.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantStatusChange>> ResumeTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a tenant, cascading the removal of every tree it owns.
    /// Irreversible, so confirm against the tree count first.
    /// </summary>
    /// <param name="tenantId">The tenant to delete. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The deletion outcome carrying the number of trees removed, or a refusal -
    /// <see cref="TenantOperationStatus.ReservedTenant"/> for the default tenant.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantDeletion>> DeleteTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces a tenant's quota ceilings and burst allowance outright. A ceiling
    /// left <see langword="null"/> is authored as unbounded, which is not the
    /// same as a ceiling of <c>0</c>.
    /// </summary>
    /// <param name="tenantId">The tenant whose quotas to author. Must not be <see langword="null"/> or empty.</param>
    /// <param name="limits">The ceilings to apply.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The ceilings now in effect, or a classified refusal.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantQuotaLimits>> SetQuotasAsync(
        string tenantId,
        ExplorerTenantQuotaLimits limits,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a tenant's consumption against its ceilings, qualified by the scope
    /// the figures were read under.
    /// </summary>
    /// <param name="tenantId">The tenant to report on. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The usage reading, or a classified refusal.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    Task<TenantOperationResult<ExplorerTenantQuotaUsage>> GetQuotaUsageAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces a tenant's operator-authorized allowed region set. An operator
    /// action.
    /// </summary>
    /// <param name="tenantId">The tenant whose allowed set to author. Must not be <see langword="null"/> or empty.</param>
    /// <param name="allowedRegions">The complete desired allowed set. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The resulting allowed region ids, or a refusal -
    /// <see cref="TenantOperationStatus.RegionNotAllowed"/> when the change would
    /// revoke a region the tenant is still resident in.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="allowedRegions"/> is <see langword="null"/>.</exception>
    Task<TenantOperationResult<IReadOnlyList<string>>> AuthorizeAllowedRegionsAsync(
        string tenantId,
        IReadOnlyCollection<string> allowedRegions,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces a tenant's residency set within its allowed regions. A
    /// tenant-admin action; residency must stay a subset of the allowed set.
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

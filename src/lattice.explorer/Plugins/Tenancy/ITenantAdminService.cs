namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The full tenancy operations surface: everything a tenant administrator may
/// do to its own tenant (inherited from <see cref="ITenantSelfAdminService"/>)
/// plus the operations the cluster reserves for a <b>platform operator</b> -
/// authoring quota ceilings, widening the operator-authorized allowed region
/// set, and the tenant lifecycle. Every method folds a server refusal or a
/// transport failure into a <see cref="TenantOperationResult"/> rather than
/// throwing, so a panel stays thin and always has something to render.
/// <para>
/// This is the plugin-facing half of the seam. Nothing on it names a control-API
/// wire type: <see cref="ITenantAdminClient"/> speaks those, and this layer is
/// the only thing that ever calls it. That is the whole of D3 for tenancy -
/// widening what a tenancy plugin can reach is an edit here and nowhere else.
/// </para>
/// <para>
/// The split matters at the seam: a platform-operator surface (the Tenants
/// plugin) receives this contract, while a tenant-administrator surface (the My
/// Tenant plugin) receives only <see cref="ITenantSelfAdminService"/>, so the
/// operations below are not merely refused by the cluster - they are not
/// reachable from the plugin's source at all (issue #1785).
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
public interface ITenantAdminService : ITenantSelfAdminService
{
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
    /// same as a ceiling of <c>0</c>. An operator action: a tenant admin reads
    /// consumption against these through
    /// <see cref="ITenantSelfAdminService.GetQuotaUsageAsync"/> and nothing more.
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
    /// Replaces a tenant's operator-authorized allowed region set. An operator
    /// action: the residency a tenant chooses within that set is
    /// <see cref="ITenantSelfAdminService.SetResidencyAsync"/>.
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
}

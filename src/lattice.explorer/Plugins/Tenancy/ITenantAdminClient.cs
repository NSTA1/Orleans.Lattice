using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The Explorer's transport seam onto the cluster's tenant-administration
/// surface: one contract over both gRPC clients - the administrative
/// <c>LatticeTenantAdminApiGrpcClient</c> and the read-only
/// <c>LatticeTenantSelfServiceApiGrpcClient</c> - so the rest of the seam calls
/// one thing and a test substitutes one fake.
/// <para>
/// This is the <em>transport</em> boundary, not the plugin-facing one: it speaks
/// the control API's own types deliberately, exactly as the Backups and Access
/// plugins' control clients do. <see cref="ITenantAdminService"/> is the layer
/// that projects those onto the Explorer's own domain model, and it is the only
/// thing a tenancy plugin ever reaches. Nothing on this interface is exposed
/// through <see cref="ITenancyDomain"/>.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>The exception contract.</b> An implementation surfaces the facade's own
/// documented refusals - <see cref="TenantNotFoundException"/>,
/// <see cref="TenantAlreadyExistsException"/>,
/// <see cref="ReservedTenantOperationException"/>,
/// <see cref="TenantRegionNotAllowedException"/>,
/// <see cref="TenantLastRegionException"/>,
/// <see cref="TenantLastAdminSubjectException"/>,
/// <see cref="TenantGrantNotFoundException"/>, and
/// <see cref="TenantGrantTransitionException"/> - plus
/// <see cref="LatticeAuthorizationDeniedException"/> for a refused caller and
/// <see cref="TenancyUnavailableException"/> for a cluster that does not serve
/// the surface at all. <see cref="ITenantAdminService"/> classifies each into
/// its own <see cref="TenantOperationStatus"/>.
/// </para>
/// <para>
/// <b>What the gRPC implementation can reconstruct.</b> The wire carries a
/// status code and a message, not an exception type, and the binding maps every
/// precondition refusal onto a single
/// <see cref="StatusCode.FailedPrecondition"/>. So
/// <see cref="GrpcTenantAdminClient"/> reconstructs only the refusals whose
/// status code is unambiguous and lets the rest surface as
/// <see cref="RpcException"/>, which the service classifies as
/// <see cref="TenantOperationStatus.PreconditionFailed"/> with the server's
/// specific reason carried verbatim.
/// </para>
/// </remarks>
public interface ITenantAdminClient
{
    /// <summary>
    /// Resolves the tenant the caller's own credential is operating as. Requires
    /// no special authorization; a caller with no tenant in context resolves to
    /// the reserved default tenant.
    /// </summary>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>A descriptor for the caller's current tenant.</returns>
    Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the tenants the caller is authorized to access, ascending by id.
    /// Scoped fail-closed to the caller's subject, so a caller who administers
    /// nothing gets an empty list rather than a denial.
    /// </summary>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The accessible tenants; empty when none.</returns>
    Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the read-only lifecycle status, per-region residency, and quota
    /// ceilings of one tenant the caller may see.
    /// </summary>
    /// <param name="tenantId">The tenant to inspect. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The tenant's read-only status report.</returns>
    Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers a new active tenant, seeding its tenant-admin subjects. An
    /// empty or <see langword="null"/> <paramref name="adminSubjects"/> asks the
    /// server to seed the calling subject, so the creator can read the tenant
    /// back.
    /// </summary>
    /// <param name="tenantId">The tenant id to create. Must not be <see langword="null"/> or empty.</param>
    /// <param name="adminSubjects">The subjects to seed, or <see langword="null"/> to seed the caller.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The creation result.</returns>
    Task<TenantCreationResult> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default);

    /// <summary>Suspends a tenant, refusing its data-plane operations until it is resumed.</summary>
    /// <param name="tenantId">The tenant to suspend. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The transition result.</returns>
    Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default);

    /// <summary>Resumes a suspended tenant, returning it to the active state.</summary>
    /// <param name="tenantId">The tenant to resume. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The transition result.</returns>
    Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a tenant, cascading the removal of every tree it owns.
    /// Irreversible.
    /// </summary>
    /// <param name="tenantId">The tenant to delete. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The deletion result, carrying the number of trees removed.</returns>
    Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces a tenant's quota ceilings and burst allowance outright.
    /// </summary>
    /// <param name="tenantId">The tenant whose quotas to author. Must not be <see langword="null"/> or empty.</param>
    /// <param name="quotas">The ceilings to apply.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The update result, carrying the quotas now in effect.</returns>
    Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
        string tenantId,
        TenantQuotasDescriptor quotas,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces a tenant's operator-authorized allowed region set. An operator
    /// action; revoking a region the tenant is still resident in is refused.
    /// </summary>
    /// <param name="tenantId">The tenant whose allowed set to author. Must not be <see langword="null"/> or empty.</param>
    /// <param name="allowedRegions">The complete desired allowed set. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The authorization result, carrying the resulting allowed set.</returns>
    Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId,
        IReadOnlyCollection<string> allowedRegions,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces a tenant's residency set within its allowed regions. A
    /// tenant-admin action; a region outside the allowed set, and a change that
    /// would remove the last resident region, are both refused.
    /// </summary>
    /// <param name="tenantId">The tenant whose residency to author. Must not be <see langword="null"/> or empty.</param>
    /// <param name="residencyRegions">The complete desired residency set. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The residency-change result.</returns>
    Task<TenantResidencyChangeResult> SetTenantResidencyAsync(
        string tenantId,
        IReadOnlyCollection<string> residencyRegions,
        CancellationToken cancellationToken = default);

    /// <summary>Reads a tenant's per-region residency status, ordered by region id.</summary>
    /// <param name="tenantId">The tenant to report on. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The per-region status report.</returns>
    Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a tenant's consumption against its quota ceilings, qualified by the
    /// scope the figures were read under.
    /// </summary>
    /// <param name="tenantId">The tenant to report on. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The usage-against-quota report.</returns>
    Task<TenantQuotaUsageReport> GetTenantQuotaUsageAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>Lists the subjects holding tenant-admin authority over a tenant.</summary>
    /// <param name="tenantId">The tenant whose admin subjects to list. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The tenant's live admin-subject set.</returns>
    Task<TenantAdminSubjectReport> ListTenantAdminSubjectsAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Grants a subject tenant-admin authority over a tenant. Idempotent on an
    /// existing member.
    /// </summary>
    /// <param name="tenantId">The tenant to grant authority over. Must not be <see langword="null"/> or empty.</param>
    /// <param name="subjectId">The subject to grant. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The change result, carrying the resulting admin-subject set.</returns>
    Task<TenantAdminSubjectChangeResult> AddTenantAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Revokes a subject's tenant-admin authority over a tenant. Idempotent on a
    /// non-member; removing the last admin subject is refused.
    /// </summary>
    /// <param name="tenantId">The tenant to revoke authority over. Must not be <see langword="null"/> or empty.</param>
    /// <param name="subjectId">The subject to revoke. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The change result, carrying the resulting admin-subject set.</returns>
    Task<TenantAdminSubjectChangeResult> RemoveTenantAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists a tenant's cross-tenant grants in both directions - issued and
    /// received - in every lifecycle state.
    /// </summary>
    /// <param name="tenantId">The tenant whose grants to list. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The tenant's issued and received grants.</returns>
    Task<TenantGrantReport> ListCrossTenantGrantsAsync(
        string tenantId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Offers a cross-tenant grant, creating it <b>pending</b>. A tenant-admin
    /// action on the <em>granting</em> tenant. The grant authorizes nothing
    /// until the grantee approves it.
    /// </summary>
    /// <param name="granterTenantId">The tenant offering a scope of its own data. Must not be <see langword="null"/> or empty.</param>
    /// <param name="granteeTenantId">The tenant the grant is offered to. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope the grant covers. Must not be <see langword="null"/> or empty.</param>
    /// <param name="operations">The operations the grant will authorize once active.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The change result, carrying the pending grant as committed.</returns>
    Task<TenantGrantChangeResult> OfferCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantAccess operations,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Approves a pending cross-tenant grant so it begins to authorize. A
    /// tenant-admin action on the <em>grantee</em> tenant, so an admin of the
    /// granting tenant cannot approve its own offer.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must not be <see langword="null"/> or empty.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope the grant covers. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The change result, carrying the grant as committed.</returns>
    Task<TenantGrantChangeResult> ApproveCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Declines a pending cross-tenant grant, closing it terminally. A
    /// tenant-admin action on the <em>grantee</em> tenant.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must not be <see langword="null"/> or empty.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope the grant covers. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The change result, carrying the grant as committed.</returns>
    Task<TenantGrantChangeResult> RejectCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Withdraws an active cross-tenant grant, closing it terminally. A
    /// tenant-admin action on <em>either</em> party, so neither side is trapped
    /// in the agreement.
    /// </summary>
    /// <param name="granterTenantId">The tenant that offered the grant. Must not be <see langword="null"/> or empty.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope the grant covers. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The change result, carrying the grant as committed.</returns>
    Task<TenantGrantChangeResult> RevokeCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default);
}

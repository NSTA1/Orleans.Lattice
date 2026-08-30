namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>tenant access administration</b> control facade: one
/// coherent, discoverable, fail-closed surface for managing which subjects hold
/// tenant-admin authority over a tenant - list, add, and remove. It is a sibling
/// of <see cref="ILatticeTenantAdmin"/>, added append-only alongside it so the
/// tenant lifecycle surface is unchanged, exactly as
/// <see cref="ILatticeTenantRegionAdmin"/> was. Every transport binding (gRPC,
/// MCP) is a thin adapter over this single surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> Before it, the only way a subject became a tenant
/// admin was the <c>adminSubjects</c> seed on
/// <see cref="ILatticeTenantAdmin.CreateTenantAsync"/>, so tenant membership could
/// not be changed after creation over any public API. Membership of the
/// admin-subject set <em>is</em> the tenant-admin capability: it decides what the
/// read-only self-service surface
/// (<see cref="ILatticeTenantSelfService.ListAccessibleTenantsAsync"/> /
/// <see cref="ILatticeTenantSelfService.GetTenantAsync"/>) shows a caller, and who
/// may drive the tenant-tier operations on
/// <see cref="ILatticeTenantRegionAdmin"/> and on this facade itself.
/// </para>
/// <para>
/// <b>Tenant-tier, fail-closed authorization.</b> Every operation here is a
/// <i>tenant-admin</i> action: authorized when the caller is a platform operator
/// (cluster-wide <see cref="Orleans.Lattice.LatticeOperation.Admin"/> on the
/// reserved auth policy tree, which the auth gate's control-plane isolation grants
/// only to an operator) <b>or</b> a live admin subject on the target tenant's own
/// record. This is deliberately the <em>same</em> two-tier rule
/// <see cref="ILatticeTenantRegionAdmin.SetResidencyAsync"/> applies, and
/// deliberately <em>not</em> the operator-only tier that gates the
/// <see cref="ILatticeTenantAdmin"/> lifecycle mutations: a tenant's admins must be
/// able to manage their own membership without a platform operator in the loop,
/// while remaining unable to create, suspend, delete, or set quotas on any tenant.
/// Both tiers are independent of the data-plane <c>DefaultEffect</c>, so an
/// unmatched request always resolves to deny.
/// </para>
/// <para>
/// <b>Existence is never probeable.</b> A caller that is neither a platform
/// operator nor an admin subject of the target tenant is told <i>denied</i>, never
/// <i>not found</i>, whether or not the tenant exists, so the surface cannot be
/// used to enumerate tenants.
/// </para>
/// <para>
/// <b>Invariants.</b> A tenant can never be left with an empty admin-subject set
/// (an unbypassable guard raising
/// <see cref="TenantLastAdminSubjectException"/>), and the reserved default tenant
/// (<see cref="Orleans.Lattice.TenantId.DefaultId"/>) can never have its
/// membership mutated - it names the cluster's own legacy state, so granting it a
/// tenant admin would hand out authority over the whole legacy keyspace. Both
/// mutations are idempotent and stamped through the tenant record's CRDT merge, so
/// concurrent adds and removes from any replica converge.
/// </para>
/// </remarks>
public interface ILatticeTenantAccessAdmin
{
    /// <summary>
    /// Lists a tenant's live tenant-admin subjects, in ordinal order (a
    /// <b>tenant-admin</b> action). Read-only.
    /// </summary>
    /// <param name="tenantId">The tenant id whose admin subjects to list. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tenant's live admin-subject set.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator and no tenant with that id is registered.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin subject of that tenant (also raised, rather than a not-found, when a non-operator names a tenant that does not exist).</exception>
    Task<TenantAdminSubjectReport> ListAdminSubjectsAsync(
        string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Grants <paramref name="subjectId"/> tenant-admin authority over
    /// <paramref name="tenantId"/> by adding it to the tenant's admin-subject set
    /// (a <b>tenant-admin</b> action). Idempotent - adding a subject that is
    /// already a member reports <see cref="TenantAdminSubjectChangeResult.Changed"/>
    /// <see langword="false"/> and writes nothing.
    /// </summary>
    /// <remarks>
    /// When the cluster runs a real upstream identity directory and
    /// <c>ValidationRequired</c> is set, the subject id is resolved against it
    /// before the grant lands, so a typo'd, retired, or not-yet-provisioned id can
    /// never be recorded as a live tenant-admin grant - the same contract
    /// <see cref="ILatticeTenantAdmin.CreateTenantAsync"/> applies to an explicit
    /// seed set.
    /// </remarks>
    /// <param name="tenantId">The tenant id to grant authority over. Must be a valid, non-empty tenant id.</param>
    /// <param name="subjectId">The subject id to grant tenant-admin authority to. Must not be <c>null</c>, empty, or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The change result, carrying the resulting admin-subject set.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id, or <paramref name="subjectId"/> is <c>null</c>, empty, or whitespace.</exception>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator and no tenant with that id is registered.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin subject of that tenant.</exception>
    Task<TenantAdminSubjectChangeResult> AddAdminSubjectAsync(
        string tenantId, string subjectId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Revokes <paramref name="subjectId"/>'s tenant-admin authority over
    /// <paramref name="tenantId"/> by removing it from the tenant's admin-subject
    /// set (a <b>tenant-admin</b> action). Idempotent - removing a subject that is
    /// not a member reports <see cref="TenantAdminSubjectChangeResult.Changed"/>
    /// <see langword="false"/> and writes nothing. Removing the tenant's
    /// <em>last</em> admin subject is refused fail-closed, so a tenant can never be
    /// orphaned - including when two concurrent removals of <em>different</em>
    /// subjects would together empty the set, which is detected on the registry's
    /// merged result and repaired before the refusal is raised.
    /// </summary>
    /// <param name="tenantId">The tenant id to revoke authority over. Must be a valid, non-empty tenant id.</param>
    /// <param name="subjectId">The subject id to revoke tenant-admin authority from. Must not be <c>null</c>, empty, or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The change result, carrying the resulting admin-subject set.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id, or <paramref name="subjectId"/> is <c>null</c>, empty, or whitespace.</exception>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator and no tenant with that id is registered.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="TenantLastAdminSubjectException">The removal would leave the tenant with no admin subjects.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin subject of that tenant.</exception>
    Task<TenantAdminSubjectChangeResult> RemoveAdminSubjectAsync(
        string tenantId, string subjectId, CancellationToken cancellationToken = default);
}

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>tenant administration</b> control facade: one coherent,
/// discoverable, authorized surface for the tenant lifecycle - create, suspend,
/// resume, and delete (delete cascading the tenant's trees). Every transport
/// binding (the gRPC service, the MCP tool group) is a thin adapter over this
/// single surface, so the control semantics are written and tested once and no
/// transport concern leaks into the control logic. Mirrors the sibling
/// <see cref="TreeAdmin.ILatticeTreeAdmin"/> / replication control facades.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed authorization.</b> Every operation authorizes the caller
/// through the Lattice access gate (the whole-scope
/// <see cref="Orleans.Lattice.LatticeOperation.Admin"/> gate) before it touches
/// the tenant registry. An unauthenticated caller, or one the gate denies, is
/// refused with a <see cref="Orleans.Lattice.LatticeAuthorizationDeniedException"/>
/// and no lifecycle change is made. The binding layer additionally gates the
/// whole surface behind an explicit opt-in capability, so a cluster that does not
/// enable it exposes nothing.
/// </para>
/// <para>
/// <b>Reserved default tenant.</b> The well-known legacy-adoption default tenant
/// (<see cref="Orleans.Lattice.TenantId.DefaultId"/>) can never be suspended or
/// deleted - those operations fail closed with a
/// <see cref="ReservedTenantOperationException"/> - because it names the cluster's
/// own legacy state.
/// </para>
/// </remarks>
public interface ILatticeTenantAdmin
{
    /// <summary>
    /// Creates a new tenant in the <see cref="TenantLifecycleStatus.Active"/>
    /// status, seeding its tenant-admin subjects. Fails closed with a
    /// <see cref="TenantAlreadyExistsException"/> when a tenant with the same id is
    /// already registered (create is not an idempotent upsert), so it can never
    /// reset or reuse another tenant's definition.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Admin subjects decide visibility.</b> The read-only self-service surface
    /// (<see cref="ILatticeTenantSelfService.ListAccessibleTenantsAsync"/> /
    /// <see cref="ILatticeTenantSelfService.GetTenantAsync"/>) resolves what a
    /// caller may see from admin-subject membership, so a tenant created with
    /// <em>no</em> admin subjects is invisible to list and get for every caller -
    /// including the identity that created it - even though a platform operator
    /// can still mutate it.
    /// </para>
    /// <para>
    /// <b>Caller-seeding default.</b> When <paramref name="adminSubjects"/> is
    /// <c>null</c> or empty, the create seeds the <em>calling</em> subject as the
    /// new tenant's sole admin subject, so a create followed by a read-back works
    /// out of the box. Supplying an explicit collection overrides that default
    /// entirely - the caller is <em>not</em> added on top - so an operator can
    /// hand a tenant to another identity. A caller that cannot be resolved to a
    /// subject (anonymous, or a system-origin call that bypasses the gate) seeds
    /// nothing, leaving the tenant deliberately subject-less.
    /// </para>
    /// </remarks>
    /// <param name="tenantId">The tenant id to create. Must be a valid, non-empty tenant id.</param>
    /// <param name="adminSubjects">
    /// The tenant-admin subject ids to seed onto the new tenant, or <c>null</c> /
    /// empty to seed the calling subject. Individual entries must not be
    /// <c>null</c>, empty, or whitespace.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The creation result describing the new tenant, including the admin subjects that were seeded.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id, or <paramref name="adminSubjects"/> contains a <c>null</c>, empty, or whitespace entry.</exception>
    /// <exception cref="TenantAlreadyExistsException">A tenant with the same id is already registered.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is not authorized to administer tenants.</exception>
    Task<TenantCreationResult> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Suspends a tenant: transitions it to
    /// <see cref="TenantLifecycleStatus.Suspended"/>. Idempotent - suspending an
    /// already-suspended tenant reports <see cref="TenantStatusChangeResult.Changed"/>
    /// <see langword="false"/> and makes no change.
    /// </summary>
    /// <param name="tenantId">The tenant id to suspend. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status-change result.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is not authorized to administer tenants.</exception>
    Task<TenantStatusChangeResult> SuspendTenantAsync(
        string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resumes a tenant: transitions it back to
    /// <see cref="TenantLifecycleStatus.Active"/>. Idempotent - resuming an
    /// already-active tenant reports <see cref="TenantStatusChangeResult.Changed"/>
    /// <see langword="false"/> and makes no change.
    /// </summary>
    /// <param name="tenantId">The tenant id to resume. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status-change result.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is not authorized to administer tenants.</exception>
    Task<TenantStatusChangeResult> ResumeTenantAsync(
        string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a tenant, cascading the delete to every tree the tenant owns (each
    /// of the tenant's <c>t/{tenantId}/*</c> trees is soft-deleted) before the
    /// tenant's registry record is removed. Fails closed with a
    /// <see cref="TenantNotFoundException"/> when the tenant is not registered.
    /// </summary>
    /// <param name="tenantId">The tenant id to delete. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The deletion result, including the number of trees cascaded.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is not authorized to administer tenants.</exception>
    Task<TenantDeletionResult> DeleteTenantAsync(
        string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Authors a tenant's resource quotas and burst allowance, replacing whatever
    /// quotas the tenant currently carries with <paramref name="quotas"/>. This is
    /// the control-plane surface for per-tenant capacity governance: until a
    /// tenant's quotas are authored it is created unbounded, so this is how an
    /// operator gives a tenant caps (or lifts them again by passing
    /// <see cref="TenantQuotasDescriptor.Unbounded"/>). Governed by the same
    /// fail-closed tenant-admin access gate as the lifecycle mutations. The
    /// reserved default tenant can never be given quotas - it names the cluster's
    /// own legacy state and is permanently unbounded - so that target fails closed
    /// with a <see cref="ReservedTenantOperationException"/>.
    /// </summary>
    /// <param name="tenantId">The tenant id whose quotas to author. Must be a valid, non-empty tenant id.</param>
    /// <param name="quotas">The quotas to apply. <see cref="TenantQuotasDescriptor.BurstPercent"/> must be non-negative.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The update result, carrying the quotas now in effect.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id, or <paramref name="quotas"/> has a negative <see cref="TenantQuotasDescriptor.BurstPercent"/>.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is not authorized to administer tenants.</exception>
    Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
        string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default);
}

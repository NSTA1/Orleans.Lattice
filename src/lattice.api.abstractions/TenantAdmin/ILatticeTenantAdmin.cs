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
    /// status. Fails closed with a <see cref="TenantAlreadyExistsException"/> when
    /// a tenant with the same id is already registered (create is not an
    /// idempotent upsert), so it can never reset or reuse another tenant's
    /// definition.
    /// </summary>
    /// <param name="tenantId">The tenant id to create. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The creation result describing the new tenant.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantAlreadyExistsException">A tenant with the same id is already registered.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is not authorized to administer tenants.</exception>
    Task<TenantCreationResult> CreateTenantAsync(
        string tenantId, CancellationToken cancellationToken = default);

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
}

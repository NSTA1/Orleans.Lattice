namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>tenant self-awareness</b> facade: the read-only
/// counterpart to <see cref="ILatticeTenantAdmin"/>. It lets a caller discover
/// the tenant context it is operating in and inspect the tenants it is authorized
/// to see - the tenant its credential resolves to, the set of tenants it may
/// access, and the read-only lifecycle and per-region residency of one such
/// tenant - without any ability to mutate a tenant. Every transport binding (the
/// MCP tool group, and any future gRPC service) is a thin adapter over this one
/// surface, so the leak-free, fail-closed semantics are written and tested once.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed and leak-free.</b> Enumeration and inspection are scoped to the
/// caller's resolved subject through the tenancy policy engine: the facade never
/// returns a tenant the caller is not authorized to see, and
/// <see cref="GetTenantAsync"/> unifies "no such tenant" and "you may not see
/// this tenant" into a single <see cref="TenantNotFoundException"/> so a caller
/// can never probe for the existence of a tenant outside its authority. An
/// anonymous or unresolved caller is treated as having access to no tenant beyond
/// its own resolved (default) context.
/// </para>
/// <para>
/// <b>No lifecycle authority.</b> This facade never creates, suspends, resumes, or
/// deletes a tenant; those remain the exclusive, separately opted-in surface of
/// <see cref="ILatticeTenantAdmin"/>. It is safe to expose wherever tenancy is
/// enabled without granting any administrative capability.
/// </para>
/// </remarks>
public interface ILatticeTenantSelfService
{
    /// <summary>
    /// Resolves the tenant the current caller's credential is operating as - the
    /// tenant surfaced by the ambient tenant-context resolver. Requires no special
    /// authorization because it reports only the caller's own context; a caller
    /// with no tenant in context resolves to the reserved
    /// <see cref="Orleans.Lattice.TenantId.Default"/> tenant.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A descriptor for the caller's current tenant.</returns>
    Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the tenants the current caller is authorized to access, in ascending
    /// tenant-id order. The set is scoped to the caller's resolved subject: it
    /// contains the tenants the caller is a registered administrator of, plus the
    /// caller's own current tenant when that is a non-default tenant. It never
    /// includes a tenant the caller cannot see, so an anonymous or non-privileged
    /// caller operating under the default tenant gets an empty list.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The accessible tenants, ascending by id; empty when none.</returns>
    Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the lifecycle status and per-region residency of one tenant the
    /// caller is authorized to see. Fails closed with a
    /// <see cref="TenantNotFoundException"/> when the tenant does not exist
    /// <em>or</em> the caller is not authorized to see it - the two cases are
    /// deliberately indistinguishable so no caller can probe for a tenant outside
    /// its authority.
    /// </summary>
    /// <param name="tenantId">The tenant id to inspect. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The read-only status report for the tenant.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantNotFoundException">No such tenant is registered, or the caller is not authorized to see it.</exception>
    Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default);
}

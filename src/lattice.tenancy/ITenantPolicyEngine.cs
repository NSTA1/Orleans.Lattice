namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-silo tenant-policy decision engine: answers tenant-scoped policy
/// questions against a compiled snapshot of the tenant registry, synchronously
/// and in-memory. It resolves which tenants a subject may act as, validates a
/// subject's active-tenant selection, and resolves cross-tenant grants. The
/// snapshot is refreshed off the registry's change-feed by the compiled
/// tenant-policy snapshot maintainer, so decisions reflect committed registry
/// edits without a restart (eventual snapshot consistency).
/// </summary>
/// <remarks>
/// <para>
/// This engine is a decision surface only. Registering it does not wire
/// enforcement: nothing on the data path consults the engine until a later
/// feature wires it in. It answers questions; it does not fence traffic.
/// </para>
/// <para>
/// Every evaluate method is a warm, allocation-light lookup over the immutable
/// compiled snapshot: dictionary and set probes, no storage I/O and no LINQ
/// materialization on the decision path. A denial allocates its human-readable
/// reason; a plain allow does not.
/// </para>
/// </remarks>
public interface ITenantPolicyEngine
{
    /// <summary>
    /// The monotonically increasing epoch of the current compiled snapshot. It
    /// advances every time the snapshot is rebuilt from a committed registry
    /// change, so a caller can detect that its cached decisions may be stale. A
    /// later strict-consistency feature can fence enforcement on this epoch; this
    /// engine only produces it.
    /// </summary>
    long CurrentEpoch { get; }

    /// <summary>
    /// Resolves the set of tenants <paramref name="subjectId"/> may act as - the
    /// tenants for which it is a registered tenant-admin subject.
    /// </summary>
    /// <param name="subjectId">The caller subject id. Must not be <c>null</c>.</param>
    /// <returns>
    /// The tenants the subject may act as, in ascending tenant-id order. An empty
    /// list when the subject administers no tenant. The returned list is a cached,
    /// immutable projection of the snapshot; callers must not mutate it.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId);

    /// <summary>
    /// Validates whether <paramref name="subjectId"/> may act as the active tenant
    /// <paramref name="activeTenant"/>: the tenant must be registered and active,
    /// and the subject must be one of its tenant-admin subjects.
    /// </summary>
    /// <param name="subjectId">The caller subject id. Must not be <c>null</c>.</param>
    /// <param name="activeTenant">The candidate active tenant.</param>
    /// <returns>An allow decision, or a denial carrying the reason.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    TenantAccessDecision ValidateActiveTenant(string subjectId, TenantId activeTenant);

    /// <summary>
    /// Resolves whether <paramref name="sourceTenant"/> holds a cross-tenant grant
    /// to perform <paramref name="operation"/> against <paramref name="scope"/> of
    /// <paramref name="targetTenant"/>'s data. A grant matches when the target
    /// tenant issued it to the source tenant, its authorized operations include
    /// the requested one, and its scope covers the requested scope.
    /// </summary>
    /// <param name="sourceTenant">The tenant requesting access.</param>
    /// <param name="targetTenant">The tenant whose data is being accessed (the granting tenant).</param>
    /// <param name="scope">The scope (tree name or prefix) being accessed. Must not be <c>null</c>.</param>
    /// <param name="operation">The operation being requested.</param>
    /// <returns>An allow decision when a matching grant exists, or a denial carrying the reason.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    TenantAccessDecision ResolveCrossTenantGrant(
        TenantId sourceTenant,
        TenantId targetTenant,
        string scope,
        TenantGrantOperations operation);
}

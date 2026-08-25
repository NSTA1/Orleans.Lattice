using System.Collections.Frozen;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The compiled, immutable per-tenant slice of a <see cref="CompiledTenantPolicy"/>
/// snapshot: a tenant's identity and status, its frozen admin-subject set, and
/// its cross-tenant grants indexed by grantee tenant id. Every lookup here is
/// allocation-free, so it sits on the engine's warm decision path.
/// </summary>
internal sealed class CompiledTenant(
    TenantId id,
    TenantStatus status,
    FrozenSet<string> admins,
    FrozenDictionary<string, CrossTenantGrant[]> tenantGrants)
{
    /// <summary>The tenant's identity.</summary>
    public TenantId Id => id;

    /// <summary>The tenant's resolved lifecycle status.</summary>
    public TenantStatus Status => status;

    /// <summary>The tenant's admin-subject set. Exposed for tests.</summary>
    internal FrozenSet<string> Admins => admins;

    /// <summary>
    /// Returns <c>true</c> when <paramref name="subjectId"/> is one of the tenant's
    /// admin subjects. A zero-allocation membership probe.
    /// </summary>
    /// <param name="subjectId">The subject id to test. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the subject administers the tenant.</returns>
    public bool IsAdmin(string subjectId) => admins.Contains(subjectId);

    /// <summary>
    /// Attempts to get the cross-tenant grants this tenant issued to the grantee
    /// tenant <paramref name="granteeTenantId"/>. A zero-allocation lookup.
    /// </summary>
    /// <param name="granteeTenantId">The grantee tenant's id text.</param>
    /// <param name="grants">The grants when present; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when at least one tenant-grantee grant exists for the grantee.</returns>
    public bool TryGetTenantGrants(string granteeTenantId, out CrossTenantGrant[]? grants) =>
        tenantGrants.TryGetValue(granteeTenantId, out grants);
}

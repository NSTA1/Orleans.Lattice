namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when removing a tenant-admin subject would empty the tenant's
/// admin-subject set, orphaning the tenant: no subject would be left able to
/// administer it, and it would disappear from every self-service surface, leaving
/// only a platform operator able to reach it. A tenant must retain at least one
/// admin subject, so the control facade rejects the removal fail-closed. This
/// guard is unbypassable and mirrors the sibling
/// <see cref="TenantLastRegionException"/> residency invariant. A transport
/// binding surfaces it as a failed-precondition outcome. Carries the offending
/// tenant and subject ids.
/// </summary>
public sealed class TenantLastAdminSubjectException : Exception
{
    /// <summary>Initialises the exception for <paramref name="tenantId"/> and <paramref name="subjectId"/>.</summary>
    /// <param name="tenantId">The tenant whose last admin subject could not be removed.</param>
    /// <param name="subjectId">The admin subject whose removal was refused.</param>
    public TenantLastAdminSubjectException(string tenantId, string subjectId)
        : base($"Removing admin subject '{subjectId}' would leave tenant '{tenantId}' with no admin subjects. "
            + "A tenant must retain at least one admin subject; add the replacement before removing the last one.")
    {
        TenantId = tenantId;
        SubjectId = subjectId;
    }

    /// <summary>The tenant whose last admin subject could not be removed.</summary>
    public string TenantId { get; }

    /// <summary>The admin subject whose removal was refused.</summary>
    public string SubjectId { get; }
}

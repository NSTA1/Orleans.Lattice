namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a tenant admin's residency change would remove the tenant's last
/// resident region, leaving it with nowhere to hold its data. A tenant must always
/// be resident in at least one region, so the control facade rejects the change
/// fail-closed. This guard is unbypassable. A transport binding surfaces it as a
/// failed-precondition outcome. Carries the offending tenant id.
/// </summary>
public sealed class TenantLastRegionException : Exception
{
    /// <summary>Initialises the exception for <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant whose last resident region could not be removed.</param>
    public TenantLastRegionException(string tenantId)
        : base($"The residency change would remove the last resident region of tenant '{tenantId}'. "
            + "A tenant must remain resident in at least one region.")
    {
        TenantId = tenantId;
    }

    /// <summary>The tenant whose last resident region could not be removed.</summary>
    public string TenantId { get; }
}

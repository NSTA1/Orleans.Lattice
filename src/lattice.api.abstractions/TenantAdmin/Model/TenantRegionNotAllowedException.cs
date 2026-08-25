namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a tenant admin tries to set residency on a region that is not in the
/// tenant's operator-authorized allowed set, or when an operator tries to revoke a
/// region from the allowed set while the tenant is still resident there. Residency
/// is always a subset of the allowed set, so the control facade rejects either
/// violation fail-closed. A transport binding surfaces this as a
/// failed-precondition outcome. Carries the offending tenant and region ids.
/// </summary>
public sealed class TenantRegionNotAllowedException : Exception
{
    /// <summary>Initialises the exception for <paramref name="tenantId"/> and <paramref name="regionId"/>.</summary>
    /// <param name="tenantId">The tenant the operation was rejected for.</param>
    /// <param name="regionId">The region that violated the allowed-set invariant.</param>
    public TenantRegionNotAllowedException(string tenantId, string regionId)
        : base($"Region '{regionId}' is not an authorized allowed region for tenant '{tenantId}', "
            + "or it is still resident and cannot be revoked; residency must be a subset of the allowed set.")
    {
        TenantId = tenantId;
        RegionId = regionId;
    }

    /// <summary>The tenant the operation was rejected for.</summary>
    public string TenantId { get; }

    /// <summary>The region that violated the allowed-set invariant.</summary>
    public string RegionId { get; }
}

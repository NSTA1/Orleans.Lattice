namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown by every operation of <see cref="ILatticeTenantDirectoryAdmin"/> and
/// <see cref="ILatticeTenantPolicyAdmin"/> - except the posture probe
/// <see cref="ILatticeTenantPolicyAdmin.GetPostureAsync"/> - while delegated tenant
/// access administration is disabled on the cluster
/// (<c>LatticeTenancyOptions.DelegatedAccessAdministrationEnabled</c> is
/// <see langword="false"/>, the default). The refusal fails closed: nothing is
/// read or written before it is raised. A transport binding surfaces it as a
/// failed-precondition outcome. Carries the tenant id the call named.
/// </summary>
/// <remarks>
/// Turning the feature off deletes nothing: tenant groups, member sets, and
/// tenant-tier rules stored while it was on are retained, inert, and become
/// manageable again when it is turned back on.
/// </remarks>
public sealed class TenantAccessAdministrationDisabledException : Exception
{
    /// <summary>Initialises the exception for a call that named <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant id the refused call named.</param>
    public TenantAccessAdministrationDisabledException(string tenantId)
        : base($"Delegated tenant access administration is disabled on this cluster, so the call for tenant "
            + $"'{tenantId}' was refused. A platform operator can enable it with "
            + "LatticeTenancyOptions.DelegatedAccessAdministrationEnabled.")
    {
        TenantId = tenantId;
    }

    /// <summary>The tenant id the refused call named.</summary>
    public string TenantId { get; }
}

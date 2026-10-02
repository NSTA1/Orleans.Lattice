using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// What the posture probe said about one tenant for the circuit's caller: the
/// standing that decides which pages are shown, and the posture itself (with the
/// tenant's access caps and usage) when it was read.
/// </summary>
/// <param name="Tenant">The tenant the posture was read for.</param>
/// <param name="Standing">The caller's standing towards the tenant.</param>
/// <param name="Posture">The posture as read, or <see langword="null"/> when none was.</param>
internal sealed record TenantAccessState(string Tenant, TenantAccessStanding Standing, TenantAccessPosture? Posture = null)
{
    /// <summary>Whether the tenant Access pages are shown: the feature is on and the caller administers the tenant or is an operator.</summary>
    public bool IsDelegated => Standing == TenantAccessStanding.Delegated;

    /// <summary>The state of a tenant whose posture could not be read.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <returns>An <see cref="TenantAccessStanding.Unavailable"/> state.</returns>
    public static TenantAccessState Unavailable(string tenant) => new(tenant, TenantAccessStanding.Unavailable);

    /// <summary>Classifies a posture the probe answered with.</summary>
    /// <param name="tenant">The tenant the posture was read for.</param>
    /// <param name="posture">The posture.</param>
    /// <returns>The state.</returns>
    public static TenantAccessState From(string tenant, TenantAccessPosture posture)
    {
        ArgumentNullException.ThrowIfNull(posture);
        var standing = !posture.Enabled
            ? TenantAccessStanding.Off
            : posture.CallerIsTenantAdmin || posture.CallerIsPlatformOperator
                ? TenantAccessStanding.Delegated
                : TenantAccessStanding.NotPermitted;
        return new TenantAccessState(tenant, standing, posture);
    }
}

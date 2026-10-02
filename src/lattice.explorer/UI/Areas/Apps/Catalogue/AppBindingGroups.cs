using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// Which groups an app role may be bound to in the tenant an app is installed
/// into, and where the caller goes to join one. A role binds to a cluster group
/// or to one of the installing tenant's own groups (<c>t/{tenant}/{name}</c>),
/// never to another tenant's group: the cluster refuses that binding, and this
/// is the same rule, applied before anything is sent.
/// </summary>
internal static class AppBindingGroups
{
    // The prefix every reserved tenant group id carries.
    private const string TenantGroupPrefix = "t/";

    /// <summary>The sentence beside a binding that names another tenant's group.</summary>
    public const string MismatchMessage =
        "That is another tenant's group. Bind the role to a cluster group or to one of this tenant's own groups.";

    /// <summary>
    /// Whether <paramref name="groupId"/> may be bound in <paramref name="installTenant"/>:
    /// any id outside the reserved <c>t/</c> namespace (a cluster group), or a
    /// well-formed tenant group id of the installing tenant itself. With no
    /// installing tenant (tenancy off) no tenant group id is bindable.
    /// </summary>
    /// <param name="installTenant">The tenant the app is installed into, or <see langword="null"/>.</param>
    /// <param name="groupId">The group a role is bound to.</param>
    /// <returns><see langword="true"/> when the cluster accepts the binding's group.</returns>
    public static bool IsBindable(string? installTenant, string groupId)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        if (!groupId.StartsWith(TenantGroupPrefix, StringComparison.Ordinal))
        {
            return true;
        }

        return installTenant is not null
            && LatticeTenantGroupId.TryParse(groupId, out var tenantGroup)
            && string.Equals(tenantGroup.Tenant.Value, installTenant, StringComparison.Ordinal);
    }

    /// <summary>
    /// Where the caller adds themself to <paramref name="groupId"/>: one of a
    /// tenant's own groups is joined on that tenant's group page, any other group
    /// on the cluster's.
    /// </summary>
    /// <param name="groupId">The group a role is bound to.</param>
    /// <returns>The group's page.</returns>
    public static ExplorerAddress JoinAddress(string groupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return LatticeTenantGroupId.TryParse(groupId, out var tenantGroup)
            ? AccessRoutes.TenantGroup(tenantGroup.Tenant.Value, tenantGroup.Name)
            : AccessRoutes.Group(groupId);
    }
}

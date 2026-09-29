using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The Tenancy area's addresses. The area has two halves: the tenant directory
/// and its administration pages are cluster-wide and never tenant-rooted
/// (<c>/tenancy</c>, <c>/tenancy/{tenant}[/grants|access|regions]</c>), while
/// "my tenant" is always rooted at the tenant it describes
/// (<c>/t/{tenant}/tenancy[/{section}]</c>).
/// </summary>
internal static class TenancyRoutes
{
    /// <summary>The area key.</summary>
    public const string AreaKey = "tenancy";

    /// <summary>The cross-tenant grants page of one tenant.</summary>
    public const string GrantsSegment = "grants";

    /// <summary>The admin-subject page of one tenant.</summary>
    public const string AccessSegment = "access";

    /// <summary>The regions page of one tenant, and the regions section of my tenant.</summary>
    public const string RegionsSegment = "regions";

    /// <summary>The my-tenant section listing the tenant's admin subjects.</summary>
    public const string MembersSegment = "members";

    /// <summary>The my-tenant section showing use against quota.</summary>
    public const string QuotaSegment = "quota";

    /// <summary>The my-tenant section holding cross-tenant grants.</summary>
    public const string SharingSegment = "sharing";

    /// <summary>The query key that opens a page's create form, used by the palette's commands.</summary>
    public const string NewQuery = "new";

    /// <summary>The value of <see cref="NewQuery"/> that opens the form.</summary>
    public const string NewValue = "true";

    /// <summary>
    /// The query key that opens the directory's "Set a tenant's regions" picker,
    /// used by the palette's command; its value is <see cref="NewValue"/>.
    /// </summary>
    public const string SetRegionsQuery = "set-regions";

    /// <summary>The tenant directory.</summary>
    public static ExplorerAddress Directory { get; } = ExplorerAddress.ForArea(AreaKey);

    /// <summary>The administration page of <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant id.</param>
    public static ExplorerAddress Tenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return ExplorerAddress.ForArea(AreaKey, tenantId);
    }

    /// <summary>The cross-tenant grants administration page of <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant id.</param>
    public static ExplorerAddress TenantGrants(string tenantId) => TenantPage(tenantId, GrantsSegment);

    /// <summary>The admin-subject administration page of <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant id.</param>
    public static ExplorerAddress TenantAccess(string tenantId) => TenantPage(tenantId, AccessSegment);

    /// <summary>The region administration page of <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant id.</param>
    public static ExplorerAddress TenantRegions(string tenantId) => TenantPage(tenantId, RegionsSegment);

    /// <summary>
    /// The my-tenant workspace of <paramref name="tenantId"/>, at
    /// <paramref name="section"/> or its overview.
    /// </summary>
    /// <param name="tenantId">The tenant id.</param>
    /// <param name="section">The section segment, or <see langword="null"/> for the overview.</param>
    public static ExplorerAddress MyTenant(string tenantId, string? section = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        var address = section is null ? ExplorerAddress.ForArea(AreaKey) : ExplorerAddress.ForArea(AreaKey, section);
        return address.WithTenant(tenantId);
    }

    /// <summary>The Apps area of <paramref name="tenantId"/>, where its installed apps are listed.</summary>
    /// <param name="tenantId">The tenant id.</param>
    public static ExplorerAddress Apps(string tenantId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return ExplorerAddress.ForArea("apps").WithTenant(tenantId);
    }

    /// <summary>Whether <paramref name="section"/> names a my-tenant section other than the overview.</summary>
    /// <param name="section">The section segment.</param>
    public static bool IsMyTenantSection(string? section) =>
        section is MembersSegment or QuotaSegment or RegionsSegment or SharingSegment;

    private static ExplorerAddress TenantPage(string tenantId, string segment)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return ExplorerAddress.ForArea(AreaKey, tenantId, segment);
    }
}

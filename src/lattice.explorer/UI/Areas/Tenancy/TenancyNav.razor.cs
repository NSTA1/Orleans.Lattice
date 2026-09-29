using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The tab row between one tenant's pages: its administration pages (overview,
/// grants, admin subjects, regions) or, with <see cref="Own"/>, the sections of
/// "my tenant" (overview, members, quota, regions, sharing).
/// </summary>
public partial class TenancyNav
{
    /// <summary>The tenant the pages describe.</summary>
    [Parameter, EditorRequired]
    public string Tenant { get; set; } = string.Empty;

    /// <summary>Whether this is "my tenant", rooted at the tenant, rather than its administration.</summary>
    [Parameter]
    public bool Own { get; set; }

    /// <summary>The current section's segment, or <see langword="null"/> for the overview.</summary>
    [Parameter]
    public string? Current { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    private string Label => Own ? $"Tenant {Tenant}" : $"Administration of tenant {Tenant}";

    private IEnumerable<(string? Section, string Text, ExplorerAddress Address)> Items
    {
        get
        {
            if (string.IsNullOrEmpty(Tenant))
            {
                yield break;
            }

            if (Own)
            {
                yield return (null, "Overview", TenancyRoutes.MyTenant(Tenant));
                yield return (TenancyRoutes.MembersSegment, "Members", TenancyRoutes.MyTenant(Tenant, TenancyRoutes.MembersSegment));
                yield return (TenancyRoutes.QuotaSegment, "Quota", TenancyRoutes.MyTenant(Tenant, TenancyRoutes.QuotaSegment));
                yield return (TenancyRoutes.RegionsSegment, "Regions", TenancyRoutes.MyTenant(Tenant, TenancyRoutes.RegionsSegment));
                yield return (TenancyRoutes.SharingSegment, "Sharing", TenancyRoutes.MyTenant(Tenant, TenancyRoutes.SharingSegment));
                yield break;
            }

            yield return (null, "Overview", TenancyRoutes.Tenant(Tenant));
            yield return (TenancyRoutes.GrantsSegment, "Grants", TenancyRoutes.TenantGrants(Tenant));
            yield return (TenancyRoutes.AccessSegment, "Admin subjects", TenancyRoutes.TenantAccess(Tenant));
            yield return (TenancyRoutes.RegionsSegment, "Regions", TenancyRoutes.TenantRegions(Tenant));
        }
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private string? CurrentFor(string? section) =>
        string.Equals(Current, section, StringComparison.Ordinal) ? "page" : null;
}

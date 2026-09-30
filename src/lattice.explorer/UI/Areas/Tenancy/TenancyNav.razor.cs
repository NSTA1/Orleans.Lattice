using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The tab row between one tenant's pages: its administration pages or, with
/// <see cref="Own"/>, the sections of "my tenant". Both name the same things
/// the same way - overview, members, quota, regions, sharing - so an operator
/// and a tenant admin describe a tenant alike.
/// </summary>
public partial class TenancyNav
{
    /// <summary>The tabs, in order: each section's segment (<see langword="null"/> for the overview) and its name.</summary>
    internal static readonly IReadOnlyList<(string? Section, string Text)> Sections =
    [
        (null, "Overview"),
        (TenancyRoutes.MembersSegment, "Members"),
        (TenancyRoutes.QuotaSegment, "Quota"),
        (TenancyRoutes.RegionsSegment, "Regions"),
        (TenancyRoutes.SharingSegment, "Sharing"),
    ];

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

            foreach (var (section, text) in Sections)
            {
                yield return (section, text, Own ? TenancyRoutes.MyTenant(Tenant, section) : AdministrationPage(section));
            }
        }
    }

    private ExplorerAddress AdministrationPage(string? section) => section switch
    {
        null => TenancyRoutes.Tenant(Tenant),
        TenancyRoutes.MembersSegment => TenancyRoutes.TenantMembers(Tenant),
        TenancyRoutes.QuotaSegment => TenancyRoutes.TenantQuota(Tenant),
        TenancyRoutes.SharingSegment => TenancyRoutes.TenantSharing(Tenant),
        _ => TenancyRoutes.TenantRegions(Tenant),
    };

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private string? CurrentFor(string? section) =>
        string.Equals(Current, section, StringComparison.Ordinal) ? "page" : null;

    // My tenant's Regions tab is the visible control of the palette's "Change residency" command.
    private string? CommandFor(string? section) =>
        Own && string.Equals(section, TenancyRoutes.RegionsSegment, StringComparison.Ordinal) ? TenancyArea.ChangeResidencyCommandId : null;
}

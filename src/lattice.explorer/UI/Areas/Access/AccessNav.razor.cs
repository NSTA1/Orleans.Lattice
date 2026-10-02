using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The Access area's navigation between its sections. Cluster-wide it is rules,
/// groups and explain; at a tenant whose access administration is delegated to
/// the caller it is that tenant's groups, members, rules and explain. The explain
/// link is the visible control of the "Explain access..." command.
/// </summary>
public partial class AccessNav
{
    /// <summary>The current section: <c>rules</c>, <c>groups</c>, <c>members</c> or <c>explain</c>.</summary>
    [Parameter]
    public string? Current { get; set; }

    /// <summary>
    /// The tenant the page's address is rooted at, or <see langword="null"/> on a
    /// cluster-wide page; the links keep it.
    /// </summary>
    [Parameter]
    public string? Tenant { get; set; }

    /// <summary>
    /// Whether the page is one of <see cref="Tenant"/>'s delegated Access pages:
    /// the posture probe reported delegated tenant access administration enabled
    /// and the caller an admin of the tenant or a platform operator. Only the
    /// delegated pages set it, and it is ignored without a <see cref="Tenant"/>.
    /// </summary>
    [Parameter]
    public bool Delegated { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    private bool IsTenantDelegated => Delegated && !string.IsNullOrEmpty(Tenant);

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address.WithTenant(Tenant)).ToHref();

    private string? CurrentFor(string section) =>
        string.Equals(Current, section, StringComparison.Ordinal) ? "page" : null;

    private static string? CommandFor(string section) =>
        string.Equals(section, AccessRoutes.ExplainSegment, StringComparison.Ordinal) ? AccessArea.ExplainCommandId : null;
}

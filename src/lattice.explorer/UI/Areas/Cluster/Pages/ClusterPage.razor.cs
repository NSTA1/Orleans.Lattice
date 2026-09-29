using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// The Cluster area's one routed page (A10, issue #3828): it reads the current
/// address into the page it names - the estate overview, the tree list, a tree's
/// administration, tools or operation status, WAL placement, or orphaned leaves -
/// and renders that page. An address that names none is not found.
/// </summary>
public partial class ClusterPage
{
    private ClusterLocation? _location;

    /// <summary>The seventh path segment below <c>/cluster</c>, for a deep tree path.</summary>
    [Parameter]
    public string? P7 { get; set; }

    /// <summary>The eighth path segment below <c>/cluster</c>, for a deep tree path.</summary>
    [Parameter]
    public string? P8 { get; set; }

    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        _location = ClusterAddresses.Parse(Address);
        if (_location is null && string.Equals(Address.Area, ClusterAddresses.AreaKey, StringComparison.Ordinal))
        {
            Navigation.NotFound();
        }
    }
}

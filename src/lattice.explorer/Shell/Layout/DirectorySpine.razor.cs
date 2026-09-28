using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Layout;

/// <summary>
/// The directory spine on the left of every page: Home, then one stop per shown
/// area in directory order, with the current stop as the marker node.
/// </summary>
/// <remarks>
/// It reads the stops and the current address from the location the layout
/// cascades, so it asks no facade itself. An unavailable area stays on the spine,
/// demoted, with its reason, and its stop leads to the page that explains it.
/// </remarks>
public partial class DirectorySpine
{
    /// <summary>Raised when a stop is followed, so a slide-in panel can close.</summary>
    [Parameter]
    public EventCallback OnNavigate { get; set; }

    /// <summary>
    /// Whether the spine is drawn as a rail - the narrower column of the medium
    /// band - keeping every label but dropping badges and reasons.
    /// </summary>
    [Parameter]
    public bool Rail { get; set; }

    private readonly string _headingId = Design.Components.LtIds.Next("lt-shell-directory-heading");

    private string NavClass => Rail
        ? "lt-spine-nav lt-shell-directory__nav lt-shell-directory__nav--rail"
        : "lt-spine-nav lt-shell-directory__nav";

    [CascadingParameter]
    internal ExplorerLocation? Location { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    private ExplorerLocation CurrentLocation => Location ?? ExplorerLocation.Initial;

    private bool IsHomeCurrent => CurrentLocation.Address.IsHome;

    private ExplorerAddress HomeAddress => Navigator.Canonicalize(ExplorerAddress.Home.WithTenant(CurrentLocation.Address.Tenant));

    private bool IsCurrent(ExplorerAreaEntry entry) =>
        string.Equals(CurrentLocation.Address.Area, entry.Area.Key, StringComparison.Ordinal);

    private ExplorerAddress AreaAddress(ExplorerAreaEntry entry) =>
        Navigator.Canonicalize(ExplorerAddress.ForArea(entry.Area.Key).WithTenant(CurrentLocation.Address.Tenant));
}

using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// The estate's replication links as an order diagram: this region as the marker
/// node, each peer region as a node, and one labelled edge per direction carrying
/// the worst link health and the summed backlog of the trees along it.
/// </summary>
public partial class ReplicationMap
{
    private readonly string _captionId = LtIds.Next("lt-replication-map");
    private IReadOnlyList<ReplicationPeerSummary> _peers = [];

    /// <summary>The reporting (local) region.</summary>
    [Parameter, EditorRequired]
    public string LocalRegionId { get; set; } = string.Empty;

    /// <summary>The links to draw, already filtered by the page.</summary>
    [Parameter, EditorRequired]
    public IReadOnlyList<ReplicationPeerStatusEntry> Links { get; set; } = [];

    /// <summary>Whether the page's filters narrowed <see cref="Links"/>, for the empty sentence.</summary>
    [Parameter]
    public bool Filtered { get; set; }

    [CascadingParameter]
    internal ExplorerLocation? Location { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    internal IReadOnlyList<ReplicationPeerSummary> Peers => _peers;

    private ExplorerAddress Current => Location?.Address ?? Navigator.Current ?? ReplicationAddresses.Estate;

    private string LocalRegionDisplay => LocalRegionId.Length == 0 ? "Unnamed region" : LocalRegionId;

    private string Caption => _peers.Count == 0
        ? $"Replication links of {LocalRegionDisplay}"
        : $"Replication links between {LocalRegionDisplay} and {ReplicationFormat.Count(_peers.Count, "peer region", "peer regions")}";

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        ArgumentNullException.ThrowIfNull(Links);
        _peers = ReplicationPeerSummary.Summarise(Links);
    }

    private bool IsSelected(string region) =>
        string.Equals(Current.GetQuery(ReplicationAddresses.RegionQuery), region, StringComparison.Ordinal);

    private string RegionHref(string region) =>
        Navigator.Canonicalize(Current.WithQuery(ReplicationAddresses.RegionQuery, IsSelected(region) ? null : region)).ToHref();
}

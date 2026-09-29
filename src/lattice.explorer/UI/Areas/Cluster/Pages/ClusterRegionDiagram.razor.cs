using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// The regions this cluster knows, drawn as an order diagram from R1's peer
/// report: this region is the marker node at the top, and each peer hangs from it
/// by a hairline edge. A peer whose every link is stalled is drawn dashed, hollow
/// and labelled "Stalled" - never by colour alone. It links to the Replication
/// area for the links themselves.
/// </summary>
public partial class ClusterRegionDiagram : IDisposable
{
    private const string ReplicationAreaKey = "replication";

    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<ClusterRegionPicture> _picture = ClusterLoad<ClusterRegionPicture>.Loading;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    private string ReplicationHref => Navigator.Canonicalize(ExplorerAddress.ForArea(ReplicationAreaKey)).ToHref();

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _picture = await ClusterLoad<ClusterRegionPicture>.RunAsync(ReadAsync, _lifetime.Token);
    }

    private Task<ClusterRegionPicture> ReadAsync(CancellationToken cancellationToken) =>
        Facades.ReplicationStatus is { } status
            ? ClusterRegionPicture.ReadAsync(status, cancellationToken)
            : Task.FromException<ClusterRegionPicture>(new NotSupportedException("This Explorer does not read the replication peer report, so no region is drawn."));

    private static string Caption(ClusterRegionPicture picture)
    {
        var stalled = picture.Peers.Count(peer => peer.IsStalled);
        var peers = picture.Peers.Count switch
        {
            0 => "no peer region",
            1 => "1 peer region",
            var count => $"{count} peer regions",
        };

        return stalled == 0
            ? $"This region, {picture.LocalRegionId}, and {peers}."
            : $"This region, {picture.LocalRegionId}, and {peers}, {stalled} stalled.";
    }

    private static LtStateRole StateOf(ClusterRegionPeer peer) => peer.Health switch
    {
        ReplicationLinkHealth.Stalled => LtStateRole.Stalled,
        ReplicationLinkHealth.Lagging => LtStateRole.Lagging,
        ReplicationLinkHealth.Healthy => LtStateRole.Healthy,
        _ => LtStateRole.Unknown,
    };

    private static string HealthText(ClusterRegionPeer peer) => peer.Health switch
    {
        ReplicationLinkHealth.Stalled => "Stalled",
        ReplicationLinkHealth.Lagging => "Lagging",
        ReplicationLinkHealth.Healthy => "Healthy",
        _ => "Unknown",
    };

    private static string Detail(ClusterRegionPeer peer) =>
        $"{ClusterFormat.Plural(peer.Links, "link")} across {ClusterFormat.Plural(peer.Trees, "tree")}"
        + (peer.Stalled > 0 && !peer.IsStalled ? $", {peer.Stalled} stalled" : string.Empty);
}

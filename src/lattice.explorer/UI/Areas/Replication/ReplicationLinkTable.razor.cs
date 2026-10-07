using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// Replication links as a booktabs table - one row per <c>(tree, peer, direction)</c> -
/// sortable by health, backlog, consecutive errors and time since contact, and a list
/// of two-line rows with a detail sheet below the compact width.
/// </summary>
public partial class ReplicationLinkTable
{
    /// <summary>Beyond this many links the table virtualises its rows.</summary>
    internal const int VirtualizeThreshold = 200;

    /// <summary>The links, in the order they are first shown.</summary>
    [Parameter, EditorRequired]
    public IReadOnlyList<ReplicationPeerStatusEntry> Links { get; set; } = [];

    /// <summary>The table's caption.</summary>
    [Parameter, EditorRequired]
    public string Caption { get; set; } = string.Empty;

    /// <summary>Whether the caption is for assistive technology only.</summary>
    [Parameter]
    public bool CaptionHidden { get; set; }

    /// <summary>
    /// Whether to show the tree column; a tree's own page hides it, and the peer
    /// becomes the row header.
    /// </summary>
    [Parameter]
    public bool ShowTree { get; set; } = true;

    /// <summary>The sentence shown when there are no links.</summary>
    [Parameter]
    public string EmptyText { get; set; } = "No replication links match these filters.";

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        ArgumentNullException.ThrowIfNull(Links);
        ArgumentException.ThrowIfNullOrWhiteSpace(Caption);
    }

    private static object RowKeyOf(ReplicationPeerStatusEntry link) =>
        (link.TreeId, link.PeerRegionId, link.Direction);

    private static string CompactSummary(ReplicationPeerStatusEntry link) =>
        (link.Direction == ReplicationLinkDirection.Outbound ? "To " : "From ")
        + link.PeerRegionId + " - " + ReplicationFormat.Backlog(link.EntriesBehind, link.BytesBehind)
        + (ReplicationFormat.StallReason(link.StallReason) is { } reason ? " - " + reason : string.Empty);

    private string DetailTitleOf(ReplicationPeerStatusEntry link) =>
        (ShowTree ? link.TreeId + ": " : string.Empty)
        + (link.Direction == ReplicationLinkDirection.Outbound ? "to " : "from ") + link.PeerRegionId;

    private string? TreeHref(string treeId) =>
        ReplicationAddresses.ForTree(treeId) is { } address ? Navigator.Canonicalize(address).ToHref() : null;
}

using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A link to one of a tree's Cluster pages, by logical id. A tree whose id has no
/// address (too deep for the area's routes) renders as plain mono text instead.
/// </summary>
public partial class ClusterTreeLink
{
    private string? _href;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>The link text; the tree id when <see langword="null"/>.</summary>
    [Parameter]
    public string? Text { get; set; }

    /// <summary>Which of the tree's pages to link to, by its address word: tools, reshard, resize or snapshot. The tree's own page when <see langword="null"/>.</summary>
    [Parameter]
    public string? View { get; set; }

    /// <summary>The tenant a tenant-rooted address names, or <see langword="null"/> on a cluster-wide address; links keep it.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        var view = View switch
        {
            "tools" => ClusterTreeView.Tools,
            "reshard" => ClusterTreeView.Reshard,
            "resize" => ClusterTreeView.Resize,
            "snapshot" => ClusterTreeView.Snapshot,
            _ => ClusterTreeView.Overview,
        };

        _href = ClusterAddresses.TryTree(TreeId, view, out var address)
            ? Navigator.Canonicalize(address.WithTenant(Scope)).ToHref()
            : null;
    }
}

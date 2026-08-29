using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Plugins.Topology;

/// <summary>
/// The topology surface's state, load path and radial layout. The markup lives
/// in <c>TopologyTab.razor</c> and the browser-side pan / zoom module lifetime
/// in <c>TopologyTab.Interop.cs</c>; all three are one partial class.
/// </summary>
public partial class TopologyTab
{
    private static readonly IReadOnlyList<TopologyNode> NoRoots = Array.Empty<TopologyNode>();

    private IReadOnlyList<TopologyNode> _roots = NoRoots;
    private TopologyGraph? _graph;
    private Dictionary<string, RadialPoint> _points = new(StringComparer.Ordinal);
    private string _homeViewBox = HomeViewBoxFallback;
    private bool _showLeaves;
    private bool _truncated;
    private bool _loading;
    private string? _error;

    private ElementReference _svg;

    private const string HomeViewBoxFallback = "0 0 100 100";

    private static string F(double value) => value.ToString("0.##", CultureInfo.InvariantCulture);

    /// <inheritdoc />
    protected override async Task OnInitializedAsync() => await LoadAsync();

    private async Task LoadAsync()
    {
        _loading = true;
        _error = null;
        StateHasChanged();

        try
        {
            var fetch = await Surface.GetAsync(Selection.Id, TabToken);
            _roots = fetch.Roots;
            _truncated = fetch.Truncated;
            Rebuild();
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _error = ex.Message;
        }
        finally
        {
            _loading = false;
            StateHasChanged();
        }
    }

    private void OnToggleLeaves(ChangeEventArgs e)
    {
        _showLeaves = e.Value is true;
        Rebuild();
    }

    private async Task OnNodeClickAsync(TopologyNode node)
    {
        if (!node.HasMoreChildren || _loading)
        {
            return;
        }

        _loading = true;
        StateHasChanged();
        try
        {
            var fetch = await Surface.ExpandAsync(Selection.Id, node.ShardIndex, node.NodeId, TabToken);
            var children = fetch.Roots.Count == 1 && string.Equals(fetch.Roots[0].NodeId, node.NodeId, StringComparison.Ordinal)
                ? fetch.Roots[0].Children
                : fetch.Roots;
            _roots = TopologyTree.WithExpanded(_roots, node.NodeId, children);
            Rebuild();
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _error = ex.Message;
        }
        finally
        {
            _loading = false;
            StateHasChanged();
        }
    }

    private void Rebuild()
    {
        _graph = TopologyLayout.Build(_roots, _showLeaves);
        BuildRadial();
    }

    private void BuildRadial()
    {
        _points = new Dictionary<string, RadialPoint>(StringComparer.Ordinal);

        if (_graph is null || _graph.Nodes.Count == 0)
        {
            _homeViewBox = HomeViewBoxFallback;
            return;
        }

        var frame = RadialFrame.Build(_graph);
        foreach (var node in _graph.Nodes)
        {
            _points[node.Node.NodeId] = frame.Project(node.Column, node.Level);
        }

        var extent = frame.Extent;
        var size = extent * 2;
        _homeViewBox = $"{F(-extent)} {F(-extent)} {F(size)} {F(size)}";
    }

    private static string NodeTooltip(PositionedNode node)
    {
        var n = node.Node;
        var kind = n.Kind switch
        {
            NodeKind.ShardRoot => "shard root",
            NodeKind.Internal => "internal",
            NodeKind.Leaf => "leaf",
            _ => n.Kind.ToString(),
        };
        return $"{kind} (shard {n.ShardIndex}, depth {n.Depth})\nkeys: {n.SubtreeKeyCount:N0}, tombstones: {n.SubtreeTombstoneCount:N0}";
    }
}

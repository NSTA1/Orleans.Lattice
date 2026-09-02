using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Topology;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Plugins.Topology;

/// <summary>
/// The topology surface's state, load path and radial layout. The markup lives
/// in <c>TopologyTab.razor</c> and the browser-side pan / zoom module lifetime
/// in <c>TopologyTab.Interop.cs</c>; all three are one partial class.
/// </summary>
public partial class TopologyTab
{
    private static readonly IReadOnlyList<TopologyNode> NoRoots = Array.Empty<TopologyNode>();

    /// <summary>
    /// The explanation behind the "truncated" marker, from one place rather than
    /// a tooltip no keyboard or touch caller could reach.
    /// </summary>
    private const string TruncatedExplanation =
        "The graph stopped early because it reached the depth or node budget, so what you see is "
        + "a complete subtree rather than the whole structure. Expand a node marked '+' to fetch "
        + "the rest of that branch.";

    /// <summary>
    /// What the surface says when the cluster answered but reported no nodes.
    /// Built once, and explicit that this is not a refusal: "No structure
    /// reported for this id." read equally well as a missing grant.
    /// </summary>
    private static readonly ExplorerStateMessage NoStructureReported =
        ExplorerStateCopy.Empty(ExplorerSubjects.Shards) with
        {
            Headline = "No structure for this selection",
            Explanation = "The cluster answered but reported no shard roots or nodes for this id. "
                + "Nothing is being hidden from you: the table may hold no data yet, or it may "
                + "have been removed since the catalog was listed.",
            Remedy = "Refresh, or check that the table still exists in the catalog.",
            ActionLabel = ExplorerVocabulary.RetryAction,
        };

    private IReadOnlyList<TopologyNode> _roots = NoRoots;
    private TopologyGraph? _graph;
    private Dictionary<string, RadialPoint> _points = new(StringComparer.Ordinal);
    private Dictionary<string, NodeVisual> _visuals = new(StringComparer.Ordinal);
    private string _homeViewBox = HomeViewBoxFallback;
    private bool _showLeaves;
    private bool _truncated;
    private bool _loading;
    private string? _error;
    private ExplorerStateMessage? _failure;

    private EventCallback _reload;

    private ElementReference _svg;

    private const string HomeViewBoxFallback = "0 0 100 100";

    private static string F(double value) => value.ToString("0.##", CultureInfo.InvariantCulture);

    /// <summary>
    /// The state the surface is in, or <see langword="null"/> when it has a
    /// graph to draw. A cluster that answered with nothing is deliberately not
    /// worded as a failure, and a failure is deliberately not worded as an
    /// empty tree.
    /// </summary>
    /// <remarks>
    /// Read on the render path, and twice per pass, so it only ever selects an
    /// already-built message. The failure copy quotes the cluster's own words
    /// and therefore has to be composed; it is composed once, where the failure
    /// is caught.
    /// </remarks>
    private ExplorerStateMessage? State
    {
        get
        {
            if (_failure is not null)
            {
                return _failure;
            }

            if (_graph is { Nodes.Count: > 0 })
            {
                return null;
            }

            return _loading
                ? ExplorerStateCopy.Loading(ExplorerSubjects.Shards)
                : NoStructureReported;
        }
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        // Bound once rather than per render, so the state block's retry does not
        // allocate a delegate on every pass.
        _reload = EventCallback.Factory.Create(this, LoadAsync);
        await LoadAsync();
    }

    private async Task LoadAsync()
    {
        _loading = true;
        _error = null;
        _failure = null;
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
            Fail(ex);
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
            Fail(ex);
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

    /// <summary>
    /// Records a failed read, composing the copy here rather than in the
    /// <c>State</c> property: the failure quotes the cluster's own words and so
    /// has to be built, and the property is read on every render pass.
    /// </summary>
    private void Fail(Exception ex)
    {
        _error = ex.Message;
        _failure = ExplorerStateCopy.Failed(ExplorerSubjects.Shards, _error);
    }

    /// <summary>
    /// Activates an expandable node from the keyboard. Enter and Space are the
    /// two keys a <c>role="button"</c> owes its caller; an SVG group gets
    /// neither for free, which is why the expansion path used to be reachable
    /// only with a pointer.
    /// </summary>
    /// <remarks>
    /// Space's own default (scrolling the nearest scroll container) is not
    /// suppressed. Blazor's <c>preventDefault</c> is a render-time flag rather
    /// than a per-event decision, so suppressing it here would also suppress
    /// Tab's default and trap focus inside the graph - a worse defect than the
    /// one being fixed. Activation is what matters and it works; the graph's own
    /// canvas does not scroll, so in the ordinary case nothing moves.
    /// </remarks>
    private Task OnNodeKeyDownAsync(KeyboardEventArgs args, TopologyNode node) =>
        string.Equals(args.Key, "Enter", StringComparison.Ordinal)
        || string.Equals(args.Key, " ", StringComparison.Ordinal)
            ? OnNodeClickAsync(node)
            : Task.CompletedTask;

    private void BuildRadial()
    {
        _points = new Dictionary<string, RadialPoint>(StringComparer.Ordinal);
        _visuals = new Dictionary<string, NodeVisual>(StringComparer.Ordinal);

        if (_graph is null || _graph.Nodes.Count == 0)
        {
            _homeViewBox = HomeViewBoxFallback;
            return;
        }

        var frame = RadialFrame.Build(_graph);
        foreach (var node in _graph.Nodes)
        {
            var point = frame.Project(node.Column, node.Level);
            _points[node.Node.NodeId] = point;

            // Every per-node string the render path needs is composed here, on
            // the layout pass, and read out of the dictionary afterwards. The
            // node loop runs on every render of every pan, zoom and reflow, so
            // composing a name, a transform and two glyph fragments there cost
            // four allocations per node per pass.
            _visuals[node.Node.NodeId] = new NodeVisual(
                Describe(node),
                "translate(" + F(point.X) + "," + F(point.Y) + ")",
                BuildGlyphs(node));
        }

        var extent = frame.Extent;
        var size = extent * 2;
        _homeViewBox = $"{F(-extent)} {F(-extent)} {F(size)} {F(size)}";
    }

    /// <summary>
    /// The glyphs drawn inside a node: its leaf-count badge and, when it has
    /// unfetched children, the expansion marker.
    /// </summary>
    /// <remarks>
    /// Emitted as markup rather than as Razor elements because <c>&lt;text&gt;</c>
    /// is Razor's own markup-transition tag inside a code block and would never
    /// reach the document as an SVG element. The expansion marker is
    /// <c>aria-hidden</c>: the node already carries the whole story in its
    /// accessible name, and a lone "+" adds nothing but noise.
    /// </remarks>
    private static MarkupString BuildGlyphs(PositionedNode node)
    {
        var badge = node.LeafBadge > 0;
        var expandable = node.Node.HasMoreChildren;

        if (!badge && !expandable)
        {
            return default;
        }

        var markup = string.Empty;
        if (badge)
        {
            markup += "<text class=\"lx-topo-badge\" x=\"0\" y=\"4\">"
                + node.LeafBadge.ToString(CultureInfo.InvariantCulture)
                + "</text>";
        }

        if (expandable)
        {
            markup += "<text class=\"lx-topo-expand\" aria-hidden=\"true\" x=\"0\" y=\""
                + F(RadialLayout.NodeRadius + 12)
                + "\">+</text>";
        }

        return (MarkupString)markup;
    }

    /// <summary>
    /// A node's accessible name and its visual tooltip - one string, so the two
    /// cannot say different things. Composed once per layout pass.
    /// </summary>
    private static string Describe(PositionedNode node)
    {
        var n = node.Node;
        var kind = n.Kind switch
        {
            NodeKind.ShardRoot => "shard root",
            NodeKind.Internal => "internal",
            NodeKind.Leaf => "leaf",
            _ => n.Kind.ToString(),
        };

        var name = $"{kind} (shard {n.ShardIndex}, depth {n.Depth}), keys: {n.SubtreeKeyCount:N0}, tombstones: {n.SubtreeTombstoneCount:N0}";
        return n.HasMoreChildren ? name + ". Activate to expand." : name;
    }

    /// <summary>
    /// The per-node strings the render path reads: everything about drawing one
    /// node that does not change between renders of the same layout.
    /// </summary>
    private readonly record struct NodeVisual(string Label, string Transform, MarkupString Glyphs);
}

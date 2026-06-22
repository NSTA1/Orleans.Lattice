using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// Lays out a topology forest for rendering: assigns each node an abstract grid
/// position, buckets nodes onto a cool-to-hot load colour scale, and collapses
/// leaves into a per-parent count badge when leaf display is off.
/// </summary>
/// <remarks>
/// <para>
/// <b>Load scale.</b> A node's load bucket is derived from its
/// <see cref="TopologyNode.SubtreeKeyCount"/> (the live keys beneath it),
/// normalised against the busiest visible node. The normalised ratio is mapped
/// into <see cref="LoadBuckets"/> buckets, <c>0</c> (coolest) to
/// <see cref="LoadBuckets"/> - 1 (hottest). Using subtree key density keeps the
/// graph dependent only on the structure response; per-shard ops/sec hotness is
/// a future blend.
/// </para>
/// <para>
/// <b>Leaf collapsing.</b> With leaves off (the default), leaf nodes are not
/// placed; each parent instead carries a <see cref="PositionedNode.LeafBadge"/>
/// equal to the number of leaf children it owns. With leaves on, leaves are
/// placed as individual nodes and no badge is shown.
/// </para>
/// </remarks>
public static class TopologyLayout
{
    /// <summary>The number of buckets on the load colour scale.</summary>
    public const int LoadBuckets = 5;

    /// <summary>Builds the positioned graph for the given roots.</summary>
    /// <param name="roots">The shard roots (or an expanded subtree's roots).</param>
    /// <param name="showLeaves">Whether to place leaf nodes individually.</param>
    public static TopologyGraph Build(IReadOnlyList<TopologyNode> roots, bool showLeaves)
    {
        ArgumentNullException.ThrowIfNull(roots);

        if (roots.Count == 0)
        {
            return TopologyGraph.Empty;
        }

        var visible = new List<(TopologyNode Node, int Level)>();
        CollectVisible(roots, level: 0, showLeaves, visible);

        if (visible.Count == 0)
        {
            return TopologyGraph.Empty;
        }

        var maxLoad = visible.Max(v => v.Node.SubtreeKeyCount);

        var positions = new Dictionary<string, (double Column, int Level)>(StringComparer.Ordinal);
        var nodes = new List<PositionedNode>(visible.Count);
        double nextColumn = 0;

        foreach (var root in roots)
        {
            AssignColumns(root, level: 0, showLeaves, maxLoad, ref nextColumn, positions, nodes);
        }

        var edges = BuildEdges(roots, showLeaves, positions);

        var levelCount = visible.Max(v => v.Level) + 1;
        var columnCount = (int)Math.Ceiling(nextColumn);

        return new TopologyGraph
        {
            Nodes = nodes,
            Edges = edges,
            ColumnCount = columnCount,
            LevelCount = levelCount,
            MaxLoad = maxLoad,
        };
    }

    /// <summary>Maps a subtree key count to a load bucket against a ceiling.</summary>
    public static int LoadBucketFor(long subtreeKeyCount, long maxLoad)
    {
        if (maxLoad <= 0 || subtreeKeyCount <= 0)
        {
            return 0;
        }

        var ratio = (double)subtreeKeyCount / maxLoad;
        var bucket = (int)Math.Floor(ratio * LoadBuckets);
        return Math.Clamp(bucket, 0, LoadBuckets - 1);
    }

    private static IReadOnlyList<TopologyNode> VisibleChildren(TopologyNode node, bool showLeaves)
    {
        if (node.Children.Count == 0)
        {
            return Array.Empty<TopologyNode>();
        }

        if (showLeaves)
        {
            return node.Children;
        }

        return node.Children.Where(c => c.Kind != NodeKind.Leaf).ToArray();
    }

    private static int LeafChildCount(TopologyNode node) =>
        node.Children.Count(c => c.Kind == NodeKind.Leaf);

    private static void CollectVisible(
        IReadOnlyList<TopologyNode> nodes,
        int level,
        bool showLeaves,
        List<(TopologyNode, int)> visible)
    {
        foreach (var node in nodes)
        {
            visible.Add((node, level));
            CollectVisible(VisibleChildren(node, showLeaves), level + 1, showLeaves, visible);
        }
    }

    private static double AssignColumns(
        TopologyNode node,
        int level,
        bool showLeaves,
        long maxLoad,
        ref double nextColumn,
        Dictionary<string, (double, int)> positions,
        List<PositionedNode> nodes)
    {
        var children = VisibleChildren(node, showLeaves);

        double column;
        if (children.Count == 0)
        {
            column = nextColumn;
            nextColumn += 1;
        }
        else
        {
            double sum = 0;
            foreach (var child in children)
            {
                sum += AssignColumns(child, level + 1, showLeaves, maxLoad, ref nextColumn, positions, nodes);
            }

            column = sum / children.Count;
        }

        positions[node.NodeId] = (column, level);
        nodes.Add(new PositionedNode
        {
            Node = node,
            Column = column,
            Level = level,
            LoadBucket = LoadBucketFor(node.SubtreeKeyCount, maxLoad),
            LeafBadge = showLeaves ? 0 : LeafChildCount(node),
        });

        return column;
    }

    private static List<GraphEdge> BuildEdges(
        IReadOnlyList<TopologyNode> roots,
        bool showLeaves,
        Dictionary<string, (double Column, int Level)> positions)
    {
        var edges = new List<GraphEdge>();
        var stack = new Stack<TopologyNode>(roots);

        while (stack.Count > 0)
        {
            var node = stack.Pop();
            if (!positions.TryGetValue(node.NodeId, out var from))
            {
                continue;
            }

            foreach (var child in VisibleChildren(node, showLeaves))
            {
                if (!positions.TryGetValue(child.NodeId, out var to))
                {
                    continue;
                }

                edges.Add(new GraphEdge
                {
                    FromId = node.NodeId,
                    ToId = child.NodeId,
                    FromColumn = from.Column,
                    FromLevel = from.Level,
                    ToColumn = to.Column,
                    ToLevel = to.Level,
                });
                stack.Push(child);
            }
        }

        return edges;
    }
}

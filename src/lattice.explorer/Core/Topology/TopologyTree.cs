namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// Pure tree transforms used when lazily expanding the topology view.
/// </summary>
public static class TopologyTree
{
    /// <summary>
    /// Returns a copy of <paramref name="roots"/> in which the node whose id is
    /// <paramref name="nodeId"/> has its children replaced with
    /// <paramref name="children"/> and its <see cref="TopologyNode.HasMoreChildren"/>
    /// flag cleared. The original forest is left unchanged; nodes not on the path
    /// to the target are shared by reference.
    /// </summary>
    public static IReadOnlyList<TopologyNode> WithExpanded(
        IReadOnlyList<TopologyNode> roots,
        string nodeId,
        IReadOnlyList<TopologyNode> children)
    {
        ArgumentNullException.ThrowIfNull(roots);
        ArgumentException.ThrowIfNullOrEmpty(nodeId);
        ArgumentNullException.ThrowIfNull(children);

        var result = new TopologyNode[roots.Count];
        for (var i = 0; i < roots.Count; i++)
        {
            result[i] = Replace(roots[i], nodeId, children);
        }

        return result;
    }

    private static TopologyNode Replace(TopologyNode node, string nodeId, IReadOnlyList<TopologyNode> children)
    {
        if (string.Equals(node.NodeId, nodeId, StringComparison.Ordinal))
        {
            return node with
            {
                Children = children,
                ChildCount = children.Count,
                HasMoreChildren = false,
            };
        }

        if (node.Children.Count == 0)
        {
            return node;
        }

        TopologyNode[]? rebuilt = null;
        for (var i = 0; i < node.Children.Count; i++)
        {
            var replaced = Replace(node.Children[i], nodeId, children);
            if (!ReferenceEquals(replaced, node.Children[i]))
            {
                rebuilt ??= node.Children.ToArray();
                rebuilt[i] = replaced;
            }
        }

        return rebuilt is null ? node : node with { Children = rebuilt };
    }
}

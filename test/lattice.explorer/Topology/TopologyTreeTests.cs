using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Tests.Topology;

[TestFixture]
public class TopologyTreeTests
{
    private static TopologyNode Node(string id, bool hasMore = false, IReadOnlyList<TopologyNode>? children = null) =>
        new()
        {
            NodeId = id,
            HasMoreChildren = hasMore,
            ChildCount = children?.Count ?? 0,
            Children = children ?? Array.Empty<TopologyNode>(),
        };

    [Test]
    public void WithExpanded_ReplacesMatchingNodeChildrenAndClearsFlag()
    {
        var roots = new[]
        {
            Node("r", children: new[] { Node("a", hasMore: true) }),
        };

        var result = TopologyTree.WithExpanded(roots, "a", new[] { Node("a1"), Node("a2") });

        var a = result.Single().Children.Single();
        Assert.Multiple(() =>
        {
            Assert.That(a.NodeId, Is.EqualTo("a"));
            Assert.That(a.HasMoreChildren, Is.False);
            Assert.That(a.ChildCount, Is.EqualTo(2));
            Assert.That(a.Children.Select(c => c.NodeId), Is.EqualTo(new[] { "a1", "a2" }));
        });
    }

    [Test]
    public void WithExpanded_LeavesOriginalForestUnchanged()
    {
        var target = Node("a", hasMore: true);
        var roots = new[] { Node("r", children: new[] { target }) };

        _ = TopologyTree.WithExpanded(roots, "a", new[] { Node("a1") });

        Assert.That(target.Children, Is.Empty);
        Assert.That(target.HasMoreChildren, Is.True);
    }

    [Test]
    public void WithExpanded_UnknownId_ReturnsEquivalentForest()
    {
        var roots = new[] { Node("r", children: new[] { Node("a") }) };

        var result = TopologyTree.WithExpanded(roots, "missing", new[] { Node("x") });

        Assert.That(result.Single().Children.Single().NodeId, Is.EqualTo("a"));
    }
}

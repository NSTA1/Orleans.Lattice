using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Tests.Topology;

[TestFixture]
public class TopologyLayoutTests
{
    private static TopologyNode Node(
        string id,
        NodeKind kind = NodeKind.Internal,
        long keys = 0,
        IReadOnlyList<TopologyNode>? children = null,
        bool hasMore = false,
        int shard = 0) =>
        new()
        {
            NodeId = id,
            Kind = kind,
            ShardIndex = shard,
            SubtreeKeyCount = keys,
            HasMoreChildren = hasMore,
            ChildCount = children?.Count ?? 0,
            Children = children ?? Array.Empty<TopologyNode>(),
        };

    [Test]
    public void Build_EmptyRoots_ReturnsEmptyGraph()
    {
        var graph = TopologyLayout.Build(Array.Empty<TopologyNode>(), showLeaves: false);

        Assert.That(graph.Nodes, Is.Empty);
        Assert.That(graph.Edges, Is.Empty);
    }

    [Test]
    public void LoadBucketFor_NonPositiveInputs_ReturnZero()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TopologyLayout.LoadBucketFor(10, 0), Is.EqualTo(0));
            Assert.That(TopologyLayout.LoadBucketFor(0, 100), Is.EqualTo(0));
        });
    }

    [Test]
    public void LoadBucketFor_MaxLoad_MapsToTopBucket()
    {
        Assert.That(TopologyLayout.LoadBucketFor(100, 100), Is.EqualTo(TopologyLayout.LoadBuckets - 1));
    }

    [Test]
    public void LoadBucketFor_DistributesAcrossBuckets()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TopologyLayout.LoadBucketFor(19, 100), Is.EqualTo(0));
            Assert.That(TopologyLayout.LoadBucketFor(20, 100), Is.EqualTo(1));
            Assert.That(TopologyLayout.LoadBucketFor(60, 100), Is.EqualTo(3));
        });
    }

    [Test]
    public void Build_InternalNode_IsCenteredOverChildren()
    {
        var root = Node("r", NodeKind.ShardRoot, keys: 10, children: new[]
        {
            Node("a", NodeKind.Internal, keys: 5),
            Node("b", NodeKind.Internal, keys: 5),
        });

        var graph = TopologyLayout.Build(new[] { root }, showLeaves: true);

        var rootPos = graph.Nodes.Single(n => n.Node.NodeId == "r");
        var a = graph.Nodes.Single(n => n.Node.NodeId == "a");
        var b = graph.Nodes.Single(n => n.Node.NodeId == "b");

        Assert.Multiple(() =>
        {
            Assert.That(rootPos.Level, Is.EqualTo(0));
            Assert.That(a.Level, Is.EqualTo(1));
            Assert.That(b.Level, Is.EqualTo(1));
            Assert.That(rootPos.Column, Is.EqualTo((a.Column + b.Column) / 2).Within(1e-9));
        });
    }

    [Test]
    public void Build_LeavesOff_CollapsesLeavesIntoBadge()
    {
        var root = Node("r", NodeKind.ShardRoot, keys: 10, children: new[]
        {
            Node("l1", NodeKind.Leaf, keys: 5),
            Node("l2", NodeKind.Leaf, keys: 5),
        });

        var graph = TopologyLayout.Build(new[] { root }, showLeaves: false);

        Assert.Multiple(() =>
        {
            Assert.That(graph.Nodes.Select(n => n.Node.NodeId), Is.EquivalentTo(new[] { "r" }));
            Assert.That(graph.Nodes.Single().LeafBadge, Is.EqualTo(2));
            Assert.That(graph.Edges, Is.Empty);
        });
    }

    [Test]
    public void Build_LeavesOn_PlacesLeavesWithoutBadge()
    {
        var root = Node("r", NodeKind.ShardRoot, keys: 10, children: new[]
        {
            Node("l1", NodeKind.Leaf, keys: 5),
            Node("l2", NodeKind.Leaf, keys: 5),
        });

        var graph = TopologyLayout.Build(new[] { root }, showLeaves: true);

        Assert.Multiple(() =>
        {
            Assert.That(graph.Nodes.Select(n => n.Node.NodeId), Is.EquivalentTo(new[] { "r", "l1", "l2" }));
            Assert.That(graph.Nodes.Single(n => n.Node.NodeId == "r").LeafBadge, Is.EqualTo(0));
            Assert.That(graph.Edges, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Build_AssignsLoadBucketsFromBusiestVisibleNode()
    {
        var root = Node("r", NodeKind.ShardRoot, keys: 100, children: new[]
        {
            Node("a", NodeKind.Internal, keys: 100),
            Node("b", NodeKind.Internal, keys: 0),
        });

        var graph = TopologyLayout.Build(new[] { root }, showLeaves: true);

        Assert.Multiple(() =>
        {
            Assert.That(graph.MaxLoad, Is.EqualTo(100));
            Assert.That(graph.Nodes.Single(n => n.Node.NodeId == "a").LoadBucket, Is.EqualTo(TopologyLayout.LoadBuckets - 1));
            Assert.That(graph.Nodes.Single(n => n.Node.NodeId == "b").LoadBucket, Is.EqualTo(0));
        });
    }

    [Test]
    public void Build_BuildsEdgesBetweenVisibleNodes()
    {
        var root = Node("r", NodeKind.ShardRoot, children: new[]
        {
            Node("a", NodeKind.Internal, children: new[] { Node("a1", NodeKind.Internal) }),
        });

        var graph = TopologyLayout.Build(new[] { root }, showLeaves: false);

        Assert.That(graph.Edges.Select(e => (e.FromId, e.ToId)),
            Is.EquivalentTo(new[] { ("r", "a"), ("a", "a1") }));
    }
}

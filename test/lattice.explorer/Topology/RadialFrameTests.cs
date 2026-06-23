using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Tests.Topology;

[TestFixture]
public class RadialFrameTests
{
    private static TopologyNode Node(
        string id,
        NodeKind kind = NodeKind.Internal,
        long keys = 0,
        IReadOnlyList<TopologyNode>? children = null,
        int shard = 0) =>
        new()
        {
            NodeId = id,
            Kind = kind,
            ShardIndex = shard,
            SubtreeKeyCount = keys,
            ChildCount = children?.Count ?? 0,
            Children = children ?? Array.Empty<TopologyNode>(),
        };

    private static double ClosestSameLevelDistance(TopologyGraph graph, RadialFrame frame)
    {
        var byLevel = graph.Nodes
            .GroupBy(n => n.Level)
            .Select(g => g.Select(n => frame.Project(n.Column, n.Level)).ToArray());

        var closest = double.PositiveInfinity;
        foreach (var points in byLevel)
        {
            for (var i = 0; i < points.Length; i++)
            {
                for (var j = i + 1; j < points.Length; j++)
                {
                    var dx = points[i].X - points[j].X;
                    var dy = points[i].Y - points[j].Y;
                    var distance = Math.Sqrt((dx * dx) + (dy * dy));
                    if (distance < closest)
                    {
                        closest = distance;
                    }
                }
            }
        }

        return closest;
    }

    private static TopologyGraph WideForest(int shardCount, int leavesPerShard, bool showLeaves)
    {
        var roots = new List<TopologyNode>(shardCount);
        for (var s = 0; s < shardCount; s++)
        {
            var leaves = new List<TopologyNode>(leavesPerShard);
            for (var l = 0; l < leavesPerShard; l++)
            {
                leaves.Add(Node($"s{s}-l{l}", NodeKind.Leaf, keys: 50, shard: s));
            }

            roots.Add(Node($"s{s}", NodeKind.Internal, keys: 500, children: leaves, shard: s));
        }

        return TopologyLayout.Build(roots, showLeaves);
    }

    [Test]
    public void Build_EmptyGraph_HasNoRingsAndPositiveExtent()
    {
        var frame = RadialFrame.Build(TopologyGraph.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(frame.RingRadii, Is.Empty);
            Assert.That(frame.Extent, Is.GreaterThan(0));
            Assert.That(frame.RingRadiusFor(0), Is.EqualTo(0));
        });
    }

    [Test]
    public void Build_SmallTree_KeepsCompactBaselineRings()
    {
        // Two shard roots, three leaves each: small enough that the fixed
        // baseline already separates nodes, so density scaling is a no-op.
        var graph = WideForest(shardCount: 2, leavesPerShard: 3, showLeaves: true);
        var frame = RadialFrame.Build(graph);

        Assert.Multiple(() =>
        {
            Assert.That(frame.RingRadii, Has.Count.EqualTo(2));
            Assert.That(frame.RingRadiusFor(0), Is.EqualTo(RadialLayout.RingRadius(0, rootCount: 2)));
            Assert.That(frame.RingRadiusFor(1), Is.EqualTo(RadialLayout.RingRadius(1, rootCount: 2)));
        });
    }

    [Test]
    public void Build_DenseLeafRing_SeparatesEveryLeaf()
    {
        // 64 shard roots x 9 leaves: the leaf ring is far too crowded for the
        // fixed baseline and would otherwise smear into an overlapping donut.
        var graph = WideForest(shardCount: 64, leavesPerShard: 9, showLeaves: true);
        var frame = RadialFrame.Build(graph);

        var closest = ClosestSameLevelDistance(graph, frame);

        Assert.Multiple(() =>
        {
            Assert.That(graph.Nodes, Has.Count.EqualTo(64 + (64 * 9)));
            // No two markers on the same ring overlap (centres at least a diameter apart).
            Assert.That(closest, Is.GreaterThanOrEqualTo(2 * RadialLayout.NodeRadius));
            // The dense leaf ring sits well outside the fixed baseline.
            Assert.That(frame.RingRadiusFor(1), Is.GreaterThan(RadialLayout.RingRadius(1, rootCount: 64)));
        });
    }

    [Test]
    public void Build_DenseSingleRing_SeparatesNodesEvenWithLeavesHidden()
    {
        // Default (leaves hidden) view of a wide tree: 64 shard roots on one
        // inner ring still overcrowd the fixed baseline.
        var graph = WideForest(shardCount: 64, leavesPerShard: 9, showLeaves: false);
        var frame = RadialFrame.Build(graph);

        var closest = ClosestSameLevelDistance(graph, frame);

        Assert.Multiple(() =>
        {
            Assert.That(graph.Nodes, Has.Count.EqualTo(64));
            Assert.That(closest, Is.GreaterThanOrEqualTo(2 * RadialLayout.NodeRadius));
        });
    }

    [Test]
    public void Build_RingsAreStrictlyOrderedOutward()
    {
        var graph = WideForest(shardCount: 64, leavesPerShard: 9, showLeaves: true);
        var frame = RadialFrame.Build(graph);

        for (var level = 1; level < frame.RingRadii.Count; level++)
        {
            Assert.That(
                frame.RingRadii[level],
                Is.GreaterThanOrEqualTo(frame.RingRadii[level - 1] + RadialLayout.MinRingGap));
        }
    }

    [Test]
    public void RingRadiusFor_OutOfRangeLevel_ClampsIntoRange()
    {
        var graph = WideForest(shardCount: 2, leavesPerShard: 3, showLeaves: true);
        var frame = RadialFrame.Build(graph);

        Assert.Multiple(() =>
        {
            Assert.That(frame.RingRadiusFor(-5), Is.EqualTo(frame.RingRadii[0]));
            Assert.That(frame.RingRadiusFor(99), Is.EqualTo(frame.RingRadii[^1]));
        });
    }

    [Test]
    public void Extent_FramesOutermostRingWithMargin()
    {
        var graph = WideForest(shardCount: 64, leavesPerShard: 9, showLeaves: true);
        var frame = RadialFrame.Build(graph);

        var content = frame.RingRadii[^1] + RadialLayout.NodeRadius + RadialLayout.Padding;
        Assert.That(content / frame.Extent, Is.EqualTo(1 - RadialLayout.MarginFraction).Within(1e-9));
    }
}

using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration coverage for the tree-structure query endpoint
/// (<see cref="ILatticeStateQuery.GetTreeStructureAsync"/>): node-graph
/// accuracy against the diagnostic ground truth, deterministic child
/// ordering, depth and node-count budgets with continuation markers, sub-path
/// descent, shard filtering, and the not-found path.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeStructureIntegrationTests
{
    private StructureClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new StructureClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static int CountNodes(IEnumerable<NodeStateSummary> nodes)
    {
        var total = 0;
        foreach (var node in nodes)
        {
            total += 1 + CountNodes(node.Children);
        }
        return total;
    }

    private static void AssertWellFormed(NodeStateSummary node, int expectedDepth)
    {
        Assert.That(node.Depth, Is.EqualTo(expectedDepth), $"node {node.NodeId} depth");

        if (node.Kind == NodeKind.Leaf)
        {
            Assert.That(node.ChildCount, Is.EqualTo(0), "leaves have no children");
            Assert.That(node.Children, Is.Empty);
        }
        else
        {
            Assert.That(node.Children, Has.Count.EqualTo(node.ChildCount),
                "a fully-expanded internal node must include every immediate child");
            Assert.Multiple(() =>
            {
                Assert.That(node.SubtreeKeyCount, Is.EqualTo(node.Children.Sum(c => c.SubtreeKeyCount)),
                    "an internal node's live-key aggregate must equal the sum of its immediate children");
                Assert.That(node.SubtreeTombstoneCount, Is.EqualTo(node.Children.Sum(c => c.SubtreeTombstoneCount)),
                    "an internal node's tombstone aggregate must equal the sum of its immediate children");
            });
        }

        // Children are returned in ascending key-range-low order.
        string? prevLow = null;
        foreach (var child in node.Children)
        {
            if (prevLow is not null && child.KeyRangeLow is not null)
            {
                Assert.That(string.CompareOrdinal(child.KeyRangeLow, prevLow), Is.GreaterThanOrEqualTo(0),
                    "children must be ordered by ascending key-range low bound");
            }
            prevLow = child.KeyRangeLow ?? prevLow;
            AssertWellFormed(child, expectedDepth + 1);
        }
    }

    [Test]
    public async Task GetTreeStructure_not_found_for_unknown_tree()
    {
        var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest { TreeId = "no-such-tree" });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Roots, Is.Empty);
    }

    [Test]
    public async Task GetTreeStructure_matches_diagnostic_totals()
    {
        const int keyCount = 60;
        var tree = await _fixture.CreatePopulatedTreeAsync("struct-totals", keyCount, shardCount: 2);
        var report = await tree.DiagnoseAsync(deep: true);

        var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-totals",
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));

        var totalLive = result.Roots.Sum(r => r.SubtreeKeyCount);
        var totalTombstones = result.Roots.Sum(r => r.SubtreeTombstoneCount);

        Assert.Multiple(() =>
        {
            Assert.That(totalLive, Is.EqualTo(keyCount),
                "the fixture writes exactly the requested live keys");
            Assert.That(totalTombstones, Is.Zero,
                "the fixture writes no tombstones");
            Assert.That(totalLive, Is.EqualTo(report.TotalLiveKeys),
                "structure live-key total must match the diagnostic report");
            Assert.That(totalTombstones, Is.EqualTo(report.TotalTombstones),
                "structure tombstone total must match the diagnostic report");
        });

        // Each shard root's kind must agree with the diagnostic shard report.
        foreach (var root in result.Roots)
        {
            var shard = report.Shards.Single(s => s.ShardIndex == root.ShardIndex);
            var expectedKind = shard.RootIsLeaf ? NodeKind.Leaf : NodeKind.Internal;
            Assert.That(root.Kind, Is.EqualTo(expectedKind), $"shard {root.ShardIndex} root kind");
            AssertWellFormed(root, expectedDepth: 0);
        }
    }

    [Test]
    public async Task GetTreeStructure_produces_a_multi_level_graph()
    {
        await _fixture.CreatePopulatedTreeAsync("struct-multilevel", keyCount: 80, shardCount: 2);

        var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-multilevel",
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        Assert.That(result.Roots, Is.Not.Empty);
        Assert.That(result.Roots.Any(r => r.Kind == NodeKind.Internal && r.Children.Count > 0),
            Is.True, "80 keys over MaxLeafKeys=4 must yield at least one internal-rooted shard with children");
    }

    [Test]
    public async Task GetTreeStructure_honours_depth_limit_with_continuation_markers()
    {
        await _fixture.CreatePopulatedTreeAsync("struct-depth", keyCount: 80, shardCount: 2);

        var shallow = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-depth",
            DepthLimit = 0,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        // A depth-0 read lists each root's immediate children but does not expand
        // them: any internal child must be flagged as having more children and
        // carry no children of its own, and the response must report truncation.
        var truncatedChild = shallow.Roots
            .SelectMany(r => r.Children)
            .FirstOrDefault(c => c.Kind == NodeKind.Internal);
        Assert.That(truncatedChild, Is.Not.Null, "expected an internal child to be depth-limited");
        Assert.Multiple(() =>
        {
            Assert.That(truncatedChild!.Children, Is.Empty, "a depth-limited internal node must not expand its children");
            Assert.That(truncatedChild.HasMoreChildren, Is.True, "a truncated internal node must flag more children");
            Assert.That(shallow.Truncated, Is.True, "a depth-limited response must report truncation");
        });

        var deep = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-depth",
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        var deepChild = deep.Roots
            .SelectMany(r => r.Children)
            .Single(c => c.NodeId == truncatedChild!.NodeId);
        Assert.That(deepChild.Children, Is.Not.Empty, "a fully-expanded internal node must list its children");
    }

    [Test]
    public async Task GetTreeStructure_honours_max_nodes_budget()
    {
        await _fixture.CreatePopulatedTreeAsync("struct-budget", keyCount: 80, shardCount: 2);

        const int budget = 3;
        var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-budget",
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = budget,
        });

        Assert.Multiple(() =>
        {
            Assert.That(CountNodes(result.Roots), Is.LessThanOrEqualTo(budget),
                "the node-count budget must bound the response");
            Assert.That(result.Truncated, Is.True, "an over-budget response must report truncation");
        });
    }

    [Test]
    public async Task GetTreeStructure_descends_into_subpath()
    {
        await _fixture.CreatePopulatedTreeAsync("struct-subpath", keyCount: 80, shardCount: 2);

        var full = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-subpath",
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        // Find an internal node that itself has internal children to descend into.
        var internalRoot = full.Roots.First(r => r.Kind == NodeKind.Internal && r.Children.Count > 0);

        var subtree = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-subpath",
            SubPathNodeId = internalRoot.NodeId,
            ShardIndex = internalRoot.ShardIndex,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        Assert.That(subtree.Roots, Has.Count.EqualTo(1));
        var descended = subtree.Roots[0];
        Assert.Multiple(() =>
        {
            Assert.That(descended.NodeId, Is.EqualTo(internalRoot.NodeId), "sub-path must return the requested node");
            Assert.That(descended.SubtreeKeyCount, Is.EqualTo(internalRoot.SubtreeKeyCount),
                "the descended subtree must report the same aggregate as the whole-tree view");
            Assert.That(descended.ChildCount, Is.EqualTo(internalRoot.ChildCount));
        });
    }

    [Test]
    public async Task GetTreeStructure_shard_filter_returns_single_shard()
    {
        await _fixture.CreatePopulatedTreeAsync("struct-filter", keyCount: 60, shardCount: 2);

        var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-filter",
            ShardIndex = 1,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        Assert.That(result.Roots, Has.Count.LessThanOrEqualTo(1));
        foreach (var root in result.Roots)
        {
            Assert.That(root.ShardIndex, Is.EqualTo(1), "a shard-filtered read must only return the requested shard");
        }
    }

    [Test]
    public async Task GetTreeStructure_subpath_node_from_another_tree_is_rejected()
    {
        // A sub-path node id is an opaque, caller-supplied grain id. Binding one
        // tree's internal node under a different tree's id must not leak that
        // node's subtree - the request must be treated as not-found.
        await _fixture.CreatePopulatedTreeAsync("struct-tenant-a", keyCount: 80, shardCount: 2);
        await _fixture.CreatePopulatedTreeAsync("struct-tenant-b", keyCount: 80, shardCount: 2);

        var aStructure = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-tenant-a",
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });
        var aInternal = aStructure.Roots.First(r => r.Kind == NodeKind.Internal && r.Children.Count > 0);

        var escaped = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-tenant-b",
            SubPathNodeId = aInternal.NodeId,
            ShardIndex = aInternal.ShardIndex,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        Assert.That(escaped.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "a sub-path node belonging to another tree must not be served");
        Assert.That(escaped.Roots, Is.Empty);
    }

    [Test]
    public async Task GetTreeStructure_malformed_subpath_node_is_not_found()
    {
        await _fixture.CreatePopulatedTreeAsync("struct-malformed", keyCount: 10, shardCount: 2);

        var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "struct-malformed",
            SubPathNodeId = "not-a-valid-grain-id",
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Roots, Is.Empty);
    }

    [Test]
    public async Task GetTreeStructure_inspects_view_tree_as_read_only()
    {
        await _fixture.RegisterViewBackingTreeAsync("view-struct-probe");

        var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest { TreeId = "view-struct-probe" });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found),
            "a materialised view is a read-only tree and must be inspectable");
        Assert.That(result.Roots, Is.Not.Empty, "an empty view tree still exposes its root leaf");
    }

    [Test]
    public async Task GetTreeStructure_treats_system_tree_as_not_found()
    {
        var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest { TreeId = "_lattice_struct-probe" });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "system trees must stay invisible to the structure surface");
        Assert.That(result.Roots, Is.Empty);
    }
}

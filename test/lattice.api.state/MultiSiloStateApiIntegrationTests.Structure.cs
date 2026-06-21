using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// F-115 tree-structure multi-silo coverage. A tree whose shards are distributed
/// across silos must yield the same node graph regardless of which silo serves the
/// facade, and a sub-path descent steered at a node belonging to a different tree
/// must be rejected no matter where the originating tree's shards live.
/// </summary>
public sealed partial class MultiSiloStateApiIntegrationTests
{
    [Test]
    public async Task GetTreeStructure_aggregates_across_silos()
    {
        const string treeId = "multisilo-struct";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 80, shardCount: MultiSiloStateApiClusterFixture.ShardCount);

        var fromOwner = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = treeId,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });
        var fromOther = await _fixture.QueryFromOtherSilo().GetTreeStructureAsync(new StructureRequest
        {
            TreeId = treeId,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        Assert.That(fromOther.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.Multiple(() =>
        {
            Assert.That(fromOther.Roots.Sum(r => r.SubtreeKeyCount), Is.EqualTo(fromOwner.Roots.Sum(r => r.SubtreeKeyCount)),
                "a facade on any silo must observe the same live-key total");
            Assert.That(fromOther.Roots.Count, Is.EqualTo(fromOwner.Roots.Count),
                "a facade on any silo must observe the same shard-root count");
        });
    }

    [Test]
    public async Task GetTreeStructure_rejects_cross_tree_subpath_across_silos()
    {
        await _fixture.CreatePopulatedTreeAsync("multisilo-struct-a", keyCount: 80, shardCount: MultiSiloStateApiClusterFixture.ShardCount);
        await _fixture.CreatePopulatedTreeAsync("multisilo-struct-b", keyCount: 80, shardCount: MultiSiloStateApiClusterFixture.ShardCount);

        var aStructure = await _fixture.Query.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "multisilo-struct-a",
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });
        var aInternal = aStructure.Roots.First(r => r.Kind == NodeKind.Internal && r.Children.Count > 0);

        var escaped = await _fixture.QueryFromOtherSilo().GetTreeStructureAsync(new StructureRequest
        {
            TreeId = "multisilo-struct-b",
            SubPathNodeId = aInternal.NodeId,
            ShardIndex = aInternal.ShardIndex,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });

        Assert.That(escaped.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "a sub-path node from another tree must be rejected on any serving silo");
        Assert.That(escaped.Roots, Is.Empty);
    }
}

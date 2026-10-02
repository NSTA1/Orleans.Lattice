using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Issue #4180: the shard count and the whole-tree structure the state query
/// serves (the Explorer's Data area) enumerate the tree's shards from its
/// routing, and the tree's stateless worker caches that routing per activation
/// with nothing to invalidate it on a reshard. Each test warms the routing,
/// grows or shrinks the tree, and requires the answer to match the persisted map.
/// </summary>
/// <remarks>
/// A tree per test, because each fixed read refreshes the activation it lands
/// on and would hide a sibling read's defect.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeStateQueryReshardRoutingIntegrationTests
{
    private const int InitialShards = 4;

    private StructureClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new StructureClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeRegistry Registry =>
        _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [TestCase(8)]
    [TestCase(2)]
    public async Task GetPhysicalShardCountAsync_after_a_reshard_matches_the_persisted_map(int target)
    {
        var treeId = await CreateWarmedTreeAsync();

        await ReshardAsync(treeId, target);
        var persisted = await PersistedIndicesAsync(treeId);

        var count = await _fixture.Query.GetPhysicalShardCountAsync(treeId);

        Assert.That(count, Is.EqualTo(persisted.Count), $"persisted shards [{string.Join(", ", persisted)}]");
    }

    [TestCase(8)]
    [TestCase(2)]
    public async Task GetTreeStructureAsync_after_a_reshard_reports_a_root_per_persisted_shard(int target)
    {
        var treeId = await CreateWarmedTreeAsync();

        await ReshardAsync(treeId, target);
        var persisted = await PersistedIndicesAsync(treeId);

        var structure = await _fixture.Query.GetTreeStructureAsync(new StructureRequest { TreeId = treeId, MaxNodes = 10_000 });

        Assert.That(structure.Roots.Select(root => root.ShardIndex), Is.EqualTo(persisted));
    }

    private async Task<string> CreateWarmedTreeAsync()
    {
        var treeId = $"reshard-query-{Guid.NewGuid():N}";
        var tree = await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 120, shardCount: InitialShards);
        var warmed = await tree.GetRoutingAsync();
        Assert.That(warmed.Map.GetPhysicalShardIndices(), Has.Count.EqualTo(InitialShards), "precondition: the warmed map");
        return treeId;
    }

    private async Task<IReadOnlyList<int>> PersistedIndicesAsync(string treeId)
    {
        var map = await Registry.GetShardMapAsync(treeId);
        Assert.That(map, Is.Not.Null, "precondition: the reshard persisted a map");
        return map!.GetPhysicalShardIndices();
    }

    /// <summary>Drives the reshard coordinator and the splits or folds it starts until it is idle.</summary>
    private async Task ReshardAsync(string treeId, int target)
    {
        var client = _fixture.Cluster.Client;
        await client.GetGrain<ILattice>(treeId).ReshardAsync(target);
        var reshard = client.GetGrain<ITreeReshardGrain>(treeId);
        for (var pass = 0; pass < 200; pass++)
        {
            if (await reshard.IsIdleAsync())
            {
                return;
            }

            await reshard.RunReshardPassAsync();
            for (var index = 0; index < 16; index++)
            {
                var split = client.GetGrain<ITreeShardSplitGrain>($"{treeId}/{index}");
                if (!await split.IsIdleAsync())
                {
                    await split.RunSplitPassAsync();
                }

                var fold = client.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{index}");
                if (!await fold.IsIdleAsync())
                {
                    await fold.RunConsolidationPassAsync();
                }
            }

            await Task.Delay(20);
        }

        Assert.Fail($"The reshard to {target} shards did not converge.");
    }
}

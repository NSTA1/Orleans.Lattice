using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issues #4176 and #4180: the tree's <c>[StatelessWorker]</c> grain caches its
/// routing per activation, and neither an alias swap (resize) nor a map change
/// (reshard) invalidates it. A routed key operation heals on its first
/// stale-routing exception; the reads below route no key, so each must resolve
/// the tree's current routing itself or it answers from the retired tree or the
/// pre-reshard map for as long as the activation lives.
/// </summary>
/// <remarks>
/// Every test warms the routing of the tree's only activation (a single silo, and
/// calls made one at a time, so the stateless worker never needs a second one)
/// before changing the topology, and then makes no routed call through the logical
/// tree, which would heal the cache and hide the defect.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class NonRoutedReadsAfterTopologyChangeIntegrationTests
{
    private const int MaxLeafKeys = 4;
    private const int InitialShards = 2;
    private const int GrownShards = 4;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private ILatticeRegistry Registry => _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task ScanEntryHistoryAsync_after_a_resize_reads_the_current_physical_trees_log()
    {
        var treeId = $"history-resize-{Guid.NewGuid():N}";
        var tree = await CreatePopulatedTreeAsync(treeId, keyCount: 0);
        await tree.SetAsync("order-1", [1]);
        var before = await tree.ScanEntryHistoryAsync("order-1", null, null, 100, null);
        Assert.That(before.Revisions, Has.Count.EqualTo(1), "precondition: one write, one revision");

        var physical = await ResizeAsync(treeId);

        // Written through the physical tree so the warmed logical activation never
        // routes a key, which would heal its cache.
        await _cluster.Client.GetGrain<ILattice>(physical).SetAsync("order-1", [2]);

        var after = await tree.ScanEntryHistoryAsync("order-1", null, null, 100, null);

        Assert.That(
            after.Revisions.Select(r => r.ValueLength).ToList(),
            Has.Count.EqualTo(2),
            "history must read the resized tree's log, which holds the copied first write and the second one");
    }

    [Test]
    public async Task ScanEntryHistoryAsync_after_a_reshard_reports_every_write()
    {
        // A reshard keeps the physical tree, and the history log is partitioned by
        // key over a count pinned when the tree registers, so this holds either way;
        // it guards the reshard half of #4176's requirement.
        var treeId = $"history-reshard-{Guid.NewGuid():N}";
        var tree = await CreatePopulatedTreeAsync(treeId, keyCount: 40);
        await tree.SetAsync("order-1", [1]);
        _ = await tree.ScanEntryHistoryAsync("order-1", null, null, 100, null);

        await GrowAsync(treeId);
        await tree.SetAsync("order-1", [2]);

        var after = await tree.ScanEntryHistoryAsync("order-1", null, null, 100, null);

        Assert.That(after.Revisions, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_after_a_grow_accepts_a_new_shard()
    {
        var (tree, added) = await WarmThenGrowAsync();

        var digest = await tree.GetLeafProjectionDigestAsync(added);

        Assert.That(digest.Hash, Is.Not.Null);
    }

    [Test]
    public async Task GetLeafProjectionDigestForRangeAsync_after_a_grow_accepts_a_new_shard()
    {
        var (tree, added) = await WarmThenGrowAsync();

        var digest = await tree.GetLeafProjectionDigestForRangeAsync(added, null, null);

        Assert.That(digest.Hash, Is.Not.Null);
    }

    [Test]
    public async Task RebuildLeafProjectionAsync_after_a_grow_accepts_a_new_shard()
    {
        var (tree, added) = await WarmThenGrowAsync();

        Assert.That(async () => await tree.RebuildLeafProjectionAsync(added), Throws.Nothing);
    }

    [Test]
    public async Task CompactShardAsync_after_a_grow_accepts_a_new_shard()
    {
        var (tree, added) = await WarmThenGrowAsync();

        Assert.That(async () => await tree.CompactShardAsync(added), Throws.Nothing);
    }

    [Test]
    public async Task InspectOrphanedLeavesAsync_after_a_grow_walks_every_current_shard()
    {
        var (tree, _) = await WarmThenGrowAsync();

        var first = await AuditAsync(tree);
        _ = await tree.GetRoutingAsync(forceRefresh: true);
        var refreshed = await AuditAsync(tree);

        Assert.That(first, Is.EqualTo(refreshed), "the audit must walk the leaves of the current map, as a refreshed one does");
    }

    [Test]
    public async Task GetMaterialiserLagAsync_after_a_resize_reads_the_current_physical_tree()
    {
        var treeId = $"lag-resize-{Guid.NewGuid():N}";
        var tree = await CreatePopulatedTreeAsync(treeId, keyCount: 20);
        _ = await tree.GetRoutingAsync();
        await ResizeAsync(treeId);

        Assert.That(async () => await tree.GetMaterialiserLagAsync(), Throws.Nothing);
    }

    [Test]
    public async Task WarmUpAsync_after_a_resize_warms_the_current_physical_tree()
    {
        var treeId = $"warm-resize-{Guid.NewGuid():N}";
        var tree = await CreatePopulatedTreeAsync(treeId, keyCount: 20);
        _ = await tree.GetRoutingAsync();
        await ResizeAsync(treeId);

        Assert.That(async () => await tree.WarmUpAsync(), Throws.Nothing);
    }

    private static async Task<int> AuditAsync(ILattice tree)
    {
        var walked = 0;
        string? resume = null;
        for (var batch = 0; batch < 100; batch++)
        {
            var report = await tree.InspectOrphanedLeavesAsync(resume);
            walked += report.LeavesWalked;
            if (report.ResumeFrom is null)
            {
                return walked;
            }

            resume = report.ResumeFrom;
        }

        Assert.Fail("The orphaned-leaf audit did not complete.");
        return walked;
    }

    private async Task<ILattice> CreatePopulatedTreeAsync(string treeId, int keyCount)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { MaxLeafKeys = MaxLeafKeys, ShardCount = InitialShards });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"k-{i:D4}", [(byte)i]);
        }

        return tree;
    }

    /// <summary>
    /// Populates a tree, warms its activation's routing, grows it, and returns a
    /// physical shard index the warmed map does not contain.
    /// </summary>
    private async Task<(ILattice Tree, int Added)> WarmThenGrowAsync()
    {
        var treeId = $"grow-{Guid.NewGuid():N}";
        var tree = await CreatePopulatedTreeAsync(treeId, keyCount: 40);
        var warmed = await tree.GetRoutingAsync();
        Assert.That(warmed.Map.GetPhysicalShardIndices(), Has.Count.EqualTo(InitialShards), "precondition: the warmed map");

        await GrowAsync(treeId);

        var persisted = await Registry.GetShardMapAsync(treeId);
        Assert.That(persisted, Is.Not.Null, "precondition: the grow persisted a map");
        var added = persisted!.GetPhysicalShardIndices().Except(warmed.Map.GetPhysicalShardIndices()).ToList();
        Assert.That(added, Is.Not.Empty, "precondition: the grow added a shard");
        return (tree, added[0]);
    }

    /// <summary>Resizes the tree to completion and returns the physical tree it now aliases.</summary>
    private async Task<string> ResizeAsync(string treeId)
    {
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(16, 16);
        for (var pass = 0; pass < 50 && !await resize.IsIdleAsync(); pass++)
        {
            await resize.RunResizePassAsync();
        }

        var physical = await Registry.ResolveAsync(treeId);
        Assert.That(physical, Is.Not.EqualTo(treeId), "precondition: the resize swapped the alias");
        return physical;
    }

    /// <summary>Drives a reshard to <see cref="GrownShards"/> and the splits it starts until idle.</summary>
    private async Task GrowAsync(string treeId)
    {
        await _cluster.Client.GetGrain<ILattice>(treeId).ReshardAsync(GrownShards);
        var reshard = _cluster.Client.GetGrain<ITreeReshardGrain>(treeId);
        for (var pass = 0; pass < 100; pass++)
        {
            if (await reshard.IsIdleAsync())
            {
                return;
            }

            await reshard.RunReshardPassAsync();
            var map = await Registry.GetShardMapAsync(treeId)
                ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, InitialShards);
            foreach (var index in map.GetPhysicalShardIndices())
            {
                var split = _cluster.Client.GetGrain<ITreeShardSplitGrain>($"{treeId}/{index}");
                if (!await split.IsIdleAsync())
                {
                    await split.RunSplitPassAsync();
                }
            }

            await Task.Delay(20);
        }

        Assert.Fail("The grow did not converge.");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

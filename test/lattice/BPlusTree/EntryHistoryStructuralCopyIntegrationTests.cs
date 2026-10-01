using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A key written once keeps exactly one revision in its history after the tree is
/// resized and resharded. Both operations, and the sibling redistribution of a leaf
/// split, copy entries with their original hybrid logical clock, and each copy is
/// appended to the write-ahead log the history read falls back to on a tree with no
/// history view. A copy replays a revision a user already wrote, so it must not be
/// reported as another one (issue #4149).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class EntryHistoryStructuralCopyIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;

    private TestCluster Cluster => _fixture.Cluster;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync() => await _fixture.DisposeAsync();

    [Test]
    public async Task ScanEntryHistoryAsync_after_resize_and_reshard_reports_one_revision_for_a_key_written_once()
    {
        var treeId = $"history-copy-{Guid.NewGuid():N}";
        await Cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .RegisterAsync(treeId, new TreeRegistryEntry { MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys, ShardCount = 2 });
        var tree = Cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Twelve keys overflow the four-key leaves, so the seed itself splits leaves.
        for (var i = 0; i < 12; i++)
        {
            await tree.SetAsync($"order-{1000 + i}", new byte[29]);
        }

        var resize = Cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(16, 16);
        await resize.RunResizePassAsync();
        await tree.ReshardAsync(4);
        await DriveReshardToCompletionAsync(treeId);

        var page = await tree.ScanEntryHistoryAsync("order-1003", null, null, 100, null);

        Assert.That(
            page.Revisions.Select(r => $"{r.Kind} {r.Hlc} {r.ValueLength} B").ToList(),
            Has.Count.EqualTo(1),
            "a key written once has one revision, whatever copies the split, resize and reshard made of it");
    }

    private async Task DriveReshardToCompletionAsync(string treeId)
    {
        var reshard = Cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);
        var registry = Cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        for (var i = 0; i < 50; i++)
        {
            if (await reshard.IsIdleAsync())
            {
                return;
            }

            await reshard.RunReshardPassAsync();
            var physical = await registry.ResolveAsync(treeId);
            var map = await registry.GetShardMapAsync(physical)
                ?? await registry.GetShardMapAsync(treeId)
                ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2);
            foreach (var index in map.GetPhysicalShardIndices())
            {
                var split = Cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{index}");
                if (!await split.IsIdleAsync())
                {
                    await split.RunSplitPassAsync();
                }
            }
        }

        Assert.Fail("Reshard did not converge.");
    }
}

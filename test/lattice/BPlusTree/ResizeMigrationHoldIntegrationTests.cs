using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end liveness of the resize's hold on shard migrations (issue #4452).
/// A completed resize holds splits, folds and reshards while the copy it
/// replaced still mirrors into the resized copy; the hold must end when the real
/// purge of that copy clears its shadow-forward state - driven here through the
/// deletion grain's own purge, never through the test-only seam - and must never
/// begin for a resize that copied nothing. A hold that never released would
/// block a tree's topology for good.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ResizeMigrationHoldIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private ILatticeRegistry Registry => _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private async Task<(ILattice Tree, Dictionary<string, string> Expected)> CreatePopulatedTreeAsync(string treeId)
    {
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = new Dictionary<string, string>();
        for (var i = 0; i < 200; i++)
        {
            var key = $"key-{i:D4}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes($"value-{i}"));
            expected[key] = $"value-{i}";
        }

        return (tree, expected);
    }

    private async Task ResizeToCompletionAsync(string treeId, int maxLeafKeys)
    {
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(maxLeafKeys, maxLeafKeys);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsIdleAsync(), Is.True, "precondition: the resize completed");
    }

    private async Task AssertReplacedCopyMirrorsNowhereAsync(string replaced, string treeId)
    {
        foreach (var index in await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId))
        {
            Assert.That(
                await _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{replaced}/{index}").GetMirrorDestinationAsync(),
                Is.Null,
                $"a purged shard {replaced}/{index} must answer that it mirrors nowhere, not throw");
        }
    }

    private async Task SplitAndVerifyAsync(string treeId, ILattice tree, Dictionary<string, string> expected)
    {
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "the split must complete once the hold is released");

        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual is null ? null : Encoding.UTF8.GetString(actual), Is.EqualTo(value), key);
        }
    }

    [Test]
    public async Task The_purge_of_a_first_resizes_retired_copy_releases_the_hold()
    {
        var treeId = $"hold-first-{Guid.NewGuid():N}";
        var (tree, expected) = await CreatePopulatedTreeAsync(treeId);
        await ResizeToCompletionAsync(treeId, 64);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");

        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.True, "the retired copy still mirrors into the resized one");
        Assert.ThrowsAsync<InvalidOperationException>(() => split.SplitAsync(sourceShardIndex: 0));

        // The retirement purge the soft-delete reminder would run once the window
        // expires: a first resize retires the shards under the logical id itself.
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).PurgePhysicalAsync();

        await AssertReplacedCopyMirrorsNowhereAsync(treeId, treeId);
        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.False, "the purge must release the hold");
        await SplitAndVerifyAsync(treeId, tree, expected);
    }

    [Test]
    public async Task The_purge_of_a_later_resizes_derived_copy_releases_the_hold()
    {
        var treeId = $"hold-derived-{Guid.NewGuid():N}";
        var (tree, expected) = await CreatePopulatedTreeAsync(treeId);
        await ResizeToCompletionAsync(treeId, 64);
        var firstCopy = await Registry.ResolveAsync(treeId);
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).PurgePhysicalAsync();

        await ResizeToCompletionAsync(treeId, 32);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        Assert.That(await Registry.ResolveAsync(treeId), Is.Not.EqualTo(firstCopy), "precondition: a second copy");
        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.True, "the first copy still mirrors into the second");

        // Purging a derived copy also removes its registry row; its shards must
        // still answer the probe, and answer that they mirror nowhere.
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(firstCopy).PurgePhysicalAsync();

        Assert.That(await Registry.GetEntryAsync(firstCopy), Is.Null, "precondition: the purge unregistered the copy");
        await AssertReplacedCopyMirrorsNowhereAsync(firstCopy, treeId);
        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.False, "the purge must release the hold");
        await SplitAndVerifyAsync(treeId, tree, expected);
    }

    [Test]
    public async Task An_empty_tree_resize_never_holds_shard_migrations()
    {
        // The empty-tree fast path re-pins the registry and records the resize
        // complete without a copy; nothing mirrors, so nothing may be held.
        var treeId = $"hold-empty-{Guid.NewGuid():N}";
        await _fixture.CreateTreeAsync(treeId);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);

        await resize.ResizeAsync(64, 64);

        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.False);
        Assert.DoesNotThrowAsync(() => _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0").SplitAsync(sourceShardIndex: 0));
        await _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0").RunSplitPassAsync();
    }
}

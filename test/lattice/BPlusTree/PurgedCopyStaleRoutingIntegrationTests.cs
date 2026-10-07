using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A routing activation that still caches a resized tree's old physical copy
/// must keep being refused once that copy is purged, exactly as it is during
/// the soft-delete window (issue #4503). Before the fix the purge cleared the
/// copy's shards, which then answered a stale router's read as the empty tree
/// and accepted - and lost - its write. Each test acts as that stale router: it
/// stamps the routed logical tree id the way <c>LatticeGrain</c> does and calls
/// the purged copy's shards directly, which is what a cached (copy, map) pair
/// addresses.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PurgedCopyStaleRoutingIntegrationTests
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

    private IShardRootGrain Shard(string physicalTreeId, int index) =>
        _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{index}");

    private async Task<(ILattice Tree, Dictionary<string, string> Expected)> CreatePopulatedTreeAsync(string treeId)
    {
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = new Dictionary<string, string>();
        for (var i = 0; i < 80; i++)
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

    /// <summary>Runs <paramref name="call"/> stamped as a call a router for <paramref name="logicalTreeId"/> sent.</summary>
    private static async Task<T> RoutedAsync<T>(string logicalTreeId, Func<Task<T>> call)
    {
        var key = LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey;
        var previous = RequestContext.Get(key);
        RequestContext.Set(key, logicalTreeId);
        try
        {
            return await call();
        }
        finally
        {
            if (previous is null) RequestContext.Remove(key);
            else RequestContext.Set(key, previous);
        }
    }

    private static Task RoutedAsync(string logicalTreeId, Func<Task> call) =>
        RoutedAsync(logicalTreeId, async () => { await call(); return true; });

    /// <summary>
    /// Asserts that a stale router for <paramref name="logicalTreeId"/> is refused by
    /// every shard of the purged <paramref name="purgedCopy"/>, for a read and a
    /// write, with the stale-routing signal that makes it refresh and retry.
    /// </summary>
    private Task AssertStaleRouterRefusedAsync(string logicalTreeId, string purgedCopy, IReadOnlyList<int> shards)
    {
        Assert.That(shards, Is.Not.Empty, "precondition: the copy had shards");
        Assert.Multiple(() =>
        {
            foreach (var index in shards)
            {
                var shard = Shard(purgedCopy, index);
                var read = Assert.ThrowsAsync<StaleTreeRoutingException>(
                    () => RoutedAsync(logicalTreeId, () => shard.GetAsync("key-0001")),
                    $"a stale router's read on purged shard {purgedCopy}/{index} must be refused, not answered as empty");
                if (read is not null) Assert.That(read.StalePhysicalTreeId, Is.EqualTo(purgedCopy));
                Assert.ThrowsAsync<StaleTreeRoutingException>(
                    () => RoutedAsync(logicalTreeId, () => shard.SetAsync("key-0001", Encoding.UTF8.GetBytes("stale-write"))),
                    $"a stale router's write on purged shard {purgedCopy}/{index} must be refused, not accepted and lost");
            }
        });
        return Task.CompletedTask;
    }

    private static async Task AssertLogicalReadsAsync(ILattice tree, Dictionary<string, string> expected)
    {
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual is null ? null : Encoding.UTF8.GetString(actual), Is.EqualTo(value), key);
        }
    }

    [Test]
    public async Task A_stale_router_is_refused_by_a_purged_first_resize_copy()
    {
        var treeId = $"purged-first-{Guid.NewGuid():N}";
        var (tree, expected) = await CreatePopulatedTreeAsync(treeId);
        var shards = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);
        await ResizeToCompletionAsync(treeId, 64);

        // In the soft-delete window the retired copy refuses the stale router.
        Assert.ThrowsAsync<StaleTreeRoutingException>(
            () => RoutedAsync(treeId, () => Shard(treeId, shards[0]).GetAsync("key-0001")));

        // The purge the soft-delete reminder runs: a first resize retires the
        // shards under the logical id itself.
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).PurgePhysicalAsync();

        await AssertStaleRouterRefusedAsync(treeId, treeId, shards);
        await AssertLogicalReadsAsync(tree, expected);
    }

    [Test]
    public async Task A_stale_router_is_refused_by_a_purged_later_resize_copy_which_is_not_resurrected()
    {
        var treeId = $"purged-derived-{Guid.NewGuid():N}";
        var (tree, expected) = await CreatePopulatedTreeAsync(treeId);
        await ResizeToCompletionAsync(treeId, 64);
        var firstCopy = await Registry.ResolveAsync(treeId);
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).PurgePhysicalAsync();
        var shards = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);

        await ResizeToCompletionAsync(treeId, 32);
        Assert.That(await Registry.ResolveAsync(treeId), Is.Not.EqualTo(firstCopy), "precondition: a second copy");
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(firstCopy).PurgePhysicalAsync();
        Assert.That(await Registry.GetEntryAsync(firstCopy), Is.Null, "precondition: the purge unregistered the copy");

        await AssertStaleRouterRefusedAsync(treeId, firstCopy, shards);
        Assert.That(await Registry.GetEntryAsync(firstCopy), Is.Null, "a stale write must not re-register the purged copy");
        await AssertLogicalReadsAsync(tree, expected);
    }

    [Test]
    public async Task A_purged_copy_seeded_by_an_unrouted_call_or_reactivated_still_refuses_a_stale_router()
    {
        var treeId = $"purged-reseed-{Guid.NewGuid():N}";
        var (tree, expected) = await CreatePopulatedTreeAsync(treeId);
        var shards = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);
        await ResizeToCompletionAsync(treeId, 64);
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).PurgePhysicalAsync();

        var shard = Shard(treeId, shards[0]);

        // An unrouted read of the purged copy answers it as empty and must not
        // re-open it to routed traffic; nor may a reactivation.
        Assert.That(await shard.GetAsync("key-0001"), Is.Null);
        await shard.ForceDeactivateAsync();
        await Task.Delay(200);

        await AssertStaleRouterRefusedAsync(treeId, treeId, shards);
        await AssertLogicalReadsAsync(tree, expected);
    }

    [Test]
    public async Task An_unrouted_terminal_on_a_purged_copy_is_refused_and_seeds_nothing()
    {
        var treeId = $"purged-terminal-{Guid.NewGuid():N}";
        var (tree, expected) = await CreatePopulatedTreeAsync(treeId);
        var shards = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);
        await ResizeToCompletionAsync(treeId, 64);
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).PurgePhysicalAsync();

        // A maintenance verb with no routed stamp is refused as on any purged
        // tree, rather than re-seeding an empty root on the retired copy.
        Assert.ThrowsAsync<LatticeTreePurgedException>(() => Shard(treeId, shards[0]).WarmUpAsync());

        await AssertStaleRouterRefusedAsync(treeId, treeId, shards);
        await AssertLogicalReadsAsync(tree, expected);
    }

    [Test]
    public async Task The_hold_releases_after_the_purge_and_the_purged_copy_still_refuses_a_stale_router()
    {
        var treeId = $"purged-hold-{Guid.NewGuid():N}";
        var (tree, expected) = await CreatePopulatedTreeAsync(treeId);
        var shards = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);
        await ResizeToCompletionAsync(treeId, 64);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.True, "precondition: the retired copy mirrors");

        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).PurgePhysicalAsync();

        // The purge tombstone is not a mirror: the hold's probe still reads no
        // destination, before and after a stale router has been refused.
        foreach (var index in shards)
            Assert.That(await Shard(treeId, index).GetMirrorDestinationAsync(), Is.Null);
        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.False, "the purge must release the hold");

        await AssertStaleRouterRefusedAsync(treeId, treeId, shards);
        foreach (var index in shards)
            Assert.That(await Shard(treeId, index).GetMirrorDestinationAsync(), Is.Null);
        Assert.That(await resize.HoldsShardMigrationsAsync(), Is.False, "a refused stale router must not re-arm the hold");

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);
        await AssertLogicalReadsAsync(tree, expected);
    }

    [Test]
    public async Task A_purged_tree_id_is_reused_by_a_write_through_its_own_router()
    {
        var treeId = $"purged-reuse-{Guid.NewGuid():N}";
        var (tree, _) = await CreatePopulatedTreeAsync(treeId);
        var shards = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);
        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId);
        await deletion.DeleteTreeAsync();
        await deletion.PurgePhysicalAsync();

        // Its own router still resolves the id to itself, so the purge tombstone
        // does not refuse it: a read answers empty and a write reuses the id
        // (issue #3940).
        Assert.That(await RoutedAsync(treeId, () => Shard(treeId, shards[0]).GetAsync("key-0001")), Is.Null);
        await tree.SetAsync("reused", Encoding.UTF8.GetBytes("again"));
        Assert.That(Encoding.UTF8.GetString((await tree.GetAsync("reused"))!), Is.EqualTo("again"));
        Assert.That(await tree.GetAsync("key-0001"), Is.Null, "the purge removed the old data");
    }
}

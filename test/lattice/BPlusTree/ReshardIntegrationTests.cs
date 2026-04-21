using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class ReshardIntegrationTests
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

    /// <summary>
    /// Registers <paramref name="treeId"/> in the lattice registry with the
    /// cluster's <see cref="FourShardClusterFixture.TestShardCount"/> pin so
    /// the tree starts at 4 physical shards (matching the fixture's legacy
    /// semantics) rather than the global <c>LatticeConstants.DefaultShardCount</c>.
    /// Must be called before any operation that would otherwise trigger the
    /// resolver's lazy-seed fallback.
    /// </summary>
    private async Task RegisterTreeAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = FourShardClusterFixture.SmallMaxLeafKeys,
            ShardCount = FourShardClusterFixture.TestShardCount,
        });
    }

    private async Task WaitForReshardAsync(ILattice tree, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(60));
        while (DateTime.UtcNow < deadline)
        {
            if (await tree.IsReshardCompleteAsync()) return;
            await Task.Delay(200);
        }
        Assert.Fail("Reshard did not complete within the allotted time.");
    }

    /// <summary>
    /// Drives the reshard coordinator and all dispatched per-shard split
    /// coordinators to completion synchronously. Integration timers in
    /// <see cref="Orleans.TestingHost.TestCluster"/> can take many seconds
    /// per tick and the test budget would otherwise blow past the default
    /// NUnit timeout; this helper lets us make deterministic progress.
    /// </summary>
    private async Task DriveReshardToCompletionAsync(string treeId, ILattice tree)
    {
        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        for (int i = 0; i < 50; i++)
        {
            if (await reshard.IsIdleAsync()) return;

            // Kick the coordinator to dispatch any new splits it wants to run.
            await reshard.RunReshardPassAsync();

            // Drive every per-shard split coordinator to completion. Until
            // the first split commits, the registry has no persisted map —
            // fall back to the default layout so we still reach the shard
            // coordinators that were just dispatched.
            var map = await registry.GetShardMapAsync(treeId)
                ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, FourShardClusterFixture.TestShardCount);
            foreach (var idx in map.GetPhysicalShardIndices())
            {
                var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{idx}");
                if (!await split.IsIdleAsync())
                    await split.RunSplitPassAsync();
            }

            await Task.Delay(50);
        }

        // Diagnostic: on failure, surface map + phase.
        var finalMap = await registry.GetShardMapAsync(treeId);
        var distinct = finalMap?.GetPhysicalShardIndices().Count ?? -1;
        var isDone = await reshard.IsIdleAsync();
        Assert.Fail($"Reshard did not converge. reshard.IsComplete={isDone}, map.distinct={distinct}");
    }

    [Test]
    public async Task ReshardAsync_grows_shard_count_and_preserves_all_data()
    {
        var treeId = $"reshard-grow-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Pre-populate.
        const int keyCount = 300;
        var expected = new Dictionary<string, byte[]>(keyCount);
        for (int i = 0; i < keyCount; i++)
        {
            var key = $"k-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"v-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }

        await tree.ReshardAsync(6);
        await DriveReshardToCompletionAsync(treeId, tree);

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var map = await registry.GetShardMapAsync(treeId);
        Assert.That(map, Is.Not.Null);
        Assert.That(map!.GetPhysicalShardIndices().Count, Is.GreaterThanOrEqualTo(6),
            "ShardMap should contain at least the target number of distinct physical shards.");

        // All pre-reshard keys must still be readable.
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' missing after reshard");
            Assert.That(actual, Is.EqualTo(value).AsCollection, $"Wrong value for '{key}' after reshard");
        }

        Assert.That(await tree.CountAsync(), Is.EqualTo(keyCount), "Count must match pre-reshard total.");
    }

    [Test]
    public void ReshardAsync_throws_when_target_not_greater_than_current()
    {
        var treeId = $"reshard-invalid-{Guid.NewGuid():N}";
        RegisterTreeAsync(treeId).GetAwaiter().GetResult();
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Seed a key so the empty-tree fast-path (which allows any target) is
        // bypassed and the grow-only validation below is actually exercised.
        tree.SetAsync("seed", [1]).GetAwaiter().GetResult();

        // Cluster is configured with 4 shards; any request for ≤4 must throw.
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => tree.ReshardAsync(4));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => tree.ReshardAsync(2));
    }

    [Test]
    public void ReshardAsync_throws_when_target_below_two()
    {
        var treeId = $"reshard-lt-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => tree.ReshardAsync(1));
    }

    [Test]
    public async Task IsReshardCompleteAsync_true_before_any_reshard()
    {
        var treeId = $"reshard-fresh-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        Assert.That(await tree.IsReshardCompleteAsync(), Is.True);
    }

    [Test]
    public async Task ReshardAsync_is_idempotent_for_same_target()
    {
        var treeId = $"reshard-idem-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k", [1]);

        await tree.ReshardAsync(5);
        // Same target while in progress must not throw.
        await tree.ReshardAsync(5);
        await DriveReshardToCompletionAsync(treeId, tree);

        Assert.That(await tree.GetAsync("k"), Is.Not.Null);
    }

    [Test]
    public async Task ReshardAsync_concurrent_writes_during_reshard_are_preserved()
    {
        var treeId = $"reshard-concurrent-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Seed with a handful of keys.
        for (int i = 0; i < 50; i++)
            await tree.SetAsync($"seed-{i:D3}", Encoding.UTF8.GetBytes($"v-{i}"));

        // Kick off reshard.
        var reshardTask = tree.ReshardAsync(6);

        // Race writes while the reshard is in flight.
        var writeTasks = new List<Task>();
        for (int i = 0; i < 100; i++)
        {
            var i2 = i;
            writeTasks.Add(tree.SetAsync($"live-{i2:D3}", Encoding.UTF8.GetBytes($"live-{i2}")));
        }

        await Task.WhenAll(writeTasks);
        await reshardTask;
        await DriveReshardToCompletionAsync(treeId, tree);

        // Every written key must be present.
        for (int i = 0; i < 50; i++)
            Assert.That(await tree.GetAsync($"seed-{i:D3}"), Is.Not.Null, $"seed-{i:D3} lost");
        for (int i = 0; i < 100; i++)
            Assert.That(await tree.GetAsync($"live-{i:D3}"), Is.Not.Null, $"live-{i:D3} lost");
    }
}

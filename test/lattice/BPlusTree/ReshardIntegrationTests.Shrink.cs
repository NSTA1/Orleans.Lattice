using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The shrink half of the online reshard, end to end on a real cluster: data is
/// preserved through the folds, writes that overlap them are kept, the retired
/// shards are cleaned up, and a retired shard index is never handed out again.
/// </summary>
public partial class ReshardIntegrationTests
{
    [Test]
    public async Task ReshardAsync_shrinks_shard_count_preserves_all_data_and_retires_the_old_shards()
    {
        var treeId = $"reshard-shrink-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var expected = await PopulateAsync(tree, "k", 300);

        // Record every shard's leaf chain before the shrink, so the retired
        // shards' leaves can be proven cleared afterwards.
        var leavesBefore = new Dictionary<int, List<GrainId>>();
        for (int idx = 0; idx < FourShardClusterFixture.TestShardCount; idx++)
            leavesBefore[idx] = await CollectLeafChainAsync(treeId, idx);
        Assert.That(leavesBefore.Values.Sum(l => l.Count), Is.GreaterThan(FourShardClusterFixture.TestShardCount),
            "precondition: the shards must hold real leaf chains for the cleanup assertions to mean anything");

        await tree.ReshardAsync(2);
        await DriveShrinkToCompletionAsync(treeId);

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var live = (await registry.GetShardMapAsync(treeId))!.GetPhysicalShardIndices();
        Assert.That(live, Has.Count.EqualTo(2), "the map must hold exactly the target number of shards");
        Assert.That((await registry.GetEntryAsync(treeId))!.ShardCount, Is.EqualTo(2),
            "a completed shrink re-pins the registry's shard count, so healing never folds below it");

        await AssertAllPresentAsync(tree, expected, "after the shrink");

        // The old shards are cleaned up: every retired shard root is a
        // tombstone with no leaves, and every leaf it used to hold is cleared.
        var retired = Enumerable.Range(0, FourShardClusterFixture.TestShardCount).Where(i => !live.Contains(i)).ToList();
        Assert.That(retired, Has.Count.EqualTo(2));
        foreach (var idx in retired)
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{idx}");
            Assert.That(await shard.IsRetiredAsync(), Is.True, $"shard {idx} must be retired");
            Assert.That(await shard.GetLeftmostLeafIdAsync(), Is.Null, $"retired shard {idx} must hold no leaves");

            foreach (var leafId in leavesBefore[idx])
            {
                var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);
                Assert.That(await leaf.CountAsync(), Is.Zero, $"leaf {leafId} of retired shard {idx} must be cleared");
                Assert.That(await leaf.GetNextSiblingAsync(), Is.Null);
            }

            // A caller still holding the pre-shrink map is redirected, never
            // served: a routed op is refused as stale routing and a range read
            // returns an empty page for map-version reconciliation to correct.
            Assert.ThrowsAsync<StaleShardRoutingException>(() => shard.GetAsync("k-00000"));
            Assert.ThrowsAsync<StaleShardRoutingException>(() => shard.SetAsync("k-00000", [9]));
            var page = await shard.GetSortedKeysBatchAsync(null, null, 100);
            Assert.That(page.Keys, Is.Empty);
            Assert.That(page.HasMore, Is.False);
        }

        foreach (var idx in live)
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{idx}");
            Assert.That(await shard.IsRetiredAsync(), Is.False);
        }

        await tree.SetAsync("after-shrink", [42]);
        Assert.That(await tree.GetAsync("after-shrink"), Is.EqualTo(new byte[] { 42 }).AsCollection);
    }

    [Test]
    public async Task ReshardAsync_shrink_preserves_writes_made_while_its_folds_run()
    {
        var treeId = $"reshard-shrink-live-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var expected = await PopulateAsync(tree, "seed", 50);

        await tree.ReshardAsync(2);

        // Keep writing - overwrites and new keys - until the shrink has fully
        // finished, so the writes overlap every fold's drain, freeze, swap,
        // finalise and retirement rather than landing before the first tick.
        // The drive waits for the writer to finish a further full round after
        // every pass, so the overlap is guaranteed rather than left to how the
        // writer's thread happens to be scheduled.
        using var stop = new CancellationTokenSource();
        var completedRounds = 0;
        var writer = Task.Run(async () =>
        {
            var written = new Dictionary<string, byte[]>();
            while (!stop.IsCancellationRequested)
            {
                var round = Volatile.Read(ref completedRounds);
                for (int i = 0; i < 25; i++)
                {
                    var key = $"live-{i:D3}";
                    var value = Encoding.UTF8.GetBytes($"live-{i}-{round}");
                    await tree.SetAsync(key, value);
                    written[key] = value;
                }
                Interlocked.Increment(ref completedRounds);
            }
            return written;
        });

        async Task AwaitAnotherWriterRoundAsync()
        {
            var seen = Volatile.Read(ref completedRounds);
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (Volatile.Read(ref completedRounds) == seen)
            {
                if (writer.IsCompleted) await writer;
                if (DateTime.UtcNow > deadline) Assert.Fail("The writer made no progress across a fold pass.");
                await Task.Delay(5);
            }
        }

        await DriveShrinkToCompletionAsync(treeId, AwaitAnotherWriterRoundAsync);
        await tree.SetAsync("after-drive", [1]);
        stop.Cancel();
        var live = await writer;
        var rounds = Volatile.Read(ref completedRounds);

        Assert.That(rounds, Is.GreaterThan(1), "precondition: the writer must have overlapped several fold ticks");
        foreach (var (key, value) in live) expected[key] = value;
        expected["after-drive"] = [1];
        await AssertAllPresentAsync(tree, expected, "after a shrink with overlapping writes");
    }

    [Test]
    public async Task ReshardAsync_grow_after_a_shrink_never_reuses_a_retired_shard_index()
    {
        var treeId = $"reshard-shrink-grow-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var expected = await PopulateAsync(tree, "g", 200);

        await tree.ReshardAsync(2);
        await DriveShrinkToCompletionAsync(treeId);

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var retired = Enumerable.Range(0, FourShardClusterFixture.TestShardCount)
            .Except((await registry.GetShardMapAsync(treeId))!.GetPhysicalShardIndices())
            .ToList();

        await tree.ReshardAsync(4);
        await DriveReshardToCompletionAsync(treeId, tree);

        var grown = (await registry.GetShardMapAsync(treeId))!.GetPhysicalShardIndices();
        Assert.That(grown, Has.Count.EqualTo(4));
        Assert.That(grown.Intersect(retired), Is.Empty,
            "a split must allocate a fresh index; a retired shard refuses every routed operation, so a split into one could never drain");
        await AssertAllPresentAsync(tree, expected, "after shrinking and growing again");
    }

    [Test]
    public async Task ReshardAsync_on_an_emptied_shrunk_tree_returns_retired_shards_to_service()
    {
        var treeId = $"reshard-shrink-empty-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var seeded = await PopulateAsync(tree, "e", 100);

        await tree.ReshardAsync(2);
        await DriveShrinkToCompletionAsync(treeId);
        foreach (var key in seeded.Keys) await tree.DeleteAsync(key);

        // The empty-tree path rebuilds the identity map over indices 0..3,
        // two of which the shrink retired.
        await tree.ReshardAsync(4);
        Assert.That(await tree.IsReshardCompleteAsync(), Is.True, "an empty tree is re-pinned without a coordinator");

        // Every shard the rebuilt map routes to must serve again. Driven on the
        // shard roots directly: a router still caching the pre-reshard map
        // would keep sending these keys to the two survivors and never reach
        // the revived shards at all.
        for (int idx = 0; idx < 4; idx++)
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{idx}");
            Assert.That(await shard.IsRetiredAsync(), Is.False, $"shard {idx} is routed again and must be in service");
            await shard.SetAsync($"direct-{idx}", [(byte)idx]);
            Assert.That(await shard.GetAsync($"direct-{idx}"), Is.EqualTo(new[] { (byte)idx }).AsCollection);
        }

        var written = await PopulateAsync(tree, "after", 100);
        foreach (var (key, value) in written)
            Assert.That(await tree.GetAsync(key), Is.EqualTo(value).AsCollection, $"'{key}' after re-growing an emptied, shrunk tree");
    }

    private static async Task<Dictionary<string, byte[]>> PopulateAsync(ILattice tree, string prefix, int count)
    {
        var expected = new Dictionary<string, byte[]>(count);
        for (int i = 0; i < count; i++)
        {
            var key = $"{prefix}-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"v-{prefix}-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }

        return expected;
    }

    private static async Task AssertAllPresentAsync(ILattice tree, Dictionary<string, byte[]> expected, string when)
    {
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.EqualTo(value).AsCollection, $"Wrong or missing value for '{key}' {when}");
        }

        Assert.That(await tree.CountAsync(), Is.EqualTo(expected.Count), $"count {when}");
        var scanned = new List<string>();
        await foreach (var key in tree.KeysAsync()) scanned.Add(key);
        Assert.That(scanned, Is.EquivalentTo(expected.Keys), $"scan {when}");
    }

    /// <summary>
    /// Drives the reshard coordinator and every consolidation it starts to
    /// completion synchronously, for the same reason
    /// <see cref="DriveReshardToCompletionAsync"/> drives split coordinators.
    /// </summary>
    private async Task DriveShrinkToCompletionAsync(string treeId, Func<Task>? afterEachPass = null)
    {
        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);
        for (int i = 0; i < 200; i++)
        {
            if (await reshard.IsIdleAsync()) return;
            await reshard.RunReshardPassAsync();
            for (int idx = 0; idx < FourShardClusterFixture.TestShardCount; idx++)
            {
                var fold = _cluster.GrainFactory.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{idx}");
                if (!await fold.IsIdleAsync())
                    await fold.RunConsolidationPassAsync();
            }

            if (afterEachPass is not null) await afterEachPass();
            await Task.Delay(50);
        }

        Assert.Fail("Shrink did not converge.");
    }

    private async Task<List<GrainId>> CollectLeafChainAsync(string treeId, int shardIndex)
    {
        var leaves = new List<GrainId>();
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{shardIndex}");
        var current = await shard.GetLeftmostLeafIdAsync();
        while (current is { } id && leaves.Count < 10_000)
        {
            leaves.Add(id);
            current = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetNextSiblingAsync();
        }

        return leaves;
    }
}

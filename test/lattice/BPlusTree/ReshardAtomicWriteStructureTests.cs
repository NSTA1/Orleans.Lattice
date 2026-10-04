using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression fixture: an online reshard (grow or shrink) raced by a
/// continuous stream of <see cref="ILattice.SetManyAtomicAsync"/> batches
/// over a fixed key universe must leave every surviving shard structurally
/// consistent - <see cref="ILattice.CountAsync"/> equals the number of
/// distinct keys and <see cref="ILattice.ScanKeysAsync"/> returns each key
/// exactly once.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ReshardAtomicWriteStructureTests
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
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [TestCase(2)]
    [TestCase(8)]
    [Repeat(5)]
    public async Task Reshard_raced_by_atomic_batches_leaves_count_and_scan_consistent(int target)
    {
        var treeId = $"reshard-atomic-{target}-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var keys = Enumerable.Range(0, 16).Select(i => $"k-{i:D2}").ToList();

        List<KeyValuePair<string, byte[]>> Batch(int r) => keys
            .Select((k, i) => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes($"v-{r:D5}-{i:D2}")))
            .ToList();

        var seed = Batch(0);
        for (var i = 0; i < keys.Count; i++) await tree.SetAsync(keys[i], seed[i].Value);
        await tree.SetManyAtomicAsync(seed);

        await tree.ReshardAsync(target);

        using var stop = new CancellationTokenSource();
        var round = 0;
        var writer = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
                await tree.SetManyAtomicAsync(Batch(Interlocked.Increment(ref round)));
        });

        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        while (!await reshard.IsIdleAsync())
        {
            Assert.That(Environment.TickCount64, Is.LessThan(deadline), "Reshard did not complete.");
            await reshard.RunReshardPassAsync();
            for (var idx = 0; idx < Math.Max(target, FourShardClusterFixture.TestShardCount); idx++)
            {
                var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{idx}");
                if (!await split.IsIdleAsync()) await split.RunSplitPassAsync();
                var fold = _cluster.GrainFactory.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{idx}");
                if (!await fold.IsIdleAsync()) await fold.RunConsolidationPassAsync();
            }

            await Task.Delay(50);
        }

        await Task.Delay(300);
        stop.Cancel();
        await writer;

        var count = await tree.CountAsync();
        var scanned = new List<string>();
        await foreach (var k in tree.ScanKeysAsync(maxAttempts: 5)) scanned.Add(k);

        if (count != keys.Count || !scanned.OrderBy(k => k, StringComparer.Ordinal).SequenceEqual(keys))
        {
            TestContext.Out.WriteLine($"count={count} scanned=[{string.Join(",", scanned)}]");
            await DumpAsync(tree, treeId);
        }

        Assert.That(count, Is.EqualTo(keys.Count), "CountAsync");
        Assert.That(scanned, Is.EquivalentTo(keys), "ScanKeysAsync");
    }

    private async Task DumpAsync(ILattice tree, string treeId)
    {
        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        TestContext.Out.WriteLine($"map v{routing.Map.Version} physical=[{string.Join(",", routing.Map.GetPhysicalShardIndices())}] perShard=[{string.Join(",", await tree.CountPerShardAsync())}]");
        foreach (var idx in routing.Map.GetPhysicalShardIndices())
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{idx}");
            TestContext.Out.WriteLine($"shard {idx}: count={await shard.CountAsync()}");
            var leafId = await shard.GetLeftmostLeafIdAsync();
            var hops = 0;
            while (leafId is { } id && hops++ < 64)
            {
                var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id);
                var range = await leaf.GetKeyRangeAsync();
                var lk = await leaf.GetKeysAsync();
                var pending = await leaf.GetPendingKeysAsync();
                var owners = string.Join(",", lk.Select(k => $"{k}->{routing.Map.Resolve(k)}"));
                TestContext.Out.WriteLine($"  leaf {id} range={range} count={await leaf.CountAsync()} keys=[{owners}] pending=[{string.Join(",", pending)}]");
                leafId = await leaf.GetNextSiblingAsync();
            }
        }
    }
}

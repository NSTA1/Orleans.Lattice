using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Diagnostics;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end regression coverage for the bounded outbound shard-forward
/// deadline. Reproduces the shape of the real-Azure wedge that motivated the
/// fix: a reshard runs while a sustained batch of concurrent writes fans out
/// across the migrating shards, so foreground writes ride the cross-shard
/// shadow-forward / migration path during the swap window. With a finite
/// <see cref="LatticeOptions.ShardForwardTimeout"/> a forward that parks
/// against a shard whose ownership is changing is abandoned and retried
/// rather than pinning the foreground turn, so the whole workload completes
/// within a bounded wall-clock budget and every key survives.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShardForwardDeadlineIntegrationTests
{
    private ShardForwardDeadlineClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int InitialShardCount = 4;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ShardForwardDeadlineClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private async Task RegisterTreeAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = ShardForwardDeadlineClusterFixture.SmallMaxLeafKeys,
            ShardCount = InitialShardCount,
        });
    }

    /// <summary>
    /// Drives the reshard coordinator and every dispatched per-shard split
    /// coordinator to completion synchronously, mirroring the deterministic
    /// progress helper used by the other reshard integration tests so the
    /// test budget does not depend on TestCluster timer cadence.
    /// </summary>
    private async Task DriveReshardToCompletionAsync(string treeId)
    {
        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        for (int i = 0; i < 80; i++)
        {
            if (await reshard.IsIdleAsync()) return;

            await reshard.RunReshardPassAsync();

            var map = await registry.GetShardMapAsync(treeId)
                ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, InitialShardCount);
            foreach (var idx in map.GetPhysicalShardIndices())
            {
                var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{idx}");
                if (!await split.IsIdleAsync())
                    await split.RunSplitPassAsync();
            }

            await Task.Delay(50);
        }

        Assert.Fail("Reshard did not converge within the drive budget.");
    }

    [Test]
    public async Task Reshard_under_concurrent_write_load_does_not_wedge_with_short_forward_deadline()
    {
        var treeId = $"forward-deadline-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Seed enough keys (with the pinned MaxLeafKeys=4) that the tree spans
        // every initial shard, so the cross-shard forward path is genuinely
        // exercised once the reshard starts moving virtual slots.
        const int seedCount = 200;
        var expected = new Dictionary<string, byte[]>(seedCount);
        for (int i = 0; i < seedCount; i++)
        {
            var key = $"seed-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"v-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }

        // Kick off the reshard and immediately fan a large batch of concurrent
        // writes across the tree while the swap window is open. Each write may
        // ride the shadow-forward / migration path for a moved virtual slot.
        var reshardTask = tree.ReshardAsync(8);

        const int liveCount = 300;
        var liveWrites = new List<Task>(liveCount);
        for (int i = 0; i < liveCount; i++)
        {
            var key = $"live-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"live-{i}");
            expected[key] = value;
            liveWrites.Add(tree.SetAsync(key, value));
        }

        // The headline assertion: the concurrent write batch must drain within
        // a bounded budget. Before the fix a forward parked against a migrating
        // shard pinned the foreground turn and the fan-out saturated at its
        // in-flight limit, so this Task.WhenAll never completed. The budget is
        // generous relative to the 2s forward deadline so a healthy run with a
        // few transient timeout-and-retry cycles still passes comfortably.
        var sw = Stopwatch.StartNew();
        var allWrites = Task.WhenAll(liveWrites);
        var winner = await Task.WhenAny(allWrites, Task.Delay(TimeSpan.FromSeconds(60)));
        Assert.That(winner, Is.SameAs(allWrites),
            "Concurrent writes did not drain within 60s during reshard - the write pipeline wedged.");
        await allWrites; // observe any write fault
        sw.Stop();

        await reshardTask;
        await DriveReshardToCompletionAsync(treeId);

        // Topology actually grew.
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var finalMap = await registry.GetShardMapAsync(treeId);
        Assert.That(finalMap, Is.Not.Null);
        Assert.That(finalMap!.GetPhysicalShardIndices().Count, Is.GreaterThanOrEqualTo(8),
            "ShardMap should contain at least the target number of distinct physical shards.");

        // No data loss: every seeded and live-written key survives the swap,
        // since convergence is guaranteed by the coordinator's authoritative
        // leaf-chain drain even when a per-write forward was abandoned.
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' missing after reshard");
            Assert.That(actual, Is.EqualTo(value).AsCollection, $"Wrong value for '{key}' after reshard");
        }

        Assert.That(await tree.CountAsync(), Is.EqualTo(expected.Count),
            "Count must match the total of seeded plus live-written keys.");
    }
}

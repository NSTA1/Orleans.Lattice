using System.Collections.Concurrent;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A range delete acknowledged after an atomic saga was decided on a key must
/// take effect at once and survive the saga's terminal, even when the leaf owning
/// the key has a clock ahead of the routing silo's wall time (issue #4530). The
/// delete's single issue stamp used to be the facade's wall-clock tick, which a
/// leaf clock pushed ahead - by a future-dated merged or replicated row, or by
/// cross-silo clock skew - could out-rank: the delete then sorted below the
/// saga's prepare, the read gate kept serving the prepared value, and the
/// terminal's drain installed it over the tombstone. The issue's probe, adopted.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class DeleteRangeAfterSagaDecisionIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        TerminalHold.ReleaseAll();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Range_delete_acknowledged_after_the_decision_survives_the_terminal_drain(bool leafClockAhead)
    {
        var client = _cluster.Client;
        var treeId = $"range-delete-after-decision-{Guid.NewGuid():N}";
        var tree = client.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k", "pre"u8.ToArray());
        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        var shardIndex = routing.Map.Resolve("k");
        var shardKey = $"{routing.PhysicalTreeId}/{shardIndex}";
        var sibling = Enumerable.Range(0, 10_000).Select(i => $"k-sib-{i}").First(s => routing.Map.Resolve(s) == shardIndex);
        if (leafClockAhead)
        {
            // Push the leaf clock ahead of wall time, as any future-dated merged
            // or replicated row, or cross-silo clock skew, does.
            var future = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks + TimeSpan.FromMinutes(10).Ticks };
            await client.GetGrain<IShardRootGrain>(shardKey).MergeManyAsync(
                new() { [sibling] = LwwValue<byte[]>.Create("f"u8.ToArray(), future) });
        }

        var hold = TerminalHold.Arm(shardKey);
        Task saga;
        try
        {
            saga = tree.SetManyAtomicAsync([new("k", "saga"u8.ToArray())]);
            await hold.Entered.Task.WaitAsync(TimeSpan.FromSeconds(60));
            Assert.That(await tree.GetAsync("k"), Is.EqualTo("saga"u8.ToArray()), "precondition: the saga is decided");

            var deleted = await tree.DeleteRangeAsync("k", "k\u0001");
            Assert.That(deleted, Is.EqualTo(1));
            Assert.That(await tree.GetAsync("k"), Is.Null, "an acknowledged range delete must be visible at once");
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        await saga;
        Assert.That(await tree.GetAsync("k"), Is.Null, "the saga's terminal must not reinstate a key deleted after its decision");
    }

    [Test]
    public async Task The_shard_probe_reports_the_highest_leaf_clock_over_exactly_the_range_it_covers()
    {
        var client = _cluster.Client;
        var treeId = $"range-clock-probe-{Guid.NewGuid():N}";
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .RegisterAsync(treeId, new TreeRegistryEntry { MaxLeafKeys = 4, ShardCount = 1 });
        var tree = client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < 40; i++)
            await tree.SetAsync($"a{i:D3}", "v"u8.ToArray());
        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        var shard = client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{routing.Map.Resolve("a000")}");

        // One leaf deep in the range gets a clock ten minutes ahead.
        var future = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks + TimeSpan.FromMinutes(10).Ticks };
        await shard.MergeManyAsync(new() { ["a030x"] = LwwValue<byte[]>.Create("f"u8.ToArray(), future) });

        var (whole, pages) = await DrainProbeAsync(shard, "a000", "a040");
        Assert.That(whole, Is.GreaterThanOrEqualTo(future), "the probe must see the leaf whose clock is ahead");
        Assert.That(pages, Is.GreaterThan(1), "precondition: the probe yielded and resumed by key");

        var (head, _) = await DrainProbeAsync(shard, "a000", "a002");
        Assert.That(head, Is.LessThan(future), "the probe stops at the end of its range");
        Assert.That(head, Is.GreaterThan(HybridLogicalClock.Zero));
    }

    private static async Task<(HybridLogicalClock Max, int Pages)> DrainProbeAsync(IShardRootGrain shard, string start, string end)
    {
        var max = HybridLogicalClock.Zero;
        var pages = 0;
        string? from = start;
        while (from is not null)
        {
            var page = await shard.GetRangeClockBoundedAsync(from, end);
            pages++;
            if (page.MaxClock > max) max = page.MaxClock;
            from = page.ResumeFromInclusive;
        }

        return (max, pages);
    }

    /// <summary>
    /// Holds the first <see cref="IShardRootGrain.AppendTxTerminalAsync"/> sent to an
    /// armed shard until the test releases it, so the saga sits decided but not
    /// yet drained.
    /// </summary>
    private static class TerminalHold
    {
        private static readonly ConcurrentDictionary<string, Hold> Holds = new();

        public static Hold Arm(string shardKey) => Holds[shardKey] = new Hold();

        public static void ReleaseAll()
        {
            foreach (var hold in Holds.Values) hold.Release.TrySetResult();
        }

        public static async Task InvokeAsync(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.Name == nameof(IShardRootGrain.AppendTxTerminalAsync)
                && context.TargetId.Type.ToString() == "shardroot"
                && Holds.TryRemove(context.TargetId.Key.ToString()!, out var hold))
            {
                hold.Entered.TrySetResult();
                await hold.Release.Task;
            }

            await context.Invoke();
        }
    }

    private sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            // Small scan pages, so the range clock probe yields and resumes.
            siloBuilder.ConfigureLattice(o => o.MaxLeavesPerScanPage = 2);
            siloBuilder.AddOutgoingGrainCallFilter(TerminalHold.InvokeAsync);
        }
    }
}

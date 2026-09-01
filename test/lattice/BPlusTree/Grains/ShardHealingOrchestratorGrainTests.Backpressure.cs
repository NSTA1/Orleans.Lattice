using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Backpressure coverage: healing must yield to foreground traffic.
/// <para>
/// Repairing a thousand-shard tree drains a whole donor per fold, which is real
/// work on exactly the busy deployment healing exists to help. The requirement
/// is therefore not "healing is cheap" but "healing is invisible": while the
/// tree is serving, the orchestrator adds no load of its own at all - it neither
/// starts a fold nor pushes an existing one along.
/// </para>
/// </summary>
public partial class ShardHealingOrchestratorGrainTests
{
    /// <summary>
    /// Loads every shard of the tree at <paramref name="opsPerShard"/>
    /// operations per second, uniformly, so the tree is unambiguously eligible
    /// for healing on shape and the only thing that can refuse it is load.
    /// </summary>
    private static void LoadForeground(Harness h, int shardCount, long opsPerShard)
        => LoadUniformly(h, shardCount, opsPerShard);

    [Test]
    public async Task Healing_yields_while_the_tree_is_serving_foreground_traffic()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        LoadForeground(h, 8, opsPerShard: LatticeOptions.DefaultHotShardOpsPerSecondThreshold + 50);

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Backpressure));
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty);
        });
        await h.ConsolidationOf(7).DidNotReceive().StartAsync(Arg.Any<int>());
    }

    [Test]
    public async Task Backpressure_also_stops_the_orchestrator_driving_an_existing_fold()
    {
        // Yielding must be total. An orchestrator that stopped admitting but
        // kept pushing a drain along would still be adding load to a tree that
        // is already busy, which is the cost the user would actually feel.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        MarkInFlight(h, donor: 7, survivor: 6);
        LoadForeground(h, 8, opsPerShard: 5_000);

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Backpressure));
        await h.ConsolidationOf(7).DidNotReceive().RunConsolidationPassAsync();
    }

    [Test]
    public async Task Healing_resumes_the_moment_foreground_load_subsides()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        LoadForeground(h, 8, opsPerShard: 5_000);
        await h.Grain.RunHealingPassAsync();
        Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Backpressure));

        LoadForeground(h, 8, opsPerShard: 1);
        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Admitted));
            Assert.That(h.State.State.InFlightDonorShardIndices, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Backpressure_measures_the_median_shard_not_the_summed_tree()
    {
        // The distinction that decides whether a badly over-split tree could
        // ever heal at all. A thousand near-idle shards sum to a large tree
        // rate, so a summed threshold would report the most damaged tree on the
        // box as the busiest and refuse to heal it forever. The median reports
        // it idle. (Hypothetical: no production tree was found in this shape -
        // see the note in ShardHealingDecisionCoreTests.)
        var h = CreateGrain(physicalShardCount: 256, baseShardCount: 16, virtualShardCount: 1024);
        LoadForeground(h, 256, opsPerShard: 1);

        await h.Grain.RunHealingPassAsync();

        var report = await h.Grain.GetHealingReportAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.MedianShardOpsPerSecond, Is.EqualTo(1d).Within(1e-9),
                "256 shards at 1 op/s each is a median of 1, not a tree total of 256");
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.Admitted));
        });
    }

    [Test]
    public async Task Backpressure_yields_at_exactly_the_configured_threshold()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { ShardHealingBackpressureOpsPerSecond = 100d });
        LoadForeground(h, 8, opsPerShard: 100);

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Backpressure),
            "the threshold is inclusive so a tree exactly at it yields");
    }

    [Test]
    public async Task Backpressure_admits_just_below_the_configured_threshold()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { ShardHealingBackpressureOpsPerSecond = 100d });
        LoadForeground(h, 8, opsPerShard: 99);

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Admitted));
    }

    [Test]
    public async Task A_zero_threshold_heals_regardless_of_load()
    {
        // The documented "0 disables backpressure" value: an operator who wants
        // a damaged tree repaired now, load or no load.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { ShardHealingBackpressureOpsPerSecond = 0d });
        LoadForeground(h, 8, opsPerShard: 100_000);

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Admitted));
    }

    [Test]
    public async Task A_loaded_tree_costs_no_consolidation_traffic_at_all()
    {
        // The strongest form of the invisibility claim: under sustained
        // foreground load, across many sweeps, the orchestrator issues not one
        // call to any consolidation coordinator.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [] });
        LoadForeground(h, 8, opsPerShard: 5_000);

        for (var i = 0; i < 25; i++) await h.Grain.RunHealingPassAsync();

        for (var shard = 0; shard < 8; shard++)
        {
            await h.ConsolidationOf(shard).DidNotReceive().StartAsync(Arg.Any<int>());
            await h.ConsolidationOf(shard).DidNotReceive().RunConsolidationPassAsync();
        }
    }
}

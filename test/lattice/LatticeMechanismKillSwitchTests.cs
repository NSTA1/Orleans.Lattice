using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// One test per kill switch: turning a mechanism off must restore the behaviour
/// that preceded it <b>exactly</b>, not approximately.
/// <para>
/// Every mechanism in the bounded-cold-start epic ships enabled, so each needs
/// its own escape hatch - an operator who hits trouble at three in the morning
/// must be able to disable one behaviour rather than roll the image back. A
/// documented kill switch that silently stopped reaching its mechanism would be
/// worse than none at all, which is why these assertions drive the real
/// scheduler and the real decision cores rather than reading option values
/// back.
/// </para>
/// <para>
/// Two mechanisms are switched at the leaf grain and are proved where they act,
/// against the real capture and rehydrate paths:
/// <c>BPlusLeafGrainTests.CaptureSnapshotAsync_writes_the_legacy_row_graph_when_the_binary_encoding_is_disabled</c>
/// for the snapshot codec, and the partial-hydration fixture for bounded
/// hydration. What is asserted here for those two is the property this fixture
/// owns: that the switch survives per-tree option resolution, since a knob
/// dropped from that projection is invisible to every grain.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeMechanismKillSwitchTests
{
    private static readonly TimeSpan Ceiling = TimeSpan.FromHours(1);
    private static readonly TimeSpan Floor = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A small but positive stagger for the cadence fixtures. It must be
    /// positive: <c>Task.Delay(TimeSpan.Zero, ...)</c> completes without arming
    /// a timer, so a zero stagger runs the first pass during service start
    /// rather than on a step the test drives.
    /// </summary>
    private static readonly TimeSpan Stagger = TimeSpan.FromSeconds(1);

    // ------------------------------------------------------------- WAL GC (S2)

    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static LatticeWalGcReport Report(long entriesTrimmed) =>
        new("tree", null, null, null, null, 1, entriesTrimmed, null, null, null);

    private static (LatticeWalGcScheduler Scheduler, VirtualTimeProvider Time) CreateScheduler(
        LatticeOptions options,
        long entriesTrimmedPerPass)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(_ => Task.FromResult<IReadOnlyList<string>>(new[] { "alpha" }));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Report(entriesTrimmedPerPass));

        var time = new VirtualTimeProvider();
        var scheduler = new LatticeWalGcScheduler(
            factory,
            gc,
            Monitor(options),
            Substitute.For<ILogger<LatticeWalGcScheduler>>(),
            time);

        return (scheduler, time);
    }

    private static Task Parked(Task parked) => parked.WaitAsync(TimeSpan.FromSeconds(30));

    /// <summary>Starts the scheduler and drives it through its first pass, leaving it parked on the next cadence delay.</summary>
    private static async Task StartAndRunFirstPassAsync(LatticeWalGcScheduler scheduler, VirtualTimeProvider time)
    {
        var armed = time.NextTimerAsync();
        await scheduler.StartAsync(CancellationToken.None);
        await Parked(armed);

        var next = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(next);
    }

    [Test]
    public async Task Wal_gc_adaptive_cadence_off_restores_the_fixed_ceiling_tick()
    {
        // A pass that reclaims entries is exactly the signal that collapses the
        // interval to the floor. With the floor switched off it must not: the
        // cadence stays pinned at the ceiling however much backlog is found,
        // which is the historical fixed-interval tick.
        var (scheduler, time) = CreateScheduler(
            new LatticeOptions
            {
                WalGcInterval = Ceiling,
                WalGcMinInterval = TimeSpan.Zero,
                WalGcStartupDelay = Stagger,
            },
            entriesTrimmedPerPass: 500);

        await StartAndRunFirstPassAsync(scheduler, time);

        Assert.That(time.LastScheduledDelay, Is.EqualTo(Ceiling));
        for (var pass = 0; pass < 3; pass++)
        {
            var next = time.NextTimerAsync();
            time.Advance(time.LastScheduledDelay);
            await Parked(next);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Ceiling),
                "a reclaiming tree must not shorten its cadence once the adaptive band is collapsed");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Wal_gc_adaptive_cadence_on_shortens_the_cadence_for_the_same_backlog()
    {
        // The mirror of the test above, so the kill switch is proved to be what
        // changed the behaviour rather than the fixture.
        var (scheduler, time) = CreateScheduler(
            new LatticeOptions
            {
                WalGcInterval = Ceiling,
                WalGcMinInterval = Floor,
                WalGcStartupDelay = Stagger,
            },
            entriesTrimmedPerPass: 500);

        await StartAndRunFirstPassAsync(scheduler, time);

        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task A_floor_above_the_ceiling_collapses_the_band_the_same_way()
    {
        // The second documented spelling of the same kill switch: an operator
        // who raises the floor past the ceiling gets the fixed tick rather than
        // an inverted band.
        var (scheduler, time) = CreateScheduler(
            new LatticeOptions
            {
                WalGcInterval = Ceiling,
                WalGcMinInterval = Ceiling + TimeSpan.FromHours(1),
                WalGcStartupDelay = Stagger,
            },
            entriesTrimmedPerPass: 500);

        await StartAndRunFirstPassAsync(scheduler, time);

        Assert.That(time.LastScheduledDelay, Is.EqualTo(Ceiling));

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Wal_gc_startup_delay_at_the_interval_restores_the_historical_first_pass_deferral()
    {
        // Before the responsive cadence landed, the first pass was drawn from
        // [interval / 2, interval) - 30 to 60 minutes at the shipped ceiling.
        // Setting the stagger window to the interval reproduces that draw
        // exactly, which is the setting an operator wants when the early first
        // pass itself is the problem. Note this is a different switch from
        // TimeSpan.Zero, which means "no stagger, run immediately".
        var (scheduler, time) = CreateScheduler(
            new LatticeOptions
            {
                WalGcInterval = Ceiling,
                WalGcMinInterval = Floor,
                WalGcStartupDelay = Ceiling,
            },
            entriesTrimmedPerPass: 0);

        var armed = time.NextTimerAsync();
        await scheduler.StartAsync(CancellationToken.None);
        await Parked(armed);

        Assert.Multiple(() =>
        {
            Assert.That(time.LastScheduledDelay, Is.GreaterThanOrEqualTo(Ceiling / 2));
            Assert.That(time.LastScheduledDelay, Is.LessThan(Ceiling));
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public void Wal_gc_disabled_outright_still_means_a_non_positive_interval()
    {
        // The coarsest switch of the three is unchanged by the epic and remains
        // the way to hand WAL collection back to an explicit caller.
        var options = new LatticeOptions { WalGcInterval = TimeSpan.Zero };

        Assert.That(new LatticeOptionsValidator().Validate("tree", options).Succeeded, Is.True,
            "a kill switch that fails startup validation is not a kill switch");
    }

    // --------------------------------------------------- split admission (S4)

    private static ShardSplitSample BulkIngestShard() => new()
    {
        Rate = 5_000d,
        Entries = ShardSplitSample.EntriesNotSampled,
        OwnedSlots = 2,
        IsSplitting = false,
        InCooldown = false,
    };

    [Test]
    public void Split_admission_shape_gate_off_restores_rate_only_admission()
    {
        // Pre-#1834 admission had no skew clause, no shard ceiling and no
        // occupancy floor: a shard over the rate threshold with slots to
        // subdivide was admitted. Zeroing all three restores exactly that, so a
        // bulk ingest shatters the tree again - which is the point. An operator
        // reaching for this switch is choosing the old behaviour knowingly.
        var reverted = new LatticeOptions
        {
            HotShardMinSkewRatio = 1.0d,
            MaxPhysicalShardsPerTree = 0,
            HotShardMinShardEntries = 0,
        };

        Assert.That(new LatticeOptionsValidator().Validate("tree", reverted).Succeeded, Is.True,
            "the documented revert must pass startup validation");

        var policy = ShardSplitAdmissionPolicy.FromOptions(reverted);
        var defaults = ShardSplitAdmissionPolicy.FromOptions(new LatticeOptions());
        var sample = BulkIngestShard();

        Assert.Multiple(() =>
        {
            Assert.That(
                ShardSplitAdmissionCore.Evaluate(sample, defaults, treeSkewRatio: 1.0d, physicalShardCount: 8),
                Is.EqualTo(ShardSplitAdmissionOutcome.UniformLoad),
                "the shipped defaults must refuse the bulk-ingest shape");
            Assert.That(
                ShardSplitAdmissionCore.Evaluate(sample, policy, treeSkewRatio: 1.0d, physicalShardCount: 8),
                Is.EqualTo(ShardSplitAdmissionOutcome.Admitted),
                "with the shape gate reverted the same shard is admitted on rate alone, as it was before");
        });
    }

    [Test]
    public void Reverting_only_the_skew_gate_leaves_the_new_shard_ceiling_in_place()
    {
        // The trap in the S4 revert, pinned so the documentation cannot drift
        // from it: the physical-shard ceiling is a separate knob that did not
        // exist before, so an operator who zeroes only the skew ratio still
        // cannot grow past 256 shards and would conclude the revert had not
        // worked.
        var partial = new LatticeOptions { HotShardMinSkewRatio = 1.0d };
        var policy = ShardSplitAdmissionPolicy.FromOptions(partial);

        Assert.That(
            ShardSplitAdmissionCore.Evaluate(
                BulkIngestShard(),
                policy,
                treeSkewRatio: 1.0d,
                physicalShardCount: LatticeOptions.DefaultMaxPhysicalShardsPerTree),
            Is.EqualTo(ShardSplitAdmissionOutcome.ShardCeilingReached));
    }

    // -------------------------------------------------------- heal loop (S11)

    private static ShardHealingSample OverSplitUniformTree() => new()
    {
        PhysicalShardCount = 12,
        BaseShardCount = 4,
        SkewRatio = 1.0d,
        MedianShardOpsPerSecond = 0d,
        InFlightConsolidations = 0,
        IsSplitting = false,
        InTreeMaintenance = false,
        InCooldown = false,
    };

    [Test]
    public void Healing_off_leaves_an_over_split_tree_completely_alone()
    {
        var sample = OverSplitUniformTree();

        Assert.Multiple(() =>
        {
            Assert.That(
                ShardHealingDecisionCore.Decide(sample, ShardHealingPolicy.FromOptions(new LatticeOptions())),
                Is.EqualTo(ShardHealingDecision.Admitted),
                "the shipped defaults must heal an idle over-split tree");
            Assert.That(
                ShardHealingDecisionCore.Decide(
                    sample,
                    ShardHealingPolicy.FromOptions(new LatticeOptions { ShardHealingEnabled = false })),
                Is.EqualTo(ShardHealingDecision.Disabled),
                "the kill switch must refuse before any other clause is even consulted");
        });
    }

    [Test]
    public void A_zero_consolidation_cap_pauses_admission_without_disabling_the_mechanism()
    {
        // The two knobs answer different operator questions and both are
        // supported: ShardHealingEnabled stops the mechanism (no reminder, no
        // timer, no shard polling), while a zero cap keeps the observer running
        // and publishing the tree's backlog while admitting nothing.
        var paused = new LatticeOptions { MaxConcurrentShardConsolidations = 0 };

        Assert.Multiple(() =>
        {
            Assert.That(new LatticeOptionsValidator().Validate("tree", paused).Succeeded, Is.True);
            Assert.That(
                ShardHealingDecisionCore.Decide(OverSplitUniformTree(), ShardHealingPolicy.FromOptions(paused)),
                Is.EqualTo(ShardHealingDecision.AdmissionClosed));
            Assert.That(paused.ShardHealingEnabled, Is.True,
                "pausing admission must not be confused with disabling the mechanism");
        });
    }

    // ------------------------------------------------------- pre-warm (#1820)

    [Test]
    public void Leaf_cache_pre_warm_off_restores_the_untracked_warm_up()
    {
        var settings = TestOptionsResolver
            .Create(baseOptions: new LatticeOptions { LeafCachePreWarmCount = 0 })
            .GetLeafAccessTrackingSettings("tree");

        Assert.Multiple(() =>
        {
            Assert.That(settings.IsEnabled, Is.False);
            Assert.That(settings, Is.EqualTo(LeafAccessTrackingSettings.Disabled),
                "a disabled feature must resolve to the singleton disabled settings, so no timer is armed "
                + "and no access is recorded");
        });
    }

    // -------------------------------- leaf-grain switches: reachability (S3/S9)

    [TestCase(false)]
    [TestCase(true)]
    public async Task The_binary_snapshot_encoding_switch_reaches_the_grain(bool enabled)
    {
        var resolved = await TestOptionsResolver
            .Create(baseOptions: new LatticeOptions { LeafSnapshotBinaryEncodingEnabled = enabled })
            .ResolveAsync("tree");

        Assert.That(resolved.LeafSnapshotBinaryEncodingEnabled, Is.EqualTo(enabled));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task The_partial_hydration_switch_reaches_the_grain(bool enabled)
    {
        var resolved = await TestOptionsResolver
            .Create(baseOptions: new LatticeOptions { LeafPartialHydrationEnabled = enabled })
            .ResolveAsync("tree");

        Assert.That(resolved.LeafPartialHydrationEnabled, Is.EqualTo(enabled));
    }
}

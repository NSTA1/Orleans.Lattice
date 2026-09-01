using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;
namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <see cref="ShardHealingOrchestratorGrain"/>, the steady-state
/// driver that makes over-split healing happen automatically on an existing
/// deployment rather than being a capability an operator must discover and
/// invoke.
/// <para>
/// Every test here drives <c>RunHealingPassAsync</c> directly and reads the
/// clock through an injected <see cref="VirtualTimeProvider"/>, so nothing in
/// this suite depends on wall-clock timing, thread-pool ordering, or GC
/// behaviour.
/// </para>
/// </summary>
[TestFixture]
public partial class ShardHealingOrchestratorGrainTests
{
    private const string TreeId = "healing-test-tree";

    /// <summary>
    /// The harness a test drives: the grain, the substitutes it talks to, and
    /// the mutable shard map the registry hands back - so a test can model a
    /// fold committing by shrinking the map, exactly as the real coordinator
    /// does when it swaps the routing.
    /// </summary>
    private sealed class Harness
    {
        public required ShardHealingOrchestratorGrain Grain { get; init; }
        public required FakePersistentState<ShardHealingOrchestratorState> State { get; init; }
        public required ILatticeRegistry Registry { get; init; }
        public required ILattice Lattice { get; init; }
        public required VirtualTimeProvider Clock { get; init; }
        public required LatticeOptions Options { get; init; }
        public required Func<int, IShardRootGrain> ShardOf { get; init; }
        public required Func<int, ITreeShardConsolidationGrain> ConsolidationOf { get; init; }
        public required IReminderRegistry Reminders { get; init; }

        /// <summary>The map the registry currently reports for the tree.</summary>
        public required Func<ShardMap> CurrentMap { get; init; }

        /// <summary>Replaces the reported routing map, modelling a committed fold.</summary>
        public required Action<ShardMap> SetMap { get; init; }
    }

    /// <summary>
    /// Builds an over-split tree: <paramref name="physicalShardCount"/>
    /// physical shards over <paramref name="virtualShardCount"/> virtual slots,
    /// against a pinned base of <paramref name="baseShardCount"/>.
    /// </summary>
    private static Harness CreateGrain(
        int physicalShardCount = 8,
        int baseShardCount = 2,
        int virtualShardCount = 64,
        LatticeOptions? options = null,
        ShardHealingOrchestratorState? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("healing", TreeId));
        // The sweep timer is armed through the activation's service provider, so
        // a test that drives EnsureRunningAsync needs a timer registry wired in.
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITimerRegistry)).Returns(Substitute.For<ITimerRegistry>());
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();

        options ??= new LatticeOptions();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var lattice = Substitute.For<ILattice>();
        lattice.IsResizeCompleteAsync().Returns(true);
        lattice.IsReshardCompleteAsync().Returns(true);
        lattice.IsMergeCompleteAsync().Returns(true);
        lattice.IsSnapshotCompleteAsync().Returns(true);
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var map = ShardMap.CreateDefault(virtualShardCount, physicalShardCount);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(_ => map);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = baseShardCount,
            }));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);

        // Every shard is idle and quiescent by default, so a test that does not
        // say otherwise is measuring the uniformly-loaded shape healing exists
        // to repair.
        var shardSubs = new Dictionary<int, IShardRootGrain>();
        IShardRootGrain Shard(int i)
        {
            if (shardSubs.TryGetValue(i, out var s)) return s;
            var sub = Substitute.For<IShardRootGrain>();
            sub.GetHotnessAsync().Returns(new ShardHotness { Reads = 0, Writes = 0, Window = TimeSpan.FromSeconds(30) });
            sub.IsSplittingAsync().Returns(false);
            sub.HasPendingBulkOperationAsync().Returns(false);
            shardSubs[i] = sub;
            return sub;
        }
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            return Shard(int.Parse(key[(key.LastIndexOf('/') + 1)..]));
        });

        // One consolidation coordinator per donor shard index, idle by default.
        var consolidationSubs = new Dictionary<int, ITreeShardConsolidationGrain>();
        ITreeShardConsolidationGrain Consolidation(int i)
        {
            if (consolidationSubs.TryGetValue(i, out var c)) return c;
            var sub = Substitute.For<ITreeShardConsolidationGrain>();
            sub.GetProgressAsync().Returns(new ShardConsolidationProgress { InProgress = false, Complete = true });
            sub.IsIdleAsync().Returns(true);
            consolidationSubs[i] = sub;
            return sub;
        }
        grainFactory.GetGrain<ITreeShardConsolidationGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            return Consolidation(int.Parse(key[(key.LastIndexOf('/') + 1)..]));
        });

        var state = existingState is null
            ? new FakePersistentState<ShardHealingOrchestratorState>()
            : new FakePersistentState<ShardHealingOrchestratorState> { State = existingState };

        var clock = new VirtualTimeProvider();
        var grain = new ShardHealingOrchestratorGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<ShardHealingOrchestratorGrain>(),
            state)
        {
            TimeProvider = clock,
        };

        return new Harness
        {
            Grain = grain,
            State = state,
            Registry = registry,
            Lattice = lattice,
            Clock = clock,
            Options = options,
            ShardOf = Shard,
            ConsolidationOf = Consolidation,
            Reminders = reminderRegistry,
            CurrentMap = () => map,
            SetMap = m => map = m,
        };
    }

    /// <summary>
    /// Builds a routing map whose slots are distributed over an explicit set of
    /// physical shard indices, so a test can model a partially-healed tree
    /// whose surviving indices are no longer contiguous.
    /// </summary>
    private static ShardMap MapOver(int virtualShardCount, params int[] physicalShards)
    {
        var slots = new int[virtualShardCount];
        for (var i = 0; i < virtualShardCount; i++) slots[i] = physicalShards[i % physicalShards.Length];
        return new ShardMap { Slots = slots };
    }

    /// <summary>Marks a coordinator as mid-fold, folding <paramref name="donor"/> onto <paramref name="survivor"/>.</summary>
    private static void MarkInFlight(Harness h, int donor, int survivor)
        => h.ConsolidationOf(donor).GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = true,
            DonorShardIndex = donor,
            SurvivorShardIndex = survivor,
            Phase = ShardConsolidationPhase.Drain,
        });

    /// <summary>Gives every shard the same rate, producing a perfectly uniform tree (skew 1.0).</summary>
    private static void LoadUniformly(Harness h, int shardCount, long opsPerShard)
    {
        for (var i = 0; i < shardCount; i++)
        {
            h.ShardOf(i).GetHotnessAsync().Returns(new ShardHotness
            {
                Reads = opsPerShard, Writes = 0, Window = TimeSpan.FromSeconds(1),
            });
        }
    }

    // --- The kill switch --------------------------------------------------

    [Test]
    public async Task RunHealingPass_admits_nothing_when_the_kill_switch_is_off()
    {
        var h = CreateGrain(options: new LatticeOptions { ShardHealingEnabled = false });

        await h.Grain.RunHealingPassAsync();

        var report = await h.Grain.GetHealingReportAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.Disabled));
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty);
        });
        await h.ConsolidationOf(7).DidNotReceive().StartAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunHealingPass_polls_no_shard_when_the_kill_switch_is_off()
    {
        // Switching healing off must stop the work, not merely the outcome: an
        // orchestrator that still swept every shard of a thousand-shard tree
        // would leave the operator's kill switch looking ineffective.
        var h = CreateGrain(options: new LatticeOptions { ShardHealingEnabled = false });

        await h.Grain.RunHealingPassAsync();

        await h.ShardOf(0).DidNotReceive().GetHotnessAsync();
        await h.Lattice.DidNotReceive().IsResizeCompleteAsync();
    }

    [Test]
    public async Task RunHealingPass_leaves_the_tree_consistent_when_the_switch_is_flipped_mid_heal()
    {
        // A fold already in flight is left to its own resumable, idempotent
        // coordinator. Healing stops admitting and stops driving; it never
        // cancels or tears anything.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        MarkInFlight(h, donor: 7, survivor: 6);

        h.Options.ShardHealingEnabled = false;
        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.EqualTo(new[] { 7 }),
                "the in-flight fold must remain tracked so re-enabling healing resumes cleanly");
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Disabled));
        });
        await h.ConsolidationOf(7).DidNotReceive().RunConsolidationPassAsync();
        await h.ConsolidationOf(7).DidNotReceive().CancelAsync();
    }

    [Test]
    public async Task EnsureRunning_registers_no_reminder_when_the_kill_switch_is_off()
    {
        var h = CreateGrain(options: new LatticeOptions { ShardHealingEnabled = false });

        await h.Grain.EnsureRunningAsync();

        await h.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunHealingPass_admits_nothing_when_the_concurrency_cap_is_zero()
    {
        // Zero is the supported "pause admission but keep watching" setting, so
        // unlike the kill switch it must still publish the tree's backlog.
        var h = CreateGrain(options: new LatticeOptions { MaxConcurrentShardConsolidations = 0 });

        await h.Grain.RunHealingPassAsync();

        var report = await h.Grain.GetHealingReportAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.AdmissionClosed));
            Assert.That(report.Backlog, Is.EqualTo(6), "8 physical shards against a base of 2");
        });
        await h.ConsolidationOf(7).DidNotReceive().StartAsync(Arg.Any<int>());
    }

    // --- The steady state -------------------------------------------------

    [Test]
    public async Task RunHealingPass_reports_not_over_split_for_a_healthy_tree()
    {
        var h = CreateGrain(physicalShardCount: 4, baseShardCount: 4);

        await h.Grain.RunHealingPassAsync();

        var report = await h.Grain.GetHealingReportAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.NotOverSplit));
            Assert.That(report.Backlog, Is.Zero);
            Assert.That(report.PhysicalShardCount, Is.EqualTo(4));
            Assert.That(report.BaseShardCount, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task RunHealingPass_polls_no_shard_for_a_healthy_tree()
    {
        // The steady-state cost claim. A healthy tree is settled from the
        // routing map alone, so the observer never touches a shard grain - which
        // is what makes running this forever, on every tree, free.
        var h = CreateGrain(physicalShardCount: 4, baseShardCount: 4);

        await h.Grain.RunHealingPassAsync();

        await h.ShardOf(0).DidNotReceive().GetHotnessAsync();
        await h.ShardOf(0).DidNotReceive().IsSplittingAsync();
        await h.Lattice.DidNotReceive().IsResizeCompleteAsync();
    }

    // --- Admission --------------------------------------------------------

    [Test]
    public async Task RunHealingPass_consolidates_an_over_split_uniformly_loaded_tree()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        LoadUniformly(h, 8, opsPerShard: 10);

        await h.Grain.RunHealingPassAsync();

        var report = await h.Grain.GetHealingReportAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.Admitted));
            Assert.That(h.State.State.InFlightDonorShardIndices, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task RunHealingPass_retires_the_higher_index_of_the_cheapest_adjacent_pair()
    {
        // The planner's rule, observed through the orchestrator: among equally
        // cheap adjacent pairs the lowest wins, and within a tied pair the
        // higher physical index retires - so the healed map drifts back toward
        // the dense low-index identity shape the tree started with.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);

        await h.Grain.RunHealingPassAsync();

        var donor = h.State.State.InFlightDonorShardIndices.Single();
        Assert.That(donor, Is.EqualTo(1), "an evenly-loaded map ties everywhere, so the lowest pair folds first");
        await h.ConsolidationOf(1).Received(1).StartAsync(0);
    }

    [Test]
    public async Task RunHealingPass_admits_at_most_one_fold_per_sweep()
    {
        // The cadence, not a burst, is what spreads healing out. Two sweeps
        // against an unchanged map must not start two different folds.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);

        await h.Grain.RunHealingPassAsync();
        MarkInFlight(h, donor: 7, survivor: 6);
        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.InFlightDonorShardIndices, Has.Count.EqualTo(1),
            "the default cap of one fold must hold across sweeps");
    }

    [Test]
    public async Task RunHealingPass_reports_at_capacity_while_a_fold_runs()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        MarkInFlight(h, donor: 7, survivor: 6);

        await h.Grain.RunHealingPassAsync();

        var report = await h.Grain.GetHealingReportAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.AtCapacity));
            Assert.That(report.InFlightConsolidations, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RunHealingPass_admits_a_second_fold_when_the_cap_allows_it()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { MaxConcurrentShardConsolidations = 2 },
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        MarkInFlight(h, donor: 7, survivor: 6);

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.InFlightDonorShardIndices, Has.Count.EqualTo(2),
            "a raised cap must actually admit a second concurrent fold");
    }

    [Test]
    public async Task A_second_concurrent_fold_never_reuses_a_shard_of_the_first()
    {
        // A shard cannot be draining onto a neighbour and absorbing another at
        // the same time, so the second fold's pair must be disjoint from the
        // first's.
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { MaxConcurrentShardConsolidations = 2 },
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        MarkInFlight(h, donor: 7, survivor: 6);

        await h.Grain.RunHealingPassAsync();

        var donors = h.State.State.InFlightDonorShardIndices;
        Assert.That(donors, Has.Count.EqualTo(2));
        var second = donors[1];
        Assert.Multiple(() =>
        {
            Assert.That(second, Is.Not.EqualTo(7), "the first fold's donor must not be re-picked");
            Assert.That(second, Is.Not.EqualTo(6), "the first fold's survivor must not become a donor");
        });
        await h.ConsolidationOf(second).Received(1).StartAsync(Arg.Is<int>(s => s != 6 && s != 7));
    }

    [Test]
    public async Task RunHealingPass_waits_rather_than_guessing_when_a_coordinator_cannot_be_polled()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { MaxConcurrentShardConsolidations = 2 },
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        h.ConsolidationOf(7).GetProgressAsync()
            .Returns<Task<ShardConsolidationProgress>>(_ => throw new TimeoutException("silo unreachable"));

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.NoFoldablePair),
                "without the unreachable fold's survivor the orchestrator cannot know which pair is safe");
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.EqualTo(new[] { 7 }),
                "an unreachable coordinator is not evidence its fold finished");
        });
    }

    [Test]
    public async Task RunHealingPass_reports_no_foldable_pair_when_the_coordinator_refuses()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        h.ConsolidationOf(1).StartAsync(Arg.Any<int>())
            .Returns<Task>(_ => throw new InvalidOperationException("a split is in flight on the survivor"));

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.NoFoldablePair));
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty,
                "a refused start must not be tracked as in flight");
        });
    }

    // --- Hysteresis with the splitter -------------------------------------

    [Test]
    public async Task RunHealingPass_refuses_a_skewed_tree()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        LoadUniformly(h, 8, opsPerShard: 10);
        // One shard carrying 40x the median: concentrated load a fold would
        // make worse, and exactly what the splitter is for.
        h.ShardOf(0).GetHotnessAsync().Returns(new ShardHotness
        {
            Reads = 400, Writes = 0, Window = TimeSpan.FromSeconds(1),
        });

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.SkewedLoad));
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty);
        });
    }

    [Test]
    public async Task RunHealingPass_serialises_behind_an_in_flight_split()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        h.ShardOf(3).IsSplittingAsync().Returns(true);

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.SplitInFlight));
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty);
        });
    }

    [Test]
    public async Task Observing_a_split_arms_the_post_split_cooldown()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        h.ShardOf(3).IsSplittingAsync().Returns(true);

        await h.Grain.RunHealingPassAsync();
        var armedAt = h.State.State.CooldownUntilUtc;

        // The split finishes, but the tree's shape has just changed, so healing
        // must still stand off until the window elapses.
        h.ShardOf(3).IsSplittingAsync().Returns(false);
        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(armedAt, Is.Not.Null);
            Assert.That(armedAt, Is.EqualTo(h.Clock.GetUtcNow().UtcDateTime + LatticeOptions.DefaultShardHealingCooldown));
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Cooldown));
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty);
        });
    }

    [Test]
    public async Task Healing_resumes_once_the_cooldown_elapses()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        h.ShardOf(3).IsSplittingAsync().Returns(true);
        await h.Grain.RunHealingPassAsync();
        h.ShardOf(3).IsSplittingAsync().Returns(false);

        h.Clock.Advance(LatticeOptions.DefaultShardHealingCooldown + TimeSpan.FromSeconds(1));
        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Admitted));
            Assert.That(h.State.State.InFlightDonorShardIndices, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_zero_cooldown_leaves_only_the_skew_dead_band()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { ShardHealingCooldown = TimeSpan.Zero });
        h.ShardOf(3).IsSplittingAsync().Returns(true);
        await h.Grain.RunHealingPassAsync();
        h.ShardOf(3).IsSplittingAsync().Returns(false);

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.CooldownUntilUtc, Is.Null);
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.Admitted));
        });
    }

    // --- Tree maintenance -------------------------------------------------

    [TestCase("resize")]
    [TestCase("reshard")]
    [TestCase("merge")]
    [TestCase("snapshot")]
    public async Task RunHealingPass_stands_aside_for_tree_maintenance(string operation)
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        switch (operation)
        {
            case "resize": h.Lattice.IsResizeCompleteAsync().Returns(false); break;
            case "reshard": h.Lattice.IsReshardCompleteAsync().Returns(false); break;
            case "merge": h.Lattice.IsMergeCompleteAsync().Returns(false); break;
            default: h.Lattice.IsSnapshotCompleteAsync().Returns(false); break;
        }

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.TreeMaintenance));
            Assert.That(h.State.State.InFlightDonorShardIndices, Is.Empty);
        });
    }

    [Test]
    public async Task RunHealingPass_stands_aside_for_a_pending_bulk_graft()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        h.ShardOf(5).HasPendingBulkOperationAsync().Returns(true);

        await h.Grain.RunHealingPassAsync();

        Assert.That(h.State.State.LastDecision, Is.EqualTo(ShardHealingDecision.TreeMaintenance));
    }

    // --- Lifecycle --------------------------------------------------------

    [Test]
    public async Task EnsureRunning_registers_the_keepalive_reminder()
    {
        var h = CreateGrain();

        await h.Grain.EnsureRunningAsync();

        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shard-healing", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task EnsureRunning_is_idempotent()
    {
        var h = CreateGrain();

        await h.Grain.EnsureRunningAsync();
        await h.Grain.EnsureRunningAsync();

        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shard-healing", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task EnsureRunning_starts_once_the_kill_switch_is_flipped_back_on()
    {
        // Deliberately not latched while disabled, so re-enabling healing takes
        // effect on the next call rather than needing a reactivation.
        var options = new LatticeOptions { ShardHealingEnabled = false };
        var h = CreateGrain(options: options);
        await h.Grain.EnsureRunningAsync();

        options.ShardHealingEnabled = true;
        await h.Grain.EnsureRunningAsync();

        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shard-healing", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task Stop_unregisters_the_reminder()
    {
        var h = CreateGrain();
        var reminder = Substitute.For<IGrainReminder>();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), "shard-healing").Returns(reminder);

        await h.Grain.EnsureRunningAsync();
        await h.Grain.StopAsync();

        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public async Task Stop_is_idempotent_when_no_reminder_exists()
    {
        var h = CreateGrain();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), "shard-healing").Returns((IGrainReminder?)null);

        await h.Grain.StopAsync();
        await h.Grain.StopAsync();

        await h.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task Stop_survives_a_reminder_service_failure()
    {
        var h = CreateGrain();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), "shard-healing")
            .Returns<Task<IGrainReminder?>>(_ => throw new InvalidOperationException("reminder service down"));

        Assert.That(async () => await h.Grain.StopAsync(), Throws.Nothing);
        await Task.CompletedTask;
    }

    [Test]
    public async Task ReceiveReminder_ignores_an_unrelated_reminder_name()
    {
        var h = CreateGrain();

        await h.Grain.ReceiveReminder("some-other-reminder", new TickStatus());

        await h.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ReceiveReminder_does_nothing_when_the_kill_switch_is_off()
    {
        var h = CreateGrain(options: new LatticeOptions { ShardHealingEnabled = false });

        await h.Grain.ReceiveReminder("shard-healing", new TickStatus());

        await h.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ReceiveReminder_re_registers_a_drifted_period()
    {
        var h = CreateGrain();

        await h.Grain.ReceiveReminder("shard-healing",
            new TickStatus(DateTime.UtcNow, TimeSpan.FromHours(9), DateTime.UtcNow));

        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shard-healing", TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    [Test]
    public async Task GetHealingReport_reports_nothing_observed_before_the_first_sweep()
    {
        var h = CreateGrain();

        var report = await h.Grain.GetHealingReportAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Decision, Is.EqualTo(ShardHealingDecision.NotObserved));
            Assert.That(report.ObservedAtTicks, Is.Zero);
            Assert.That(report.Backlog, Is.Zero);
        });
    }

    [Test]
    public async Task GetHealingReport_carries_the_sweep_statistics()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        LoadUniformly(h, 8, opsPerShard: 12);

        await h.Grain.RunHealingPassAsync();
        var report = await h.Grain.GetHealingReportAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.PhysicalShardCount, Is.EqualTo(8));
            Assert.That(report.BaseShardCount, Is.EqualTo(2));
            Assert.That(report.Backlog, Is.EqualTo(6));
            Assert.That(report.SkewRatio, Is.EqualTo(1.0d).Within(1e-9));
            Assert.That(report.MedianShardOpsPerSecond, Is.EqualTo(12d).Within(1e-9));
            Assert.That(report.ObservedAtTicks, Is.EqualTo(h.Clock.GetUtcNow().UtcDateTime.Ticks));
        });
    }
}

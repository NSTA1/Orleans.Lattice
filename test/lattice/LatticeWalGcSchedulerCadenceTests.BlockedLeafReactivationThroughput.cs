using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for the throughput of the blocked-leaf reactivation sweep (issue
/// #2768): that its per-leaf rate limits are applied per leaf, and so that a
/// tree carrying many blocked leaves can actually drain.
/// <para>
/// The sibling fixture asserts the sweep's <b>bound</b>. This one asserts the
/// property that bound was starving. Every limit the sweep applies - the
/// minimum block age, the retry cooldown, the attempt budget - is written and
/// reasoned about per blocking leaf, but the floor used to name exactly one
/// blocking consumer per pass, which collapsed all three into a single per-tree
/// rate limit of roughly one leaf per cooldown. Against a tree carrying
/// thousands of blocked leaves that is not slow convergence but none, and it
/// was measured in the field as 2 attempts and 0 heals across 46 blocked
/// passes while the WAL grew without bound.
/// </para>
/// <para>
/// These assertions are therefore about <i>rate</i>, and they are deliberately
/// two-sided. A test that only asserted "more than one leaf is touched" would
/// be satisfied by an unbounded sweep, which is a materially larger blast
/// radius than the defect; a test that only asserted the cap would be satisfied
/// by the starved behaviour this change exists to remove. Both bounds are
/// pinned.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private static GrainId ThroughputLeafGrainId(string key) => GrainId.Create("bplusleaf", key);

    /// <summary>
    /// A blocking consumer id in the production shape
    /// <c>{prefix}{treeId}_{grainId}</c>, so <c>TryResolveLeafGrainId</c> parses
    /// it back to a real grain id rather than to a token invented here.
    /// </summary>
    private static string ThroughputConsumerId(string key, string treeId = StrandedTree) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{ThroughputLeafGrainId(key)}";

    /// <summary>
    /// A blocked report naming several blocking consumers, which is the shape
    /// the floor produces once it carries a bounded set rather than only the
    /// first unusable pin it encountered.
    /// </summary>
    private static LatticeWalGcReport BlockedReportNamingAll(params string[] consumerIds) =>
        new("tree", null, null, null, null, 1, 0, null, null, null, false, false,
            WalGcCursorFloorState.BlockedByUnusablePin, consumerIds[0], consumerIds);

    /// <summary>
    /// A grain factory serving the tree registry and a distinct leaf substitute
    /// per grain id, so "which leaves were touched" is observable rather than
    /// merely "how many calls were made".
    /// </summary>
    private static (IGrainFactory Factory, Dictionary<GrainId, IBPlusLeafGrain> Leaves) FactoryWithBlockedLeaves(
        string treeId,
        params string[] leafKeys)
    {
        var factory = FactoryWithTrees(treeId);
        var leaves = new Dictionary<GrainId, IBPlusLeafGrain>();

        foreach (var key in leafKeys)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.DriveStarvedCheckpointAsync().Returns(_ => Task.FromResult(LeafStarvationDriveOutcome.Lifted));
            leaves[ThroughputLeafGrainId(key)] = leaf;
        }

        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves[call.Arg<GrainId>()]);

        return (factory, leaves);
    }

    /// <summary>
    /// Runs <paramref name="passes"/> passes and returns how many touches each
    /// one issued, so a rate can be asserted rather than only a total.
    /// </summary>
    private static async Task<List<int>> TouchesPerPassAsync(
        VirtualTimeProvider time,
        InstrumentRecorder recorder,
        int passes)
    {
        var perPass = new List<int>(passes);
        var running = 0;

        for (var i = 0; i < passes; i++)
        {
            await TickAsync(time);
            var total = Outcomes(recorder, "attempted");
            perPass.Add(total - running);
            running = total;
        }

        return perPass;
    }

    [Test]
    public async Task ExecuteAsync_touches_every_reported_blocking_leaf_rather_than_only_the_first()
    {
        // The defect of issue #2768 in one assertion. Three leaves are blocking
        // and all three are reported; before this change only the first could
        // ever be touched, so the other two waited a full retry cooldown each
        // for their turn at the head of the report.
        var consumers = new[]
        {
            ThroughputConsumerId("leaf-a"),
            ThroughputConsumerId("leaf-b"),
            ThroughputConsumerId("leaf-c"),
        };

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNamingAll(consumers)));
        var time = new VirtualTimeProvider();
        var (factory, leaves) = FactoryWithBlockedLeaves(StrandedTree, "leaf-a", "leaf-b", "leaf-c");

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Past the minimum block age, so every reported blocker is eligible.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        Assert.Multiple(() =>
        {
            foreach (var (grainId, leaf) in leaves)
            {
                Assert.That(
                    leaf.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.DriveStarvedCheckpointAsync)),
                    Is.True,
                    $"every reported blocking leaf must be touched, not only the first; {grainId} was not.");
            }
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_touches_more_than_one_blocking_leaf_in_a_single_pass()
    {
        // The rate clause, stated as a rate rather than as a total. A sweep
        // that touched one leaf per pass would still eventually touch all of
        // them across enough passes, so a total alone cannot distinguish the
        // starved behaviour from the fixed one - only the per-pass delta can.
        var consumers = Enumerable.Range(0, 8)
            .Select(i => ThroughputConsumerId($"leaf-{i}"))
            .ToArray();

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNamingAll(consumers)));
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaves(
            StrandedTree, Enumerable.Range(0, 8).Select(i => $"leaf-{i}").ToArray());

        using var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        var perPass = await TouchesPerPassAsync(time, recorder, passes: 40);
        var busiest = perPass.Max();

        Assert.Multiple(() =>
        {
            Assert.That(busiest, Is.GreaterThan(1),
                "a pass must be able to touch more than one blocking leaf, or a tree with many of them can never drain.");
            Assert.That(busiest, Is.LessThanOrEqualTo(4),
                "and must never exceed the per-pass touch budget, or the remedy becomes a reactivation stampede.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_ages_and_cools_each_blocking_leaf_on_its_own_clock()
    {
        // The half of the fix that is not about the report shape. Carrying more
        // blockers achieves nothing if the rate limiter still runs per tree, so
        // this pins that the minimum block age and the retry cooldown are both
        // per consumer.
        //
        // A is blocking from the start and is touched once its own five minutes
        // elapse. B is revealed later: it must serve its OWN minimum block age
        // rather than inheriting A's elapsed time, and it must then be touched
        // even though A is still inside its fifteen-minute cooldown. Held per
        // tree, B's touch would be suppressed by A's cooldown.
        var consumerA = ThroughputConsumerId("leaf-a");
        var consumerB = ThroughputConsumerId("leaf-b");
        var reportB = false;

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(reportB
                ? BlockedReportNamingAll(consumerA, consumerB)
                : BlockedReportNamingAll(consumerA)));
        var time = new VirtualTimeProvider();
        var (factory, leaves) = FactoryWithBlockedLeaves(StrandedTree, "leaf-a", "leaf-b");
        var leafA = leaves[ThroughputLeafGrainId("leaf-a")];
        var leafB = leaves[ThroughputLeafGrainId("leaf-b")];

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // +6: A has served its own minimum and is touched. B is not blocking.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));
        await leafA.Received(1).DriveStarvedCheckpointAsync();

        // B appears. It is NOT immediately eligible: the tree has been blocked
        // for six minutes, but B has not.
        reportB = true;
        await TickAsync(time);
        await leafB.DidNotReceive().DriveStarvedCheckpointAsync();

        // +6 more: B has now served its own minimum, and is touched even though
        // A is still cooling down.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        Assert.Multiple(() =>
        {
            Assert.That(
                leafB.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.DriveStarvedCheckpointAsync)),
                Is.EqualTo(1),
                "a newly revealed blocker must be touched once it has served its own minimum block age.");
            Assert.That(
                leafA.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.DriveStarvedCheckpointAsync)),
                Is.EqualTo(1),
                "and must not re-touch a leaf that is still inside its own retry cooldown.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_reports_a_timed_out_touch_as_undelivered_rather_than_as_a_fault()
    {
        // Issue #2768's diagnostic half. The sweep's probe is a grain call into
        // an activation that is, by hypothesis, unable to complete, so the
        // expected failure is a response timeout - and a timeout says something
        // materially different from a fault: the activation request WAS
        // delivered and a caller-side timeout does not cancel it.
        //
        // Folded into 'attempted' with no 'healed', a timed-out touch reads as
        // "reactivation does not heal this leaf" when the truth is "the sweep
        // never found out". Those call for opposite responses, so they get
        // separate arms.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNamingAll(ThroughputConsumerId("leaf-a"))));
        var time = new VirtualTimeProvider();
        var (factory, leaves) = FactoryWithBlockedLeaves(StrandedTree, "leaf-a");
        leaves[ThroughputLeafGrainId("leaf-a")].DriveStarvedCheckpointAsync()
            .Returns<Task<LeafStarvationDriveOutcome>>(_ => throw new TimeoutException("response did not arrive on time"));

        using var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "undelivered"), Is.EqualTo(1),
                "a probe that timed out must be reported on its own arm, or an unreachable leaf is indistinguishable from an unhealable one.");
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(1),
                "and must still count its cost, because the touch was issued.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_falls_back_to_the_single_named_blocker_when_no_set_is_carried()
    {
        // The compatibility clause, and it is not dead code: LatticeWalGcReport
        // is a public record whose BlockingConsumerIds is optional, so a report
        // constructed by anything other than the current GC still names exactly
        // one blocker. That report must continue to drive the sweep rather than
        // silently disabling it.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        await leaf.Received().DriveStarvedCheckpointAsync();

        await scheduler.StopAsync(CancellationToken.None);
    }
}

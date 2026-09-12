using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for the bounded reactivation sweep that heals a tree whose cursor
/// floor is blocked by a dormant leaf's unusable durable pin (#2710
/// Limitation 2, with the blocking consumer named per #2464).
/// <para>
/// The population is dormant leaves and that is structural, not incidental:
/// <see cref="LatticeWalGc"/> skips any consumer present in the live cursor
/// registry before it evaluates the pin at all, so a consumer can only be
/// reported as blocking if its leaf is not activated. Every leaf-local driver
/// of the repair therefore cannot reach it, because there is no activation for
/// them to run in, and the only timer that reaches a leaf resolves dirty leaves
/// - which a dormant clean leaf is not. Something outside the leaf must touch
/// it.
/// </para>
/// <para>
/// These tests assert the <b>bound</b> as hard as they assert the heal. An
/// unbounded version of this sweep would reactivate arbitrary dormant leaves
/// from a background service, which is a materially larger blast radius than
/// the leak it fixes - so "does not reactivate" is the property under test just
/// as much as "does".
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private const string StrandedTree = "stranded";

    private static GrainId BlockedLeafGrainId() => GrainId.Create("bplusleaf", "leaf-2710");

    private static string BlockedConsumerId(string treeId = StrandedTree) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{BlockedLeafGrainId()}";

    /// <summary>
    /// A blocked report that also names the blocking consumer, which is the
    /// shape the real GC produces once it short-circuits on an unusable pin.
    /// </summary>
    private static LatticeWalGcReport BlockedReportNaming(string? consumerId) =>
        new("tree", null, null, null, null, 1, 0, null, null, null, false, false,
            WalGcCursorFloorState.BlockedByUnusablePin, consumerId);

    /// <summary>
    /// A grain factory serving both the tree registry and the blocked leaf, so
    /// a reactivation attempt is observable as a call on the returned leaf.
    /// </summary>
    private static (IGrainFactory Factory, IBPlusLeafGrain Leaf) FactoryWithBlockedLeaf(
        params string[] treeIds)
    {
        var factory = FactoryWithTrees(treeIds);
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetTreeIdAsync().Returns(_ => Task.FromResult<string?>(treeIds.FirstOrDefault()));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        return (factory, leaf);
    }

    /// <summary>
    /// Drives passes until virtual time has advanced at least
    /// <paramref name="atLeast"/>, so a test can age a block past the sweep's
    /// minimum without hard-coding how many floor-rate passes that takes.
    /// </summary>
    private static async Task AdvanceAtLeastAsync(
        VirtualTimeProvider time,
        TimeSpan atLeast,
        int maxPasses = 500)
    {
        var start = time.GetUtcNow();
        var guard = 0;
        while (time.GetUtcNow() - start < atLeast)
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(maxPasses), "the cadence stopped advancing virtual time.");
        }
    }

    [Test]
    public async Task ExecuteAsync_does_not_reactivate_a_freshly_blocked_leaf()
    {
        // The post-restart guard, and the reason the sweep has a minimum block
        // age at all. Immediately after a restart every dormant leaf is absent
        // from the live registry and its durable pin is at its stalest - which
        // is the exact window the durable floor exists to survive (#919).
        // Sweeping then would reactivate a large population at the worst moment,
        // and would be touching leaves that were about to re-report anyway.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await TickAsync(time);

        await leaf.DidNotReceive().GetTreeIdAsync();

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_reactivates_a_leaf_that_has_been_blocking_long_enough()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // A blocked tree holds the floor, so ageing past the minimum is a
        // matter of letting it poll. This is the heal: activation replays the
        // WAL forward, which advances the leaf's checkpoint, which is what makes
        // the already-shipped activation-time repair applicable.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        await leaf.Received().GetTreeIdAsync();
        factory.Received().GetGrain<IBPlusLeafGrain>(BlockedLeafGrainId());

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_does_not_reactivate_the_same_leaf_on_every_pass()
    {
        // The rate limit. A blocked tree polls at the floor precisely because it
        // is blocked, so an unthrottled sweep would reactivate the same leaf
        // every 30 seconds for as long as it failed to heal - turning a bounded
        // remedy into sustained churn against the population least able to
        // absorb it.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Ten minutes of floor-rate polling is ~20 passes, all of them blocked
        // and all naming the same consumer.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(10));

        await leaf.Received(1).GetTreeIdAsync();

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_never_reactivates_when_no_consumer_is_named()
    {
        // The live-leaf guard, expressed at the seam that actually enforces it.
        // A consumer present in the live registry is skipped by the GC before
        // the pin is evaluated, so it can never appear as a blocking consumer -
        // which means "blocked but naming nobody" is the shape a live leaf
        // produces, and it must never resolve to a reactivation. Reactivating a
        // leaf that is already serving traffic is the blast-radius incident this
        // whole path is bounded to avoid, and it is strictly worse than the
        // missed reclaim it would be trying to fix.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(consumerId: null)));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(20));

        await leaf.DidNotReceive().GetTreeIdAsync();
        factory.DidNotReceive().GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_never_reactivates_a_tree_that_is_not_blocked()
    {
        // The other half of the same guard: a healthy tree must not be swept
        // even if a consumer id is somehow present on its report. The sweep is
        // gated on the blocked state, not merely on the field being populated.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(
                new LatticeWalGcReport("tree", null, null, null, null, 1, 5, null, null, null,
                    false, false, WalGcCursorFloorState.Available, BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(20));

        await leaf.DidNotReceive().GetTreeIdAsync();

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_restarts_the_block_age_when_a_different_leaf_becomes_the_blocker()
    {
        // A tree with several blocked leaves reveals them one at a time,
        // because the floor short-circuits on the first unusable pin. Each newly
        // revealed leaf must serve its own minimum block age rather than
        // inheriting its predecessor's, or a tree that drains would fire its
        // remaining reactivations back to back - which is the stampede the bound
        // exists to prevent, arriving by a side door.
        var consumerId = BlockedConsumerId();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(consumerId)));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));
        await leaf.Received(1).GetTreeIdAsync();

        // The first leaf healed; a second dormant leaf is now at the head.
        consumerId = $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{StrandedTree}_{GrainId.Create("bplusleaf", "leaf-second")}";
        await TickAsync(time);
        await TickAsync(time);

        await leaf.Received(1).GetTreeIdAsync();

        // ...and it is reactivated once it has served the minimum itself.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));
        await leaf.Received(2).GetTreeIdAsync();

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_does_not_resolve_a_consumer_id_it_cannot_parse()
    {
        // Fail closed. An id that does not carry the expected prefix and tree
        // must yield no grain at all rather than a best guess, because a guess
        // would activate an unrelated leaf - the same incident as touching a
        // live one, reached by a parsing bug instead of a logic one.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming("not-a-materialiser-consumer-id")));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(10));

        await leaf.DidNotReceive().GetTreeIdAsync();
        factory.DidNotReceive().GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_survives_a_leaf_that_throws_on_reactivation()
    {
        // A heal attempt must never fail the GC pass. Failing to reactivate
        // retains WAL, which is the safe direction; letting the exception escape
        // would take down the scheduler for every tree in the silo.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        leaf.GetTreeIdAsync().Returns<Task<string?>>(_ => throw new TimeoutException("silo busy"));

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        await leaf.Received().GetTreeIdAsync();

        // The pass still completed and the tree is still being collected.
        await gc.Received().RunOnceAsync(StrandedTree, Arg.Any<CancellationToken>());
        var before = gc.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync));
        await TickAsync(time);
        var after = gc.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync));
        Assert.That(after, Is.GreaterThan(before),
            "a failed reactivation must not stop the scheduler collecting the tree.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    // ---------------------------------------------------------- outcome accounting

    [Test]
    public async Task ExecuteAsync_gives_up_on_a_leaf_that_stays_blocked_across_the_attempt_budget()
    {
        // The production case this bound exists for: a leaf whose snapshot
        // capture cannot complete is touched, fails, and stays blocked. No
        // number of further touches will heal it, and the activation-time
        // repair path already retries such a leaf without backpressure - so an
        // unbounded sweep would stack a second retry loop on top of a first,
        // against exactly the leaves least able to absorb it.
        //
        // The window stops inside the first cycle, deliberately. Abandonment is
        // a pause and not a verdict (issue #2783), so "exactly the budget" is
        // only true of a cycle; the cross-cycle bound is pinned separately by
        // ExecuteAsync_lengthens_the_rearm_backoff_with_each_abandonment.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Past the full budget and the cooldown that would have carried a
        // fourth attempt, but short of the 30-minute re-arm backoff.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(60));

        await leaf.Received(3).GetTreeIdAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(3),
                "the sweep must issue exactly its attempt budget within a cycle, no more.");
            Assert.That(Outcomes(recorder, "abandoned"), Is.EqualTo(1),
                "giving up is an alarm and must be reported exactly once per cycle, not once per pass.");
            Assert.That(Outcomes(recorder, "rearmed"), Is.Zero,
                "the budget must stay spent until the backoff has actually elapsed.");
            Assert.That(Outcomes(recorder, "healed"), Is.Zero,
                "a leaf that never stopped blocking must never be credited as healed.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_publishes_a_healed_outcome_when_a_swept_leaf_stops_blocking()
    {
        // The only evidence the sweep accomplishes anything. Without it a sweep
        // that reactivates leaves and achieves nothing is indistinguishable
        // from one that works.
        var blocked = true;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(blocked
                ? BlockedReportNaming(BlockedConsumerId())
                : Report(entriesTrimmed: 12)));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));
        await leaf.Received(1).GetTreeIdAsync();

        // The capture succeeded, the pin lifted, and the tree reclaims again.
        blocked = false;
        await TickAsync(time);

        var outcomes = recorder.Counted
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        Assert.That(outcomes, Is.Not.Empty, "the instrument must have published for this tree.");
        Assert.Multiple(() =>
        {
            Assert.That(outcomes, Does.Contain("attempted"));
            Assert.That(outcomes.Count(o => string.Equals(o, "healed", StringComparison.Ordinal)),
                Is.EqualTo(1), "a swept consumer that stops blocking is credited exactly once.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_does_not_credit_a_heal_for_a_leaf_it_never_reactivated()
    {
        // Keeps the healed/attempted ratio meaningful. A tree that blocks
        // briefly and clears on its own - a leaf touched by ordinary traffic,
        // say - must not inflate the sweep's apparent effectiveness.
        var blocked = true;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(blocked
                ? BlockedReportNaming(BlockedConsumerId())
                : Report(entriesTrimmed: 3)));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Clears well inside the minimum block age, so no sweep ever ran.
        await TickAsync(time);
        blocked = false;
        await TickAsync(time);
        await TickAsync(time);

        await leaf.DidNotReceive().GetTreeIdAsync();
        Assert.That(
            recorder.Counted.Select(m => m.Tag(LatticeMetrics.TagOutcome) as string),
            Does.Not.Contain("healed"),
            "a consumer the sweep never touched must not be credited to the sweep.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// A deliberately coarse interval floor for the re-arm fixtures. The
    /// backoff is measured in tens of minutes and the ceiling in hours, so a
    /// 30-second floor would need thousands of passes to reach a second cycle;
    /// at five minutes the same virtual span costs a tenth of the passes and
    /// every sweep deadline still lands exactly on a pass boundary, because the
    /// minimum block age (5 min) and retry cooldown (15 min) are both whole
    /// multiples of it.
    /// </summary>
    private static readonly TimeSpan SweepPass = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Counts real occurrences of one outcome on the reactivation instrument,
    /// excluding the zero-valued primes (#2783) that every outcome now mints on
    /// every pass.
    /// </summary>
    private static int Outcomes(InstrumentRecorder recorder, string outcome) =>
        recorder.Counted.Count(m =>
            string.Equals(m.Tag(LatticeMetrics.TagOutcome) as string, outcome, StringComparison.Ordinal));

    [Test]
    public async Task ExecuteAsync_rearms_the_attempt_budget_after_the_backoff_elapses()
    {
        // R1, and the defect of issue #2783 in one assertion. Abandonment used
        // to be terminal for the life of the process, so a tree whose budget
        // was spent during a transient memory-pressure burst stayed
        // extinguished through exactly the quiet window in which a touch would
        // have worked. Converging then required an operator restarting the
        // silo, which is the one remedy this subsystem may not require.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Budget spent by +35, abandoned at +40, base backoff 30 min, so the
        // fourth touch lands at +70 and the fifth is not due until +85.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(75));

        await leaf.Received(4).GetTreeIdAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "rearmed"), Is.EqualTo(1),
                "the sweep must restore the budget once the backoff has elapsed.");
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(4),
                "a re-arm is the decision to try again, so it is followed by an attempt.");
            Assert.That(Outcomes(recorder, "abandoned"), Is.EqualTo(1),
                "the restored cycle has not yet been spent, so there is no second alarm.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_lengthens_the_rearm_backoff_with_each_abandonment()
    {
        // R2, and the reason a re-arm is not a hot loop. "Three touches per
        // cycle" is satisfied by a cycle that repeats every second, so the
        // within-cycle bound proves nothing on its own: what makes a hopeless
        // tree cheap is that the gap between cycles doubles. Over five virtual
        // hours the backoffs are 30, 60 and 120 minutes, which admits three
        // cycles; a backoff that did not escalate would admit four.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Cycles start at +0, +70 and +165; the fourth would start at +320.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(300), maxPasses: 200);

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "rearmed"), Is.EqualTo(2),
                "the second backoff must be longer than the first, so only two re-arms fit.");
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(9),
                "three bounded cycles of three touches, not a cycle that repeats at the cooldown rate.");
            Assert.That(Outcomes(recorder, "abandoned"), Is.EqualTo(3));
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_stops_lengthening_the_rearm_backoff_at_the_ceiling()
    {
        // R3. The doubling is what keeps a hopeless tree cheap; the ceiling is
        // what keeps it alive. Unbounded doubling would push the interval past
        // any window in which a repair could plausibly be noticed, which is
        // permanent abandonment again with extra steps - so the sixth and
        // seventh cycles must be spaced by the same six hours as the fifth.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Backoffs 30, 60, 120, 240, then 360 twice: cycles start at +0, +70,
        // +165, +320, +595, +990 and +1385. Uncapped doubling would give 480
        // and 960 instead, so the sixth cycle would start at +1110 and the
        // seventh not until +2105.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(1500), maxPasses: 400);

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "rearmed"), Is.EqualTo(6),
                "a saturated backoff must keep re-arming at the ceiling rate, not keep doubling away.");
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(21),
                "seven bounded cycles of three touches.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_does_not_charge_the_attempt_budget_for_a_faulted_touch()
    {
        // R4, the issue's own named defect. The attempt used to be stamped
        // before the call, so a touch that never reached the leaf - a busy
        // silo, a timeout - was charged as if it had proved activation would
        // not heal the leaf. That spends the evidence budget on a measurement
        // nobody took. The cooldown is stamped up front and still rate-limits
        // the retry, which is what makes the refund safe.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        leaf.GetTreeIdAsync().Returns<Task<string?>>(_ => throw new TimeoutException("silo busy"));

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Touches at +5, +20, +35, +50 and +65, still one cooldown apart.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(75));

        await leaf.Received(5).GetTreeIdAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(5),
                "a faulted touch must stay one cooldown apart from the next, not collapse into a stampede.");
            Assert.That(Outcomes(recorder, "abandoned"), Is.Zero,
                "faults that never reached the leaf must not condemn it as unhealable.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_caps_the_faulted_touches_a_cycle_will_excuse()
    {
        // R4b, and the boundary that keeps R4 honest. Refunding a fault is
        // right; refunding without limit is not, because a leaf that faults
        // every time would then be touched once per cooldown for the life of
        // the process, never reaching abandonment and so never decaying onto
        // the escalating backoff. The cap makes the worst case finite.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        leaf.GetTreeIdAsync().Returns<Task<string?>>(_ => throw new TimeoutException("silo busy"));

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Three refunds then three charged touches per cycle: cycles start at
        // +0, +115 and +255. Unlimited refunds would instead touch the leaf
        // every 15 minutes forever and never abandon at all.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(300), maxPasses: 200);

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "abandoned"), Is.EqualTo(2),
                "a permanently faulting leaf must still reach abandonment and decay onto the backoff.");
            Assert.That(Outcomes(recorder, "rearmed"), Is.EqualTo(2));
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(16),
                "six touches per cycle - three excused, three charged - not one every cooldown forever.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_shortens_the_backoff_when_another_leaf_heals()
    {
        // R5. A bare timer is a floor, not the goal: the sweep should re-arm on
        // evidence that the blocking condition has lifted. A healed outcome
        // anywhere in this silo is exactly that evidence - it says a blocked
        // leaf activated, replayed, captured a snapshot and resolved its pin,
        // so the memory headroom and replay capacity a stranded tree also needs
        // demonstrably exist right now. That collapses the stranded tree's
        // 30-minute wait to the 15-minute floor.
        const string HealerTree = "healer";
        var healerBlocked = false;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var tree = (string)call[0];
                if (!string.Equals(tree, HealerTree, StringComparison.Ordinal))
                {
                    return Task.FromResult(BlockedReportNaming(BlockedConsumerId(tree)));
                }

                return Task.FromResult(healerBlocked
                    ? BlockedReportNaming(BlockedConsumerId(HealerTree))
                    : Report(entriesTrimmed: 4));
            });
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree, HealerTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The stranded tree spends its budget by +35 and is abandoned at +40.
        // The other tree is healthy throughout, so nothing has healed yet and
        // the stranded tree's next re-arm is the full 30 minutes away, at +70.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(40));
        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "abandoned"), Is.EqualTo(1),
                "the stranded tree must be abandoned before the heal, or the test proves nothing.");
            Assert.That(Outcomes(recorder, "rearmed"), Is.Zero,
                "and must not yet have re-armed, or the heal is not what causes the re-arm.");
        });

        // Now a second tree blocks and is swept: first observed at +45, touched
        // at +50. It must not be abandoned itself, because an abandoned tree is
        // never credited with a heal.
        healerBlocked = true;
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(10));

        // Its capture completes at +55, which credits a heal and is the
        // evidence. The stranded tree reads it on the following pass, at +60 -
        // ten minutes before the unaccelerated backoff would have allowed it.
        healerBlocked = false;
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(10));

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "rearmed"), Is.EqualTo(1),
                "a heal elsewhere must collapse the stranded tree's backoff to the floor.");
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(4),
                "the accelerated re-arm is followed by a real touch, not merely recorded.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_primes_every_reactivation_outcome_at_zero()
    {
        // R6, and the reason this epic lost time twice. A Counter exports
        // nothing at all until its first Add, so an absent series was equally
        // consistent with "the sweep healed nothing" and "the build carrying
        // the sweep never landed" - and both readings were taken as evidence.
        // Priming above every early return makes absence say the second thing
        // and only the second thing.
        //
        // This is the one fixture that reads the unfiltered measurements: every
        // other outcome assertion runs on the zero-excluding projection, so
        // without this arm the primes would be invisible to the suite meant to
        // protect them.
        const string QuietTree = "walgc-reactivation-primed";
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, QuietTree);
        var scheduler = CreateScheduler(FactoryWithTrees(QuietTree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        var primed = recorder.Measurements
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(primed, Is.EquivalentTo(new[] { "attempted", "healed", "abandoned", "rearmed" }),
                "every outcome must be minted, so a reader can tell a measured zero from a missing build.");
            Assert.That(recorder.Measurements.Select(m => m.Value), Is.All.Zero,
                "a prime must mint the series without claiming an event occurred.");
            Assert.That(recorder.Counted, Is.Empty,
                "and must stay invisible to any fixture counting real occurrences.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }
}

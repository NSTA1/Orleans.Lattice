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
    private static async Task AdvanceAtLeastAsync(VirtualTimeProvider time, TimeSpan atLeast)
    {
        var start = time.GetUtcNow();
        var guard = 0;
        while (time.GetUtcNow() - start < atLeast)
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(500), "the cadence stopped advancing virtual time.");
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
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Long enough for the full budget plus the cooldown that would have
        // carried a fourth attempt had the budget not stopped it.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(75));

        await leaf.Received(3).GetTreeIdAsync();

        var outcomes = recorder.Measurements
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        Assert.That(outcomes, Is.Not.Empty, "the instrument must have published for this tree.");
        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Count(o => string.Equals(o, "attempted", StringComparison.Ordinal)),
                Is.EqualTo(3), "the sweep must issue exactly its attempt budget, no more.");
            Assert.That(outcomes.Count(o => string.Equals(o, "abandoned", StringComparison.Ordinal)),
                Is.EqualTo(1), "giving up is an alarm and must be reported exactly once, not once per pass.");
            Assert.That(outcomes, Does.Not.Contain("healed"),
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

        var outcomes = recorder.Measurements
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
            recorder.Measurements.Select(m => m.Tag(LatticeMetrics.TagOutcome) as string),
            Does.Not.Contain("healed"),
            "a consumer the sweep never touched must not be credited to the sweep.");

        await scheduler.StopAsync(CancellationToken.None);
    }
}

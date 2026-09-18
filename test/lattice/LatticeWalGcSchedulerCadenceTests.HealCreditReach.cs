using System.Globalization;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the reachability of the blocked-leaf heal credit (issue #3175).
/// <para>
/// <b>The defect.</b> <c>CreditHealedConsumers</c> is the only writer of the
/// <c>healed</c> arm, and it used to be reached from exactly one place -
/// <c>ClearBlockedObservation</c>, at the foot of the branch taken when the
/// cursor floor reports usable <b>and</b> the byte-ceiling sampler holds no
/// repairable floor holder for the tree. The second condition is a different
/// axis entirely: it arrived with the dormant floor-holder repair (issues #3154
/// and #3158) and was retrofitted through the same observation map, which
/// silently took the blocked episode's lifecycle hostage. AND-ed together, the
/// credit became unreachable for any tree that unblocks while still over its
/// byte ceiling - which is the definition of a tree with a WAL retention
/// problem, and therefore exactly the population the instrument exists to
/// measure.
/// </para>
/// <para>
/// <b>Measured as a natural experiment on the live repocontext container</b>,
/// before the entitlement fix of issue #3176 moved both trees off the blocked
/// path. Two trees, one process, one build, both running this code, differing
/// in the single variable this diagnosis names:
/// </para>
/// <list type="table">
///   <item><description>
///     <c>repo-context-vector-payload</c> - 176 passes, 9 blocked and
///     <b>167 over_ceiling</b>, 52 reactivation attempts, <b>0 heals</b>. Its
///     floor holders classified <c>checkpointed_uncovered</c> on every
///     partition, so the repairable set was never empty and the credit branch
///     was never taken.
///   </description></item>
///   <item><description>
///     <c>repo-context-vector-index</c> - 177 passes, 24 blocked and
///     <b>153 reclaimed, 0 over_ceiling</b>, 49 attempts, <b>30 heals</b>. It
///     drops under its ceiling, so the repairable set empties and the same code
///     credits normally.
///   </description></item>
/// </list>
/// <para>
/// A zero produced by construction is worse than no instrument: that one was
/// read as evidence of non-convergence and sent an epic after a throughput
/// defect it cannot observe. The credit now fires on the floor-state
/// transition, which is the whole of the claim it makes, and
/// <c>ConsumerReactivationBudget.HealCredited</c> supplies the idempotence that
/// sharing an event with the retirement used to provide.
/// </para>
/// <para>
/// <b>What these fixtures do not claim.</b> A tree blocked on every pass it has
/// ever run credits nothing, and that is correct rather than defective - the
/// floor never cleared, so nothing healed. Six trees on the same container are
/// in exactly that state (blocked on 100% of passes) and are silent on this
/// defect in both directions. <c>ExecuteAsync_credits_nothing_while_the_floor
/// _stays_blocked</c> pins that, and is the control that must pass both before
/// and after the fix.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// A report that is over the byte ceiling <i>and</i> names a blocking
    /// consumer, so a fixture can flip the floor state between blocked and
    /// usable while holding every other field constant. The transition under
    /// test is the floor's, and nothing else may move with it.
    /// </summary>
    private static LatticeWalGcReport OverCeilingReportNaming(string consumerId) =>
        new("tree", null, null, null, null, 1, 0, 1_024, null, 4_096, false, true,
            WalGcCursorFloorState.BlockedByUnusablePin, consumerId);

    /// <summary>
    /// A scheduler over a tree that is permanently over its byte ceiling, holds
    /// one genuinely repairable dormant floor holder, and whose cursor floor
    /// never stops reporting blocked.
    /// </summary>
    private static LatticeWalGcScheduler SchedulerPermanentlyBlocked(
        FakePinStore pins,
        IGrainStorage storage,
        VirtualTimeProvider time)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReportNaming(RepairConsumerId(0))));

        var leaves = new LeafTouchBook();
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        return CreateScheduler(factory, gc, OrphanSweepOptions(), time, leafStateStorage: storage);
    }

    [Test]
    public async Task ExecuteAsync_credits_the_heal_when_the_floor_is_usable_while_repairable_holders_remain()
    {
        // The regression, modelled as payload reports rather than as a blocked
        // episode - which is the shape that actually produces the defect.
        //
        // This tree is never blocked at all. Its floor reports Available on
        // every pass and it is permanently over its byte ceiling, so the
        // repairable-holder branch runs, and that branch CREATES the blocked
        // observation (ObserveAndHealBlockedTreeAsync, preClassified) and
        // accrues attempts into it. Because the holder stays repairable, the
        // set is never empty, so ClearBlockedObservation - the old and only
        // credit site - is never reached, and healed stays 0 for the life of
        // the process however many consumers stop blocking.
        //
        // That is payload exactly: 176 passes, 167 over_ceiling, 52 attempts,
        // 0 heals, against a sibling tree that drops under its ceiling, empties
        // the holder set, and credits 30 heals from 49 attempts on the same
        // build.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);

        using var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Is.Not.Empty,
                "the repairable-holder drive must have run, or there is no touched consumer to credit "
                + "and this fixture proves nothing.");
            Assert.That(Outcomes(recorder, "attempted"), Is.GreaterThan(0),
                "attempts must have accrued into the observation, for the same reason.");
            Assert.That(Outcomes(recorder, "healed"), Is.GreaterThan(0),
                "the cursor floor reports usable, so the consumers the sweep touched have stopped "
                + "blocking it and must be credited - still being over the byte ceiling is a different "
                + "axis and may not suppress the credit (issue #3175).");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_credits_a_usable_floor_once_however_many_passes_follow_it()
    {
        // Crediting on the arm rather than on the observation's retirement means
        // the credit site now runs on EVERY pass whose floor reports usable, not
        // once at the end of an episode. Without per-consumer idempotence the
        // arm would advance once per pass for as long as the tree stayed over
        // its ceiling - and because a blocked or over-ceiling tree is pinned to
        // WalGcMinInterval rather than backed off, that is every 30 seconds
        // indefinitely. That would be a worse failure than the silence it
        // replaces: a permanently inflated numerator on the ratio this
        // instrument exists to feed.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var time = new VirtualTimeProvider();
        var (scheduler, _) = SchedulerRepairing(pins, storage, time);

        using var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);

        var afterFirstCredit = Outcomes(recorder, "healed");
        await AdvanceAtLeastAsync(time, PastMinBlockAge);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirstCredit, Is.GreaterThan(0),
                "the credit must have fired, or this fixture is asserting idempotence over nothing.");
            Assert.That(Outcomes(recorder, "healed"), Is.EqualTo(afterFirstCredit),
                "one seeded holder is one consumer, so it is credited exactly once however many "
                + "usable-floor passes run after it.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_credits_nothing_while_the_floor_stays_blocked()
    {
        // The insensitive control, and a real property besides: six trees on the
        // live container are blocked on 100% of the passes they have ever run,
        // and their healed=0 is correct rather than defective. A change that
        // made the credit fire from the blocked arm would satisfy the regression
        // above and be badly wrong, so this must pass in both states.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var time = new VirtualTimeProvider();
        var scheduler = SchedulerPermanentlyBlocked(pins, storage, time);

        using var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "attempted"), Is.GreaterThan(0),
                "the sweep must be running, or a zero heal count would be vacuous.");
            Assert.That(Outcomes(recorder, "healed"), Is.Zero,
                "a floor that has never cleared has healed nothing, whatever the sweep has touched.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    // --------------------------------------------- the convergence arithmetic

    /// <summary>
    /// A leaf grain id whose ordinal is zero-padded, so that ordinal and
    /// numeric order agree when the sweep falls through to its id tiebreak.
    /// </summary>
    private static GrainId PartitionedLeafGrainId(int ordinal) =>
        GrainId.Create(
            "bplusleaf",
            "leaf-3175-" + ordinal.ToString("D3", CultureInfo.InvariantCulture));

    /// <summary>
    /// The consumer id a multi-partition leaf publishes under - one per WAL
    /// partition, all naming the same leaf.
    /// </summary>
    private static string PartitionedConsumerId(int ordinal, int partition) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{StrandedTree}_"
        + $"{PartitionedLeafGrainId(ordinal)}_{partition.ToString(CultureInfo.InvariantCulture)}";

    [Test]
    public async Task ExecuteAsync_spends_the_whole_touch_budget_on_one_leaf_when_it_owns_every_reported_id()
    {
        // Characterises the denominator behind issue #3175's headline numbers,
        // so it does not have to be re-derived. It asserts CURRENT behaviour and
        // is expected to be flipped by the fix, not preserved by it.
        //
        // The report, the attempt budget and the touch budget are all keyed per
        // CONSUMER id - {tree}_{grain}_{partition} - while the remedy is per
        // LEAF: DriveStarvedCheckpointAsync resolves the partition count once
        // and repairs every partition in a single call. So a leaf contributes
        // one id per WAL partition and each of those ids spends budget asking
        // for work the first one has already started.
        //
        // The constants collide exactly: MaxReportedBlockingConsumers is 8 and
        // DefaultWalPartitions is 8, so one blocked leaf fills the entire
        // blocking report, and MaxReactivationTouchesPerPass is 4, so it also
        // consumes the entire per-pass touch budget. Measured on the live
        // container the ratio was exact - 52 touches, 13 episodes, 39
        // already-driving rejections (13 x 3) and 13 real outcomes.
        //
        // Here the second leaf is starved by the first leaf's partitions rather
        // than by any property of its own. Widening the touch budget does not
        // help, because the extra touches land on the same leaf; the fix is to
        // spend the budget per leaf, and it is deliberately not made here.
        var consumers = new[]
        {
            PartitionedConsumerId(0, 0),
            PartitionedConsumerId(0, 1),
            PartitionedConsumerId(0, 2),
            PartitionedConsumerId(0, 3),
            PartitionedConsumerId(1, 0),
        };

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNamingAll(consumers)));

        var time = new VirtualTimeProvider();
        var leaves = new LeafTouchBook();
        var factory = FactoryWithTrees(StrandedTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));

        var options = Adaptive();
        options.WalPartitions = 8;

        var scheduler = CreateScheduler(factory, gc, options, time);

        await StartAndRunFirstPassAsync(scheduler, time);

        // The budget is a PER-PASS cap, so it has to be observed on a single
        // pass. Ageing the block past the sweep's minimum takes several
        // floor-rate passes, and touches land on whichever of those first finds
        // the consumers eligible - so snapshot that pass rather than a running
        // total, which would silently sum the starved leaf's later turn back in.
        var firstTouchingPass = new List<GrainId>();
        for (var i = 0; i < 500 && firstTouchingPass.Count == 0; i++)
        {
            var before = leaves.Touched.Count;
            await TickAsync(time);
            firstTouchingPass = leaves.Touched.Skip(before).ToList();
        }

        Assert.Multiple(() =>
        {
            Assert.That(firstTouchingPass, Has.Count.EqualTo(TouchesPerPass),
                "the whole per-pass touch budget must have been spent, or the starvation below is not "
                + "the budget's doing.");
            Assert.That(firstTouchingPass.Distinct().Count(), Is.EqualTo(1),
                "four touches bought one leaf's worth of work: the budget is spent per consumer id while "
                + "the remedy is per leaf (issue #3175).");
            Assert.That(firstTouchingPass, Does.Not.Contain(PartitionedLeafGrainId(1)),
                "the second leaf is starved by the first leaf's other partitions, not by anything about "
                + "itself - which is why widening the budget cannot fix this.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }
}

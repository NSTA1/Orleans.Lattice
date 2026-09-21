using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for grading the reactivation drive on the axis its admission was
/// granted on (issue #3185).
/// <para>
/// <b>The defect.</b> Issue #3178 admits a second population into the
/// reactivation remedy: a usable, durably-checkpointed pin sitting exactly on
/// the tree's offset floor, driven for liveness. The remedy then grades every
/// drive by the leaf's own <c>LeafStarvationDriveOutcome</c>, and that verdict
/// answers a different question. It reports whether the leaf's pins are
/// <i>usable</i>, and all three of its inputs are leaf-wide: the advance is any
/// partition's, the coverage scan is every partition's, and the empty-WAL
/// release count is the leaf's. The offset floor is held by <b>one</b>
/// partition. So a sibling partition advancing satisfies the verdict while the
/// floor holder sits bit-identical, the remedy records <c>healed</c>, and the
/// tree stays pinned with every instrument reporting success.
/// </para>
/// <para>
/// <b>Measured.</b> On <c>repo-context-vector-index</c> three consecutive drives
/// of the same floor-holding pin each reported <c>drove_lifted</c> and credited
/// <c>healed</c>, while the floor stayed not merely close but byte-identical at
/// 245019 on every sample and the tree trimmed nothing in the lifetime of the
/// process.
/// </para>
/// <para>
/// <b>What these fixtures assert, and what they deliberately do not.</b> They
/// assert the <i>mechanism</i>: a drive that does not advance the admitted
/// consumer's own durable pin offset must score <c>drove_no_advance</c> and must
/// not credit <c>healed</c>. They assert nothing about whether the WAL then
/// trims, and on the population that produced this issue it will not - the
/// floor holder's own checkpoint does not advance, which is a separate defect on
/// a separate axis. Establishing even that much took three drives, a
/// per-partition telemetry scrape and a source audit of the teardown capture
/// gate, to learn what a per-partition verdict reports in a single pass. That
/// cost is the defect these fixtures close. Asserting a trim here would be the
/// same error one axis along.
/// </para>
/// <para>
/// <b>The safety property is bounded retry, not faster retry.</b> Withholding
/// the heal credit must return the consumer to the ordinary eligible pool under
/// the unchanged cooldown and attempt ceiling, never to an immediate re-drive. A
/// drive activates the leaf, so a remedy that retries promptly would reset its
/// idle timer forever - an unbounded loop that trims nothing and is strictly
/// worse than the defect. <see
/// cref="ExecuteAsync_bounds_the_retries_of_a_floor_holder_whose_pin_never_advances"/>
/// is that property as a test.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// An offset strictly above <see cref="FloorOffset"/> that a drive can move
    /// a pin to, standing in for a replay that genuinely advanced the
    /// floor-holding partition's scanned-through checkpoint and persisted it.
    /// </summary>
    private const long AdvancedOffset = FloorOffset + 500;

    /// <summary>
    /// Serves a distinct leaf substitute per grain id whose drive advances the
    /// seeded pin offset before reporting the leaf-wide success verdict.
    /// </summary>
    /// <remarks>
    /// <see cref="LeafTouchBook"/> models the live shape - a drive that reports
    /// success while the pin stays where it was - so it cannot express the
    /// other half of the discrimination. Without this book the fix would pass
    /// its own test by hard-coding <c>drove_no_advance</c>, which is the failure
    /// mode a one-sided perturbation cannot detect.
    /// </remarks>
    private sealed class AdvancingLeafBook(FakePinStore pins, Func<GrainId, string> consumerIdFor)
    {
        private readonly Dictionary<GrainId, IBPlusLeafGrain> _leaves = [];

        public List<GrainId> Touched { get; } = [];

        public IBPlusLeafGrain For(GrainId leafGrainId)
        {
            if (_leaves.TryGetValue(leafGrainId, out var leaf))
            {
                return leaf;
            }

            leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.DriveStarvedCheckpointAsync().Returns(_ =>
            {
                Touched.Add(leafGrainId);
                pins.Seed(OrphanSweepTree, consumerIdFor(leafGrainId), UsablePin, AdvancedOffset);
                return Task.FromResult(LeafStarvationDriveOutcome.Lifted);
            });

            _leaves[leafGrainId] = leaf;
            return leaf;
        }
    }

    /// <summary>
    /// The <see cref="SchedulerRepairing"/> shape, with a leaf book whose drive
    /// actually advances the pin it was admitted for.
    /// </summary>
    private static (LatticeWalGcScheduler Scheduler, AdvancingLeafBook Leaves) SchedulerRepairingWithAdvance(
        FakePinStore pins,
        IGrainStorage? storage,
        VirtualTimeProvider time,
        Func<GrainId, string> consumerIdFor)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));

        var leaves = new AdvancingLeafBook(pins, consumerIdFor);
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        return (
            CreateScheduler(factory, gc, OrphanSweepOptions(walPartitions: 1), time, leafStateStorage: storage),
            leaves);
    }

    [Test]
    public async Task ExecuteAsync_records_no_advance_when_a_driven_floor_holders_pin_does_not_move()
    {
        // The defect as a test. The leaf reports the leaf-wide success verdict
        // and the pin stays exactly where the classifier found it, which is the
        // live shape byte for byte. Before this change the remedy read that as
        // a repair.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);

        using var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Positive controls first. Every assertion below is about how a
            // drive was graded, so all of them pass vacuously if no drive
            // happened - which is exactly how a fixture that has quietly
            // stopped exercising its own subject reads as green.
            Assert.That(leaves.Touched, Does.Contain(LivenessLeafGrainId(0)),
                "the floor-holding leaf must be driven at all, or every grading assertion below is vacuous.");
            Assert.That(Outcomes(recorder, "attempted"), Is.GreaterThan(0),
                "the drive must be attempted, or the outcome arms have nothing to partition.");

            Assert.That(Outcomes(recorder, "drove_no_advance"), Is.GreaterThan(0),
                "a drive that leaves the admitted consumer's own durable pin offset exactly where it found "
                    + "it has not moved the quantity the offset floor is computed from, whatever the leaf's "
                    + "leaf-wide verdict says about its pins being usable.");
            Assert.That(Outcomes(recorder, "drove_lifted"), Is.Zero,
                "the leaf's success verdict must not be recorded for this admission. It is leaf-wide on all "
                    + "three of its inputs while the floor is held by one partition, so it is true of leaves "
                    + "whose floor-holding pin did not move at all.");
            Assert.That(Outcomes(recorder, "healed"), Is.Zero,
                "crediting a heal here is the defect: it marks the consumer repaired, so the remedy walks "
                    + "away from a pin that is still holding the whole tree's WAL, and it advances the heal "
                    + "epoch that collapses every other abandoned consumer's backoff estate-wide.");
        });
    }

    [Test]
    public async Task ExecuteAsync_still_credits_a_floor_holder_whose_drive_advances_its_pin()
    {
        // The discrimination half, and the reason the fixture above cannot be
        // satisfied by hard-coding the no-advance arm. Same admission, same
        // population, same verdict from the leaf - the ONLY difference is that
        // the pin moved. That must still read as a repair, or the change has
        // replaced a predicate that is always true with one that is always
        // false and measured nothing either way.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairingWithAdvance(
            pins, storage, time, _ => LivenessConsumerId(0));

        using var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Does.Contain(LivenessLeafGrainId(0)),
                "the floor-holding leaf must be driven at all, or every grading assertion below is vacuous.");
            Assert.That(Outcomes(recorder, "drove_lifted"), Is.GreaterThan(0),
                "a drive that moved the admitted consumer's durable pin offset off the floor did the thing "
                    + "the remedy exists to do, and must still be recorded as a success.");
            Assert.That(Outcomes(recorder, "drove_no_advance"), Is.Zero,
                "the no-advance arm must be a measurement of the pin, not a constant. If it fires here it "
                    + "fires on every drive, and the fixture above proves nothing.");
            Assert.That(Outcomes(recorder, "healed"), Is.GreaterThan(0),
                "the heal credit is withheld for a pin that did not move, not for this admission path as a "
                    + "whole. Withholding it here would stop the remedy ever reporting a repair it made.");
        });
    }

    [Test]
    public async Task ExecuteAsync_bounds_the_retries_of_a_floor_holder_whose_pin_never_advances()
    {
        // The load-bearing safety property, and the one a fix aimed only at the
        // crediting bug can break. A drive ACTIVATES the leaf, so a remedy that
        // re-drives promptly resets its idle timer on every attempt and the leaf
        // never deactivates - an unbounded retry loop that still trims nothing,
        // which is strictly worse than the defect it replaces.
        //
        // Withholding the heal credit must therefore return the consumer to the
        // ordinary eligible pool, where the unchanged retry cooldown and attempt
        // ceiling apply. It must not shorten anything.
        //
        // The bound asserted is the one the mechanism actually provides, and it
        // is deliberately NOT MaxReactivationAttempts. That ceiling is per
        // budget cycle, and an abandoned budget legitimately re-arms once its
        // backoff elapses (issue #2783) - so over a long enough window the total
        // is bounded by the 15-minute retry cooldown, not by the attempt count.
        // Pinning it to 3 would assert a quiescence the scheduler has never had
        // and does not want, and would read as this change having caused it.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        var time = new VirtualTimeProvider();
        var (scheduler, _) = SchedulerRepairing(pins, storage, time);

        using var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < DriveOpportunities; i++)
        {
            await AdvanceAtLeastAsync(time, PastMinBlockAge);
        }

        await scheduler.StopAsync(CancellationToken.None);

        // One sweep per window at minimum, so an unbounded remedy - one drive
        // per pass, the shape that pins the leaf active forever - scores at
        // least DriveOpportunities. The cooldown permits at most one drive per
        // ReactivationRetryCooldownForTest across the same span. The two differ
        // by a factor of four here, so the assertion discriminates the storm
        // from the intended rate rather than merely bounding a number.
        var elapsed = PastMinBlockAge * DriveOpportunities;
        var cooldownBoundedCeiling = (int)(elapsed / ReactivationRetryCooldownForTest) + 1;
        var attempted = Outcomes(recorder, "attempted");

        Assert.Multiple(() =>
        {
            Assert.That(attempted, Is.GreaterThan(1),
                "the consumer must stay eligible after the credit is withheld. A single attempt across the "
                    + "whole window is the defect itself - on the live tree the counter froze at 1 and stayed "
                    + "there for eleven consecutive sweeps while the pin held the floor.");
            Assert.That(attempted, Is.LessThan(DriveOpportunities),
                "the remedy must be quiescent on most passes. A drive on every pass resets the leaf's idle "
                    + "timer before it can deactivate, so the leaf never tears down, never captures, and the "
                    + "checkpoint it is being driven for can never be persisted at all.");
            Assert.That(attempted, Is.LessThanOrEqualTo(cooldownBoundedCeiling),
                "and the rate must be the one the unchanged 15-minute retry cooldown permits, not merely "
                    + "some rate below the pass rate. Nothing in this change shortens any interval.");
            Assert.That(Outcomes(recorder, "healed"), Is.Zero,
                "no number of drives that fail to move the pin adds up to a repair.");
        });
    }

    /// <summary>
    /// Sweep windows granted to
    /// <see cref="ExecuteAsync_bounds_the_retries_of_a_floor_holder_whose_pin_never_advances"/>,
    /// each long enough to license a drive. An unbounded remedy scores one
    /// attempt per window; the cooldown permits roughly a quarter of that.
    /// </summary>
    private const int DriveOpportunities = 20;

    /// <summary>
    /// The scheduler's retry cooldown, restated here because it is private. It
    /// governs the retry rate this change relies on and deliberately does not
    /// alter.
    /// </summary>
    private static readonly TimeSpan ReactivationRetryCooldownForTest = TimeSpan.FromMinutes(15);
}

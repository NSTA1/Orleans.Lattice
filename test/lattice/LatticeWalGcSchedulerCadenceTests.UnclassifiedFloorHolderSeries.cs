using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the floor-holder series on a tree the classifier never reaches
/// (issue #4227).
/// <para>
/// <b>The defect.</b> <c>floor_holder_admission</c> (issue #3258) and
/// <c>never_checkpointed_pin_offset</c> (issue #4198) were minted only inside
/// <c>ClassifyFloorHolderPinsAsync</c>, which runs only on the usable-floor
/// arm of a pass. A tree whose cursor floor reports
/// <c>BlockedByUnusablePin</c> takes the heal arm instead, classifies its
/// report-named blockers on <c>blocking_pin_state</c>, and never reaches the
/// floor-holder classifier at all - so it published no series on either
/// instrument. That is byte-identical to a tree that was never registered,
/// which is the exact ambiguity issue #3258 was filed to end, reintroduced one
/// stage earlier in the pipeline.
/// </para>
/// <para>
/// <b>Measured on the deployed repocontext container.</b>
/// <c>sys-schema-version</c> read 62 blocked passes, 0 reclaimed,
/// <c>blocking_pin_state{never_checkpointed}=8</c>, and no series at all on
/// either instrument, while fifteen sibling trees in the same scrape emitted
/// <c>floor_holder_admission</c>.
/// </para>
/// <para>
/// <b>Diagnostic only.</b> Nothing here changes admission: the refusal the
/// gate makes is correct, and widening it is the silent-data-loss change
/// reverted under PR #4189. The blocked arm's existing reactivation is
/// untouched, and the floor-holder liveness drive is still never entered.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// A scheduler over a tree whose cursor floor reports blocked on every pass,
    /// naming one never-checkpointed leaf - the <c>sys-schema-version</c> shape.
    /// </summary>
    private static (LatticeWalGcScheduler Scheduler, LeafTouchBook Leaves) SchedulerBlockedBeforeClassification(
        FakePinStore pins,
        LeafStateBook storage,
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

        return (CreateScheduler(factory, gc, OrphanSweepOptions(), time, leafStateStorage: storage), leaves);
    }

    /// <summary>
    /// Runs the blocked tree past the minimum block age, so the heal arm has
    /// classified its blocker and run its orphan sweep at least once.
    /// </summary>
    private static async Task DriveBlockedBeforeClassificationAsync()
    {
        var storage = new LeafStateBook();
        storage.PutNeverCheckpointed(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var time = new VirtualTimeProvider();
        var (scheduler, _) = SchedulerBlockedBeforeClassification(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task A_tree_blocked_before_the_floor_holder_classifier_charges_the_unreached_admission_arm()
    {
        // The regression. The tree is evaluated on every pass, every pass is
        // blocked, and the floor-holder classifier is structurally unreachable
        // from the heal arm - so before the fix the admission instrument had no
        // series for it whatever happened.
        using var admission = new AdmissionRecorder(OrphanSweepTree);
        using var states = new BlockingPinStateRecorder(
            OrphanSweepTree, LatticeMetrics.BlockingPinNeverCheckpointed.Value as string);

        await DriveBlockedBeforeClassificationAsync();

        Assert.Multiple(() =>
        {
            Assert.That(states.Total, Is.GreaterThan(0),
                "precondition: the heal arm classified the never-checkpointed blocker. This is the "
                    + "sys-schema-version reading - blocking_pin_state{never_checkpointed} non-zero - and "
                    + "without it the fixture would not be modelling the defect.");
            Assert.That(admission.Unreached, Is.GreaterThan(0),
                "the unreached arm must advance on a pass that took the floor-blocked heal arm, so a tree "
                    + "blocked before classification names itself rather than reading as a tree that was "
                    + "never registered (issue #4227).");
            Assert.That(admission.AdmittedMeasurements, Is.GreaterThan(0),
                "the admitted arm must be PUBLISHED, so the series exists for every tree the GC evaluates.");
            Assert.That(admission.BlockedMeasurements, Is.GreaterThan(0),
                "and so must the blocked arm. Measurement COUNT is asserted because Add(0) is idempotent: "
                    + "the value alone cannot tell 'primed' from 'never published'.");
            Assert.That(admission.Admitted + admission.Blocked, Is.Zero,
                "but neither may be CHARGED. The classifier never ran, so it reached no verdict about the "
                    + "floor's holder, and charging 'blocked' would report a floor wedge nobody measured.");
        });
    }

    [Test]
    public async Task A_tree_blocked_before_the_floor_holder_classifier_publishes_the_never_checkpointed_offset_arms()
    {
        // The second instrument the issue names. It is per (tree, partition),
        // so it is primed at the reserved partition value exactly as
        // blocking_pin_state is: a measured zero that says the GC evaluated this
        // tree, rather than an absence equally consistent with a dead subsystem.
        using var offsets = new NeverCheckpointedOffsetRecorder(OrphanSweepTree);

        await DriveBlockedBeforeClassificationAsync();

        Assert.Multiple(() =>
        {
            Assert.That(offsets.OffsetUsableMeasurements, Is.GreaterThan(0),
                "offset_usable must be PUBLISHED for a tree the GC evaluates even when the floor-holder "
                    + "classifier never ran on it (issue #4227).");
            Assert.That(offsets.OffsetAbsentMeasurements, Is.GreaterThan(0),
                "and so must offset_absent.");
            Assert.That(offsets.OffsetUsable + offsets.OffsetAbsent, Is.Zero,
                "but neither may be charged: the heal arm does not read the blocker's offset, so the "
                    + "split is not measured there and a charge would be invented.");
        });
    }

    [Test]
    public async Task A_tree_that_reaches_the_floor_holder_classifier_never_charges_the_unreached_arm()
    {
        // The control. 'unreached' names a pass the classifier could not reach;
        // charging it on a tree the classifier did reach would blur the very
        // distinction it exists to draw.
        var storage = new LeafStateBook();
        storage.PutNeverCheckpointed(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        using var admission = new AdmissionRecorder(OrphanSweepTree);

        await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Blocked, Is.GreaterThan(0),
                "precondition: the classifier ran and refused the floor's holder.");
            Assert.That(admission.UnreachedMeasurements, Is.GreaterThan(0),
                "the unreached arm is primed on every evaluated tree, so it is present here too.");
            Assert.That(admission.Unreached, Is.Zero,
                "but it must not be charged on a tree whose passes reach the classifier.");
        });
    }
}

using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the floor-holder classifier's coverage claim (issue #3168): the
/// classifier and the leaf's own repairer disagreed about the same leaf, and
/// the classifier was wrong.
/// <para>
/// <b>The disagreement.</b> On the live repocontext container, WAL GC
/// classified all eight partitions of leaf
/// <c>bplusleaf/af0a62b8cd1c4efc8eb91905fa766fdd</c> on tree
/// <c>repo-context-vector-payload</c> as <c>checkpointed_uncovered</c> -
/// repairable - while that leaf, driven into an activation by issue #3164 every
/// pass, answered <c>no_checkpointed_uncovered_partition</c> over four thousand
/// times. Both cannot be true of one leaf.
/// </para>
/// <para>
/// <b>Why the leaf was right.</b> <c>ClassifyCheckpoint</c> returns
/// <c>checkpointed_uncovered</c> from the persisted checkpoint <b>alone</b>. The
/// "uncovered" half is never measured and cannot be: coverage is
/// <c>_durableSnapshotOffsetsByPartition</c>, a per-activation in-memory field
/// absent from the persisted <c>LeafNodeState</c> the classifier reads. What
/// normally licenses the inference is knowing independently that the pin is
/// unusable, since the published pin is <c>min(checkpoint, covered)</c>. The
/// blocked arm has that premise by construction -
/// <c>ApplyDurableMaterialiserFloorAsync</c> names a consumer only when its pin
/// is <c>&lt;= Zero</c>. The floor-holder arm does not, and cannot: it runs only
/// in the <c>else</c> of the floor-blocked branch, so it runs only when
/// <b>no</b> dormant pin is <c>&lt;= Zero</c>, and it selects by lowest frontier
/// rather than by usability. It was asserting a coverage hole in the one
/// population structurally guaranteed not to have one.
/// </para>
/// <para>
/// <b>What is under test here.</b> These fixtures are written against the
/// disagreement rather than against either component's internals, so they fail
/// if the classifier ever again claims a coverage hole it did not measure, and
/// equally if the narrow fix is widened into "the floor-holder arm classifies
/// nothing" - which would make the tests pass by deleting the signal. Both
/// directions are asserted.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Classifies one floor-holding pin over a durably checkpointed leaf at an
    /// explicit frontier, and returns the arms recorded for it.
    /// </summary>
    private static async Task<InstrumentRecorder> ClassifyOneHolderAsync(HybridLogicalClock frontier)
    {
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), frontier);

        var time = new VirtualTimeProvider();
        var (scheduler, _) = SchedulerRepairing(pins, storage, time);

        using var states = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        return states;
    }

    /// <summary>
    /// The arms this classification actually advanced, ignoring the zero-primed
    /// ones. Priming records every arm at zero, so a test that forgot to filter
    /// would pass against any verdict whatsoever.
    /// </summary>
    private static string[] AdvancedPinArms(InstrumentRecorder states) =>
        [.. states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagStatus) as string ?? string.Empty)
            .Distinct()];

    [Test]
    public async Task A_usable_floor_holding_pin_is_not_claimed_to_be_uncovered()
    {
        // Acceptance criterion 1, as a test. The leaf is durably checkpointed
        // and its pin carries a real frontier, which is precisely the live
        // shape: ResolveDurablePinForPartition publishes min(checkpoint,
        // covered), so a positive frontier is proof that coverage was present
        // when the pin was written. Claiming 'uncovered' over it is the defect.
        var states = await ClassifyOneHolderAsync(new HybridLogicalClock { WallClockTicks = 1_000 });

        Assert.That(AdvancedPinArms(states), Is.EquivalentTo(new[] { "checkpointed_coverage_unknown" }),
            "a floor-holding pin that is not known to be unusable must not be reported as a coverage "
                + "hole. The 'uncovered' half of 'checkpointed_uncovered' is inferred from the pin being "
                + "unusable and is never measured, and this arm has no such premise - it runs only when "
                + "the cursor floor reports usable, which is exactly when no dormant pin is at the "
                + "sentinel. Seeing 'checkpointed_uncovered' here is issue #3168 restored.");
    }

    [Test]
    public async Task An_unusable_floor_holding_pin_is_still_claimed_to_be_uncovered()
    {
        // The other direction, and the reason the fix is a discriminator rather
        // than a retreat. A pin AT the sentinel whose consumer is present in the
        // live registry is skipped by ApplyDurableMaterialiserFloorAsync before
        // its pin is ever evaluated, so it can hold a floor that reports usable
        // while being genuinely unusable. Over a proven durable checkpoint that
        // is a real coverage hole, and it must still be named one.
        var states = await ClassifyOneHolderAsync(HybridLogicalClock.Zero);

        Assert.That(AdvancedPinArms(states), Is.EquivalentTo(new[] { "checkpointed_uncovered" }),
            "the fix must narrow the claim, not withdraw it. If this arm reports "
                + "'checkpointed_coverage_unknown' then the gate is keyed on the call site rather than on "
                + "the pin, and issue #3164's remedy has been silently disabled for the population it was "
                + "built to reach.");
    }

    [Test]
    public async Task The_two_verdicts_differ_only_by_the_pin_and_not_by_the_leaf()
    {
        // The perturbation-proof form, and what makes the pair above evidence
        // rather than two independent assertions. Both runs read the SAME
        // durable leaf state through the same accessor; the only thing that
        // differs is the frontier. So the classification cannot be an artefact
        // of how the leaf was seeded, which was the competing hypothesis for
        // #3168 - that the two components read different durable state and one
        // was stale. They do not: the classifier's ReadPersistedCheckpoint
        // mirrors the leaf's GetPersistedCheckpointForPartition slot for slot.
        var usable = AdvancedPinArms(
            await ClassifyOneHolderAsync(new HybridLogicalClock { WallClockTicks = 1_000 }));
        var unusable = AdvancedPinArms(await ClassifyOneHolderAsync(HybridLogicalClock.Zero));

        Assert.Multiple(() =>
        {
            Assert.That(usable, Is.Not.EquivalentTo(unusable),
                "one leaf, one accessor, two pins, and the verdict must follow the pin. Identical arms "
                    + "here mean the frontier is not being consulted at all.");
            Assert.That(usable, Has.Length.EqualTo(1));
            Assert.That(unusable, Has.Length.EqualTo(1));
        });
    }

    [Test]
    public async Task A_usable_floor_holder_is_not_driven_into_the_reactivation_remedy()
    {
        // Acceptance criterion 2 at the seam that actually cost something. The
        // misclassification was not merely a wrong label: issue #3164 drives
        // every pin classified 'checkpointed_uncovered' into TryRepairZeroCover-
        // ageAsync, so on the live container a healthy, fully covered, merely
        // idle leaf was reactivated every pass, for ever, to be told there was
        // nothing to repair. The reactivation is the cost, so the exclusion is
        // asserted there and not only on the metric.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), new HybridLogicalClock { WallClockTicks = 1_000 });

        var leaves = await DriveAsync(pins, storage);

        Assert.That(leaves.Touched, Is.Empty,
            "driving this leaf can only spend an activation and a durable read to discover that its "
                + "coverage was never missing. A non-empty set here is the futile loop issue #3168 "
                + "removes, and it is the loop that recorded 4,424 declines on one leaf.");
    }

    [Test]
    public async Task A_usable_holder_beside_an_unusable_one_is_the_only_one_driven()
    {
        // The discrimination test. Every fixture above can be satisfied by an
        // arm that classifies nothing and drives nothing; this one cannot. Both
        // pins hold the floor, both leaves are durably checkpointed and
        // byte-identical in storage, both are sampled - and they must be treated
        // oppositely purely on the evidence their pins carry.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);
        storage.PutLive(RepairLeafGrainId(1), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), HybridLogicalClock.Zero);
        pins.Seed(OrphanSweepTree, RepairConsumerId(1), new HybridLogicalClock { WallClockTicks = 1_000 });

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Does.Contain(RepairLeafGrainId(0)),
                "the holder at the blocking sentinel is a real coverage hole over a proven checkpoint and "
                    + "must still be driven, or the fix has disabled the remedy rather than aimed it.");
            Assert.That(leaves.Touched, Does.Not.Contain(RepairLeafGrainId(1)),
                "and the holder beside it with a usable pin must not be, however much WAL it is holding. "
                    + "Its floor is held by a healthy pin that is merely the oldest, which is a "
                    + "frontier-advance problem and is not repaired by touching the leaf.");
        });
    }
}

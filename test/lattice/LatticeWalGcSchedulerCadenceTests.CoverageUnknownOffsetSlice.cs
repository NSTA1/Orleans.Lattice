using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the offset-availability split of the
/// <c>checkpointed_coverage_unknown</c> arm (issue #3199).
/// <para>
/// <b>The gap this closes.</b> Issue #3168 established that a floor-holding pin
/// which is not known to be unusable must be classified
/// <c>checkpointed_coverage_unknown</c> rather than claimed to be a coverage
/// hole. That arm is correct, but it covers two states with <b>opposite</b>
/// remedies. A candidate whose durable offset is <c>&gt;= 0</c> is still driven
/// for liveness when that offset equals the tree's offset floor (issue #3178),
/// so the escape hatch is open. A candidate that reported no offset constrains
/// no offset floor at all, can therefore never satisfy that equality, and has
/// already been promoted out of issue #3164's coverage repair by the frontier
/// gate - the triple exclusion of issue #3199.
/// </para>
/// <para>
/// <b>Why no existing instrument can express it.</b>
/// <c>orleans.lattice.wal.gc.blocking_pin_state</c> records the classifier's
/// verdict and nothing about the candidate that produced it, and both
/// floor-holder sample lists are recorded through the same call, so the two
/// cases are byte-identical there. The routing bit is already computed by the
/// sweep and was simply discarded. These fixtures assert it is now recorded,
/// and - equally - that the arm which is <i>not</i> taken is still primed, since
/// an unprimed zero cannot distinguish "no candidate lacked an offset" from
/// "nothing was ever classified here", which is the exact ambiguity that let
/// issue #3158 hide on the one tree its classifier existed to diagnose.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// A durable offset that is present and usable. Any non-negative value
    /// selects the <c>offset_usable</c> arm; the sweep's routing test is
    /// <c>offset &lt; 0</c> and nothing here depends on the magnitude.
    /// </summary>
    private const long UsableHolderOffset = 42L;

    /// <summary>
    /// Classifies floor-holding pins and returns both the blocking-pin-state
    /// arms and the offset-availability arms recorded for the same run, so the
    /// two can be compared against each other rather than asserted separately.
    /// </summary>
    private static async Task<(InstrumentRecorder States, InstrumentRecorder Offsets)>
        ClassifyHoldersAsync(Action<FakePinStore, LeafStateBook> seed)
    {
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        seed(pins, storage);

        var time = new VirtualTimeProvider();
        var (scheduler, _) = SchedulerRepairing(pins, storage, time);

        var states = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);
        var offsets = new InstrumentRecorder(
            LatticeMetrics.WalGcCoverageUnknownPinOffset, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        return (states, offsets);
    }

    /// <summary>
    /// The offset arms this run actually advanced, ignoring the zero-primed
    /// ones. Both arms are primed on every classification, so a test that
    /// forgot to filter would pass against any routing whatsoever.
    /// </summary>
    private static string[] AdvancedOffsetArms(InstrumentRecorder offsets) =>
        [.. offsets.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagStatus) as string ?? string.Empty)
            .Distinct()];

    /// <summary>Every arm name the run touched at all, advanced or primed.</summary>
    private static string[] TouchedOffsetArms(InstrumentRecorder offsets) =>
        [.. offsets.Measurements
            .Select(m => m.Tag(LatticeMetrics.TagStatus) as string ?? string.Empty)
            .Distinct()];

    [Test]
    public async Task A_coverage_unknown_holder_carrying_a_durable_offset_records_offset_usable()
    {
        // The benign half of the split. This candidate is latched out of the
        // coverage repair by the frontier gate, but it carries an offset, so
        // issue #3178's liveness drive can still admit it when that offset
        // equals the tree's floor. An operator reading a tree sitting entirely
        // on this arm is looking at a latch that is live but not stuck.
        var (states, offsets) = await ClassifyHoldersAsync((pins, storage) =>
        {
            storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);
            pins.Seed(
                OrphanSweepTree,
                RepairConsumerId(0),
                new HybridLogicalClock { WallClockTicks = 1_000 },
                UsableHolderOffset);
        });

        using (states)
        using (offsets)
        {
            Assert.Multiple(() =>
            {
                Assert.That(AdvancedPinArms(states),
                    Is.EquivalentTo(new[] { "checkpointed_coverage_unknown" }),
                    "the precondition for this fixture: the split only has meaning on the "
                        + "coverage-unknown arm, so if issue #3168's classification has regressed this "
                        + "test is measuring something else and must say so rather than pass.");
                Assert.That(AdvancedOffsetArms(offsets),
                    Is.EquivalentTo(new[] { "offset_usable" }),
                    "a candidate whose durable offset is >= 0 is routed into the offset-bearing "
                        + "sample list by the sweep, and that is the bit this instrument exists to "
                        + "record. Seeing 'offset_absent' here inverts the very conjunct issue #3199 "
                        + "turns on and would make a healthy tree read as permanently stuck.");
            });
        }
    }

    [Test]
    public async Task A_coverage_unknown_holder_with_no_durable_offset_records_offset_absent()
    {
        // The other half, and the one issue #3199 is about. No offset means no
        // constraint on the offset floor, so the equality gate that would drive
        // this leaf for liveness can never be satisfied - while the frontier
        // gate has already promoted it out of the coverage repair.
        var (states, offsets) = await ClassifyHoldersAsync((pins, storage) =>
        {
            storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);
            pins.Seed(
                OrphanSweepTree,
                RepairConsumerId(0),
                new HybridLogicalClock { WallClockTicks = 1_000 });
        });

        using (states)
        using (offsets)
        {
            Assert.Multiple(() =>
            {
                Assert.That(AdvancedPinArms(states),
                    Is.EquivalentTo(new[] { "checkpointed_coverage_unknown" }));
                Assert.That(AdvancedOffsetArms(offsets),
                    Is.EquivalentTo(new[] { "offset_absent" }),
                    "this is the slice whose emptiness or otherwise settles issue #3199. A pin here "
                        + "is excluded from the blocked arm (its frontier is above Zero), from the "
                        + "issue #3164 coverage repair (the frontier gate promoted its state), and from "
                        + "the issue #3178 liveness drive (it carries no offset) - and the pin store "
                        + "merges monotonic-max on both axes, so no later action by the leaf can clear "
                        + "any of the three.");
            });
        }
    }

    [Test]
    public async Task Both_offset_arms_are_primed_even_when_no_holder_is_coverage_unknown()
    {
        // The property that makes the live reading conclusive in EITHER
        // direction, and the whole reason this instrument is worth adding
        // rather than deriving. The holder here classifies 'checkpointed_-
        // uncovered', so neither offset arm is advanced - but both must still
        // be minted, or an empty 'offset_absent' slice on the live container
        // would be indistinguishable from nothing having been classified at
        // all. That ambiguity is precisely how issue #3158 stayed invisible.
        var (states, offsets) = await ClassifyHoldersAsync((pins, storage) =>
        {
            storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, RepairConsumerId(0), HybridLogicalClock.Zero);
        });

        using (states)
        using (offsets)
        {
            Assert.Multiple(() =>
            {
                Assert.That(AdvancedPinArms(states),
                    Is.EquivalentTo(new[] { "checkpointed_uncovered" }),
                    "the precondition: this fixture needs a classification that is NOT coverage "
                        + "unknown, so that the priming is observed in isolation from any recording.");
                Assert.That(AdvancedOffsetArms(offsets), Is.Empty,
                    "nothing may be recorded when the state that resolved is not coverage unknown. "
                        + "Recording here would make the two panels disagree and would overstate the "
                        + "stuck population.");
                Assert.That(TouchedOffsetArms(offsets),
                    Is.EquivalentTo(new[] { "offset_usable", "offset_absent" }),
                    "and both arms must nonetheless exist as measured zeroes. If priming is made "
                        + "conditional on the recorded state, a zero stops meaning 'measured and not "
                        + "this' and starts meaning 'never looked at' - which is the exact defect "
                        + "issue #3199's decisive test cannot tolerate, in either direction.");
            });
        }
    }

    [Test]
    public async Task The_offset_arms_sum_to_the_coverage_unknown_arm_over_a_mixed_population()
    {
        // The completeness invariant, and the discrimination test. Every
        // fixture above is satisfiable by an instrument that records one arm
        // and never the other; this one is not. Three holders, all durably
        // checkpointed and byte-identical in storage, differing only in the
        // pin each carries: one unusable, one usable with an offset, one usable
        // without. The split must account for exactly the coverage-unknown
        // population - no more, since the unusable one is a real coverage hole
        // and belongs to neither arm, and no less.
        var (states, offsets) = await ClassifyHoldersAsync((pins, storage) =>
        {
            for (var i = 0; i < 3; i++)
            {
                storage.PutLive(RepairLeafGrainId(i), OrphanSweepTree);
            }

            pins.Seed(OrphanSweepTree, RepairConsumerId(0), HybridLogicalClock.Zero);
            pins.Seed(
                OrphanSweepTree,
                RepairConsumerId(1),
                new HybridLogicalClock { WallClockTicks = 1_000 },
                UsableHolderOffset);
            pins.Seed(
                OrphanSweepTree,
                RepairConsumerId(2),
                new HybridLogicalClock { WallClockTicks = 2_000 });
        });

        using (states)
        using (offsets)
        {
            var coverageUnknown = states.Measurements
                .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "checkpointed_coverage_unknown")
                .Sum(m => m.Value);
            var usable = offsets.Measurements
                .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "offset_usable")
                .Sum(m => m.Value);
            var absent = offsets.Measurements
                .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "offset_absent")
                .Sum(m => m.Value);

            Assert.Multiple(() =>
            {
                Assert.That(coverageUnknown, Is.GreaterThan(0),
                    "the precondition: two of the three holders carry a usable pin, so the "
                        + "coverage-unknown arm must be reached. Zero means the population under test "
                        + "is not the one this fixture describes.");
                Assert.That(usable + absent, Is.EqualTo(coverageUnknown),
                    "sum by (tree) over this instrument must equal the coverage-unknown arm of "
                        + "blocking_pin_state on the same tree. That equality is what licenses reading "
                        + "the two panels against each other, and a divergence is a defect in one of "
                        + "them rather than a finding about the estate. Note both sides are cumulative "
                        + "tallies over however many passes ran, not populations, so the invariant is "
                        + "the equality and not any particular count.");
                Assert.That(usable, Is.GreaterThan(0),
                    "the holder that carried an offset must reach the usable arm.");
                Assert.That(absent, Is.GreaterThan(0),
                    "and the one that did not must reach the absent arm. Both non-zero is what stops "
                        + "this test being satisfied by an instrument that always picks the same arm.");
                Assert.That(usable, Is.EqualTo(absent),
                    "and they must be equal, because exactly one holder of each kind was seeded and "
                        + "both are classified on every pass that reaches them. An imbalance means the "
                        + "routing is being decided by something other than the candidate's own "
                        + "offset - which is the whole quantity this instrument claims to report.");
            });
        }
    }
}

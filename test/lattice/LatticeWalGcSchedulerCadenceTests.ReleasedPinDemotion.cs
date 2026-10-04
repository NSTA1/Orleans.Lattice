using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for issue #3605: the floor-holder classifier's coverage demotion
/// (issue #3168) keyed on the frontier alone, and so still claimed a coverage
/// hole over a pin the floor itself treats as covered.
/// <para>
/// <b>The disagreement.</b> A leaf releasing a partition it never wrote to
/// (issue #3453) publishes its pin at <c>(Zero, offset &gt;= 0)</c>: no frontier,
/// because nothing was materialised, but a real offset, because the partition is
/// covered up to it. The floor's issue #3094 exemption reads exactly that shape
/// as covered and lets the trim pass it. The classifier read the same pin as
/// <c>checkpointed_uncovered</c>, which admits it to the repair drive
/// unconditionally - so a leaf the floor had already stopped waiting on was
/// woken every sweep to repair a hole it does not have.
/// </para>
/// <para>
/// Both directions are asserted, so the fix cannot pass by withdrawing the
/// claim: a pin at <c>(&lt;= Zero, offset &lt; 0)</c> must still be named
/// uncovered (that arm is
/// <c>An_unusable_floor_holding_pin_is_still_claimed_to_be_uncovered</c>).
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// The offset a released, never-written partition is published at. Any
    /// value <c>&gt;= 0</c> reproduces the shape; this one is above
    /// <see cref="ReleasedFloorOffset"/> so a fixture can place it off the floor.
    /// </summary>
    private const long ReleasedPinOffset = 120;

    /// <summary>
    /// An offset strictly below <see cref="ReleasedPinOffset"/>, for the pin that
    /// holds the tree's offset floor.
    /// </summary>
    private const long ReleasedFloorOffset = 50;

    [Test]
    public async Task A_released_floor_holding_pin_is_not_claimed_to_be_uncovered()
    {
        // The defect stated as a test. The leaf is durably checkpointed and its
        // pin is (Zero, 120): the issue #3453 release shape. The floor's #3094
        // exemption counts it as covered, so claiming 'uncovered' over it is the
        // classifier contradicting the floor about the same pin.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), HybridLogicalClock.Zero, ReleasedPinOffset);

        var time = new VirtualTimeProvider();
        var (scheduler, _) = SchedulerRepairing(pins, storage, time);

        using var states = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(AdvancedPinArms(states), Is.EquivalentTo(new[] { "checkpointed_coverage_unknown" }),
            "a pin at (Zero, offset >= 0) is a released never-written partition, which the floor's "
                + "issue #3094 exemption already treats as covered. Reporting 'checkpointed_uncovered' here "
                + "is issue #3605: the demotion is keyed on the frontier alone and misses the offset half "
                + "of the exemption it is meant to mirror.");
    }

    [Test]
    public async Task A_released_pin_above_the_offset_floor_is_not_driven()
    {
        // The cost of the misclassification. Leaf 0 is a genuine coverage hole
        // (Zero, -1) and must be driven. Leaf 2 holds the offset floor but has
        // never checkpointed, so it is refused and the floor is NOT admitted.
        // Leaf 1 is the released (Zero, 120) pin above that floor.
        //
        // Classified correctly it is coverage_unknown, which the offset gate
        // admits only on or behind an admitted floor - neither holds, so it is
        // left alone. Misclassified as uncovered it is admitted unconditionally
        // and woken every sweep for a hole it does not have.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);
        storage.PutLive(RepairLeafGrainId(1), OrphanSweepTree);
        storage.PutNeverCheckpointed(RepairLeafGrainId(2), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);
        pins.Seed(OrphanSweepTree, RepairConsumerId(1), HybridLogicalClock.Zero, ReleasedPinOffset);
        pins.Seed(OrphanSweepTree, RepairConsumerId(2), UsablePin, ReleasedFloorOffset);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Does.Contain(RepairLeafGrainId(0)),
                "the genuine coverage hole at (Zero, -1) must still be driven; the fix narrows the "
                    + "uncovered claim and must not withdraw it.");
            Assert.That(leaves.Touched, Does.Not.Contain(RepairLeafGrainId(1)),
                "the released (Zero, 120) pin must not be driven as a repair candidate. Seeing it here "
                    + "means it was classified checkpointed_uncovered and admitted unconditionally, which "
                    + "is issue #3605: an activation spent every sweep on a pin the floor already counts "
                    + "as covered.");
            Assert.That(leaves.Touched, Does.Not.Contain(RepairLeafGrainId(2)),
                "a never-checkpointed leaf is never driven, at any offset.");
        });
    }
}

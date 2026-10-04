using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// The offset-reader clause of the WAL GC trim predicate (issue #4579): an entry
/// at or above the lowest offset an offset-reading consumer (the replication
/// shipper) has not durably consumed in its partition is refused however every
/// other admitting arm reads, except the retention TTL ceiling. A WAL partition
/// is not HLC-ordered in offset, so the consumer's HLC cursor alone cannot hold
/// an entry it has not read.
/// </summary>
[TestFixture]
public sealed class WalGcTrimCoreConsumerOffsetFloorTests
{
    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public void ClassifyEntry_refuses_an_entry_at_the_consumer_floor_that_the_cursor_admits()
    {
        // The defect shape: entry stamped 10 below the cursor 20, at the first
        // offset the shipper has not acknowledged.
        var verdict = WalGcTrimCore.ClassifyEntry(
            Hlc(10), null, entryOffset: 4, minCursor: Hlc(20), ttlCeiling: null,
            causalStable: null, blockedFloor: null, offsetAdmission: null, consumerOffsetFloor: 4);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.CursorFloor));
    }

    [Test]
    public void ClassifyEntry_refuses_an_entry_above_the_consumer_floor_that_the_materialiser_offset_admission_admits()
    {
        // The leaf has checkpointed past the entry and the uncovered cursor has
        // passed its stamp, so the offset admission alone would release it.
        var admission = new WalGcOffsetAdmission(Floor: 10, UncoveredCursor: Hlc(20));

        var verdict = WalGcTrimCore.ClassifyEntry(
            Hlc(10), null, entryOffset: 5, minCursor: Hlc(20), ttlCeiling: null,
            causalStable: null, blockedFloor: null, offsetAdmission: admission, consumerOffsetFloor: 4);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.CursorFloor));
    }

    [Test]
    public void ClassifyEntry_admits_an_entry_below_the_consumer_floor()
    {
        var verdict = WalGcTrimCore.ClassifyEntry(
            Hlc(10), null, entryOffset: 3, minCursor: Hlc(20), ttlCeiling: null,
            causalStable: null, blockedFloor: null, offsetAdmission: null, consumerOffsetFloor: 4);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.Eligible));
    }

    [Test]
    public void ClassifyEntry_lets_the_retention_ceiling_trim_past_the_consumer_floor()
    {
        // An operator retention window stays a bound: a consumer that falls
        // behind it detects the trimmed gap on its next read.
        var verdict = WalGcTrimCore.ClassifyEntry(
            Hlc(10), null, entryOffset: 9, minCursor: Hlc(20), ttlCeiling: Hlc(15),
            causalStable: null, blockedFloor: null, offsetAdmission: null, consumerOffsetFloor: 4);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.Eligible));
    }

    [Test]
    public void IsEntryEligible_without_a_consumer_floor_is_unchanged()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalGcTrimCore.IsEntryEligible(
                Hlc(10), null, 9, Hlc(20), null, null, null, null), Is.True);
            Assert.That(WalGcTrimCore.IsEntryEligible(
                Hlc(10), null, 9, Hlc(20), null, null, null, null, consumerOffsetFloor: 9), Is.False);
        });
    }
}

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public sealed class CausalAppliedIdentityRecordTests
{
    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public void A_recorded_identity_is_found_only_for_its_own_origin()
    {
        var record = new CausalAppliedIdentityRecord();
        record.Record("a", Hlc(5), capacity: 10);

        Assert.Multiple(() =>
        {
            Assert.That(record.Contains("a", Hlc(5)), Is.True);
            Assert.That(record.Contains("a", Hlc(4)), Is.False, "an identity is exact, never a frontier");
            Assert.That(record.Contains("b", Hlc(5)), Is.False);
        });
    }

    [Test]
    public void Past_its_capacity_the_record_forgets_the_oldest_identity_of_the_origin()
    {
        var record = new CausalAppliedIdentityRecord();
        record.Record("a", Hlc(3), capacity: 2);
        record.Record("a", Hlc(1), capacity: 2);
        record.Record("a", Hlc(2), capacity: 2);
        record.Record("b", Hlc(1), capacity: 2);

        Assert.Multiple(() =>
        {
            Assert.That(record.Count("a"), Is.EqualTo(2));
            Assert.That(record.Contains("a", Hlc(1)), Is.False, "the oldest is the one the low watermark covers first");
            Assert.That(record.Contains("a", Hlc(3)), Is.True);
            Assert.That(record.Contains("b", Hlc(1)), Is.True, "capacity is per origin");
        });
    }

    [Test]
    public void Record_rejects_an_empty_origin()
    {
        Assert.That(() => new CausalAppliedIdentityRecord().Record("", Hlc(1), 1), Throws.InstanceOf<ArgumentException>());
    }
}

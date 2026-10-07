namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public sealed class CausalFrontierCoreTests
{
    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public void A_write_strictly_below_the_low_watermark_and_not_held_is_met()
    {
        Assert.That(CausalFrontierCore.Decide(Hlc(4), Hlc(5), held: false, lost: false), Is.EqualTo(CausalDependencyVerdict.Met));
    }

    [Test]
    public void The_low_watermark_is_strict()
    {
        Assert.That(CausalFrontierCore.Decide(Hlc(5), Hlc(5), held: false, lost: false), Is.EqualTo(CausalDependencyVerdict.Unmet),
            "a write at exactly the floor may still be in flight");
    }

    [Test]
    public void A_held_write_below_the_low_watermark_is_unmet()
    {
        Assert.That(CausalFrontierCore.Decide(Hlc(4), Hlc(5), held: true, lost: false), Is.EqualTo(CausalDependencyVerdict.Unmet),
            "acknowledged is not applied while it is parked");
    }

    [Test]
    public void A_lost_write_is_lost_wherever_the_low_watermark_is()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CausalFrontierCore.Decide(Hlc(4), Hlc(5), held: false, lost: true), Is.EqualTo(CausalDependencyVerdict.Lost));
            Assert.That(CausalFrontierCore.Decide(Hlc(9), HybridLogicalClock.Zero, held: false, lost: true), Is.EqualTo(CausalDependencyVerdict.Lost));
        });
    }

    [Test]
    public void One_lost_mark_does_not_pin_other_writes_of_the_origin()
    {
        // The per-identity form: a lost write at 1 leaves a later write at 2 decidable.
        Assert.That(CausalFrontierCore.Decide(Hlc(2), Hlc(3), held: false, lost: false), Is.EqualTo(CausalDependencyVerdict.Met));
    }

    [Test]
    public void Without_a_low_watermark_nothing_is_met()
    {
        Assert.That(CausalFrontierCore.Decide(Hlc(1), HybridLogicalClock.Zero, held: false, lost: false), Is.EqualTo(CausalDependencyVerdict.Unmet));
    }
}

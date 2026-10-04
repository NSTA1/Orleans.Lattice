using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationReceiveDedup"/>, the receiver's pure cycle-break and
/// high-water-mark rules (spec/replication/Replication.tla action
/// <c>Deliver</c>).
/// </summary>
[TestFixture]
public class ReplicationReceiveDedupTests
{
    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void IsOwnOrigin_is_true_only_for_the_receivers_own_cluster()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationReceiveDedup.IsOwnOrigin("site-a", "site-a"), Is.True);
            Assert.That(ReplicationReceiveDedup.IsOwnOrigin("site-b", "site-a"), Is.False);
            Assert.That(ReplicationReceiveDedup.IsOwnOrigin("SITE-A", "site-a"), Is.False);
        });
    }

    [Test]
    public void AdvancesHighWaterMark_takes_the_maximum()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationReceiveDedup.AdvancesHighWaterMark(Hlc(5), Hlc(6)), Is.True);
            Assert.That(ReplicationReceiveDedup.AdvancesHighWaterMark(Hlc(5), Hlc(5)), Is.False);
            Assert.That(ReplicationReceiveDedup.AdvancesHighWaterMark(Hlc(5), Hlc(4)), Is.False);
            Assert.That(ReplicationReceiveDedup.AdvancesHighWaterMark(HybridLogicalClock.Zero, Hlc(1)), Is.True);
        });
    }
}

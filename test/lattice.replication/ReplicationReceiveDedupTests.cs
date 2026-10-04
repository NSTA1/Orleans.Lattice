using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationReceiveDedup"/>, the receiver's pure cycle-break,
/// pinned-floor and high-water-mark rules (spec/replication/Replication.tla action
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
    public void IsCoveredByPinnedFloor_drops_at_and_below_the_floor_only()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationReceiveDedup.IsCoveredByPinnedFloor(Hlc(4), Hlc(5), false, false), Is.True);
            Assert.That(ReplicationReceiveDedup.IsCoveredByPinnedFloor(Hlc(5), Hlc(5), false, false), Is.True);
            Assert.That(ReplicationReceiveDedup.IsCoveredByPinnedFloor(Hlc(5, 1), Hlc(5), false, false), Is.False);
            Assert.That(ReplicationReceiveDedup.IsCoveredByPinnedFloor(Hlc(6), Hlc(5), false, false), Is.False);
        });
    }

    [Test]
    public void IsCoveredByPinnedFloor_drops_nothing_when_no_floor_is_pinned()
    {
        Assert.That(
            ReplicationReceiveDedup.IsCoveredByPinnedFloor(Hlc(1), HybridLogicalClock.Zero, false, false),
            Is.False);
    }

    [Test]
    public void IsCoveredByPinnedFloor_is_bypassed_by_a_bootstrap_drain()
    {
        Assert.That(ReplicationReceiveDedup.IsCoveredByPinnedFloor(Hlc(1), Hlc(5), true, false), Is.False);
    }

    [Test]
    public void IsCoveredByPinnedFloor_is_bypassed_by_a_saga_prepare_phase_entry()
    {
        Assert.That(ReplicationReceiveDedup.IsCoveredByPinnedFloor(Hlc(1), Hlc(5), false, true), Is.False);
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

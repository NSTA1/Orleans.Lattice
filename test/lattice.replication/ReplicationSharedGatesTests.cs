using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Covers <see cref="ReplicationCadence"/> and <see cref="LeafReReplayRanges"/>,
/// the small gates the replication maintenance and re-replay paths share.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class ReplicationSharedGatesTests
{
    [Test]
    public void IsDue_fires_on_the_first_tick_and_then_once_the_interval_elapses()
    {
        var interval = TimeSpan.FromTicks(100);

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationCadence.IsDue(nowTicks: 5, lastTicks: 0, interval), Is.True, "never run");
            Assert.That(ReplicationCadence.IsDue(nowTicks: 150, lastTicks: 100, interval), Is.False);
            Assert.That(ReplicationCadence.IsDue(nowTicks: 200, lastTicks: 100, interval), Is.True);
        });
    }

    [Test]
    public void AnyContains_reports_membership_in_any_range()
    {
        IReadOnlyList<LeafReReplayRange> ranges =
        [
            new LeafReReplayRange { StartKey = "a", EndKey = "c" },
            new LeafReReplayRange { StartKey = "m", EndKey = null },
        ];

        Assert.Multiple(() =>
        {
            Assert.That(LeafReReplayRanges.AnyContains(ranges, "b"), Is.True);
            Assert.That(LeafReReplayRanges.AnyContains(ranges, "z"), Is.True);
            Assert.That(LeafReReplayRanges.AnyContains(ranges, "d"), Is.False);
            Assert.That(LeafReReplayRanges.AnyContains([], "b"), Is.False);
        });
    }
}

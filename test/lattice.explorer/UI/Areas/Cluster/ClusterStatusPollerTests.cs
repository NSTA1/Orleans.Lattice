using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// The Cluster area's status poller (issue 3958): it asks at the steady
/// interval while reads succeed, backs off with a bounded doubling while they
/// fail - so a page rides out a reconnect - returns to the steady interval once a
/// read succeeds again, and stops once the operation settles. Driven on a manual
/// clock, never on the wall clock.
/// </summary>
[TestFixture]
public sealed class ClusterStatusPollerTests
{
    [Test]
    [TestCase(0, 2)]
    [TestCase(1, 4)]
    [TestCase(3, 16)]
    [TestCase(4, 30)]
    [TestCase(40, 30)]
    public void The_wait_doubles_per_failure_and_is_capped(int failures, int seconds)
    {
        Assert.That(ClusterStatusPoller.Delay(failures), Is.EqualTo(TimeSpan.FromSeconds(seconds)));
    }

    [Test]
    public void A_failed_read_backs_off_a_successful_one_returns_to_the_interval_and_settling_stops()
    {
        var time = new ManualTimeProvider();
        using var poller = new ClusterStatusPoller(time);
        var outcomes = new Queue<ClusterPollOutcome>([ClusterPollOutcome.Failed, ClusterPollOutcome.Running, ClusterPollOutcome.Settled]);
        var reads = 0;
        poller.Follow(_ =>
        {
            reads++;
            return Task.FromResult(outcomes.Dequeue());
        });

        Advance(time, ClusterStatusPoller.Interval);
        ReadsReach(() => reads, 1);

        Advance(time, ClusterStatusPoller.Interval);
        Assert.That(reads, Is.EqualTo(1), "after a failed read the poller waits twice as long");
        Advance(time, ClusterStatusPoller.Interval);
        ReadsReach(() => reads, 2);

        Advance(time, ClusterStatusPoller.Interval);
        ReadsReach(() => reads, 3);

        Assert.Multiple(() =>
        {
            Assert.That(SpinWait.SpinUntil(() => !poller.IsFollowing, TimeSpan.FromSeconds(10)), Is.True);
            Assert.That(time.ArmedTimers, Is.Zero, "a settled operation is no longer followed");
        });
    }

    [Test]
    public void Stopping_disarms_the_follow()
    {
        var time = new ManualTimeProvider();
        using var poller = new ClusterStatusPoller(time);
        poller.Follow(_ => Task.FromResult(ClusterPollOutcome.Running));
        Assert.That(time.ArmedTimers, Is.EqualTo(1));

        poller.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(poller.IsFollowing, Is.False);
            Assert.That(time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public void Following_needs_a_refresh()
    {
        using var poller = new ClusterStatusPoller(new ManualTimeProvider());

        Assert.That(() => poller.Follow(null!), Throws.ArgumentNullException);
    }

    private static void ReadsReach(Func<int> reads, int expected) =>
        Assert.That(SpinWait.SpinUntil(() => reads() == expected, TimeSpan.FromSeconds(10)), Is.True, $"the poller reads {expected} time(s)");

    private static void Advance(ManualTimeProvider time, TimeSpan delta)
    {
        // The next wait is armed on the follow's continuation once the read
        // returns; wait for it before moving the clock again.
        Assert.That(SpinWait.SpinUntil(() => time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "the poller re-arms");
        time.Advance(delta);
    }
}

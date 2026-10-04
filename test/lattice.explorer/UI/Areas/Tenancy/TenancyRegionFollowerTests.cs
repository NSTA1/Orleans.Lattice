using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// The Regions page's follower (issue #4114): it reads at the steady interval
/// after a change, doubles the wait after each read that brings no change (or
/// fails) up to a bound, returns to the steady interval on a change, and stops
/// once every region is steady or the page stops it. Driven on a manual clock.
/// </summary>
[TestFixture]
public sealed class TenancyRegionFollowerTests
{
    [Test]
    public void A_quiet_read_backs_off_a_change_returns_to_the_interval_and_a_steady_read_stops()
    {
        var time = new ManualTimeProvider();
        using var follower = new TenancyRegionFollower(time);
        var outcomes = new Queue<TenancyRegionFollowOutcome>([TenancyRegionFollowOutcome.Unchanged, TenancyRegionFollowOutcome.Changed, TenancyRegionFollowOutcome.Steady]);
        var reads = 0;
        follower.Follow(_ =>
        {
            Interlocked.Increment(ref reads);
            return Task.FromResult(outcomes.Dequeue());
        });
        Assert.That(follower.IsFollowing, Is.True);

        Advance(time, TenancyRegionFollower.Interval);
        ReadsReach(() => Volatile.Read(ref reads), 1);

        Advance(time, TenancyRegionFollower.Interval);
        Assert.That(Volatile.Read(ref reads), Is.EqualTo(1), "after a quiet read the follower waits twice as long");
        Advance(time, TenancyRegionFollower.Interval);
        ReadsReach(() => Volatile.Read(ref reads), 2);

        Advance(time, TenancyRegionFollower.Interval);
        ReadsReach(() => Volatile.Read(ref reads), 3);

        Assert.Multiple(() =>
        {
            FollowBarriers.Reaches(() => !follower.IsFollowing, "a steady read stops the follow");
            Assert.That(time.ArmedTimers, Is.Zero, "every region is steady, so nothing is followed");
        });
    }

    [Test]
    public void Stopping_or_disposing_disarms_the_follow()
    {
        var time = new ManualTimeProvider();
        var follower = new TenancyRegionFollower(time);
        follower.Follow(_ => Task.FromResult(TenancyRegionFollowOutcome.Unchanged));
        Assert.That(time.ArmedTimers, Is.EqualTo(1));

        follower.Stop();
        Assert.That((follower.IsFollowing, time.ArmedTimers), Is.EqualTo((false, 0)));

        follower.Follow(_ => Task.FromResult(TenancyRegionFollowOutcome.Unchanged));
        follower.Dispose();
        follower.Dispose();
        Assert.That((follower.IsFollowing, time.ArmedTimers), Is.EqualTo((false, 0)));

        follower.Follow(_ => Task.FromResult(TenancyRegionFollowOutcome.Unchanged));
        Assert.That((follower.IsFollowing, time.ArmedTimers), Is.EqualTo((false, 0)), "a disposed follower starts nothing");
    }

    [Test]
    public void Following_needs_a_read()
    {
        using var follower = new TenancyRegionFollower(new ManualTimeProvider());

        Assert.That(() => follower.Follow(null!), Throws.ArgumentNullException);
    }

    private static void ReadsReach(Func<int> reads, int expected) =>
        FollowBarriers.ReadsReach(reads, expected);

    // The next wait is armed on the follow's continuation once the read returns;
    // FollowBarriers.Advance waits for it before moving the clock again.
    private static void Advance(ManualTimeProvider time, TimeSpan delta) =>
        FollowBarriers.Advance(time, delta);
}

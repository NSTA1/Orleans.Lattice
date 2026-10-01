using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

/// <summary>
/// The shared operation follower (#4122): one read at once, then reads on the
/// circuit's clock until the operation is terminal or gone, keeping the last
/// status through a failed read and backing off while reads fail. Driven on a
/// manual clock, never on the wall clock.
/// </summary>
[TestFixture]
public sealed class OperationFollowerTests
{
    [Test]
    public async Task A_terminal_first_read_is_not_followed()
    {
        var time = new ManualTimeProvider();
        using var follower = new OperationFollower(time);
        var reads = 0;

        await follower.StartAsync(_ =>
        {
            reads++;
            return Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Succeeded));
        }, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reads, Is.EqualTo(1));
            Assert.That(follower.Status!.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(follower.IsFollowing, Is.False);
            Assert.That(time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public async Task A_running_operation_is_followed_until_it_finishes()
    {
        var time = new ManualTimeProvider();
        using var follower = new OperationFollower(time);
        var states = new Queue<LatticeOperationState>([LatticeOperationState.Running, LatticeOperationState.Running, LatticeOperationState.Succeeded]);
        var reads = 0;
        var changes = 0;
        follower.Changed += () => Interlocked.Increment(ref changes);

        await follower.StartAsync(_ =>
        {
            reads++;
            return Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(states.Dequeue()));
        }, CancellationToken.None);

        Assert.That(follower.IsFollowing, Is.True);
        Advance(time);
        ReadsReach(() => reads, 2);
        Advance(time);
        ReadsReach(() => reads, 3);

        Assert.Multiple(() =>
        {
            Assert.That(SpinWait.SpinUntil(() => !follower.IsFollowing, TimeSpan.FromSeconds(10)), Is.True, "a finished operation is no longer followed");
            Assert.That(follower.Status!.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(changes, Is.EqualTo(3), "every read raises Changed");
        });
    }

    [Test]
    public async Task An_operation_that_is_not_found_stops_the_follow_and_keeps_the_last_status()
    {
        var time = new ManualTimeProvider();
        using var follower = new OperationFollower(time);
        var answers = new Queue<LatticeOperationStatus?>([OperationTestStatus.Of(LatticeOperationState.Running), null]);

        await follower.StartAsync(_ => Task.FromResult(answers.Dequeue()), CancellationToken.None);
        Advance(time);

        Assert.Multiple(() =>
        {
            Assert.That(SpinWait.SpinUntil(() => follower.NotFound, TimeSpan.FromSeconds(10)), Is.True);
            Assert.That(SpinWait.SpinUntil(() => !follower.IsFollowing, TimeSpan.FromSeconds(10)), Is.True);
            Assert.That(follower.Status!.State, Is.EqualTo(LatticeOperationState.Running));
        });
    }

    [Test]
    public async Task A_failed_read_is_kept_backed_off_and_cleared_by_the_next_good_read()
    {
        var time = new ManualTimeProvider();
        using var follower = new OperationFollower(time);
        var fail = new InvalidOperationException("the cluster is unreachable");
        var reads = 0;

        await follower.StartAsync(_ =>
        {
            reads++;
            return reads == 2
                ? Task.FromException<LatticeOperationStatus?>(fail)
                : Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Running));
        }, CancellationToken.None);

        Advance(time);
        ReadsReach(() => reads, 2);
        Assert.Multiple(() =>
        {
            Assert.That(SpinWait.SpinUntil(() => follower.LastError is not null, TimeSpan.FromSeconds(10)), Is.True);
            Assert.That(follower.LastError, Is.SameAs(fail));
            Assert.That(follower.Status, Is.Not.Null, "the last good status survives a failed read");
            Assert.That(follower.IsFollowing, Is.True, "a failed read keeps following");
        });

        Advance(time);
        Assert.That(reads, Is.EqualTo(2), "after a failed read the follower waits twice as long");
        Advance(time);
        ReadsReach(() => reads, 3);
        Assert.That(SpinWait.SpinUntil(() => follower.LastError is null, TimeSpan.FromSeconds(10)), Is.True);
    }

    [Test]
    public async Task A_first_read_that_fails_still_follows()
    {
        var time = new ManualTimeProvider();
        using var follower = new OperationFollower(time);

        await follower.StartAsync(_ => Task.FromException<LatticeOperationStatus?>(new TimeoutException()), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(follower.LastError, Is.InstanceOf<TimeoutException>());
            Assert.That(follower.IsFollowing, Is.True);
        });
    }

    [Test]
    public async Task Refresh_reads_now_and_does_nothing_before_a_start()
    {
        var time = new ManualTimeProvider();
        using var follower = new OperationFollower(time);
        await follower.RefreshAsync(CancellationToken.None);
        var reads = 0;

        await follower.StartAsync(_ =>
        {
            reads++;
            return Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Running) with { CancelRequested = reads > 1 });
        }, CancellationToken.None);
        await follower.RefreshAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reads, Is.EqualTo(2));
            Assert.That(follower.Status!.CancelRequested, Is.True);
        });
    }

    [Test]
    public async Task Stopping_and_disposing_disarm_the_follow()
    {
        var time = new ManualTimeProvider();
        var follower = new OperationFollower(time);
        await follower.StartAsync(_ => Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Running)), CancellationToken.None);
        Assert.That(time.ArmedTimers, Is.EqualTo(1));

        follower.Stop();
        Assert.That(time.ArmedTimers, Is.Zero);

        await follower.StartAsync(_ => Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Running)), CancellationToken.None);
        follower.Dispose();
        await follower.RefreshAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(follower.IsFollowing, Is.False);
            Assert.That(time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public void Starting_needs_a_read()
    {
        using var follower = new OperationFollower(new ManualTimeProvider());

        Assert.That(() => follower.StartAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
    }

    private static void ReadsReach(Func<int> reads, int expected) =>
        Assert.That(SpinWait.SpinUntil(() => reads() == expected, TimeSpan.FromSeconds(10)), Is.True, $"the follower reads {expected} time(s)");

    private static void Advance(ManualTimeProvider time)
    {
        Assert.That(SpinWait.SpinUntil(() => time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "the follower re-arms");
        time.Advance(ClusterStatusPoller.Interval);
    }
}

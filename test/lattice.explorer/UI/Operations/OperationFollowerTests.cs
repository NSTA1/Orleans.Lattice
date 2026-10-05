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
            Interlocked.Increment(ref reads);
            return Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Succeeded));
        }, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Volatile.Read(ref reads), Is.EqualTo(1));
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
            // The follower reads on its own continuation, not on the test's
            // thread, so a plain ++ here can lose an increment and strand every
            // later barrier on a count the follower has already passed.
            Interlocked.Increment(ref reads);
            return Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(states.Dequeue()));
        }, CancellationToken.None);

        Assert.That(follower.IsFollowing, Is.True);
        Advance(time);
        ReadsReach(() => Volatile.Read(ref reads), 2);
        Advance(time);
        ReadsReach(() => Volatile.Read(ref reads), 3);

        Assert.Multiple(() =>
        {
            FollowBarriers.Reaches(() => !follower.IsFollowing, "a finished operation is no longer followed");
            Assert.That(follower.Status!.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(Volatile.Read(ref changes), Is.EqualTo(3), "every read raises Changed");
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
            FollowBarriers.Reaches(() => follower.NotFound, "an operation that is gone is reported not found");
            FollowBarriers.Reaches(() => !follower.IsFollowing, "an operation that is gone is no longer followed");
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
            var read = Interlocked.Increment(ref reads);
            return read == 2
                ? Task.FromException<LatticeOperationStatus?>(fail)
                : Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Running));
        }, CancellationToken.None);

        Advance(time);
        ReadsReach(() => Volatile.Read(ref reads), 2);
        Assert.Multiple(() =>
        {
            FollowBarriers.Reaches(() => follower.LastError is not null, "a failed read is recorded");
            Assert.That(follower.LastError, Is.SameAs(fail));
            Assert.That(follower.Status, Is.Not.Null, "the last good status survives a failed read");
            Assert.That(follower.IsFollowing, Is.True, "a failed read keeps following");
        });

        Advance(time);
        Assert.That(Volatile.Read(ref reads), Is.EqualTo(2), "after a failed read the follower waits twice as long");
        Advance(time);
        ReadsReach(() => Volatile.Read(ref reads), 3);
        FollowBarriers.Reaches(() => follower.LastError is null, "a good read clears the recorded failure");
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
            var read = Interlocked.Increment(ref reads);
            return Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Running) with { CancelRequested = read > 1 });
        }, CancellationToken.None);
        await follower.RefreshAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Volatile.Read(ref reads), Is.EqualTo(2));
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

    // A follow is replaced while its first read is still out (#4513): that read runs on the
    // caller's token, not the poller's, so nothing cancels it. Whatever it answers belongs
    // to the operation no longer followed and must not stand in for the current one's.
    [Test]
    public async Task A_late_answer_from_a_replaced_follow_does_not_overwrite_the_current_status()
    {
        var time = new ManualTimeProvider();
        using var follower = new OperationFollower(time);
        var earlier = new TaskCompletionSource<LatticeOperationStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var replaced = follower.StartAsync(_ => earlier.Task, CancellationToken.None);
        await follower.StartAsync(_ => Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Running)), CancellationToken.None);
        var changes = 0;
        follower.Changed += () => Interlocked.Increment(ref changes);

        earlier.SetResult(OperationTestStatus.Of(LatticeOperationState.Succeeded));
        await replaced;

        Assert.Multiple(() =>
        {
            Assert.That(follower.Status!.State, Is.EqualTo(LatticeOperationState.Running));
            Assert.That(follower.NotFound, Is.False);
            Assert.That(follower.IsFollowing, Is.True, "the current operation is still followed");
            Assert.That(time.ArmedTimers, Is.EqualTo(1), "only the current follow reads on");
            Assert.That(Volatile.Read(ref changes), Is.Zero, "a superseded read raises no change");
        });
    }

    [Test]
    public async Task A_late_miss_or_fault_from_a_replaced_follow_does_not_touch_the_current_status()
    {
        var time = new ManualTimeProvider();
        using var follower = new OperationFollower(time);
        var missing = new TaskCompletionSource<LatticeOperationStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failing = new TaskCompletionSource<LatticeOperationStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = follower.StartAsync(_ => missing.Task, CancellationToken.None);
        var second = follower.StartAsync(_ => failing.Task, CancellationToken.None);
        await follower.StartAsync(_ => Task.FromResult<LatticeOperationStatus?>(OperationTestStatus.Of(LatticeOperationState.Running)), CancellationToken.None);

        missing.SetResult(null);
        failing.SetException(new TimeoutException("the cluster did not answer"));
        await first;
        await second;

        Assert.Multiple(() =>
        {
            Assert.That(follower.Status!.State, Is.EqualTo(LatticeOperationState.Running));
            Assert.That(follower.NotFound, Is.False, "the replaced follow's miss is not the current operation's");
            Assert.That(follower.LastError, Is.Null, "the replaced follow's fault is not the current operation's");
        });
    }

    [Test]
    public async Task A_read_that_answers_after_disposal_changes_nothing()
    {
        var follower = new OperationFollower(new ManualTimeProvider());
        var late = new TaskCompletionSource<LatticeOperationStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = follower.StartAsync(_ => late.Task, CancellationToken.None);
        var changes = 0;
        follower.Changed += () => Interlocked.Increment(ref changes);

        follower.Dispose();
        late.SetResult(OperationTestStatus.Of(LatticeOperationState.Running));
        await start;

        Assert.Multiple(() =>
        {
            Assert.That(follower.Status, Is.Null);
            Assert.That(follower.IsFollowing, Is.False);
            Assert.That(Volatile.Read(ref changes), Is.Zero);
        });
    }

    private static void ReadsReach(Func<int> reads, int expected) =>
        FollowBarriers.ReadsReach(reads, expected);

    // The next wait is armed on the follow's continuation once the read returns;
    // FollowBarriers.Advance waits for it before moving the clock again.
    private static void Advance(ManualTimeProvider time) =>
        FollowBarriers.Advance(time, ClusterStatusPoller.Interval);
}

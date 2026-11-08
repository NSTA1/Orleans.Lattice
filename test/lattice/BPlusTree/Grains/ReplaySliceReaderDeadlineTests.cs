using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6007: the per-read deadline a starvation drive gives
/// <see cref="ReplaySliceReader"/>.
/// </summary>
[TestFixture]
public sealed class ReplaySliceReaderDeadlineTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromMilliseconds(150);

    /// <summary>
    /// A caller token far beyond <see cref="Deadline"/>, so a reader that never
    /// applied the deadline fails with a cancellation instead of hanging.
    /// </summary>
    private static CancellationTokenSource Guard() => new(TimeSpan.FromSeconds(10));

    private static (ReplaySliceReader Reader, ILeafReplayCoordinatorGrain Coordinator) Create(
        Func<CancellationToken, Task<IReadOnlyList<CommitLogSliceEntry>>> read, TimeSpan deadline)
    {
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => read(call.ArgAt<CancellationToken>(3)));
        var reader = new ReplaySliceReader(coordinator, "deadline-tree", 3, readDeadline: deadline);
        return (reader, coordinator);
    }

    [Test]
    public void A_read_that_honours_cancellation_is_given_up_at_the_deadline_with_the_typed_fault()
    {
        var (reader, _) = Create(
            async token =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return [];
            },
            Deadline);
        using var guard = Guard();

        var fault = Assert.ThrowsAsync<ReplaySliceReadDeadlineExceededException>(
            async () => await reader.ReadSliceAsync(41, 99, onNarrowed: null, guard.Token));

        Assert.Multiple(() =>
        {
            Assert.That(fault!.TreeId, Is.EqualTo("deadline-tree"));
            Assert.That(fault.Partition, Is.EqualTo(3));
            Assert.That(fault.FromExclusive, Is.EqualTo(41));
            Assert.That(fault.Deadline, Is.EqualTo(Deadline));
            Assert.That(fault, Is.Not.InstanceOf<OperationCanceledException>().And.Not.InstanceOf<TimeoutException>(),
                "no catch written for a cancellation or a timeout may absorb it on the way out of the replay");
        });
    }

    [Test]
    public void A_read_that_ignores_cancellation_is_still_given_up_at_the_deadline()
    {
        var never = new TaskCompletionSource<IReadOnlyList<CommitLogSliceEntry>>();
        var (reader, _) = Create(_ => never.Task, Deadline);
        using var guard = Guard();

        Assert.ThrowsAsync<ReplaySliceReadDeadlineExceededException>(
            async () => await reader.ReadSliceAsync(-1, 99, onNarrowed: null, guard.Token),
            "the caller-side WaitAsync belt must end the wait when storage ignores the token");
        Assert.That(never.Task.IsCompleted, Is.False);
    }

    [Test]
    public void A_cancelled_caller_sees_a_cancellation_not_the_deadline_fault()
    {
        var (reader, _) = Create(
            async token =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return [];
            },
            TimeSpan.FromMinutes(5));
        using var caller = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var fault = Assert.CatchAsync<OperationCanceledException>(
            async () => await reader.ReadSliceAsync(-1, 99, onNarrowed: null, caller.Token));
        Assert.That(fault, Is.Not.Null);
    }

    [Test]
    public async Task Without_a_deadline_the_read_receives_the_callers_own_token()
    {
        CancellationToken received = default;
        var (reader, _) = Create(
            token =>
            {
                received = token;
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([]);
            },
            default);
        using var caller = new CancellationTokenSource();

        await reader.ReadSliceAsync(-1, 99, onNarrowed: null, caller.Token);

        Assert.Multiple(() =>
        {
            Assert.That(reader.ReadDeadline, Is.EqualTo(Timeout.InfiniteTimeSpan));
            Assert.That(received, Is.EqualTo(caller.Token),
                "a reader without a deadline must pass its caller's token unchanged, as before");
        });
    }
}

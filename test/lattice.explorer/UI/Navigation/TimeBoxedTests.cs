using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>Deterministic deadline, caller-cancellation and late-fault outcomes for area calls.</summary>
[TestFixture]
public sealed class TimeBoxedTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task RunAsync_completed_work_returns_its_value(bool taskBacked)
    {
        var result = await TimeBoxed.RunAsync(
            _ => taskBacked ? new ValueTask<int>(Task.FromResult(42)) : ValueTask.FromResult(42),
            TimeSpan.FromSeconds(1), new ManualTimeProvider(), CancellationToken.None);

        Assert.That(result, Is.EqualTo((TimeBoxed.Outcome.Completed, 42, (Exception?)null)));
    }

    [Test]
    public async Task RunAsync_pending_work_completes_before_the_deadline_and_releases_its_timer()
    {
        var time = new ManualTimeProvider();
        var work = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken received = default;
        var pending = TimeBoxed.RunAsync(
            token => { received = token; return new ValueTask<int>(work.Task); },
            TimeSpan.FromSeconds(1), time, CancellationToken.None);

        Assert.That(pending.IsCompleted, Is.False);
        work.SetResult(42);
        var result = await pending.WaitAsync(TimeSpan.FromSeconds(5));
        time.Advance(TimeSpan.FromSeconds(1));

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo((TimeBoxed.Outcome.Completed, 42, (Exception?)null)));
            Assert.That(received.IsCancellationRequested, Is.False);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task RunAsync_faulted_work_preserves_the_original_error(bool asynchronous)
    {
        var error = new InvalidOperationException("directory unavailable");

        var result = await TimeBoxed.RunAsync<int>(
            _ => asynchronous ? ValueTask.FromException<int>(error) : throw error,
            TimeSpan.FromSeconds(1), new ManualTimeProvider(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(TimeBoxed.Outcome.Faulted));
            Assert.That(result.Value, Is.Zero);
            Assert.That(result.Error, Is.SameAs(error));
        });
    }

    [Test]
    public void RunAsync_already_cancelled_caller_never_invokes_the_work()
    {
        using var caller = new CancellationTokenSource();
        caller.Cancel();
        var calls = 0;

        Assert.ThrowsAsync<OperationCanceledException>(() => TimeBoxed.RunAsync(
            _ => { calls++; return ValueTask.FromResult(42); },
            TimeSpan.FromSeconds(1), new ManualTimeProvider(), caller.Token));
        Assert.That(calls, Is.Zero);
    }

    [TestCase(false)]
    [TestCase(true)]
    public void RunAsync_caller_cancellation_during_the_call_propagates(bool synchronousThrow)
    {
        using var caller = new CancellationTokenSource();

        var error = Assert.CatchAsync<OperationCanceledException>(() => TimeBoxed.RunAsync<int>(
            token =>
            {
                caller.Cancel();
                if (synchronousThrow)
                {
                    token.ThrowIfCancellationRequested();
                }

                return ValueTask.FromCanceled<int>(token);
            },
            TimeSpan.FromSeconds(1), new ManualTimeProvider(), caller.Token));

        Assert.That(error!.CancellationToken.IsCancellationRequested, Is.True);
    }

    [Test]
    public async Task RunAsync_deadline_cancelled_work_reports_timeout_not_a_fault()
    {
        var time = new ManualTimeProvider();
        using var caller = new CancellationTokenSource();

        var result = await TimeBoxed.RunAsync<int>(
            token =>
            {
                time.Advance(TimeSpan.FromSeconds(1));
                return ValueTask.FromCanceled<int>(token);
            },
            TimeSpan.FromSeconds(1), time, caller.Token);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo((TimeBoxed.Outcome.TimedOut, 0, (Exception?)null)));
            Assert.That(caller.IsCancellationRequested, Is.False);
        });
    }

    [Test]
    public async Task RunAsync_work_ignoring_the_deadline_stops_waiting_and_observes_a_late_fault()
    {
        var time = new ManualTimeProvider();
        var work = new TaskCompletionSource<int>();
        CancellationToken received = default;
        var pending = TimeBoxed.RunAsync(
            token => { received = token; return new ValueTask<int>(work.Task); },
            TimeSpan.FromSeconds(1), time, CancellationToken.None);

        Assert.That(pending.IsCompleted, Is.False);
        time.Advance(TimeSpan.FromSeconds(1));
        var result = await pending.WaitAsync(TimeSpan.FromSeconds(5));
        var lateError = new InvalidOperationException("late directory failure");
        work.SetException(lateError);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo((TimeBoxed.Outcome.TimedOut, 0, (Exception?)null)));
            Assert.That(received.IsCancellationRequested, Is.True);
            Assert.That(work.Task.IsFaulted, Is.True);
        });
    }

    [Test]
    public async Task RunAsync_work_ignoring_caller_cancellation_stops_waiting_and_propagates_it()
    {
        var work = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var caller = new CancellationTokenSource();
        var pending = TimeBoxed.RunAsync(_ => new ValueTask<int>(work.Task),
            TimeSpan.FromSeconds(1), new ManualTimeProvider(), caller.Token);

        caller.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await pending.WaitAsync(TimeSpan.FromSeconds(5)));
        work.SetResult(42);
        Assert.That(await work.Task, Is.EqualTo(42));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task RunAsync_unrequested_cancellation_is_a_fault(bool asynchronous)
    {
        var error = new OperationCanceledException("remote cancellation");

        var result = await TimeBoxed.RunAsync<int>(
            _ => asynchronous ? ValueTask.FromException<int>(error) : throw error,
            TimeSpan.FromSeconds(1), new ManualTimeProvider(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(TimeBoxed.Outcome.Faulted));
            Assert.That(result.Error, Is.SameAs(error));
        });
    }
}

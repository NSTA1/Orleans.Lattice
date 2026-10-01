using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Testing;

/// <summary>
/// Unit coverage for <see cref="InterleaveProbe"/>, the shared deterministic
/// proof that a call answers while a test-held turn is still held. Each failure
/// branch is pinned as well as the pass, because a probe that passed a call
/// which queued behind the hold would certify the very defect it exists to catch.
/// </summary>
[TestFixture]
public sealed class InterleaveProbeTests
{
    /// <summary>Used only where the call is meant to hang, so the bound is the expected outcome.</summary>
    private static readonly TimeSpan ShortHang = TimeSpan.FromMilliseconds(200);

    /// <summary>Never reached by a call that answers, so it can never decide a passing case.</summary>
    private static readonly TimeSpan LongHang = TimeSpan.FromMinutes(10);

    [Test]
    public async Task AnswersWhileHeldAsync_returns_the_result_of_a_call_that_answers_while_held()
    {
        var hold = new TaskCompletionSource();
        var answer = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var probe = InterleaveProbe.AnswersWhileHeldAsync(answer.Task, hold.Task, "the read", LongHang);
        answer.SetResult(42);

        Assert.Multiple(async () =>
        {
            Assert.That(await probe, Is.EqualTo(42));
            Assert.That(hold.Task.IsCompleted, Is.False, "the probe never releases the hold itself");
        });
    }

    [Test]
    public void AnswersWhileHeldAsync_does_not_wait_out_the_hang_bound_for_a_call_that_already_answered()
    {
        var probe = InterleaveProbe.AnswersWhileHeldAsync(
            Task.CompletedTask, new TaskCompletionSource().Task, "the read", TimeSpan.FromMinutes(10));

        Assert.That(probe.IsCompletedSuccessfully, Is.True,
            "an answered call must pass without arming a wait on the hang bound");
    }

    [Test]
    public void AnswersWhileHeldAsync_fails_a_call_that_never_answers_while_held_and_names_it()
    {
        var call = new TaskCompletionSource().Task;

        var failure = CaptureFailure(() =>
            InterleaveProbe.AnswersWhileHeldAsync(call, new TaskCompletionSource().Task, "the status read", ShortHang));

        Assert.That(failure, Does.Contain("the status read did not answer while the hold was in force"));
    }

    [Test]
    public void AnswersWhileHeldAsync_fails_when_the_hold_is_released_before_the_call_answers()
    {
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var call = new TaskCompletionSource().Task;

        var failure = CaptureFailure(async () =>
        {
            var probe = InterleaveProbe.AnswersWhileHeldAsync(call, hold.Task, "the read", TimeSpan.FromMinutes(10));
            hold.SetResult();
            await probe;
        });

        Assert.That(failure, Does.Contain("the read did not answer before the hold ended"));
    }

    [Test]
    public void AnswersWhileHeldAsync_fails_a_call_that_answers_just_after_the_hold_ends_on_its_own()
    {
        // Synchronous continuations make the order deterministic: the hold ends,
        // then the queued call answers, before the probe looks at either.
        var hold = new TaskCompletionSource();
        var answer = new TaskCompletionSource<int>();

        var failure = CaptureFailure(async () =>
        {
            var probe = InterleaveProbe.AnswersWhileHeldAsync(answer.Task, hold.Task, "the read", TimeSpan.FromMinutes(10));
            hold.SetResult();
            answer.SetResult(1);
            await probe;
        });

        Assert.That(failure, Does.Contain("the read did not answer before the hold ended"));
    }

    [Test]
    public void AnswersWhileHeldAsync_fails_when_the_hold_was_released_before_the_probe()
    {
        var failure = CaptureFailure(() => InterleaveProbe.AnswersWhileHeldAsync(
            new TaskCompletionSource().Task, Task.CompletedTask, "the read", TimeSpan.FromMinutes(10)));

        Assert.That(failure, Does.Contain("the hold was already released when the read was probed"));
    }

    [Test]
    public void AnswersWhileHeldAsync_names_a_call_that_timed_out_while_held_as_queued_behind_the_hold()
    {
        var call = Task.FromException<int>(new TimeoutException("Response did not arrive on time"));

        var failure = CaptureFailure(() =>
            InterleaveProbe.AnswersWhileHeldAsync(call, new TaskCompletionSource().Task, "the read", LongHang));

        Assert.That(failure, Does.Contain("the read did not answer while the hold was in force")
            .And.Contain("Response did not arrive on time"));
    }

    [Test]
    public void AnswersWhileHeldAsync_surfaces_the_call_s_own_fault()
    {
        var call = Task.FromException<int>(new InvalidOperationException("boom"));

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            InterleaveProbe.AnswersWhileHeldAsync(call, new TaskCompletionSource().Task, "the read", LongHang));
    }

    [Test]
    public void AnswersWhileHeldAsync_rejects_null_arguments()
    {
        var held = new TaskCompletionSource().Task;
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => InterleaveProbe.AnswersWhileHeldAsync(null!, held, "x"));
            Assert.ThrowsAsync<ArgumentNullException>(() =>
                InterleaveProbe.AnswersWhileHeldAsync(Task.CompletedTask, null!, "x"));
            Assert.ThrowsAsync<ArgumentNullException>(() =>
                InterleaveProbe.AnswersWhileHeldAsync(Task.FromResult(1), held, null!));
        });
    }

    [Test]
    public void HangBound_is_far_above_a_loaded_runner_and_below_the_ci_hang_blame()
    {
        Assert.That(InterleaveProbe.HangBound,
            Is.GreaterThanOrEqualTo(TimeSpan.FromMinutes(1)).And.LessThan(TimeSpan.FromMinutes(10)),
            "the bound must never decide a correct call on a slow runner, and must fail a hang before CI's "
            + "ten-minute blame aborts the test host");
    }

    private static string CaptureFailure(Func<Task> probe)
    {
        var ex = Assert.CatchAsync<AssertionException>(() => probe());
        return ex!.Message;
    }
}

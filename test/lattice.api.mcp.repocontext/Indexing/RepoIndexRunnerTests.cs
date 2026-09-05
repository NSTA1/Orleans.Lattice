using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for <see cref="RepoIndexRunner"/>: the background pass owner that keeps
/// an indexing run alive independently of the client call that triggered it.
/// <para>
/// The invariants under test are the ones that only appear when a run is genuinely in
/// flight - single-flight admission, cancellation, the drain that
/// <c>CancelAndWaitAsync</c> promises, and the run-credential stamp that lets a
/// reminder-driven resume write under the same subject as the original pass - plus the
/// non-fatal reporting arms that must never let an advisory progress callback or a
/// failed failure-report take the run (or the host) down with it.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoIndexRunnerTests
{
    [Test]
    public void StartIndexAsync_and_GetProgressAsync_delegate_to_the_job_grain()
    {
        using var harness = new RepoIndexRunnerHarness();
        var progress = new RepoIndexProgress
        {
            RepoId = RepoIndexRunnerHarness.RepoId,
            Status = RepoIndexStatus.Running,
            Phase = RepoIndexPhase.Walking,
        };
        harness.Job.StartAsync(Arg.Any<RepoIndexJobRequest>()).Returns(Task.FromResult(progress));
        harness.Job.GetProgressAsync().Returns(Task.FromResult(progress));
        var runner = harness.CreateRunner();

        Assert.Multiple(async () =>
        {
            Assert.That(await runner.StartIndexAsync(harness.Request()), Is.EqualTo(progress));
            Assert.That(await runner.GetProgressAsync(RepoIndexRunnerHarness.RepoId), Is.EqualTo(progress));
        });
    }

    [Test]
    public void The_public_surface_rejects_null_arguments()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await runner.StartIndexAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await runner.GetProgressAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => runner.Enqueue(null!), Throws.ArgumentNullException);
            Assert.That(() => runner.Cancel(null!), Throws.ArgumentNullException);
            Assert.That(async () => await runner.CancelAndWaitAsync(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Enqueue_runs_the_pass_and_settles_the_job_grain_on_success()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();

        runner.Enqueue(harness.Request());
        harness.Release();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => harness.Job.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.CompleteAsync))),
            Is.True,
            "A pass that ran to the end must settle the durable job.");
    }

    [Test]
    public async Task Enqueue_is_single_flight_per_repository()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();
        var request = harness.Request();

        runner.Enqueue(request);
        // The first run is parked on the gate, so the second enqueue lands while it is
        // genuinely in flight - which is the only state single-flight is about.
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => harness.GrainFactory.ReceivedCalls().Any()),
            Is.True);
        runner.Enqueue(request);
        harness.Release();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => harness.Job.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.CompleteAsync)) == 1),
            Is.True);
        Assert.That(
            harness.Job.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.CompleteAsync)),
            Is.EqualTo(1),
            "A duplicate enqueue for a live repository is a no-op, not a second concurrent pass.");
    }

    [Test]
    public async Task Enqueue_stamps_the_authority_credential_onto_the_pass()
    {
        using var harness = new RepoIndexRunnerHarness();
        var credential = new LatticeCredential("token", "Bearer", "indexer");
        harness.RunAuthority.Resolve().Returns(credential);
        var runner = harness.CreateRunner();

        runner.Enqueue(harness.Request());
        harness.Release();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => harness.ObservedCredential is not null),
            Is.True);
        Assert.That(harness.ObservedCredential, Is.EqualTo(credential),
            "A reminder-driven resume carries no ambient credential, so the run must assume the authority's.");
    }

    [Test]
    public async Task Enqueue_leaves_the_ambient_credential_untouched_when_the_authority_resolves_none()
    {
        using var harness = new RepoIndexRunnerHarness();
        harness.RunAuthority.Resolve().Returns((LatticeCredential?)null);
        var runner = harness.CreateRunner();

        runner.Enqueue(harness.Request());
        harness.Release();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => harness.Job.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.CompleteAsync))),
            Is.True);
        Assert.That(harness.ObservedCredential, Is.Null);
    }

    [Test]
    public async Task A_failed_pass_is_reported_to_the_job_grain_as_a_described_failure()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();

        runner.Enqueue(harness.Request());
        harness.Fault(new InvalidOperationException("the structural tree is unreachable"));

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => harness.Job.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.FailAsync))),
            Is.True);
        await harness.Job.Received(1).FailAsync(
            "InvalidOperationException: the structural tree is unreachable");
    }

    [Test]
    public async Task A_failure_report_that_itself_fails_does_not_escape_the_run()
    {
        using var harness = new RepoIndexRunnerHarness();
        harness.Job.FailAsync(Arg.Any<string>()).Throws(new TimeoutException("the job grain is unreachable too"));
        var runner = harness.CreateRunner();

        runner.Enqueue(harness.Request());
        harness.Fault(new InvalidOperationException("the structural tree is unreachable"));

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => harness.Job.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.FailAsync))),
            Is.True);
        // The second fault must not escape the run: it still has to reach its finally,
        // deregister, and release any drainer. If it escaped, the handle would stay
        // mapped and this would keep reporting a live run.
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => !runner.Cancel(RepoIndexRunnerHarness.RepoId)),
            Is.True,
            "A failed failure-report must still let the run deregister.");
        await runner.CancelAndWaitAsync(RepoIndexRunnerHarness.RepoId);
    }

    [Test]
    public async Task A_progress_report_that_fails_is_swallowed_so_the_pass_still_settles()
    {
        using var harness = new RepoIndexRunnerHarness();
        harness.Job.ReportProgressAsync(Arg.Any<RepoIndexProgressUpdate>())
            .Throws(new TimeoutException("a transient progress-report failure"));
        var runner = harness.CreateRunner();

        runner.Enqueue(harness.Request());
        harness.Release();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => harness.Job.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.CompleteAsync))),
            Is.True,
            "Advisory progress must never fail the durable run.");
        await harness.Job.DidNotReceive().FailAsync(Arg.Any<string>());
    }

    [Test]
    public async Task A_cancelled_progress_report_stops_the_pass_without_settling_the_job()
    {
        using var harness = new RepoIndexRunnerHarness();
        harness.Job.ReportProgressAsync(Arg.Any<RepoIndexProgressUpdate>())
            .Throws(new OperationCanceledException());
        var runner = harness.CreateRunner();

        runner.Enqueue(harness.Request());
        harness.Release();

        // Wait for the report to have been genuinely attempted, so this asserts the
        // cancellation arm rather than racing the pass to an unrelated cancellation.
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => harness.Job.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.ReportProgressAsync))),
            Is.True);
        // A cancellation is a shutdown or a removal, not a defect: the run must leave
        // the grain alone so the resume reminder can restart it on the next activation.
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => !runner.Cancel(RepoIndexRunnerHarness.RepoId)),
            Is.True);
        Assert.Multiple(async () =>
        {
            await harness.Job.DidNotReceive().FailAsync(Arg.Any<string>());
            await harness.Job.DidNotReceive().CompleteAsync(Arg.Any<RepoIndexProgressUpdate>(), Arg.Any<long>());
        });
    }

    [Test]
    public async Task Cancel_stops_a_live_run_and_leaves_the_job_unsettled()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();
        runner.Enqueue(harness.Request());
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => harness.GrainFactory.ReceivedCalls().Any()),
            Is.True);

        var cancelled = runner.Cancel(RepoIndexRunnerHarness.RepoId);

        Assert.That(cancelled, Is.True, "Cancel reports whether it found a run to stop.");
        await runner.CancelAndWaitAsync(RepoIndexRunnerHarness.RepoId);
        Assert.Multiple(async () =>
        {
            await harness.Job.DidNotReceive().CompleteAsync(Arg.Any<RepoIndexProgressUpdate>(), Arg.Any<long>());
            await harness.Job.DidNotReceive().FailAsync(Arg.Any<string>());
        });
    }

    [Test]
    public void Cancel_reports_false_when_no_run_is_in_flight()
    {
        using var harness = new RepoIndexRunnerHarness();

        Assert.That(harness.CreateRunner().Cancel(RepoIndexRunnerHarness.RepoId), Is.False);
    }

    [Test]
    public async Task CancelAndWaitAsync_returns_immediately_when_no_run_is_in_flight()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();

        var task = runner.CancelAndWaitAsync(RepoIndexRunnerHarness.RepoId);

        Assert.That(task.IsCompletedSuccessfully, Is.True,
            "With no run in flight, CancelAndWaitAsync should return synchronously.");
        await task;
        Assert.That(runner.Cancel(RepoIndexRunnerHarness.RepoId), Is.False,
            "No live run should be registered after a no-op drain.");
    }

    [Test]
    public async Task CancelAndWaitAsync_drains_the_run_before_returning()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();
        runner.Enqueue(harness.Request());
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => harness.GrainFactory.ReceivedCalls().Any()),
            Is.True);

        await runner.CancelAndWaitAsync(RepoIndexRunnerHarness.RepoId);

        // Once the drain returns, the run has reached its finally: a fresh enqueue is
        // admitted, which is only possible when the previous handle was removed.
        Assert.That(runner.Cancel(RepoIndexRunnerHarness.RepoId), Is.False,
            "The drained run must no longer be registered as live.");
    }

    [Test]
    public async Task Host_shutdown_cancels_a_live_run_without_settling_the_job()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();
        runner.Enqueue(harness.Request());
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => harness.GrainFactory.ReceivedCalls().Any()),
            Is.True);

        harness.StopApplication();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => !runner.Cancel(RepoIndexRunnerHarness.RepoId)),
            Is.True,
            "The stopping token is linked into every run, so shutdown terminates the pass.");
        await harness.Job.DidNotReceive().CompleteAsync(Arg.Any<RepoIndexProgressUpdate>(), Arg.Any<long>());
    }

    [Test]
    public async Task Cancel_tolerates_a_run_that_finished_between_the_lookup_and_the_cancel()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();
        runner.Enqueue(harness.Request());
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => harness.GrainFactory.ReceivedCalls().Any()),
            Is.True);
        DisposeLiveRunSource(runner);

        // Disposing the source reproduces the lost race the catch arm exists for: the
        // handle is still mapped, so the lookup succeeds, but cancelling it throws.
        Assert.That(runner.Cancel(RepoIndexRunnerHarness.RepoId), Is.True);
        harness.Release();
    }

    [Test]
    public async Task CancelAndWaitAsync_tolerates_a_run_that_finished_between_the_lookup_and_the_cancel()
    {
        using var harness = new RepoIndexRunnerHarness();
        var runner = harness.CreateRunner();
        runner.Enqueue(harness.Request());
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => harness.GrainFactory.ReceivedCalls().Any()),
            Is.True);
        DisposeLiveRunSource(runner);

        // The drain must still complete: the run's writes are already done, so an
        // ObjectDisposedException here can never be allowed to hang a caller.
        harness.Release();
        await runner.CancelAndWaitAsync(RepoIndexRunnerHarness.RepoId);

        Assert.That(runner.Cancel(RepoIndexRunnerHarness.RepoId), Is.False,
            "The drain must wait until the lost-race run has deregistered.");
    }

    /// <summary>
    /// Disposes the live run's cancellation source while its handle is still mapped,
    /// which is the only way to reach the lost-race arms deterministically: the real
    /// race window is the two instructions between the map lookup and the cancel.
    /// Test-only reflection over the runner's own field; no production code changes.
    /// </summary>
    /// <param name="runner">The runner whose live run to dispose.</param>
    private static void DisposeLiveRunSource(RepoIndexRunner runner)
    {
        var runsField = typeof(RepoIndexRunner).GetField(
            "_runs",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var runs = runsField.GetValue(runner)!;
        var handle = ((System.Collections.IEnumerable)runs).Cast<object>().Single();
        var value = handle.GetType().GetProperty("Value")!.GetValue(handle)!;
        var cts = (CancellationTokenSource)value.GetType().GetProperty("Cts")!.GetValue(value)!;
        cts.Dispose();
    }
}

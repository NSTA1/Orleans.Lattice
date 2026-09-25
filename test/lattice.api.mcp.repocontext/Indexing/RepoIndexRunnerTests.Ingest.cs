using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// The runner's ingest accounting (issue #3151): every pass it runs is settled into
/// exactly one <c>repocontext.ingest.passes</c> outcome arm, and the pass's progress
/// reaches <c>/metrics</c> even when the job grain drops the report. These are the
/// production-call-site tests: each drives a real pass through
/// <see cref="RepoIndexRunner"/> rather than calling the reporter directly.
/// </summary>
public sealed partial class RepoIndexRunnerTests
{
    [Test]
    public async Task Enqueue_a_completed_pass_counts_completed_and_reports_what_it_scanned()
    {
        using var harness = new RepoIndexRunnerHarness();
        harness.WriteFile("readme.txt", "hello");
        using var reporter = new RepoContextIngestReporter();
        using var capture = new IngestMeasurementCapture(reporter);
        var runner = harness.CreateRunner(pacer: null, reporter);

        runner.Enqueue(harness.Request());
        harness.Release();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => capture.Sum(RepoContextIngestReporter.PassesInstrumentName) > 0),
            Is.True,
            "A pass that ran to the end must be counted.");

        Assert.Multiple(() =>
        {
            Assert.That(
                capture.Sum(RepoContextIngestReporter.PassesInstrumentName, outcome: RepoContextIngestReporter.PassCompletedTag),
                Is.EqualTo(1));
            Assert.That(capture.Sum(RepoContextIngestReporter.PassesInstrumentName), Is.EqualTo(1));
            Assert.That(
                capture.Sum(RepoContextIngestReporter.FilesScannedInstrumentName),
                Is.EqualTo(1),
                "The single file in the working tree must be counted as scanned, exactly once.");
            Assert.That(capture.Count(RepoContextIngestReporter.PassDurationInstrumentName), Is.EqualTo(1));
            Assert.That(
                capture.Count(RepoContextIngestReporter.FilesEmbeddedInstrumentName),
                Is.GreaterThanOrEqualTo(1),
                "files_embedded must be present (primed) even when the pass embedded nothing.");
        });
    }

    [Test]
    public async Task Enqueue_a_faulted_pass_counts_failed_and_not_completed()
    {
        using var harness = new RepoIndexRunnerHarness();
        using var reporter = new RepoContextIngestReporter();
        using var capture = new IngestMeasurementCapture(reporter);
        var runner = harness.CreateRunner(pacer: null, reporter);

        runner.Enqueue(harness.Request());
        harness.Fault(new InvalidOperationException("store refused the stream"));

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => capture.Sum(RepoContextIngestReporter.PassesInstrumentName) > 0),
            Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(
                capture.Sum(RepoContextIngestReporter.PassesInstrumentName, outcome: RepoContextIngestReporter.PassFailedTag),
                Is.EqualTo(1));
            Assert.That(capture.Sum(RepoContextIngestReporter.PassesInstrumentName), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Enqueue_a_pass_whose_completion_report_fails_counts_failed_not_completed()
    {
        // index_status reports such a pass Failed, so the metric must agree with it.
        using var harness = new RepoIndexRunnerHarness();
        harness.Job.CompleteAsync(Arg.Any<RepoIndexProgressUpdate>(), Arg.Any<long>())
            .ThrowsAsync(new InvalidOperationException("grain unavailable"));
        using var reporter = new RepoContextIngestReporter();
        using var capture = new IngestMeasurementCapture(reporter);
        var runner = harness.CreateRunner(pacer: null, reporter);

        runner.Enqueue(harness.Request());
        harness.Release();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => capture.Sum(RepoContextIngestReporter.PassesInstrumentName) > 0),
            Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(
                capture.Sum(RepoContextIngestReporter.PassesInstrumentName, outcome: RepoContextIngestReporter.PassFailedTag),
                Is.EqualTo(1));
            Assert.That(
                capture.Sum(RepoContextIngestReporter.PassesInstrumentName, outcome: RepoContextIngestReporter.PassCompletedTag),
                Is.Zero);
        });
    }

    [Test]
    public async Task Enqueue_a_pass_cancelled_by_shutdown_counts_cancelled_not_failed()
    {
        using var harness = new RepoIndexRunnerHarness();
        using var reporter = new RepoContextIngestReporter();
        using var capture = new IngestMeasurementCapture(reporter);
        var runner = harness.CreateRunner(pacer: null, reporter);

        runner.Enqueue(harness.Request());
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(() => harness.StructuralTree.ReceivedCalls().Any()),
            Is.True);
        harness.StopApplication();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => capture.Sum(RepoContextIngestReporter.PassesInstrumentName) > 0),
            Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(
                capture.Sum(RepoContextIngestReporter.PassesInstrumentName, outcome: RepoContextIngestReporter.PassCancelledTag),
                Is.EqualTo(1));
            Assert.That(capture.Sum(RepoContextIngestReporter.PassesInstrumentName), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Enqueue_progress_the_job_grain_drops_still_reaches_the_ingest_instruments()
    {
        using var harness = new RepoIndexRunnerHarness();
        harness.WriteFile("a.txt", "a");
        harness.WriteFile("b.txt", "b");
        harness.Job.ReportProgressAsync(Arg.Any<RepoIndexProgressUpdate>())
            .ThrowsAsync(new InvalidOperationException("transient report failure"));

        // Hold the pass on its durable completion, which follows the walk: while it is
        // held, the scan count can only have arrived through the progress sink, never
        // through the result the runner records once completion returns.
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Job.CompleteAsync(Arg.Any<RepoIndexProgressUpdate>(), Arg.Any<long>())
            .Returns(_ => completion.Task);
        using var reporter = new RepoContextIngestReporter();
        using var capture = new IngestMeasurementCapture(reporter);
        var runner = harness.CreateRunner(pacer: null, reporter);

        runner.Enqueue(harness.Request());
        harness.Release();

        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => harness.Job.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.CompleteAsync))),
            Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(
                capture.Sum(RepoContextIngestReporter.FilesScannedInstrumentName),
                Is.EqualTo(2),
                "A dropped progress report must still advance files_scanned while the pass is in flight.");
            Assert.That(capture.Sum(RepoContextIngestReporter.PassesInstrumentName), Is.Zero);
        });

        completion.SetResult();
        Assert.That(
            await RepoIndexRunnerHarness.WaitForAsync(
                () => capture.Sum(RepoContextIngestReporter.PassesInstrumentName) > 0),
            Is.True);
        Assert.That(capture.Sum(RepoContextIngestReporter.FilesScannedInstrumentName), Is.EqualTo(2));
    }
}

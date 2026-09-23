using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// The live pacing overlay <see cref="RepoIndexRunner.GetProgressAsync"/> attaches
/// to <c>index_status</c> (issue #3447): it is read from the silo-local pacer at
/// query time, and only for a job that is actually running.
/// </summary>
public sealed partial class RepoIndexRunnerTests
{
    private static RepoContextIndexingPacer Pacer() => new(
        new RepoContextIndexingOptions(),
        TimeProvider.System,
        NullLogger<RepoContextIndexingPacer>.Instance,
        memoryLoad: () => 0.1);

    private static RepoIndexProgress ProgressIn(RepoIndexStatus status) => new()
    {
        RepoId = RepoIndexRunnerHarness.RepoId,
        Status = status,
        Phase = RepoIndexPhase.Vectorising,
    };

    [Test]
    public async Task GetProgressAsync_running_job_with_a_pacer_carries_the_live_snapshot()
    {
        using var harness = new RepoIndexRunnerHarness();
        harness.Job.GetProgressAsync().Returns(Task.FromResult(ProgressIn(RepoIndexStatus.Running)));
        var pacer = Pacer();
        using var lease = pacer.EnterForeground();

        var progress = await harness.CreateRunner(pacer).GetProgressAsync(RepoIndexRunnerHarness.RepoId);

        Assert.That(progress.Pacing, Is.Not.Null, "A running job reports how it is being paced.");
        Assert.Multiple(() =>
        {
            Assert.That(progress.Pacing!.State, Is.EqualTo(RepoIndexPaceState.Idle));
            Assert.That(progress.Pacing.ForegroundRequests, Is.EqualTo(1),
                "The snapshot is read live at query time, so an open foreground lease shows.");
        });
    }

    [Test]
    [TestCase(RepoIndexStatus.Completed)]
    [TestCase(RepoIndexStatus.Failed)]
    public async Task GetProgressAsync_finished_job_carries_no_pacing(RepoIndexStatus status)
    {
        using var harness = new RepoIndexRunnerHarness();
        harness.Job.GetProgressAsync().Returns(Task.FromResult(ProgressIn(status)));

        var progress = await harness.CreateRunner(Pacer()).GetProgressAsync(RepoIndexRunnerHarness.RepoId);

        Assert.That(progress.Pacing, Is.Null, "A job that is not running is not being paced.");
    }

    [Test]
    public async Task GetProgressAsync_no_pacer_registered_leaves_the_progress_unchanged()
    {
        using var harness = new RepoIndexRunnerHarness();
        var stored = ProgressIn(RepoIndexStatus.Running);
        harness.Job.GetProgressAsync().Returns(Task.FromResult(stored));

        var progress = await harness.CreateRunner(pacer: null).GetProgressAsync(RepoIndexRunnerHarness.RepoId);

        Assert.That(progress, Is.EqualTo(stored));
    }
}

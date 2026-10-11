using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

public sealed partial class RepoContextBootstrapServicePassTests
{
    [Test]
    public async Task A_resumed_file_drive_adds_work_to_the_persisted_job_and_not_just_its_high_water_mark()
    {
        _harness.WriteFile("a.cs", "class A { }");
        var jobHarness = new RepoIndexJobGrainHarness();
        var job = jobHarness.CreateGrain();
        await job.StartAsync(RepoIndexJobGrainHarness.Request());
        await job.ReportProgressAsync(new RepoIndexProgressUpdate { FilesEmbedded = 60 });
        var sink = Substitute.For<IRepoIndexProgressSink>();
        sink.ReportAsync(Arg.Any<RepoIndexProgressUpdate>(), Arg.Any<CancellationToken>())
            .Returns(call => new ValueTask(job.ReportProgressAsync(call.ArgAt<RepoIndexProgressUpdate>(0))));
        var observedInsideArm = 0;
        _harness.OnIngest = async (report, ct) =>
        {
            await report(26, ct);
            await report(26, ct);
            observedInsideArm = (await job.GetProgressAsync()).FilesEmbedded;
            return 26;
        };

        await _harness.Service.RunAsync(_harness.Request(), sink);

        Assert.Multiple(() =>
        {
            Assert.That(_harness.IngestCalls, Is.EqualTo(1));
            Assert.That(observedInsideArm, Is.EqualTo(86), "Work must advance while the arm is running.");
            Assert.That(jobHarness.State.State.FilesEmbedded, Is.EqualTo(86),
                "Neither a repeated callback nor the final tally may charge the same work twice.");
        });
    }

    [Test]
    public async Task A_file_arm_fault_preserves_the_progress_reported_before_the_fault()
    {
        _harness.WriteFile("a.cs", "class A { }");
        _harness.OnIngest = async (report, ct) =>
        {
            await report(60, ct);
            throw new InvalidOperationException("file arm interrupted");
        };

        Assert.That(
            async () => await _harness.Service.RunAsync(_harness.Request(), _harness.Progress),
            Throws.InvalidOperationException.With.Message.EqualTo("file arm interrupted"));

        var reported = _harness.ProgressUpdates
            .Where(update => update.FilesEmbedded is not null)
            .Select(update => update.FilesEmbedded!.Value)
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(_harness.IngestCalls, Is.EqualTo(1), "The production file arm must have run.");
            Assert.That(reported, Is.Not.Empty);
            Assert.That(reported, Is.All.EqualTo(60),
                "The failed arm's default outcome must not reset already-persisted progress.");
        });
    }
}

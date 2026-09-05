using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for the git-sourced refresh loop on <see cref="RepoContextSelfIndexGrain"/>.
/// The grain is the reminder-driven singleton that owns a repository's index passes, so
/// it is where "fetch the ref, and only run when it moved" has to hold: a ref that has
/// not moved must start nothing, a fetch that fails must leave the last-good index
/// serving, and a run that did start must carry the resolved commit SHA and the staged
/// work tree rather than the mounted path it was seeded with.
/// </summary>
[TestFixture]
public sealed class RepoContextSelfIndexGrainGitSourceTests
{
    private const string RepoId = "acme";
    private const string FirstSha = "1111111111111111111111111111111111111111";
    private const string SecondSha = "2222222222222222222222222222222222222222";

    // Already rooted, so the registry's path normalisation is a no-op and the test's
    // expectation matches what the source hands the runner byte for byte.
    private static readonly string StagingRoot =
        Path.Combine(Path.GetTempPath(), "lattice-repocontext-git-grain-tests");

    private static readonly string MountedRoot = Path.Combine(Path.GetTempPath(), "lattice-mounted-placeholder");

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static RepoIndexJobRequest Request() => new()
    {
        RepoRoot = MountedRoot,
        RepoId = RepoId,
    };

    private static RepoContextGitSourceRegistry Registry() =>
        new(
            [
                new RepoContextGitSourceOptions
                {
                    RepoId = RepoId,
                    RemoteUrl = "https://git.example/acme.git",
                    RefreshInterval = TimeSpan.FromMinutes(11),
                }
            ],
            StagingRoot);

    private static RepoContextSelfIndexGrain CreateGrain(
        RepoContextIndexSourceGate gate,
        IRepoIndexRunner runner,
        IRepoIndexJobGrain job)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("repoContextSelfIndex", RepoId));

        // A hub arms the scan timer, which resolves ITimerRegistry from the
        // activation's service provider.
        context.ActivationServices
            .GetService(typeof(ITimerRegistry))
            .Returns(Substitute.For<ITimerRegistry>());

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IRepoIndexJobGrain>(RepoId, Arg.Any<string?>()).Returns(job);

        var options = new RepoContextIndexingOptions { Role = RepoContextIndexingRole.Hub };
        var replication = Substitute.For<ILatticeReplicationContext>();
        var cache = new RepoContextVectorCache(TimeProvider.System, options);
        var writer = new RepoContextVectorWriter(
            grainFactory, Serializer, replication, cache, RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory));

        return new RepoContextSelfIndexGrain(
            context,
            grainFactory,
            Substitute.For<IReminderRegistry>(),
            runner,
            new RepoContextEmbeddingGapScanner(grainFactory, writer),
            Substitute.For<IRepoIndexRunAuthority>(),
            TimeProvider.System,
            options,
            new RepoContextAnnIndexScheduler(
                grainFactory, options, NullLogger<RepoContextAnnIndexScheduler>.Instance),
            gate,
            NullLogger<RepoContextSelfIndexGrain>.Instance,
            new FakeSelfIndexState());
    }

    private static IRepoIndexJobGrain Job(RepoIndexStatus status, string? persistedCommitSha)
    {
        var job = Substitute.For<IRepoIndexJobGrain>();
        job.GetProgressAsync().Returns(
            new RepoIndexProgress { RepoId = RepoId, Status = status, Phase = RepoIndexPhase.Pending });
        job.GetRequestAsync().Returns(
            persistedCommitSha is null
                ? (RepoIndexJobRequest?)null
                : Request() with { CommitSha = persistedCommitSha });
        return job;
    }

    [Test]
    public async Task EnsureRunningAsync_stages_the_resolved_commit_and_runs_from_the_work_tree()
    {
        var fetcher = new StubGitFetcher(StubGitFetcher.Staged(FirstSha));
        var gate = RepoContextSourceTestDoubles.Gate(
            Registry(), RepoContextSourceTestDoubles.CredentialsFor(RepoId), fetcher);
        var runner = Substitute.For<IRepoIndexRunner>();
        var grain = CreateGrain(gate, runner, Job(RepoIndexStatus.None, persistedCommitSha: null));

        await grain.EnsureRunningAsync(Request());

        var started = runner.ReceivedCalls()
            .Single(c => c.GetMethodInfo().Name == nameof(IRepoIndexRunner.StartIndexAsync))
            .GetArguments()[0] as RepoIndexJobRequest;

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.Not.Null);
            Assert.That(started!.CommitSha, Is.EqualTo(FirstSha),
                "Every generation is stamped with the commit it was built from.");
            Assert.That(started.RepoRoot, Is.EqualTo(GitRemoteSource.WorkTreePath(StagingRoot, RepoId)),
                "A git-sourced run indexes the staged work tree, never the seeded mount path.");
            Assert.That(started.AllowPrune, Is.False,
                "Deletion comes from the commit diff, never from absence-on-disk pruning.");
        });
    }

    [Test]
    public async Task EnsureRunningAsync_starts_no_run_when_the_ref_has_not_moved()
    {
        var fetcher = new StubGitFetcher(StubGitFetcher.Unchanged(FirstSha));
        var gate = RepoContextSourceTestDoubles.Gate(
            Registry(), RepoContextSourceTestDoubles.CredentialsFor(RepoId), fetcher);
        var runner = Substitute.For<IRepoIndexRunner>();
        var grain = CreateGrain(gate, runner, Job(RepoIndexStatus.Completed, FirstSha));

        var progress = await grain.EnsureRunningAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(fetcher.LastIndexedCommitSha, Is.EqualTo(FirstSha),
                "The completed generation's commit is what the fetch compares against.");
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Completed),
                "A no-op refresh reports the last-good generation rather than a fabricated new one.");
        });
        await runner.DidNotReceive().StartIndexAsync(Arg.Any<RepoIndexJobRequest>());
    }

    [Test]
    public async Task EnsureRunningAsync_re_runs_a_commit_whose_previous_generation_did_not_complete()
    {
        var fetcher = new StubGitFetcher(StubGitFetcher.Unchanged(FirstSha));
        var gate = RepoContextSourceTestDoubles.Gate(
            Registry(), RepoContextSourceTestDoubles.CredentialsFor(RepoId), fetcher);
        var runner = Substitute.For<IRepoIndexRunner>();

        // The persisted request carries the commit, but the run that carried it failed.
        var grain = CreateGrain(gate, runner, Job(RepoIndexStatus.Failed, FirstSha));

        await grain.EnsureRunningAsync(Request());

        Assert.That(fetcher.LastIndexedCommitSha, Is.Null,
            "A commit whose run did not complete is never treated as indexed, so the refresh retries it.");
        await runner.Received(1).StartIndexAsync(Arg.Is<RepoIndexJobRequest>(r => r.CommitSha == FirstSha));
    }

    [Test]
    public async Task EnsureRunningAsync_runs_again_when_the_ref_moves_to_a_new_commit()
    {
        var fetcher = new StubGitFetcher(StubGitFetcher.Staged(SecondSha));
        var gate = RepoContextSourceTestDoubles.Gate(
            Registry(), RepoContextSourceTestDoubles.CredentialsFor(RepoId), fetcher);
        var runner = Substitute.For<IRepoIndexRunner>();
        var grain = CreateGrain(gate, runner, Job(RepoIndexStatus.Completed, FirstSha));

        await grain.EnsureRunningAsync(Request());

        await runner.Received(1).StartIndexAsync(Arg.Is<RepoIndexJobRequest>(r => r.CommitSha == SecondSha));
    }

    [Test]
    public async Task EnsureRunningAsync_starts_no_run_when_the_repository_has_no_credential()
    {
        var fetcher = new StubGitFetcher(StubGitFetcher.Staged(FirstSha));
        var gate = RepoContextSourceTestDoubles.Gate(Registry(), credentials: null, fetcher: fetcher);
        var runner = Substitute.For<IRepoIndexRunner>();
        var grain = CreateGrain(gate, runner, Job(RepoIndexStatus.Completed, FirstSha));

        var progress = await grain.EnsureRunningAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(fetcher.FetchCount, Is.Zero, "Fail-closed auth never reaches the transport.");
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Completed),
                "A failed preparation leaves the last-good index serving.");
        });
        await runner.DidNotReceive().StartIndexAsync(Arg.Any<RepoIndexJobRequest>());
    }

    [Test]
    public async Task EnsureRunningAsync_starts_no_run_when_the_transport_fails()
    {
        var gate = RepoContextSourceTestDoubles.Gate(
            Registry(), RepoContextSourceTestDoubles.CredentialsFor(RepoId), fetcher: null);
        var runner = Substitute.For<IRepoIndexRunner>();
        var grain = CreateGrain(gate, runner, Job(RepoIndexStatus.Completed, FirstSha));

        var progress = await grain.EnsureRunningAsync(Request());

        Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Completed),
            "A failed fetch never wipes or supersedes the generation already being served.");
        await runner.DidNotReceive().StartIndexAsync(Arg.Any<RepoIndexJobRequest>());
    }

    [Test]
    public async Task EnsureRunningAsync_on_a_mounted_repository_never_consults_the_job_for_a_commit()
    {
        var job = Job(RepoIndexStatus.Completed, FirstSha);
        var runner = Substitute.For<IRepoIndexRunner>();
        var grain = CreateGrain(RepoContextSourceTestDoubles.MountedOnlyGate(), runner, job);

        await grain.EnsureRunningAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(
                job.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IRepoIndexJobGrain.GetRequestAsync)),
                Is.False,
                "The mounted path must not pay for the git anchor lookup.");
        });
        await runner.Received(1).StartIndexAsync(Arg.Is<RepoIndexJobRequest>(
            r => r.RepoRoot == MountedRoot && r.CommitSha == null));
    }

    /// <summary>
    /// Minimal in-memory <see cref="IPersistentState{T}"/> so the grain can be
    /// constructed without a storage provider.
    /// </summary>
    private sealed class FakeSelfIndexState : IPersistentState<RepoContextSelfIndexState>
    {
        public RepoContextSelfIndexState State { get; set; } = new();
        public string Etag => string.Empty;
        public bool RecordExists => true;

        public Task ClearStateAsync() => Task.CompletedTask;
        public Task ReadStateAsync() => Task.CompletedTask;
        public Task WriteStateAsync() => Task.CompletedTask;
    }
}

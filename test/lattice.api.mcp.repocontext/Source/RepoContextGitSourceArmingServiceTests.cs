using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for the service that onboards configured git sources. A git-sourced
/// repository is declared in configuration rather than registered by an
/// <c>add_repo</c> call, so without this service nothing would ever start its
/// refresh loop. It must arm every declared source exactly once, retry one that
/// fails rather than abandoning it, and stand down entirely on a spoke.
/// </summary>
[TestFixture]
public sealed class RepoContextGitSourceArmingServiceTests
{
    private static readonly string StagingRoot =
        Path.Combine(Path.GetTempPath(), "lattice-repocontext-git-arming-tests");

    private static RepoContextGitSourceRegistry Registry(params string[] repoIds) =>
        new(
            repoIds.Select(repoId => new RepoContextGitSourceOptions
            {
                RepoId = repoId,
                RemoteUrl = "https://git.example/" + repoId + ".git",
            }),
            StagingRoot);

    private static RepoContextGitSourceArmingService CreateService(
        RepoContextGitSourceRegistry registry,
        IGrainFactory grainFactory,
        RepoContextIndexingRole role = RepoContextIndexingRole.Hub,
        IRepoIndexRunAuthority? runAuthority = null) =>
        new(
            grainFactory,
            RepoContextSourceTestDoubles.Gate(registry, role: role),
            new RepoContextIndexingOptions { Role = role },
            runAuthority ?? Substitute.For<IRepoIndexRunAuthority>(),
            new ImmediateTimeProvider(),
            NullLogger<RepoContextGitSourceArmingService>.Instance);

    private static IGrainFactory FactoryFor(params IRepoContextSelfIndexGrain[] grains)
    {
        var factory = Substitute.For<IGrainFactory>();
        for (var i = 0; i < grains.Length; i++)
        {
            factory.GetGrain<IRepoContextSelfIndexGrain>("repo" + i, Arg.Any<string?>()).Returns(grains[i]);
        }

        return factory;
    }

    private static IRepoContextSelfIndexGrain Grain()
    {
        var grain = Substitute.For<IRepoContextSelfIndexGrain>();
        grain.EnsureRunningAsync(Arg.Any<RepoIndexJobRequest>()).Returns(
            new RepoIndexProgress { RepoId = "repo0", Status = RepoIndexStatus.Running, Phase = RepoIndexPhase.Walking });
        return grain;
    }

    [Test]
    public async Task ExecuteAsync_arms_every_configured_git_source()
    {
        var first = Grain();
        var second = Grain();
        var service = CreateService(Registry("repo0", "repo1"), FactoryFor(first, second));

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!;

        await first.Received(1).EnsureRunningAsync(Arg.Is<RepoIndexJobRequest>(r => r.RepoId == "repo0"));
        await second.Received(1).EnsureRunningAsync(Arg.Is<RepoIndexJobRequest>(r => r.RepoId == "repo1"));
    }

    [Test]
    public async Task ExecuteAsync_arms_each_source_with_its_staging_work_tree_and_no_pruning()
    {
        var grain = Grain();
        var service = CreateService(Registry("repo0"), FactoryFor(grain));

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!;

        await grain.Received(1).EnsureRunningAsync(Arg.Is<RepoIndexJobRequest>(
            r => r.RepoRoot == GitRemoteSource.WorkTreePath(StagingRoot, "repo0")
                && !r.AllowPrune
                && !r.RespectGitignore));
    }

    [Test]
    public async Task ExecuteAsync_retries_a_source_whose_first_arm_fails()
    {
        var grain = Substitute.For<IRepoContextSelfIndexGrain>();
        var attempts = 0;
        grain.EnsureRunningAsync(Arg.Any<RepoIndexJobRequest>()).Returns(_ =>
            ++attempts == 1
                ? throw new InvalidOperationException("the silo is still starting")
                : Task.FromResult(new RepoIndexProgress
                {
                    RepoId = "repo0",
                    Status = RepoIndexStatus.Running,
                    Phase = RepoIndexPhase.Walking,
                }));

        var service = CreateService(Registry("repo0"), FactoryFor(grain));

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!;

        Assert.That(attempts, Is.EqualTo(2), "A failed arm is retried, not abandoned.");
    }

    [Test]
    public async Task ExecuteAsync_stops_arming_once_every_source_is_armed()
    {
        var grain = Grain();
        var service = CreateService(Registry("repo0"), FactoryFor(grain));

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!;

        await grain.Received(1).EnsureRunningAsync(Arg.Any<RepoIndexJobRequest>());
    }

    [Test]
    public async Task ExecuteAsync_stands_down_on_a_spoke()
    {
        var grain = Grain();
        var service = CreateService(Registry("repo0"), FactoryFor(grain), RepoContextIndexingRole.Spoke);

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!;

        await grain.DidNotReceive().EnsureRunningAsync(Arg.Any<RepoIndexJobRequest>());
    }

    [Test]
    public async Task ExecuteAsync_arms_nothing_when_no_git_source_is_configured()
    {
        var factory = Substitute.For<IGrainFactory>();
        var service = CreateService(RepoContextGitSourceRegistry.Empty, factory);

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!;

        Assert.That(factory.ReceivedCalls(), Is.Empty, "The mounted default posture arms nothing.");
    }

    [Test]
    public async Task ExecuteAsync_arms_under_the_fixed_run_credential_when_one_is_available()
    {
        var credential = new LatticeCredential("repo-index", "background-indexer");
        var runAuthority = Substitute.For<IRepoIndexRunAuthority>();
        runAuthority.Resolve().Returns(credential);

        LatticeCredential? observed = null;
        var grain = Substitute.For<IRepoContextSelfIndexGrain>();
        grain.EnsureRunningAsync(Arg.Any<RepoIndexJobRequest>()).Returns(_ =>
        {
            observed = LatticeCredentialContext.Current;
            return Task.FromResult(new RepoIndexProgress
            {
                RepoId = "repo0",
                Status = RepoIndexStatus.Running,
                Phase = RepoIndexPhase.Walking,
            });
        });

        var service = CreateService(Registry("repo0"), FactoryFor(grain), runAuthority: runAuthority);

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!;

        Assert.That(observed, Is.EqualTo(credential),
            "The arming call carries a subject the access gate can authorize.");
    }

    [Test]
    public async Task ExecuteAsync_arms_without_a_credential_when_the_authority_resolves_none()
    {
        var runAuthority = Substitute.For<IRepoIndexRunAuthority>();
        runAuthority.Resolve().Returns((LatticeCredential?)null);
        var grain = Grain();

        var service = CreateService(Registry("repo0"), FactoryFor(grain), runAuthority: runAuthority);

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!;

        await grain.Received(1).EnsureRunningAsync(Arg.Any<RepoIndexJobRequest>());
    }
}

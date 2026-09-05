using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for the single routing seam that decides which source strategy owns a
/// repository. The mount-versus-git mutual exclusion is enforced here and nowhere
/// else, and a git-sourced repository is never silently walked from a mount when its
/// preparation fails.
/// </summary>
[TestFixture]
public sealed class RepoContextIndexSourceGateTests
{
    private const string GitRepoId = "git-repo";
    private const string MountedRepoId = "mounted-repo";

    private static RepoIndexJobRequest Request(string repoId) => new()
    {
        RepoId = repoId,
        RepoRoot = Path.Combine(Path.GetTempPath(), "mount"),
    };

    private static RepoContextGitSourceRegistry Registry() =>
        new(
            [new RepoContextGitSourceOptions { RepoId = GitRepoId, RemoteUrl = "https://git.example.invalid/x.git" }],
            Path.Combine(Path.GetTempPath(), "lattice-gate-staging"));

    [Test]
    public void HasGitSources_is_false_for_the_default_deployment()
    {
        var gate = RepoContextSourceTestDoubles.MountedOnlyGate();

        Assert.Multiple(() =>
        {
            Assert.That(gate.HasGitSources, Is.False);
            Assert.That(gate.GitSources, Is.Empty);
            Assert.That(gate.StagingRoot, Is.Not.Empty);
        });
    }

    [Test]
    public void KindFor_routes_declared_repositories_to_the_git_strategy_only()
    {
        var gate = RepoContextSourceTestDoubles.Gate(Registry());

        Assert.Multiple(() =>
        {
            Assert.That(gate.KindFor(GitRepoId), Is.EqualTo(RepoContextSourceKind.GitRemote));
            Assert.That(gate.KindFor(MountedRepoId), Is.EqualTo(RepoContextSourceKind.MountedWorkspace));
            Assert.That(gate.IsGitSourced(GitRepoId), Is.True);
            Assert.That(gate.IsGitSourced(MountedRepoId), Is.False);
            Assert.That(gate.HasGitSources, Is.True);
        });
    }

    [Test]
    public void RefreshIntervalFor_uses_the_git_cadence_only_for_a_git_repository()
    {
        var fallback = TimeSpan.FromHours(3);
        var gate = RepoContextSourceTestDoubles.Gate(Registry());

        Assert.Multiple(() =>
        {
            Assert.That(
                gate.RefreshIntervalFor(GitRepoId, fallback),
                Is.EqualTo(RepoContextGitSourceOptions.DefaultRefreshInterval));
            Assert.That(gate.RefreshIntervalFor(MountedRepoId, fallback), Is.EqualTo(fallback));
        });
    }

    [Test]
    public void SeedRequest_points_at_the_staging_work_tree_with_the_sources_filters()
    {
        var registry = Registry();
        var gate = RepoContextSourceTestDoubles.Gate(registry);
        var source = registry.Find(GitRepoId)! with { IncludeGlobs = ["src/**"], ExcludeBinary = false };

        var seed = gate.SeedRequest(source);

        Assert.Multiple(() =>
        {
            Assert.That(seed.RepoId, Is.EqualTo(GitRepoId));
            Assert.That(seed.RepoRoot, Is.EqualTo(GitRemoteSource.WorkTreePath(registry.StagingRoot, GitRepoId)));
            Assert.That(seed.IncludeGlobs, Is.EqualTo(new[] { "src/**" }));
            Assert.That(seed.ExcludeBinary, Is.False);
            Assert.That(seed.AllowPrune, Is.False);
            Assert.That(seed.RespectGitignore, Is.False);
        });
    }

    [Test]
    public void SeedRequest_rejects_null_options()
    {
        var gate = RepoContextSourceTestDoubles.Gate(Registry());

        Assert.That(() => gate.SeedRequest(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void SeedRequestFor_returns_null_for_a_mounted_repository()
    {
        var gate = RepoContextSourceTestDoubles.Gate(Registry());

        Assert.Multiple(() =>
        {
            Assert.That(gate.SeedRequestFor(MountedRepoId), Is.Null);
            Assert.That(gate.SeedRequestFor(GitRepoId), Is.Not.Null);
        });
    }

    [Test]
    public void PrepareAsync_rejects_a_null_request()
    {
        var gate = RepoContextSourceTestDoubles.Gate(Registry());

        Assert.That(
            () => gate.PrepareAsync(null!, lastIndexedCommitSha: null, CancellationToken.None).AsTask(),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task PrepareAsync_passes_a_mounted_repository_straight_through()
    {
        var gate = RepoContextSourceTestDoubles.Gate(Registry());
        var request = Request(MountedRepoId);

        var preparation = await gate.PrepareAsync(request, lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Kind, Is.EqualTo(RepoContextSourceKind.MountedWorkspace));
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Proceed));
            Assert.That(preparation.Request, Is.SameAs(request), "The mounted walk must not be rewritten.");
            Assert.That(preparation.CommitSha, Is.Null, "A mount has no verifiable revision to anchor to.");
        });
    }

    [Test]
    public async Task PrepareAsync_fails_closed_for_a_git_repository_on_a_spoke()
    {
        var gate = RepoContextSourceTestDoubles.Gate(Registry(), role: RepoContextIndexingRole.Spoke);

        var preparation = await gate.PrepareAsync(
            Request(GitRepoId), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Kind, Is.EqualTo(RepoContextSourceKind.GitRemote));
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.Request, Is.Null);
        });
    }

    [Test]
    public async Task PrepareAsync_never_falls_back_to_the_mount_when_the_git_source_fails()
    {
        // The declared repository has no credential, so the git strategy stands down.
        var gate = RepoContextSourceTestDoubles.Gate(Registry());
        var request = Request(GitRepoId);

        var preparation = await gate.PrepareAsync(request, lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.Request, Is.Null,
                "Falling back would index whatever happened to be mounted under a git-declared identity.");
        });
    }

    [Test]
    public async Task MountedWorkspaceSource_is_unchanged_by_the_seam()
    {
        var mounted = new MountedWorkspaceSource();
        var request = Request(MountedRepoId);

        var preparation = await mounted.PrepareAsync(
            request, lastIndexedCommitSha: "ignored", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(mounted.Kind, Is.EqualTo(RepoContextSourceKind.MountedWorkspace));
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Proceed));
            Assert.That(preparation.Request, Is.SameAs(request));
            Assert.That(preparation.CommitSha, Is.Null);
        });
    }

    [Test]
    public void MountedWorkspaceSource_rejects_a_null_request()
    {
        Assert.That(
            () => new MountedWorkspaceSource()
                .PrepareAsync(null!, lastIndexedCommitSha: null, CancellationToken.None).AsTask(),
            Throws.ArgumentNullException);
    }
}

using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// End-to-end tests for the opt-in <see cref="GitRemoteSource"/> against a real
/// LibGit2Sharp transport and a local git remote created in a temporary directory,
/// so every case runs offline. These cover the refresh loop's contract: first sync,
/// no-change no-op, and the fail-closed paths (no credential, wrong role, no
/// configuration, unreachable remote).
/// </summary>
[TestFixture]
public sealed partial class GitRemoteSourceTests
{
    private const string RepoId = "acme";

    private static RepoIndexJobRequest Request(string repoId = RepoId) => new()
    {
        RepoId = repoId,
        RepoRoot = Path.Combine(Path.GetTempPath(), "not-used"),
    };

    private static GitRemoteSource CreateSource(
        RepoContextGitSourceRegistry registry,
        IRepoContextGitCredentialProvider? credentials = null,
        RepoContextIndexingRole role = RepoContextIndexingRole.Hub,
        IRepoContextGitFetcher? fetcher = null) =>
        new(registry,
            credentials ?? RepoContextSourceTestDoubles.CredentialsFor(RepoId),
            fetcher ?? new LibGit2SharpRepoContextGitFetcher(),
            new RepoContextIndexingOptions { Role = role },
            NullLogger<GitRemoteSource>.Instance);

    private static LocalGitRemoteFixture CreateFixture() => LocalGitRemoteFixture.Create(
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["README.md"] = "# acme\n",
            ["src/one.cs"] = "public sealed class One { }\n",
        });

    [Test]
    public void Kind_reports_the_git_remote_strategy()
    {
        var source = CreateSource(RepoContextGitSourceRegistry.Empty);

        Assert.That(source.Kind, Is.EqualTo(RepoContextSourceKind.GitRemote));
    }

    [Test]
    public void PrepareAsync_rejects_a_null_request()
    {
        var source = CreateSource(RepoContextGitSourceRegistry.Empty);

        Assert.That(
            () => source.PrepareAsync(null!, lastIndexedCommitSha: null, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task PrepareAsync_first_sync_stages_the_ref_and_stamps_the_resolved_commit()
    {
        using var fixture = CreateFixture();
        var source = CreateSource(fixture.Registry(fixture.Source(RepoId)));

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Proceed));
            Assert.That(preparation.CommitSha, Is.EqualTo(fixture.HeadSha()),
                "The generation is anchored to the commit the configured ref resolved to.");
            Assert.That(preparation.Request, Is.Not.Null);
            Assert.That(preparation.Request!.CommitSha, Is.EqualTo(fixture.HeadSha()),
                "The commit rides on the request, so the run it starts stamps the same revision.");
            Assert.That(preparation.Request!.RepoRoot, Is.EqualTo(
                GitRemoteSource.WorkTreePath(fixture.StagingRoot, RepoId)),
                "The run indexes the staged work tree, never the caller-supplied root.");
            Assert.That(preparation.Request.AllowPrune, Is.False,
                "Absence-pruning is meaningless for a commit tree: deletion comes from the diff.");
            Assert.That(preparation.Request.RespectGitignore, Is.False,
                "A commit tree already contains exactly the tracked files.");
        });

        Assert.That(File.Exists(Path.Combine(preparation.Request!.RepoRoot, "README.md")), Is.True,
            "The resolved commit is checked out into the staging work tree.");
    }

    [Test]
    public async Task PrepareAsync_is_a_no_op_when_the_ref_has_not_moved()
    {
        using var fixture = CreateFixture();
        var source = CreateSource(fixture.Registry(fixture.Source(RepoId)));

        var first = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);
        var second = await source.PrepareAsync(Request(), first.CommitSha, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Outcome, Is.EqualTo(RepoContextSourceOutcome.UpToDate));
            Assert.That(second.CommitSha, Is.EqualTo(first.CommitSha));
            Assert.That(second.Request, Is.Null, "A no-op refresh starts no run.");
        });
    }

    [Test]
    public async Task PrepareAsync_proceeds_again_once_the_ref_moves()
    {
        using var fixture = CreateFixture();
        var source = CreateSource(fixture.Registry(fixture.Source(RepoId)));

        var first = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);
        var moved = fixture.Commit(
            "second",
            new Dictionary<string, string>(StringComparer.Ordinal) { ["src/two.cs"] = "public sealed class Two { }\n" },
            deletions: null);

        var second = await source.PrepareAsync(Request(), first.CommitSha, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Outcome, Is.EqualTo(RepoContextSourceOutcome.Proceed));
            Assert.That(second.CommitSha, Is.EqualTo(moved));
            Assert.That(second.CommitSha, Is.Not.EqualTo(first.CommitSha));
        });
    }

    [Test]
    public async Task PrepareAsync_stands_down_when_no_credential_resolves()
    {
        using var fixture = CreateFixture();
        var source = CreateSource(
            fixture.Registry(fixture.Source(RepoId)),
            credentials: RepoContextSourceTestDoubles.CredentialsFor("some-other-repo"));

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.Request, Is.Null);
            Assert.That(preparation.FailureReason, Does.Contain("no credential"));
        });

        Assert.That(
            Directory.Exists(GitRemoteSource.WorkTreePath(fixture.StagingRoot, RepoId)), Is.False,
            "A fail-closed preparation never reaches the transport, so it stages nothing.");
    }

    [Test]
    public async Task PrepareAsync_stands_down_on_a_spoke()
    {
        using var fixture = CreateFixture();
        var source = CreateSource(
            fixture.Registry(fixture.Source(RepoId)), role: RepoContextIndexingRole.Spoke);

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.FailureReason, Does.Contain("Hub"));
        });
    }

    [Test]
    public async Task PrepareAsync_stands_down_when_the_repository_has_no_git_source()
    {
        var source = CreateSource(RepoContextGitSourceRegistry.Empty);

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.FailureReason, Does.Contain("no git source"));
        });
    }

    [Test]
    public async Task PrepareAsync_stands_down_when_the_remote_url_is_blank()
    {
        using var fixture = CreateFixture();
        var source = CreateSource(fixture.Registry(fixture.Source(RepoId, remoteUrl: string.Empty)));

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.FailureReason, Does.Contain("remote url"));
        });
    }

    [Test]
    public async Task PrepareAsync_accepts_an_explicitly_anonymous_source()
    {
        using var fixture = CreateFixture();
        var anonymous = fixture.Source(RepoId) with { AuthMode = RepoContextGitAuthMode.Anonymous };
        var source = CreateSource(
            fixture.Registry(anonymous),
            credentials: new RepoContextEnvironmentGitCredentialProvider(
                new Dictionary<string, RepoContextGitCredential>(StringComparer.Ordinal)));

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Proceed),
            "Anonymous is an explicit opt-in, not a fallback for a missing token.");
    }

    [Test]
    public void WorkTreePath_separates_repositories_that_sanitise_identically()
    {
        var first = GitRemoteSource.WorkTreePath(Path.GetTempPath(), "acme/one");
        var second = GitRemoteSource.WorkTreePath(Path.GetTempPath(), "acme:one");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.EqualTo(second));
            Assert.That(Path.GetFileName(first), Does.StartWith("acme_one-"));
        });
    }

    [Test]
    public void WorkTreePath_rejects_blank_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => GitRemoteSource.WorkTreePath(" ", "acme"), Throws.ArgumentException);
            Assert.That(() => GitRemoteSource.WorkTreePath(Path.GetTempPath(), " "), Throws.ArgumentException);
        });
    }
}

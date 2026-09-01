using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// The availability-coupling half of <see cref="GitRemoteSourceTests"/>: index
/// freshness now depends on a git host, so a failed, hung, or partial fetch must
/// leave the last-good index serving rather than wiping or half-applying it.
/// </summary>
public sealed partial class GitRemoteSourceTests
{
    [Test]
    public async Task PrepareAsync_stands_down_when_the_remote_is_unreachable()
    {
        using var fixture = CreateFixture();
        var missing = Path.Combine(fixture.StagingRoot, "no-such-origin");
        var source = CreateSource(fixture.Registry(fixture.Source(RepoId, remoteUrl: missing)));

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.Request, Is.Null, "A failed fetch starts no run.");
            Assert.That(preparation.CommitSha, Is.Null, "A failed fetch stamps no anchor.");
        });
    }

    [Test]
    public async Task PrepareAsync_leaves_the_last_good_staged_tree_intact_when_a_later_fetch_fails()
    {
        using var fixture = CreateFixture();
        var registry = fixture.Registry(fixture.Source(RepoId));
        var good = CreateSource(registry);

        var first = await good.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);
        var workTree = first.Request!.RepoRoot;
        var stagedBefore = Directory.GetFiles(workTree, "*", SearchOption.TopDirectoryOnly).Length;

        // Repoint the same staging tree at a remote that does not exist and refresh.
        var broken = CreateSource(
            fixture.Registry(fixture.Source(RepoId, remoteUrl: Path.Combine(fixture.StagingRoot, "gone"))));
        var second = await broken.PrepareAsync(Request(), first.CommitSha, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(File.Exists(Path.Combine(workTree, "README.md")), Is.True,
                "A failed fetch never deletes the content the last-good generation was built from.");
            Assert.That(
                Directory.GetFiles(workTree, "*", SearchOption.TopDirectoryOnly), Has.Length.EqualTo(stagedBefore));
        });
    }

    [Test]
    public async Task PrepareAsync_stands_down_when_the_fetch_overruns_its_timeout()
    {
        using var fixture = CreateFixture();
        using var release = new ManualResetEventSlim(initialState: false);
        var source = CreateSource(
            fixture.Registry(fixture.Source(RepoId) with { FetchTimeout = TimeSpan.FromMilliseconds(50) }),
            fetcher: new BlockingGitFetcher(release));

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);
        release.Set();

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.FailureReason, Does.Contain("timeout"));
        });
    }

    [Test]
    public async Task PrepareAsync_does_not_stack_a_second_fetch_onto_an_overrunning_one()
    {
        using var fixture = CreateFixture();
        using var release = new ManualResetEventSlim(initialState: false);
        var source = CreateSource(
            fixture.Registry(fixture.Source(RepoId) with { FetchTimeout = TimeSpan.FromMilliseconds(50) }),
            fetcher: new BlockingGitFetcher(release));

        var first = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);
        var second = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);
        release.Set();

        Assert.Multiple(() =>
        {
            Assert.That(first.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(second.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(second.FailureReason, Does.Contain("still running"));
        });
    }

    [Test]
    public async Task PrepareAsync_redacts_a_transport_failure_that_quotes_the_credential()
    {
        using var fixture = CreateFixture();
        var source = CreateSource(
            fixture.Registry(fixture.Source(RepoId)),
            fetcher: new LeakyGitFetcher("test-token"));

        var preparation = await source.PrepareAsync(Request(), lastIndexedCommitSha: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.FailureReason, Does.Not.Contain("test-token"),
                "A failure reason must never carry the secret it authenticated with.");
            Assert.That(preparation.FailureReason, Does.Contain(RepoContextSecretRedactor.Placeholder));
        });
    }

    /// <summary>A transport that blocks until released, to exercise the fetch timeout.</summary>
    private sealed class BlockingGitFetcher(ManualResetEventSlim release) : IRepoContextGitFetcher
    {
        public RepoContextGitFetchResult Fetch(
            RepoContextGitFetchRequest request, CancellationToken cancellationToken)
        {
            release.Wait(TimeSpan.FromSeconds(30), CancellationToken.None);
            return new RepoContextGitFetchResult { CommitSha = new string('a', 40), CheckedOut = true };
        }

        public IReadOnlyList<RepoFileEntry> ScanCommit(
            string workTreePath,
            string commitSha,
            IReadOnlyList<string>? includeGlobs,
            IReadOnlyList<string>? excludeGlobs,
            bool excludeBinary,
            CancellationToken cancellationToken) => [];
    }

    /// <summary>A transport whose failure message quotes the secret it was handed.</summary>
    private sealed class LeakyGitFetcher(string secret) : IRepoContextGitFetcher
    {
        public RepoContextGitFetchResult Fetch(
            RepoContextGitFetchRequest request, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("authentication rejected for " + secret);

        public IReadOnlyList<RepoFileEntry> ScanCommit(
            string workTreePath,
            string commitSha,
            IReadOnlyList<string>? includeGlobs,
            IReadOnlyList<string>? excludeGlobs,
            bool excludeBinary,
            CancellationToken cancellationToken) => [];
    }
}

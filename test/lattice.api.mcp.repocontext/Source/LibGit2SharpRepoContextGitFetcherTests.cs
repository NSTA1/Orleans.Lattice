using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for <see cref="LibGit2SharpRepoContextGitFetcher"/> and the commit-tree scan
/// it feeds the reconciler. The scan set is the resolved commit's tracked files, which
/// is what makes the changeset exact: the plan's add / update / removed sets are the
/// real difference between two commits rather than an inference from what happened to
/// be present on a mount.
/// </summary>
[TestFixture]
public sealed class LibGit2SharpRepoContextGitFetcherTests
{
    private const string RepoId = "delta";

    private static LocalGitRemoteFixture CreateFixture() => LocalGitRemoteFixture.Create(
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["keep.md"] = "keep\n",
            ["change.md"] = "before\n",
            ["drop.md"] = "drop\n",
        });

    private static string Stage(LocalGitRemoteFixture fixture, string? lastIndexedCommitSha, out string commitSha)
    {
        var fetcher = new LibGit2SharpRepoContextGitFetcher();
        var workTree = GitRemoteSource.WorkTreePath(fixture.StagingRoot, RepoId);
        var result = fetcher.Fetch(
            new RepoContextGitFetchRequest
            {
                Source = fixture.Source(RepoId),
                Credential = RepoContextGitCredential.Anonymous,
                WorkTreePath = workTree,
                LastIndexedCommitSha = lastIndexedCommitSha,
            },
            CancellationToken.None);

        commitSha = result.CommitSha;
        return workTree;
    }

    [Test]
    public void Fetch_rejects_a_null_request()
    {
        var fetcher = new LibGit2SharpRepoContextGitFetcher();

        Assert.That(() => fetcher.Fetch(null!, CancellationToken.None), Throws.ArgumentNullException);
    }

    [Test]
    public void ScanCommit_rejects_blank_arguments()
    {
        var fetcher = new LibGit2SharpRepoContextGitFetcher();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => fetcher.ScanCommit(" ", "abc", null, null, true, CancellationToken.None),
                Throws.ArgumentException);
            Assert.That(
                () => fetcher.ScanCommit(Path.GetTempPath(), " ", null, null, true, CancellationToken.None),
                Throws.ArgumentException);
        });
    }

    [Test]
    public void Fetch_resolves_the_configured_ref_to_its_tip_commit()
    {
        using var fixture = CreateFixture();

        Stage(fixture, lastIndexedCommitSha: null, out var commitSha);

        Assert.That(commitSha, Is.EqualTo(fixture.HeadSha()));
    }

    [Test]
    public void Fetch_leaves_the_work_tree_untouched_when_the_ref_matches_the_indexed_commit()
    {
        using var fixture = CreateFixture();
        Stage(fixture, lastIndexedCommitSha: null, out var first);

        var fetcher = new LibGit2SharpRepoContextGitFetcher();
        var result = fetcher.Fetch(
            new RepoContextGitFetchRequest
            {
                Source = fixture.Source(RepoId),
                Credential = RepoContextGitCredential.Anonymous,
                WorkTreePath = GitRemoteSource.WorkTreePath(fixture.StagingRoot, RepoId),
                LastIndexedCommitSha = first,
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.CheckedOut, Is.False);
            Assert.That(result.CommitSha, Is.EqualTo(first));
        });
    }

    [Test]
    public void ScanCommit_returns_the_commits_tracked_files()
    {
        using var fixture = CreateFixture();
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out var commitSha);

        var entries = new LibGit2SharpRepoContextGitFetcher()
            .ScanCommit(workTree, commitSha, null, null, excludeBinary: true, CancellationToken.None);

        Assert.That(
            entries.Select(e => e.RelativePath).OrderBy(p => p, StringComparer.Ordinal),
            Is.EqualTo(new[] { "change.md", "drop.md", "keep.md" }));
    }

    [Test]
    public void ScanCommit_yields_an_exact_add_modify_delete_changeset_between_two_commits()
    {
        using var fixture = CreateFixture();
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out var firstSha);
        var fetcher = new LibGit2SharpRepoContextGitFetcher();
        var before = fetcher.ScanCommit(workTree, firstSha, null, null, true, CancellationToken.None);

        fixture.Commit(
            "delta",
            new Dictionary<string, string>(StringComparer.Ordinal) { ["change.md"] = "after\n", ["added.md"] = "new\n" },
            deletions: ["drop.md"]);
        Stage(fixture, firstSha, out var secondSha);
        var after = fetcher.ScanCommit(workTree, secondSha, null, null, true, CancellationToken.None);

        // The reconcile plan is exactly what the indexer applies, so asserting on it
        // proves the delta rather than merely the scan.
        var stored = before.ToDictionary(e => e.RelativePath, e => e.Digest, StringComparer.Ordinal);
        var plan = RepoContextBootstrapPlan.Compute(stored, after);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Added.Select(e => e.RelativePath), Is.EqualTo(new[] { "added.md" }));
            Assert.That(plan.Updated.Select(e => e.RelativePath), Is.EqualTo(new[] { "change.md" }));
            Assert.That(plan.RemovedPaths, Is.EqualTo(new[] { "drop.md" }),
                "Deletion comes from the commit diff, never from absence on a mount.");
        });
    }

    [Test]
    public void ScanCommit_is_a_no_op_plan_when_the_commit_is_unchanged()
    {
        using var fixture = CreateFixture();
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out var commitSha);
        var fetcher = new LibGit2SharpRepoContextGitFetcher();
        var entries = fetcher.ScanCommit(workTree, commitSha, null, null, true, CancellationToken.None);

        var stored = entries.ToDictionary(e => e.RelativePath, e => e.Digest, StringComparer.Ordinal);
        var plan = RepoContextBootstrapPlan.Compute(
            stored, fetcher.ScanCommit(workTree, commitSha, null, null, true, CancellationToken.None));

        Assert.That(plan.IsNoOp, Is.True, "Re-scanning the same commit produces identical digests.");
    }

    [Test]
    public void ScanCommit_applies_include_and_exclude_globs()
    {
        using var fixture = CreateFixture();
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out var commitSha);
        var fetcher = new LibGit2SharpRepoContextGitFetcher();

        var included = fetcher.ScanCommit(
            workTree, commitSha, ["keep.md"], null, true, CancellationToken.None);
        var excluded = fetcher.ScanCommit(
            workTree, commitSha, null, ["drop.md"], true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(included.Select(e => e.RelativePath), Is.EqualTo(new[] { "keep.md" }));
            Assert.That(excluded.Select(e => e.RelativePath), Does.Not.Contain("drop.md"));
        });
    }

    [Test]
    public void ScanCommit_drops_a_binary_blob_when_binaries_are_excluded()
    {
        using var fixture = LocalGitRemoteFixture.Create(
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["text.md"] = "text\n",
                ["blob.bin"] = "head\0tail",
            });
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out var commitSha);
        var fetcher = new LibGit2SharpRepoContextGitFetcher();

        var withBinaries = fetcher.ScanCommit(workTree, commitSha, null, null, false, CancellationToken.None);
        var withoutBinaries = fetcher.ScanCommit(workTree, commitSha, null, null, true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(withBinaries.Select(e => e.RelativePath), Does.Contain("blob.bin"));
            Assert.That(withoutBinaries.Select(e => e.RelativePath), Does.Not.Contain("blob.bin"));
        });
    }

    [Test]
    public void ScanCommit_reports_nested_paths_with_forward_slashes()
    {
        using var fixture = LocalGitRemoteFixture.Create(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["src/deep/one.cs"] = "class One { }\n" });
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out var commitSha);

        var entries = new LibGit2SharpRepoContextGitFetcher()
            .ScanCommit(workTree, commitSha, null, null, true, CancellationToken.None);

        Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "src/deep/one.cs" }));
    }

    [Test]
    public void ScanCommit_fails_closed_on_a_commit_that_is_not_in_the_object_database()
    {
        using var fixture = CreateFixture();
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out _);

        Assert.That(
            () => new LibGit2SharpRepoContextGitFetcher().ScanCommit(
                workTree, new string('b', 40), null, null, true, CancellationToken.None),
            Throws.InstanceOf<RepoContextGitSourceException>());
    }

    [Test]
    public void ScanCommit_classifies_language_from_the_path()
    {
        using var fixture = LocalGitRemoteFixture.Create(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["one.cs"] = "class One { }\n" });
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out var commitSha);

        var entries = new LibGit2SharpRepoContextGitFetcher()
            .ScanCommit(workTree, commitSha, null, null, true, CancellationToken.None);

        Assert.That(entries[0].Language, Is.EqualTo(LanguageClassifier.Classify("one.cs")));
    }

    [Test]
    public void CommitSourceScanner_defers_to_the_directory_walk_without_a_commit()
    {
        var scanner = new RepoContextCommitSourceScanner(
            new LibGit2SharpRepoContextGitFetcher(),
            RepoContextGitSourceRegistry.Empty,
            NullLogger<RepoContextCommitSourceScanner>.Instance);

        var scanned = scanner.TryScan(
            new RepoContextBootstrapRequest { RepoId = RepoId, RepoRoot = Path.GetTempPath() },
            CancellationToken.None);

        Assert.That(scanned, Is.Null, "A mounted run must keep walking the tree exactly as before.");
    }

    [Test]
    public void CommitSourceScanner_rejects_a_null_request()
    {
        var scanner = new RepoContextCommitSourceScanner(
            new LibGit2SharpRepoContextGitFetcher(),
            RepoContextGitSourceRegistry.Empty,
            NullLogger<RepoContextCommitSourceScanner>.Instance);

        Assert.That(() => scanner.TryScan(null!, CancellationToken.None), Throws.ArgumentNullException);
    }

    [Test]
    public void CommitSourceScanner_scans_the_commit_tree_when_the_request_carries_a_commit()
    {
        using var fixture = CreateFixture();
        var workTree = Stage(fixture, lastIndexedCommitSha: null, out var commitSha);
        var scanner = new RepoContextCommitSourceScanner(
            new LibGit2SharpRepoContextGitFetcher(),
            fixture.Registry(fixture.Source(RepoId)),
            NullLogger<RepoContextCommitSourceScanner>.Instance);

        var scanned = scanner.TryScan(
            new RepoContextBootstrapRequest { RepoId = RepoId, RepoRoot = workTree, CommitSha = commitSha },
            CancellationToken.None);

        Assert.That(
            scanned!.Select(e => e.RelativePath).OrderBy(p => p, StringComparer.Ordinal),
            Is.EqualTo(new[] { "change.md", "drop.md", "keep.md" }));
    }
}

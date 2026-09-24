using System.Globalization;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Regression tests for issue #3145: a completed pass whose reconcile plan is a
/// no-op must still advance the repository marker's <c>lastIngested</c>. The pass
/// walked the tree and found every file current, which is exactly the currency
/// evidence the marker exists to record. Before the fix the marker was only
/// re-stamped when the request carried a commit SHA, so a mounted-workspace
/// repository that stayed converged read as progressively staler with every pass.
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    private const string StaleIngest = "2020-01-01T00:00:00.0000000+00:00";

    [Test]
    public async Task A_no_op_mounted_workspace_pass_advances_last_ingested()
    {
        SeedConvergedFile();
        _harness.SeedRepoNode(StaleMarker(fileCount: 1));
        var before = DateTimeOffset.UtcNow;

        var result = await _harness.Service.RunAsync(_harness.Request(), progress: null);

        var marker = _harness.ReadRepoNode();
        Assert.That(marker, Is.Not.Null);
        var stamped = DateTimeOffset.Parse(
            RepoContextValues.ReadString(marker!.LastIngested)!, CultureInfo.InvariantCulture);
        Assert.Multiple(() =>
        {
            Assert.That(result.FilesUnchanged, Is.EqualTo(1), "The pass must be a genuine no-op.");
            Assert.That(_harness.AtomicWrites, Is.Zero, "A no-op pass must still commit no chunk.");
            Assert.That(stamped, Is.GreaterThanOrEqualTo(before.AddSeconds(-1)),
                "A completed no-change pass must advance lastIngested to the time it verified the tree.");
            Assert.That(RepoContextValues.ReadInt64(marker.FileCount), Is.EqualTo(1));
            Assert.That(RepoContextValues.ReadString(marker.IndexedCommit), Is.Null,
                "A mounted workspace has no verifiable revision, so no commit is written.");
        });
    }

    [Test]
    public async Task A_no_op_pass_advances_last_ingested_on_every_pass()
    {
        SeedConvergedFile();
        _harness.SeedRepoNode(StaleMarker(fileCount: 1));

        await _harness.Service.RunAsync(_harness.Request(), progress: null);
        var first = RepoContextValues.ReadString(_harness.ReadRepoNode()!.LastIngested);
        await Task.Delay(20);
        await _harness.Service.RunAsync(_harness.Request(), progress: null);
        var second = RepoContextValues.ReadString(_harness.ReadRepoNode()!.LastIngested);

        Assert.That(
            DateTimeOffset.Parse(second!, CultureInfo.InvariantCulture),
            Is.GreaterThan(DateTimeOffset.Parse(first!, CultureInfo.InvariantCulture)),
            "Each converged pass is a fresh verification and must re-stamp the marker.");
    }

    [Test]
    public async Task A_no_op_pass_preserves_authored_marker_metadata()
    {
        // The re-stamp runs on every converged pass, so a wholesale overwrite would
        // erase caller-patched metadata every reconcile interval.
        SeedConvergedFile();
        var clock = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        var tags = new OrSet();
        tags.Add(Encoding.UTF8.GetBytes("team-a"), "caller", 1);
        _harness.SeedRepoNode(StaleMarker(fileCount: 7) with
        {
            DisplayName = RepoContextValues.Lww("Acme Platform", clock),
            DefaultBranch = RepoContextValues.Lww("trunk", clock),
            Tags = tags,
        });

        await _harness.Service.RunAsync(_harness.Request(), progress: null);

        var marker = _harness.ReadRepoNode()!;
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(marker.LastIngested), Is.Not.EqualTo(StaleIngest));
            Assert.That(RepoContextValues.ReadInt64(marker.FileCount), Is.EqualTo(1),
                "The file count is index-derived and must be refreshed.");
            Assert.That(RepoContextValues.ReadString(marker.DisplayName), Is.EqualTo("Acme Platform"));
            Assert.That(RepoContextValues.ReadString(marker.DefaultBranch), Is.EqualTo("trunk"));
            Assert.That(marker.Tags.Contains(Encoding.UTF8.GetBytes("team-a")), Is.True);
        });
    }

    [Test]
    public async Task A_no_op_pass_clears_a_stale_commit_anchor_on_a_mounted_workspace()
    {
        // Matches what a changing mounted-workspace pass already writes: a commit
        // left over from an earlier git-ref generation must not be re-asserted
        // alongside a fresh lastIngested, or the anchor would claim a revision the
        // mount cannot verify.
        SeedConvergedFile();
        _harness.SeedRepoNode(StaleMarker(fileCount: 1) with
        {
            IndexedCommit = RepoContextValues.Lww("deadbeef", new HybridLogicalClock { WallClockTicks = 5 }),
        });

        await _harness.Service.RunAsync(_harness.Request(), progress: null);

        Assert.That(RepoContextValues.ReadString(_harness.ReadRepoNode()!.IndexedCommit), Is.Null);
    }

    [Test]
    public async Task A_no_op_git_ref_pass_stamps_its_commit_and_advances_last_ingested()
    {
        SeedConvergedFile();
        _harness.SeedRepoNode(StaleMarker(fileCount: 1));
        var request = new RepoContextBootstrapRequest
        {
            RepoRoot = _harness.Request().RepoRoot,
            RepoId = RepoId,
            CommitSha = "0123456789abcdef",
        };

        await _harness.Service.RunAsync(request, progress: null);

        var marker = _harness.ReadRepoNode()!;
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(marker.IndexedCommit), Is.EqualTo("0123456789abcdef"));
            Assert.That(RepoContextValues.ReadString(marker.LastIngested), Is.Not.EqualTo(StaleIngest));
        });
    }

    [Test]
    public async Task A_no_op_pass_writes_a_marker_when_none_is_stored()
    {
        SeedConvergedFile();

        await _harness.Service.RunAsync(_harness.Request(), progress: null);

        var marker = _harness.ReadRepoNode();
        Assert.That(marker, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(marker!.RepoId, Is.EqualTo(RepoId));
            Assert.That(RepoContextValues.ReadString(marker.LastIngested), Is.Not.Null);
            Assert.That(RepoContextValues.ReadInt64(marker.FileCount), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_no_op_pass_replaces_an_undecodable_marker_instead_of_failing()
    {
        SeedConvergedFile();
        _harness.SeedRepoNodeBytes([1]);

        var result = await _harness.Service.RunAsync(_harness.Request(), progress: null);

        var marker = _harness.ReadRepoNode();
        Assert.Multiple(() =>
        {
            Assert.That(result.FilesUnchanged, Is.EqualTo(1));
            Assert.That(RepoContextValues.ReadString(marker!.LastIngested), Is.Not.Null);
            Assert.That(RepoContextValues.ReadInt64(marker.FileCount), Is.EqualTo(1));
        });
    }

    private void SeedConvergedFile() =>
        SeedUnchanged(
            "done.cs", "class Done { }",
            symbolsProcessed: true, contentProcessed: true, tokenCount: 4, crossReferenced: true);

    private static RepoNode StaleMarker(long fileCount)
    {
        var clock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        return new RepoNode
        {
            RepoId = RepoId,
            LastIngested = RepoContextValues.Lww(StaleIngest, clock),
            FileCount = RepoContextValues.Lww(fileCount, clock),
        };
    }
}

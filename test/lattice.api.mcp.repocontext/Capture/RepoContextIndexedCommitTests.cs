using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Tests for the commit anchor a git-ref-sourced generation stamps on the repository
/// node. The anchor is what makes "which revision am I serving" answerable and
/// verifiable: it merges to spokes with the rest of the node, it is projected for
/// <c>recall</c> and reported by <c>list_repos</c>, and - because it is server-derived
/// from the configured ref - it must not be forgeable through the patch tool.
/// </summary>
[TestFixture]
public sealed class RepoContextIndexedCommitTests
{
    private const string FirstSha = "1111111111111111111111111111111111111111";
    private const string SecondSha = "2222222222222222222222222222222222222222";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public void RepoNode_defaults_to_no_commit_anchor()
    {
        Assert.That(RepoContextValues.ReadString(new RepoNode().IndexedCommit), Is.Null,
            "A mounted-workspace repository has no commit anchor.");
    }

    [Test]
    public void Merge_keeps_the_later_commit_anchor()
    {
        var merged = RepoNode.Merge(
            new RepoNode { RepoId = "acme", IndexedCommit = RepoContextValues.Lww(FirstSha, Clock(100)) },
            new RepoNode { RepoId = "acme", IndexedCommit = RepoContextValues.Lww(SecondSha, Clock(200)) });

        Assert.That(RepoContextValues.ReadString(merged.IndexedCommit), Is.EqualTo(SecondSha));
    }

    [Test]
    public void Merge_is_commutative_over_the_commit_anchor()
    {
        var left = new RepoNode { RepoId = "acme", IndexedCommit = RepoContextValues.Lww(FirstSha, Clock(100)) };
        var right = new RepoNode { RepoId = "acme", IndexedCommit = RepoContextValues.Lww(SecondSha, Clock(200)) };

        Assert.That(
            RepoContextValues.ReadString(RepoNode.Merge(right, left).IndexedCommit),
            Is.EqualTo(RepoContextValues.ReadString(RepoNode.Merge(left, right).IndexedCommit)),
            "A spoke must converge on the same anchor regardless of delivery order.");
    }

    [Test]
    public void Merge_carries_a_commit_anchor_onto_a_replica_that_has_none()
    {
        var merged = RepoNode.Merge(
            new RepoNode { RepoId = "acme" },
            new RepoNode { RepoId = "acme", IndexedCommit = RepoContextValues.Lww(FirstSha, Clock(200)) });

        Assert.That(RepoContextValues.ReadString(merged.IndexedCommit), Is.EqualTo(FirstSha),
            "A spoke that has never seen an anchor picks up the hub's.");
    }

    [Test]
    public void Project_flattens_the_commit_anchor_for_recall()
    {
        Assert.That(RepoContextKeys.TryParse(RepoContextKeys.Repo("acme"), out var key), Is.True);
        var record = new RepoNode
        {
            RepoId = "acme",
            IndexedCommit = RepoContextValues.Lww(FirstSha, Clock(1)),
        };

        var view = RepoContextEntryProjection.Project(
            key, Serializer.SerializeToArray(record), Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.That(view.Fields["indexedCommit"], Is.EqualTo(FirstSha));
    }

    [Test]
    public void Project_omits_the_commit_anchor_for_a_mounted_repository()
    {
        Assert.That(RepoContextKeys.TryParse(RepoContextKeys.Repo("acme"), out var key), Is.True);
        var record = new RepoNode { RepoId = "acme", DisplayName = RepoContextValues.Lww("Acme", Clock(1)) };

        var view = RepoContextEntryProjection.Project(
            key, Serializer.SerializeToArray(record), Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.That(view.Fields.ContainsKey("indexedCommit"), Is.False);
    }

    [Test]
    public void RepoSummary_reports_no_commit_anchor_by_default()
    {
        var summary = new RepoContextRepoSummary { RepoId = "acme" };

        Assert.That(summary.IndexedCommit, Is.Null);
    }

    [Test]
    public void RepoSummary_carries_the_commit_anchor()
    {
        var summary = new RepoContextRepoSummary { RepoId = "acme", IndexedCommit = FirstSha };

        Assert.That(summary.IndexedCommit, Is.EqualTo(FirstSha));
    }

    [Test]
    public void The_commit_anchor_is_not_patchable_through_the_record_editor()
    {
        Assert.That(RepoContextKeys.TryParse(RepoContextKeys.Repo("acme"), out var key), Is.True);
        var existing = Serializer.SerializeToArray(
            new RepoNode { RepoId = "acme", IndexedCommit = RepoContextValues.Lww(FirstSha, Clock(1)) });

        Assert.That(
            () => RepoContextRecordEditor.Patch(
                key,
                existing,
                new Dictionary<string, string>(StringComparer.Ordinal) { ["indexedCommit"] = SecondSha },
                addTags: null,
                removeTags: null,
                addLinks: null,
                removeLinks: null,
                Clock(200),
                Serializer),
            Throws.InstanceOf<ModelContextProtocol.McpException>(),
            "The anchor is server-derived from the configured ref; a client must never be able to forge it.");
    }
}

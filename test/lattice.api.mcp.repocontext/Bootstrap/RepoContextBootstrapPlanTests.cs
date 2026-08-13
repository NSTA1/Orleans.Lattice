namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="RepoContextBootstrapPlan"/>: the pure, digest-driven diff
/// that partitions a scan into added / updated / unchanged files and lists the
/// stored paths to prune. This is the engine of idempotent, resumable ingestion,
/// so it is covered exhaustively at the unit tier.
/// </summary>
[TestFixture]
public sealed class RepoContextBootstrapPlanTests
{
    private static RepoFileEntry Entry(string path, string digest)
        => new(path, digest, digest.Length, "csharp");

    private static RepoFileEntry StaleEntry(string path, string digest)
        => new(path, digest, digest.Length, "csharp") { AnchorStale = true };

    [Test]
    public void A_cold_scan_over_an_empty_store_adds_every_file()
    {
        var scanned = new[] { Entry("a.cs", "d1"), Entry("b.cs", "d2") };

        var plan = RepoContextBootstrapPlan.Compute(
            new Dictionary<string, string>(), scanned);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Added.Select(e => e.RelativePath), Is.EquivalentTo(new[] { "a.cs", "b.cs" }));
            Assert.That(plan.Updated, Is.Empty);
            Assert.That(plan.Unchanged, Is.Empty);
            Assert.That(plan.RemovedPaths, Is.Empty);
            Assert.That(plan.IsNoOp, Is.False);
        });
    }

    [Test]
    public void A_rescan_of_matching_digests_is_a_no_op()
    {
        var stored = new Dictionary<string, string> { ["a.cs"] = "d1", ["b.cs"] = "d2" };
        var scanned = new[] { Entry("a.cs", "d1"), Entry("b.cs", "d2") };

        var plan = RepoContextBootstrapPlan.Compute(stored, scanned);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Added, Is.Empty);
            Assert.That(plan.Updated, Is.Empty);
            Assert.That(plan.Unchanged.Select(e => e.RelativePath), Is.EquivalentTo(new[] { "a.cs", "b.cs" }));
            Assert.That(plan.RemovedPaths, Is.Empty);
            Assert.That(plan.IsNoOp, Is.True);
        });
    }

    [Test]
    public void A_changed_digest_is_an_update_not_an_add()
    {
        var stored = new Dictionary<string, string> { ["a.cs"] = "old" };
        var scanned = new[] { Entry("a.cs", "new") };

        var plan = RepoContextBootstrapPlan.Compute(stored, scanned);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Added, Is.Empty);
            Assert.That(plan.Updated.Single().RelativePath, Is.EqualTo("a.cs"));
            Assert.That(plan.Updated.Single().Digest, Is.EqualTo("new"));
            Assert.That(plan.Unchanged, Is.Empty);
            Assert.That(plan.IsNoOp, Is.False);
        });
    }

    [Test]
    public void A_stored_file_absent_from_the_scan_is_pruned()
    {
        var stored = new Dictionary<string, string> { ["keep.cs"] = "d1", ["gone.cs"] = "d2" };
        var scanned = new[] { Entry("keep.cs", "d1") };

        var plan = RepoContextBootstrapPlan.Compute(stored, scanned);

        Assert.Multiple(() =>
        {
            Assert.That(plan.RemovedPaths, Is.EqualTo(new[] { "gone.cs" }));
            Assert.That(plan.Unchanged.Single().RelativePath, Is.EqualTo("keep.cs"));
            Assert.That(plan.IsNoOp, Is.False);
        });
    }

    [Test]
    public void A_mixed_scan_partitions_every_category_at_once()
    {
        var stored = new Dictionary<string, string>
        {
            ["same.cs"] = "s",
            ["changed.cs"] = "old",
            ["removed.cs"] = "r",
        };
        var scanned = new[]
        {
            Entry("same.cs", "s"),
            Entry("changed.cs", "new"),
            Entry("added.cs", "a"),
        };

        var plan = RepoContextBootstrapPlan.Compute(stored, scanned);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Added.Single().RelativePath, Is.EqualTo("added.cs"));
            Assert.That(plan.Updated.Single().RelativePath, Is.EqualTo("changed.cs"));
            Assert.That(plan.Unchanged.Single().RelativePath, Is.EqualTo("same.cs"));
            Assert.That(plan.RemovedPaths, Is.EqualTo(new[] { "removed.cs" }));
        });
    }

    [Test]
    public void Removed_paths_are_ordered_deterministically()
    {
        var stored = new Dictionary<string, string>
        {
            ["c.cs"] = "1",
            ["a.cs"] = "2",
            ["b.cs"] = "3",
        };

        var plan = RepoContextBootstrapPlan.Compute(stored, Array.Empty<RepoFileEntry>());

        Assert.That(plan.RemovedPaths, Is.EqualTo(new[] { "a.cs", "b.cs", "c.cs" }));
    }

    [Test]
    public void A_touched_but_unchanged_file_is_metadata_changed_not_unchanged()
    {
        // Same digest as stored, but the walk flagged the anchor stale (the file was
        // touched). It must be rewritten to refresh the anchor, so it is not a no-op,
        // yet it is content-unchanged so it counts toward the live/unchanged tally.
        var stored = new Dictionary<string, string> { ["a.cs"] = "d1" };
        var scanned = new[] { StaleEntry("a.cs", "d1") };

        var plan = RepoContextBootstrapPlan.Compute(stored, scanned);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Added, Is.Empty);
            Assert.That(plan.Updated, Is.Empty);
            Assert.That(plan.Unchanged, Is.Empty);
            Assert.That(plan.MetadataChanged.Single().RelativePath, Is.EqualTo("a.cs"));
            Assert.That(plan.IsNoOp, Is.False);
            Assert.That(plan.LiveFileCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_changed_file_that_is_also_stale_is_an_update_not_metadata_changed()
    {
        // A genuine content change dominates the anchor-stale flag: the digest
        // differs from the stored digest, so it is an update (and is re-embedded).
        var stored = new Dictionary<string, string> { ["a.cs"] = "old" };
        var scanned = new[] { StaleEntry("a.cs", "new") };

        var plan = RepoContextBootstrapPlan.Compute(stored, scanned);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Updated.Single().RelativePath, Is.EqualTo("a.cs"));
            Assert.That(plan.MetadataChanged, Is.Empty);
        });
    }

    [Test]
    public void Compute_rejects_a_null_stored_map()
        => Assert.Throws<ArgumentNullException>(
            () => RepoContextBootstrapPlan.Compute(null!, Array.Empty<RepoFileEntry>()));

    [Test]
    public void Compute_rejects_a_null_scan()
        => Assert.Throws<ArgumentNullException>(
            () => RepoContextBootstrapPlan.Compute(new Dictionary<string, string>(), null!));
}

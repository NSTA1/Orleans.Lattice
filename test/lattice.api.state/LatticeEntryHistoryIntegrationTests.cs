namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// End-to-end coverage for the per-key change-history endpoint
/// (<see cref="ILatticeStateQuery.GetEntryHistoryAsync"/>) over a real cluster.
/// Drives the facade off the durable history view: a key's revisions come back
/// in the requested order with the right kind / origin / value-or-metadata per
/// the tree's retention mode, the per-revision retention descriptor and
/// top-level <see cref="EntryHistoryResult.Bound"/> are reported, paging is
/// stable, an OR-Set key's revisions carry decoded member changes, and a tree
/// with no history view falls back to the write-ahead-log window.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class LatticeEntryHistoryIntegrationTests
{
    private EntryHistoryClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new EntryHistoryClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static EntryHistoryRequest Request(string treeId, string key, int limit = 100, bool reverse = false) =>
        new() { TreeId = treeId, Key = key, Limit = limit, Reverse = reverse };

    [Test]
    public async Task GetEntryHistoryAsync_view_returns_revisions_oldest_first()
    {
        const string tree = "hist-order-src";
        const string view = "hist-order-view";
        var source = await _fixture.RegisterTreeAsync(tree);
        await _fixture.CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });
        await source.SetAsync("k", new byte[] { 3 });
        await _fixture.DrainToZeroAsync(view);

        var result = await _fixture.Query.GetEntryHistoryAsync(Request(tree, "k"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.Bound, Is.EqualTo(EntryHistoryBound.BoundedByAge));
            Assert.That(result.Revisions, Has.Count.EqualTo(3));
            Assert.That(result.Revisions.All(r => r.Kind == HistoryRowKind.Set), Is.True);
            Assert.That(result.Revisions.All(r => r.SourceKey == "k"), Is.True);
            Assert.That(result.Revisions.All(r => r.Category == MutationCategory.User), Is.True);
            Assert.That(result.Revisions.Select(r => r.Hlc).ToList(), Is.Ordered);
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_reverse_returns_newest_first()
    {
        const string tree = "hist-rev-src";
        const string view = "hist-rev-view";
        var source = await _fixture.RegisterTreeAsync(tree);
        await _fixture.CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });
        await source.SetAsync("k", new byte[] { 3 });
        await _fixture.DrainToZeroAsync(view);

        var result = await _fixture.Query.GetEntryHistoryAsync(Request(tree, "k", reverse: true));

        Assert.That(result.Revisions, Has.Count.EqualTo(3));
        Assert.That(result.Revisions.Select(r => r.Hlc).ToList(), Is.Ordered.Descending);
    }

    [Test]
    public async Task GetEntryHistoryAsync_metadata_only_strips_value_keeps_fingerprint()
    {
        const string tree = "hist-meta-src";
        const string view = "hist-meta-view";
        var source = await _fixture.RegisterTreeAsync(tree);
        await _fixture.CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 9, 9, 9, 9 });
        await _fixture.DrainToZeroAsync(view);

        var result = await _fixture.Query.GetEntryHistoryAsync(Request(tree, "k"));

        Assert.That(result.Revisions, Has.Count.EqualTo(1));
        var revision = result.Revisions[0];
        Assert.Multiple(() =>
        {
            Assert.That(revision.ValuePreview, Is.Null);
            Assert.That(revision.ValueLength, Is.EqualTo(4));
            Assert.That(revision.ValueHash, Is.Not.Zero);
            Assert.That(revision.Retention.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
            Assert.That(revision.Retention.ValueRetained, Is.False);
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_full_value_returns_value_preview()
    {
        const string tree = "hist-full-src";
        const string view = "hist-full-view";
        var source = await _fixture.RegisterTreeAsync(tree);
        await _fixture.CreateHistoryViewAsync(tree, view);

        await source.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null);
        await source.SetAsync("k", new byte[] { 5, 6, 7 });
        await _fixture.DrainToZeroAsync(view);

        var result = await _fixture.Query.GetEntryHistoryAsync(Request(tree, "k"));

        Assert.That(result.Revisions, Has.Count.EqualTo(1));
        var revision = result.Revisions[0];
        Assert.Multiple(() =>
        {
            Assert.That(revision.ValuePreview, Is.EqualTo(new byte[] { 5, 6, 7 }));
            Assert.That(revision.Retention.Mode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(revision.Retention.ValueRetained, Is.True);
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_records_delete_as_its_own_revision()
    {
        const string tree = "hist-del-src";
        const string view = "hist-del-view";
        var source = await _fixture.RegisterTreeAsync(tree);
        await _fixture.CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.DeleteAsync("k");
        await _fixture.DrainToZeroAsync(view);

        var result = await _fixture.Query.GetEntryHistoryAsync(Request(tree, "k"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Revisions, Has.Count.EqualTo(2));
            Assert.That(result.Revisions[0].Kind, Is.EqualTo(HistoryRowKind.Set));
            Assert.That(result.Revisions[1].Kind, Is.EqualTo(HistoryRowKind.Delete));
            Assert.That(result.Revisions[1].Retention.ValueRetained, Is.False);
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_pages_through_continuation_without_gaps_or_dupes()
    {
        const string tree = "hist-page-src";
        const string view = "hist-page-view";
        var source = await _fixture.RegisterTreeAsync(tree);
        await _fixture.CreateHistoryViewAsync(tree, view);

        const int total = 7;
        for (var i = 0; i < total; i++)
        {
            await source.SetAsync("k", new byte[] { (byte)i });
        }

        await _fixture.DrainToZeroAsync(view);

        var seen = new List<HybridLogicalClock>();
        string? continuation = null;
        for (var guard = 0; guard < 20; guard++)
        {
            var result = await _fixture.Query.GetEntryHistoryAsync(new EntryHistoryRequest
            {
                TreeId = tree,
                Key = "k",
                Limit = 2,
                ContinuationToken = continuation,
            });
            seen.AddRange(result.Revisions.Select(r => r.Hlc));
            continuation = result.ContinuationToken;
            if (continuation is null)
            {
                break;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(seen, Has.Count.EqualTo(total));
            Assert.That(seen, Is.Ordered);
            Assert.That(seen.Distinct().Count(), Is.EqualTo(total));
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_honours_hlc_bounds()
    {
        const string tree = "hist-bounds-src";
        const string view = "hist-bounds-view";
        var source = await _fixture.RegisterTreeAsync(tree);
        await _fixture.CreateHistoryViewAsync(tree, view);

        for (var i = 0; i < 5; i++)
        {
            await source.SetAsync("k", new byte[] { (byte)i });
        }

        await _fixture.DrainToZeroAsync(view);

        var all = await _fixture.Query.GetEntryHistoryAsync(Request(tree, "k"));
        Assert.That(all.Revisions, Has.Count.EqualTo(5));

        var from = all.Revisions[1].Hlc;
        var to = all.Revisions[3].Hlc;

        var bounded = await _fixture.Query.GetEntryHistoryAsync(new EntryHistoryRequest
        {
            TreeId = tree,
            Key = "k",
            FromHlc = from,
            ToHlc = to,
            Limit = 100,
        });

        Assert.Multiple(() =>
        {
            Assert.That(bounded.Revisions, Has.Count.EqualTo(3));
            Assert.That(bounded.Revisions[0].Hlc, Is.EqualTo(from));
            Assert.That(bounded.Revisions[^1].Hlc, Is.EqualTo(to));
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_unknown_tree_returns_tree_not_found()
    {
        var result = await _fixture.Query.GetEntryHistoryAsync(Request("hist-no-such-tree", "k"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(result.Revisions, Is.Empty);
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_without_history_view_falls_back_to_wal_window()
    {
        const string tree = "hist-walfallback-src";
        var source = await _fixture.RegisterTreeAsync(tree);

        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });

        var result = await _fixture.Query.GetEntryHistoryAsync(Request(tree, "k"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.Bound, Is.EqualTo(EntryHistoryBound.WalWindowFallback));
            Assert.That(result.Revisions, Is.Not.Empty);
        });
    }

    [Test]
    public async Task GetEntryHistoryAsync_orset_decodes_member_changes()
    {
        const string tree = "orset-hist-src";
        const string view = "orset-hist-view";
        var source = await _fixture.RegisterTreeAsync(tree);
        await _fixture.CreateHistoryViewAsync(tree, view);

        await source.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null);
        await source.OrSet("k").AddAsync(EntryHistoryClusterFixture.Utf8("alpha"), "replica-a");
        await _fixture.DrainToZeroAsync(view);

        var result = await _fixture.Query.GetEntryHistoryAsync(Request(tree, "k"));

        Assert.That(result.Revisions, Is.Not.Empty);
        var added = result.Revisions
            .SelectMany(r => r.MemberChanges)
            .Where(m => m.Kind == CrdtMemberChangeKind.Added)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(result.Revisions.Any(r => r.Mode == LatticeMergeMode.OrSet), Is.True);
            Assert.That(added, Is.Not.Empty, "an OR-Set add should decode to an Added member change");
            Assert.That(added.Any(m => m.Element.SequenceEqual(EntryHistoryClusterFixture.Utf8("alpha"))), Is.True);
        });
    }
}

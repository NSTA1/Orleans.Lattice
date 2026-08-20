using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the static factory methods on the <c>State/StateQueryResults.cs</c>
/// typed result records - the <c>Found</c> / <c>NotFound</c> / <c>KeyNotFound</c>
/// / <c>IndexNotFound</c> builders and their <see cref="ArgumentNullException"/>
/// guards. The serialization fixture only round-trips uninitialised instances,
/// so none of this construction logic is otherwise covered.
/// </summary>
[TestFixture]
public class StateQueryResultsTests
{
    private static TreeStateSummary Summary(string treeId) => new() { TreeId = treeId };

    private static EntryRecord Entry(string key) => new() { Key = key };

    [Test]
    public void TreeSummaryResult_Found_populates_status_tree_and_summary()
    {
        var summary = Summary("orders");

        var result = TreeSummaryResult.Found(summary);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.TreeId, Is.EqualTo("orders"));
        Assert.That(result.Summary, Is.SameAs(summary));
    }

    [Test]
    public void TreeSummaryResult_Found_throws_for_null_summary()
        => Assert.That(() => TreeSummaryResult.Found(null!), Throws.ArgumentNullException);

    [Test]
    public void TreeSummaryResult_NotFound_sets_tree_not_found_and_no_summary()
    {
        var result = TreeSummaryResult.NotFound("orders");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.TreeId, Is.EqualTo("orders"));
        Assert.That(result.Summary, Is.Null);
    }

    [Test]
    public void TreeSummaryResult_NotFound_throws_for_null_tree()
        => Assert.That(() => TreeSummaryResult.NotFound(null!), Throws.ArgumentNullException);

    [Test]
    public void ShardSummariesResult_Found_populates_shards()
    {
        var shards = new[] { new ShardStateSummary() };

        var result = ShardSummariesResult.Found("orders", shards);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.TreeId, Is.EqualTo("orders"));
        Assert.That(result.Shards, Is.SameAs(shards));
    }

    [Test]
    public void ShardSummariesResult_Found_throws_for_null_tree()
        => Assert.That(() => ShardSummariesResult.Found(null!, Array.Empty<ShardStateSummary>()),
            Throws.ArgumentNullException);

    [Test]
    public void ShardSummariesResult_Found_throws_for_null_shards()
        => Assert.That(() => ShardSummariesResult.Found("orders", null!), Throws.ArgumentNullException);

    [Test]
    public void ShardSummariesResult_NotFound_sets_tree_not_found_with_empty_shards()
    {
        var result = ShardSummariesResult.NotFound("orders");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Shards, Is.Empty);
    }

    [Test]
    public void ShardSummariesResult_NotFound_throws_for_null_tree()
        => Assert.That(() => ShardSummariesResult.NotFound(null!), Throws.ArgumentNullException);

    [Test]
    public void TreeStructureResult_Found_populates_roots_and_truncation()
    {
        var roots = new[] { new NodeStateSummary { NodeId = "n0" } };

        var result = TreeStructureResult.Found("orders", roots, truncated: true);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.TreeId, Is.EqualTo("orders"));
        Assert.That(result.Roots, Is.SameAs(roots));
        Assert.That(result.Truncated, Is.True);
    }

    [Test]
    public void TreeStructureResult_Found_throws_for_null_tree()
        => Assert.That(() => TreeStructureResult.Found(null!, Array.Empty<NodeStateSummary>(), false),
            Throws.ArgumentNullException);

    [Test]
    public void TreeStructureResult_Found_throws_for_null_roots()
        => Assert.That(() => TreeStructureResult.Found("orders", null!, false), Throws.ArgumentNullException);

    [Test]
    public void TreeStructureResult_NotFound_sets_tree_not_found_with_empty_roots()
    {
        var result = TreeStructureResult.NotFound("orders");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Roots, Is.Empty);
    }

    [Test]
    public void TreeStructureResult_NotFound_throws_for_null_tree()
        => Assert.That(() => TreeStructureResult.NotFound(null!), Throws.ArgumentNullException);

    [Test]
    public void EntryScanResult_Found_populates_entries_and_token()
    {
        var entries = new[] { Entry("a") };

        var result = EntryScanResult.Found("orders", entries, "next");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entries, Is.SameAs(entries));
        Assert.That(result.ContinuationToken, Is.EqualTo("next"));
    }

    [Test]
    public void EntryScanResult_Found_throws_for_null_tree()
        => Assert.That(() => EntryScanResult.Found(null!, Array.Empty<EntryRecord>(), null),
            Throws.ArgumentNullException);

    [Test]
    public void EntryScanResult_Found_throws_for_null_entries()
        => Assert.That(() => EntryScanResult.Found("orders", null!, null), Throws.ArgumentNullException);

    [Test]
    public void EntryScanResult_NotFound_sets_tree_not_found()
    {
        var result = EntryScanResult.NotFound("orders");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Entries, Is.Empty);
    }

    [Test]
    public void EntryScanResult_NotFound_throws_for_null_tree()
        => Assert.That(() => EntryScanResult.NotFound(null!), Throws.ArgumentNullException);

    [Test]
    public void EntryScanResult_IndexNotFound_sets_index_not_found()
    {
        var result = EntryScanResult.IndexNotFound("orders");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.IndexNotFound));
        Assert.That(result.TreeId, Is.EqualTo("orders"));
    }

    [Test]
    public void EntryScanResult_IndexNotFound_throws_for_null_tree()
        => Assert.That(() => EntryScanResult.IndexNotFound(null!), Throws.ArgumentNullException);

    [Test]
    public void EntryDetailResult_Found_takes_key_from_entry()
    {
        var entry = Entry("k1");

        var result = EntryDetailResult.Found("orders", entry);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Key, Is.EqualTo("k1"));
        Assert.That(result.Entry, Is.SameAs(entry));
    }

    [Test]
    public void EntryDetailResult_Found_throws_for_null_tree()
        => Assert.That(() => EntryDetailResult.Found(null!, Entry("k")), Throws.ArgumentNullException);

    [Test]
    public void EntryDetailResult_Found_throws_for_null_entry()
        => Assert.That(() => EntryDetailResult.Found("orders", null!), Throws.ArgumentNullException);

    [Test]
    public void EntryDetailResult_TreeNotFound_sets_status_and_key()
    {
        var result = EntryDetailResult.TreeNotFound("orders", "k1");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Key, Is.EqualTo("k1"));
        Assert.That(result.Entry, Is.Null);
    }

    [Test]
    public void EntryDetailResult_TreeNotFound_throws_for_null_tree()
        => Assert.That(() => EntryDetailResult.TreeNotFound(null!, "k"), Throws.ArgumentNullException);

    [Test]
    public void EntryDetailResult_TreeNotFound_throws_for_null_key()
        => Assert.That(() => EntryDetailResult.TreeNotFound("orders", null!), Throws.ArgumentNullException);

    [Test]
    public void EntryDetailResult_KeyNotFound_sets_status_and_key()
    {
        var result = EntryDetailResult.KeyNotFound("orders", "k1");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
        Assert.That(result.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void EntryDetailResult_KeyNotFound_throws_for_null_tree()
        => Assert.That(() => EntryDetailResult.KeyNotFound(null!, "k"), Throws.ArgumentNullException);

    [Test]
    public void EntryDetailResult_KeyNotFound_throws_for_null_key()
        => Assert.That(() => EntryDetailResult.KeyNotFound("orders", null!), Throws.ArgumentNullException);

    [Test]
    public void EntryHistoryResult_Found_populates_page_and_metadata()
    {
        var revisions = new[] { new EntryRevisionRecord { SourceKey = "k1" } };

        var result = EntryHistoryResult.Found(
            "orders", "k1", revisions, "next", EntryHistoryBound.Truncated, default);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Key, Is.EqualTo("k1"));
        Assert.That(result.Revisions, Is.SameAs(revisions));
        Assert.That(result.ContinuationToken, Is.EqualTo("next"));
        Assert.That(result.Bound, Is.EqualTo(EntryHistoryBound.Truncated));
    }

    [Test]
    public void EntryHistoryResult_Found_throws_for_null_tree()
        => Assert.That(() => EntryHistoryResult.Found(
            null!, "k", Array.Empty<EntryRevisionRecord>(), null, EntryHistoryBound.BoundedByAge, default),
            Throws.ArgumentNullException);

    [Test]
    public void EntryHistoryResult_Found_throws_for_null_key()
        => Assert.That(() => EntryHistoryResult.Found(
            "orders", null!, Array.Empty<EntryRevisionRecord>(), null, EntryHistoryBound.BoundedByAge, default),
            Throws.ArgumentNullException);

    [Test]
    public void EntryHistoryResult_Found_throws_for_null_revisions()
        => Assert.That(() => EntryHistoryResult.Found(
            "orders", "k", null!, null, EntryHistoryBound.BoundedByAge, default),
            Throws.ArgumentNullException);

    [Test]
    public void EntryHistoryResult_TreeNotFound_sets_status_and_key()
    {
        var result = EntryHistoryResult.TreeNotFound("orders", "k1");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Key, Is.EqualTo("k1"));
        Assert.That(result.Revisions, Is.Empty);
    }

    [Test]
    public void EntryHistoryResult_TreeNotFound_throws_for_null_tree()
        => Assert.That(() => EntryHistoryResult.TreeNotFound(null!, "k"), Throws.ArgumentNullException);

    [Test]
    public void EntryHistoryResult_TreeNotFound_throws_for_null_key()
        => Assert.That(() => EntryHistoryResult.TreeNotFound("orders", null!), Throws.ArgumentNullException);

    [Test]
    public void EntryHistoryResult_KeyNotFound_sets_status_and_key()
    {
        var result = EntryHistoryResult.KeyNotFound("orders", "k1");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
        Assert.That(result.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void EntryHistoryResult_KeyNotFound_throws_for_null_tree()
        => Assert.That(() => EntryHistoryResult.KeyNotFound(null!, "k"), Throws.ArgumentNullException);

    [Test]
    public void EntryHistoryResult_KeyNotFound_throws_for_null_key()
        => Assert.That(() => EntryHistoryResult.KeyNotFound("orders", null!), Throws.ArgumentNullException);
}

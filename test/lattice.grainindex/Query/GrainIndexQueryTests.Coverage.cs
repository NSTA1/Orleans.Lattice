namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// Coverage for execution paths not exercised by the core query tests: cursor
/// pagination with more than one page, snapshot-cursor payload reads, and the
/// defensive key-parsing guards in <c>TryReadGrainKey</c>.
/// </summary>
public sealed partial class GrainIndexQueryTests
{
    [Test]
    public async Task ToMatchesAsync_with_page_size_1_and_multiple_matches_iterates_beyond_first_page()
    {
        // Line 176 in GrainIndexQueryExecutor: the closing-brace continuation
        // point of the payloads branch when HasMore is true.  With PageSize=1
        // and 3 matches, the first NextEntriesAsync call returns HasMore=true
        // and execution must continue to the next while-iteration.
        var index = Populated();

        var matches = new List<GrainIndexMatch>();
        await foreach (var match in index.Index.Where(s => s.Age >= 18).WithPageSize(1).ToMatchesAsync())
            matches.Add(match);

        Assert.That(matches, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task Snapshot_cursor_with_payload_scan_and_no_residual_returns_results()
    {
        // Lines 256, 258: SnapshotCursor + payloads + null residual
        // (equality on an ordered property produces a point-range, no residual).
        var index = Populated();

        var matches = new List<GrainIndexMatch>();
        await foreach (var match in index.Index.Where(s => s.Age >= 18)
            .WithExecution(GrainIndexQueryExecution.SnapshotCursor)
            .ToMatchesAsync())
        {
            matches.Add(match);
        }

        Assert.That(matches.Select(m => m.GrainKey), Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Snapshot_cursor_with_payload_scan_and_residual_applies_the_predicate()
    {
        // Lines 256, 257: SnapshotCursor + payloads + non-null residual
        // (StartsWith produces a prefix range + a residual predicate).
        var index = Populated();

        var matches = new List<GrainIndexMatch>();
        await foreach (var match in index.Index.Where(s => s.Country.StartsWith("G"))
            .WithExecution(GrainIndexQueryExecution.SnapshotCursor)
            .ToMatchesAsync())
        {
            matches.Add(match);
        }

        Assert.That(matches.Select(m => m.GrainKey), Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task A_key_with_only_one_separator_is_silently_skipped()
    {
        // Lines 296-297: TryReadGrainKey returns false when the key contains
        // exactly one separator (no second separator for the grain-key slice).
        // The key "Age\u0000" is the exact range-start for the Age property and
        // sits at the bottom of every Age scan, so the select logic includes it.
        var index = QueryTestIndex.Create(
            ("alice", QueryTestIndex.State(age: 17, country: "GB", status: TestStatus.Active)));

        // Inject a malformed key at the very bottom of the Age range.
        index.Tree.Put("Age\u0000", []);

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 0));

        // The malformed key is skipped; only the real grain is returned.
        Assert.That(keys, Is.EquivalentTo(new[] { "alice" }));
    }
}

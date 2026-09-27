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
        // TryReadGrainKey returns false when the key holds exactly one separator,
        // so there is no second one to start the grain-key slice at.
        //
        // The scan range matters as much as the key. A relational clause such as
        // `Age >= 0` starts at the property's PRESENT bound (range start plus the
        // presence flag), which sorts strictly above the bare "Age\0" range start
        // - so an earlier revision of this test injected a malformed key that the
        // scan never visited, and passed without ever reaching the guard. A
        // constant-true predicate narrows nothing, so the planner falls back to
        // the first property's FULL range, whose inclusive lower bound is exactly
        // "Age\0".
        var index = QueryTestIndex.Create(
            ("alice", QueryTestIndex.State(age: 17, country: "GB", status: TestStatus.Active)));

        index.Tree.Put("Age\u0000", []);

        var keys = await KeysAsync(index.Index.Where(s => true));

        Assert.That(keys, Is.EqualTo(new[] { "alice" }),
            "the malformed entry must be skipped, not surfaced as a grain with an empty key");
    }

    [Test]
    public async Task A_malformed_key_is_skipped_on_the_payload_carrying_scan()
    {
        // The key-only and payload-carrying scans read the grain key through
        // separate call sites, so covering one says nothing about the other.
        var index = QueryTestIndex.Create(
            ("alice", QueryTestIndex.State(age: 17, country: "GB", status: TestStatus.Active)));

        index.Tree.Put("Age\u0000", [1, 2, 3]);

        var matches = new List<GrainIndexMatch>();
        await foreach (var match in index.Index.Where(s => true).ToMatchesAsync())
        {
            matches.Add(match);
        }

        Assert.That(matches.Select(m => m.GrainKey), Is.EqualTo(new[] { "alice" }));
    }

    [Test]
    public async Task A_malformed_key_is_skipped_on_the_streaming_scan()
    {
        // The default execution pages a durable cursor; the streaming scan is a
        // separate surface with its own grain-key read site, so covering one says
        // nothing about the other.
        var index = QueryTestIndex.Create(
            ("alice", QueryTestIndex.State(age: 17, country: "GB", status: TestStatus.Active)));

        index.Tree.Put("Age\u0000", []);

        var keys = new List<string>();
        await foreach (string key in index.Index.Where(s => true)
            .WithExecution(GrainIndexQueryExecution.Stream)
            .ToKeysAsync())
        {
            keys.Add(key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "alice" }));
    }

    [Test]
    public async Task A_malformed_key_is_skipped_on_a_streaming_scan_that_carries_payloads()
    {
        var index = QueryTestIndex.Create(
            ("alice", QueryTestIndex.State(age: 17, country: "GB", status: TestStatus.Active)));

        index.Tree.Put("Age\u0000", [9]);

        var matches = new List<GrainIndexMatch>();
        await foreach (var match in index.Index.Where(s => true)
            .WithExecution(GrainIndexQueryExecution.Stream)
            .ToMatchesAsync())
        {
            matches.Add(match);
        }

        Assert.That(matches.Select(m => m.GrainKey), Is.EqualTo(new[] { "alice" }));
    }

    [Test]
    public async Task A_malformed_key_in_an_intersect_pass_does_not_advance_a_candidate()
    {
        // A multi-property conjunction buffers the most selective clause and then
        // probes every later clause's raw entry keys. That probe has its own
        // grain-key read, and a malformed key must simply not advance anything -
        // rather than probe with an empty span and keep a candidate alive that the
        // second property never matched.
        var index = QueryTestIndex.Create(
            ("alice", QueryTestIndex.State(age: 17, country: "GB", status: TestStatus.Active)),
            ("bob", QueryTestIndex.State(age: 18, country: "FR", status: TestStatus.Retired)));

        // Sits at the inclusive lower bound of the Country complement range, so
        // the follow-up pass scans it.
        index.Tree.Put("Country\u0000", []);

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Country != "ZZ"));

        Assert.That(keys, Is.EqualTo(new[] { "bob" }));
    }

    [Test]
    public async Task A_malformed_key_in_a_union_branch_is_not_reported_as_a_grain()
    {
        // The key-only union branch de-duplicates through a span probe over the
        // tree's own key, which is a third grain-key read site.
        var index = QueryTestIndex.Create(
            ("alice", QueryTestIndex.State(age: 17, country: "GB", status: TestStatus.Active)),
            ("bob", QueryTestIndex.State(age: 18, country: "FR", status: TestStatus.Retired)));

        index.Tree.Put("Country\u0000", []);

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 40 || s.Country != "ZZ"));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob" }));
    }
}

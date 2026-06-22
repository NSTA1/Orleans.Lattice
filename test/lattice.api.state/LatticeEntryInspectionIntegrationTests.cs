using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// JSON-shaped value used to exercise server-side predicate push-down through
/// the entry-scan endpoint. The predicate evaluator matches member names
/// against the value's JSON document view.
/// </summary>
public sealed class ScanPerson
{
    public int Age { get; set; }
}

/// <summary>
/// Integration coverage for the entry / key-range inspection endpoint
/// (<see cref="ILatticeStateQuery.ScanEntriesAsync"/> and
/// <see cref="ILatticeStateQuery.GetEntryAsync"/>): snapshot-isolated paging,
/// key-range scoping, value-preview truncation, predicate push-down, and
/// single-key detail reads with metadata.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeEntryInspectionIntegrationTests
{
    private EntryInspectionClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new EntryInspectionClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private async Task<List<EntryRecord>> DrainAsync(EntryScanRequest request)
    {
        var all = new List<EntryRecord>();
        var next = request;
        while (true)
        {
            var page = await _fixture.Query.ScanEntriesAsync(next);
            Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
            all.AddRange(page.Entries);
            if (page.ContinuationToken is null)
            {
                break;
            }

            next = request with { ContinuationToken = page.ContinuationToken };
        }

        return all;
    }

    [Test]
    public async Task ScanEntries_not_found_for_unknown_tree()
    {
        var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest { TreeId = "no-such-tree" });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Entries, Is.Empty);
        Assert.That(result.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task ScanEntries_pages_all_entries_in_key_order_without_duplicates()
    {
        const int count = 50;
        await _fixture.CreatePopulatedTreeAsync("scan-paging", keyCount: count, shardCount: 3);

        var entries = await DrainAsync(new EntryScanRequest { TreeId = "scan-paging", PageSize = 7 });

        var keys = entries.Select(e => e.Key).ToArray();
        var expected = Enumerable.Range(0, count).Select(EntryInspectionClusterFixture.KeyAt).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(expected), "scan must return every key in ascending order");
            Assert.That(keys, Is.Unique, "a paged scan must not duplicate entries");
            Assert.That(entries.All(e => e.ValueLength > 0), Is.True, "every record must report its value length");
        });
    }

    [Test]
    public async Task ScanEntries_respects_key_range()
    {
        await _fixture.CreatePopulatedTreeAsync("scan-range", keyCount: 50, shardCount: 3);

        var entries = await DrainAsync(new EntryScanRequest
        {
            TreeId = "scan-range",
            StartInclusive = EntryInspectionClusterFixture.KeyAt(10),
            EndExclusive = EntryInspectionClusterFixture.KeyAt(20),
            PageSize = 4,
        });

        var keys = entries.Select(e => e.Key).ToArray();
        var expected = Enumerable.Range(10, 10).Select(EntryInspectionClusterFixture.KeyAt).ToArray();
        Assert.That(keys, Is.EqualTo(expected), "the scan must honour [startInclusive, endExclusive)");
    }

    [Test]
    public async Task ScanEntries_truncates_value_preview_to_budget()
    {
        var tree = await _fixture.RegisterTreeAsync("scan-truncate", shardCount: 1);
        var big = new byte[1000];
        Random.Shared.NextBytes(big);
        await tree.SetAsync("big", big);
        var exact = new byte[64];
        Random.Shared.NextBytes(exact);
        await tree.SetAsync("exact", exact);

        var entries = await DrainAsync(new EntryScanRequest
        {
            TreeId = "scan-truncate",
            ValuePreviewBudget = 64,
            PageSize = 100,
        });

        var bigRecord = entries.Single(e => e.Key == "big");
        var exactRecord = entries.Single(e => e.Key == "exact");

        Assert.Multiple(() =>
        {
            Assert.That(bigRecord.Truncated, Is.True, "an over-budget value must be flagged truncated");
            Assert.That(bigRecord.ValueLength, Is.EqualTo(1000), "the full length is always reported");
            Assert.That(bigRecord.ValuePreview, Has.Length.EqualTo(64), "the preview is clamped to the budget");
            Assert.That(bigRecord.ValuePreview, Is.EqualTo(big[..64]), "the preview is the value prefix");

            Assert.That(exactRecord.Truncated, Is.False, "a value at exactly the budget is not truncated");
            Assert.That(exactRecord.ValuePreview, Has.Length.EqualTo(64));
            Assert.That(exactRecord.ValueLength, Is.EqualTo(64));
        });
    }

    [Test]
    public async Task ScanEntries_is_snapshot_isolated_under_concurrent_writes()
    {
        const int count = 40;
        var tree = await _fixture.CreatePopulatedTreeAsync("scan-snapshot", keyCount: count, shardCount: 3);

        // Open the snapshot and read only the first page.
        var first = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest { TreeId = "scan-snapshot", PageSize = 5 });
        Assert.That(first.ContinuationToken, Is.Not.Null);

        // Mutate the tree after the snapshot was captured: insert new keys that
        // would otherwise fall into not-yet-read pages, and overwrite an
        // existing key.
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"key-{i:D5}-inserted", EntryInspectionClusterFixture.Utf8("late"));
        }
        await tree.SetAsync(EntryInspectionClusterFixture.KeyAt(count - 1), EntryInspectionClusterFixture.Utf8("overwritten"));

        // Drain the remaining pages against the same snapshot.
        var seen = new List<EntryRecord>(first.Entries);
        var token = first.ContinuationToken;
        while (token is not null)
        {
            var page = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = "scan-snapshot",
                PageSize = 5,
                ContinuationToken = token,
            });
            seen.AddRange(page.Entries);
            token = page.ContinuationToken;
        }

        var keys = seen.Select(e => e.Key).ToArray();
        var expected = Enumerable.Range(0, count).Select(EntryInspectionClusterFixture.KeyAt).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(expected), "the snapshot scan must reflect the point-in-time view");
            Assert.That(keys.Any(k => k.Contains("inserted")), Is.False, "post-snapshot inserts must not appear");
            Assert.That(keys, Is.Unique, "snapshot paging must not duplicate entries");
        });
    }

    [Test]
    public async Task ScanEntries_pushes_predicate_down_so_only_matching_entries_return()
    {
        var tree = await _fixture.RegisterTreeAsync("scan-predicate", shardCount: 2);
        for (var age = 0; age < 40; age++)
        {
            await tree.SetAsync($"person-{age:D3}", Encoding.UTF8.GetBytes($"{{\"Age\":{age}}}"));
        }

        var predicate = LatticePredicateTranslator.Translate<ScanPerson>(p => p.Age >= 18);
        var entries = await DrainAsync(new EntryScanRequest
        {
            TreeId = "scan-predicate",
            PageSize = 8,
            Predicate = predicate,
        });

        var ages = entries
            .Select(e => int.Parse(Encoding.UTF8.GetString(e.ValuePreview).Trim('{', '}').Split(':')[1]))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(entries, Has.Count.EqualTo(22), "ages 18..39 inclusive must match");
            Assert.That(ages, Is.All.GreaterThanOrEqualTo(18), "no non-matching value may cross the wire");
        });
    }

    [Test]
    public async Task GetEntry_returns_full_record_with_metadata()
    {
        var tree = await _fixture.RegisterTreeAsync("detail-found", shardCount: 1);
        await tree.SetAsync("k1", EntryInspectionClusterFixture.Utf8("hello-world"));

        var result = await _fixture.Query.GetEntryAsync("detail-found", "k1");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result.Entry!.Key, Is.EqualTo("k1"));
            Assert.That(Encoding.UTF8.GetString(result.Entry.ValuePreview), Is.EqualTo("hello-world"));
            Assert.That(result.Entry.ValueLength, Is.EqualTo("hello-world".Length));
            Assert.That(result.Entry.Truncated, Is.False);
            Assert.That(result.Entry.Hlc, Is.Not.EqualTo(HybridLogicalClock.Zero), "a live entry carries a non-zero HLC");
            Assert.That(result.Entry.ExpiresAtTicks, Is.EqualTo(0), "a non-TTL entry does not expire");
        });
    }

    [Test]
    public async Task GetEntry_reports_ttl_expiry()
    {
        var tree = await _fixture.RegisterTreeAsync("detail-ttl", shardCount: 1);
        await tree.SetAsync("k-ttl", EntryInspectionClusterFixture.Utf8("v"), TimeSpan.FromHours(1));

        var result = await _fixture.Query.GetEntryAsync("detail-ttl", "k-ttl");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry!.ExpiresAtTicks, Is.GreaterThan(0), "a TTL entry reports an absolute expiry tick");
    }

    [Test]
    public async Task GetEntry_tree_not_found()
    {
        var result = await _fixture.Query.GetEntryAsync("no-such-tree", "k");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Entry, Is.Null);
    }

    [Test]
    public async Task GetEntry_key_not_found()
    {
        await _fixture.RegisterTreeAsync("detail-missing", shardCount: 1);

        var result = await _fixture.Query.GetEntryAsync("detail-missing", "absent");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
        Assert.That(result.Entry, Is.Null);
    }

    [Test]
    public void GetEntry_cancellation_is_observed()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await _fixture.Query.GetEntryAsync("any", "k", cts.Token));
    }

    [Test]
    public async Task ScanEntries_excludes_tombstoned_keys()
    {
        var tree = await _fixture.CreatePopulatedTreeAsync("scan-tombstone", keyCount: 20, shardCount: 2);
        await tree.DeleteAsync(EntryInspectionClusterFixture.KeyAt(5));
        await tree.DeleteAsync(EntryInspectionClusterFixture.KeyAt(12));

        var entries = await DrainAsync(new EntryScanRequest { TreeId = "scan-tombstone", PageSize = 6 });

        var keys = entries.Select(e => e.Key).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Does.Not.Contain(EntryInspectionClusterFixture.KeyAt(5)), "a tombstoned key must not surface in a scan");
            Assert.That(keys, Does.Not.Contain(EntryInspectionClusterFixture.KeyAt(12)));
            Assert.That(keys, Has.Length.EqualTo(18), "only the 18 live keys remain");
        });
    }

    [Test]
    public async Task GetEntry_tombstoned_key_is_key_not_found()
    {
        var tree = await _fixture.RegisterTreeAsync("detail-tombstone", shardCount: 1);
        await tree.SetAsync("doomed", EntryInspectionClusterFixture.Utf8("v"));
        await tree.DeleteAsync("doomed");

        var result = await _fixture.Query.GetEntryAsync("detail-tombstone", "doomed");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.KeyNotFound),
            "a tombstoned key must read as missing, not as a live entry");
        Assert.That(result.Entry, Is.Null);
    }

    [Test]
    public async Task ScanEntries_reverse_returns_descending_key_order()
    {
        const int count = 30;
        await _fixture.CreatePopulatedTreeAsync("scan-reverse", keyCount: count, shardCount: 3);

        var entries = await DrainAsync(new EntryScanRequest { TreeId = "scan-reverse", Reverse = true, PageSize = 7 });

        var keys = entries.Select(e => e.Key).ToArray();
        var expected = Enumerable.Range(0, count).Reverse().Select(EntryInspectionClusterFixture.KeyAt).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(expected), "a reverse scan must return every key in descending order");
            Assert.That(keys, Is.Unique, "a reverse paged scan must not duplicate entries");
        });
    }

    [Test]
    public async Task ScanEntries_on_empty_tree_returns_found_with_no_entries()
    {
        await _fixture.RegisterTreeAsync("scan-empty", shardCount: 2);

        var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest { TreeId = "scan-empty", PageSize = 10 });

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found), "an empty but existing tree is Found, not NotFound");
            Assert.That(result.Entries, Is.Empty);
            Assert.That(result.ContinuationToken, Is.Null, "a drained empty scan must not leak a cursor");
        });
    }

    [Test]
    public async Task ScanEntries_treats_reserved_tree_as_not_found()
    {
        await _fixture.RegisterViewBackingTreeAsync("view-scan-probe");

        var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest { TreeId = "view-scan-probe" });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "reserved trees must be invisible to the scan surface");
        Assert.That(result.Entries, Is.Empty);
    }

    [Test]
    public async Task GetEntry_treats_reserved_tree_as_not_found()
    {
        await _fixture.RegisterViewBackingTreeAsync("view-detail-probe");

        var result = await _fixture.Query.GetEntryAsync("view-detail-probe", "k");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "reserved trees must be invisible to the detail surface");
        Assert.That(result.Entry, Is.Null);
    }

    [Test]
    public async Task ScanEntries_rejects_malformed_continuation_token_as_argument_error()
    {
        await _fixture.RegisterTreeAsync("scan-bad-token", shardCount: 2);

        var request = new EntryScanRequest
        {
            TreeId = "scan-bad-token",
            ContinuationToken = "not-a-real-cursor",
        };

        // A continuation token that names an unknown/stale cursor is a malformed
        // client request, not a server fault: it must surface as ArgumentException
        // rather than leaking an InvalidOperationException through the facade.
        Assert.ThrowsAsync<ArgumentException>(async () => await _fixture.Query.ScanEntriesAsync(request));
    }
}

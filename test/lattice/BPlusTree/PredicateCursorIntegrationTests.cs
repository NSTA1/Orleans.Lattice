using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for predicate-filtered stateful cursors: the compiled
/// predicate IR is persisted on the cursor spec and re-applied server-side on
/// every page, composing with range bounds, reverse order, point-in-time mode,
/// and the zero-observable-writes snapshot path.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PredicateCursorIntegrationTests
{
    private sealed record Scored(int Index, int Score);

    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"k-{i:D4}";

    private async Task<ILattice> SeedAsync(string treeId, int count)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        for (int i = 0; i < count; i++)
            await tree.SetAsync(KeyOf(i), new Scored(i, i));
        return tree;
    }

    private static async Task<List<string>> DrainKeysAsync(ILattice tree, string cursorId, int pageSize)
    {
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, pageSize);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);
        return collected;
    }

    [Test]
    public async Task Key_cursor_filters_pages_by_predicate()
    {
        var tree = await SeedAsync($"pcur-keys-{Guid.NewGuid():N}", 10);

        var cursorId = await tree.OpenKeyCursorAsync<Scored>(s => s.Score >= 5);
        var collected = await DrainKeysAsync(tree, cursorId, 3);

        Assert.That(collected, Is.EqualTo(new[] { "k-0005", "k-0006", "k-0007", "k-0008", "k-0009" }));
    }

    [Test]
    public async Task Entry_cursor_filters_pages_by_predicate()
    {
        var tree = await SeedAsync($"pcur-ent-{Guid.NewGuid():N}", 10);

        var cursorId = await tree.OpenEntryCursorAsync<Scored>(s => s.Score < 4);
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 2);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected.Select(kv => kv.Key),
            Is.EqualTo(new[] { "k-0000", "k-0001", "k-0002", "k-0003" }));
        Assert.That(collected, Has.All.Matches<KeyValuePair<string, byte[]>>(
            kv => JsonLatticeSerializer<Scored>.Default.Deserialize(kv.Value).Score < 4));
    }

    [Test]
    public async Task Key_cursor_predicate_composes_with_range_bounds()
    {
        var tree = await SeedAsync($"pcur-range-{Guid.NewGuid():N}", 20);

        // Range [k-0005, k-0015) ∩ Score >= 8.
        var cursorId = await tree.OpenKeyCursorAsync<Scored>(
            s => s.Score >= 8,
            startInclusive: KeyOf(5),
            endExclusive: KeyOf(15));
        var collected = await DrainKeysAsync(tree, cursorId, 4);

        Assert.That(collected, Is.EqualTo(new[]
        {
            "k-0008", "k-0009", "k-0010", "k-0011", "k-0012", "k-0013", "k-0014",
        }));
    }

    [Test]
    public async Task Key_cursor_predicate_composes_with_reverse()
    {
        var tree = await SeedAsync($"pcur-rev-{Guid.NewGuid():N}", 8);

        var cursorId = await tree.OpenKeyCursorAsync<Scored>(s => s.Score >= 4, reverse: true);
        var collected = await DrainKeysAsync(tree, cursorId, 2);

        Assert.That(collected, Is.EqualTo(new[] { "k-0007", "k-0006", "k-0005", "k-0004" }));
    }

    [Test]
    public async Task Point_in_time_key_cursor_applies_predicate_across_pages()
    {
        var tree = await SeedAsync($"pcur-pit-{Guid.NewGuid():N}", 10);

        // A point-in-time cursor pins the saga-decision snapshot at open time;
        // the predicate must still filter every page over the pinned view.
        var cursorId = await tree.OpenKeyCursorAsync<Scored>(s => s.Score >= 5, pointInTime: true);
        var collected = await DrainKeysAsync(tree, cursorId, 2);

        Assert.That(collected, Is.EqualTo(new[] { "k-0005", "k-0006", "k-0007", "k-0008", "k-0009" }));
    }

    [Test]
    public async Task Snapshot_key_cursor_filters_by_predicate()
    {
        var tree = await SeedAsync($"pcur-snap-{Guid.NewGuid():N}", 12);

        var cursorId = await tree.OpenSnapshotKeyCursorAsync<Scored>(s => s.Score >= 8);
        var collected = await DrainKeysAsync(tree, cursorId, 3);

        Assert.That(collected, Is.EqualTo(new[] { "k-0008", "k-0009", "k-0010", "k-0011" }));
    }

    [Test]
    public async Task Snapshot_entry_cursor_filters_by_predicate()
    {
        var tree = await SeedAsync($"pcur-snapent-{Guid.NewGuid():N}", 12);

        var cursorId = await tree.OpenSnapshotEntryCursorAsync<Scored>(s => s.Score < 3);
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 2);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected.Select(kv => kv.Key),
            Is.EqualTo(new[] { "k-0000", "k-0001", "k-0002" }));
    }

    [Test]
    public async Task Predicate_cursor_open_throws_on_unsupported_serializer()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"pcur-bad-{Guid.NewGuid():N}");

        Assert.ThrowsAsync<NotSupportedException>(() =>
            tree.OpenKeyCursorAsync<Scored>(s => s.Score >= 0, new OpaqueScoredSerializer()));
    }

    private sealed class OpaqueScoredSerializer : ILatticeSerializer<Scored>
    {
        public byte[] Serialize(Scored value) => [];
        public Scored Deserialize(byte[] bytes) => null!;
    }
}

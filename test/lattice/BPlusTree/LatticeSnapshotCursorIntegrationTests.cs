using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for the zero-observable-writes snapshot cursor
/// surface (<c>OpenSnapshotKeyCursorAsync</c> /
/// <c>OpenSnapshotEntryCursorAsync</c>). Exercises end-to-end snapshot
/// capture, per-shard WAL-replay materialisation, page iteration, and
/// the snapshot's invisibility to writes that land after open time.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeSnapshotCursorIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<ILattice> SeedTreeAsync(string treeId, IEnumerable<string> keys)
    {
        var tree = await _fixture.CreateTreeAsync(treeId);
        foreach (var k in keys) await tree.SetAsync(k, Bytes(k));
        return tree;
    }

    [Test]
    public async Task Snapshot_key_cursor_returns_keys_at_open_time()
    {
        var tree = await SeedTreeAsync($"snap-keys-{Guid.NewGuid():N}",
            new[] { "alpha", "bravo", "charlie", "delta", "echo" });

        var cursorId = await tree.OpenSnapshotKeyCursorAsync();
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 2);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EquivalentTo(new[] { "alpha", "bravo", "charlie", "delta", "echo" }));
    }

    [Test]
    public async Task Snapshot_entry_cursor_returns_values_at_open_time()
    {
        var tree = await SeedTreeAsync($"snap-entries-{Guid.NewGuid():N}",
            new[] { "k1", "k2", "k3" });

        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 4);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected.Select(kv => kv.Key), Is.EquivalentTo(new[] { "k1", "k2", "k3" }));
        foreach (var kv in collected)
        {
            Assert.That(Encoding.UTF8.GetString(kv.Value), Is.EqualTo(kv.Key),
                "Snapshot must return the value present at open time.");
        }
    }

    [Test]
    public async Task Snapshot_cursor_hides_writes_landing_after_open()
    {
        var tree = await SeedTreeAsync($"snap-stable-{Guid.NewGuid():N}",
            new[] { "a", "b", "c" });

        var cursorId = await tree.OpenSnapshotKeyCursorAsync();

        // Mutations after snapshot open time must not be visible to
        // this cursor's subsequent pages.
        await tree.SetAsync("d", Bytes("d"));
        await tree.SetAsync("e", Bytes("e"));
        await tree.DeleteAsync("a");

        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 10);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EquivalentTo(new[] { "a", "b", "c" }),
            "Snapshot view must reflect the state at open time, not subsequent writes/deletes.");
    }

    [Test]
    public async Task Snapshot_cursor_returns_empty_for_empty_tree()
    {
        var tree = await _fixture.CreateTreeAsync($"snap-empty-{Guid.NewGuid():N}");

        var cursorId = await tree.OpenSnapshotKeyCursorAsync();
        var page = await tree.NextKeysAsync(cursorId, 10);
        await tree.CloseCursorAsync(cursorId);

        Assert.That(page.Keys, Is.Empty);
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task Snapshot_cursor_respects_range_filter()
    {
        var tree = await SeedTreeAsync($"snap-range-{Guid.NewGuid():N}",
            new[] { "a", "b", "c", "d", "e", "f" });

        var cursorId = await tree.OpenSnapshotKeyCursorAsync(
            startInclusive: "b",
            endExclusive: "e");
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 2);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EquivalentTo(new[] { "b", "c", "d" }));
    }

    [Test]
    public async Task Snapshot_key_cursor_reverse_pages_all_keys_descending()
    {
        // Regression: the per-shard snapshot-leaf fetch truncated to the first
        // `limit` keys (the smallest), so a reverse paged scan over multiple
        // shards yielded only the bottom of the range and dropped the rest. The
        // fetch must return the largest `limit` keys when the cursor is reverse.
        var keys = Enumerable.Range(0, 40).Select(i => $"key-{i:D4}").ToArray();
        var tree = await SeedTreeAsync($"snap-rev-keys-{Guid.NewGuid():N}", keys);

        var cursorId = await tree.OpenSnapshotKeyCursorAsync(reverse: true);
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 7);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        var expected = keys.Reverse().ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(collected, Is.EqualTo(expected), "a reverse snapshot scan must page every key in descending order");
            Assert.That(collected, Is.Unique);
        });
    }

    [Test]
    public async Task Snapshot_entry_cursor_reverse_pages_all_entries_descending()
    {
        var keys = Enumerable.Range(0, 40).Select(i => $"key-{i:D4}").ToArray();
        var tree = await SeedTreeAsync($"snap-rev-entries-{Guid.NewGuid():N}", keys);

        var cursorId = await tree.OpenSnapshotEntryCursorAsync(reverse: true);
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 6);
            collected.AddRange(page.Entries.Select(e => e.Key));
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EqualTo(keys.Reverse().ToArray()),
            "a reverse snapshot entry scan must page every entry in descending key order");
    }

    [Test]
    public async Task Snapshot_key_cursor_reverse_respects_range_filter()
    {
        var keys = Enumerable.Range(0, 20).Select(i => $"key-{i:D4}").ToArray();
        var tree = await SeedTreeAsync($"snap-rev-range-{Guid.NewGuid():N}", keys);

        var cursorId = await tree.OpenSnapshotKeyCursorAsync(
            startInclusive: "key-0005",
            endExclusive: "key-0012",
            reverse: true);
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 3);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        var expected = Enumerable.Range(5, 7).Reverse().Select(i => $"key-{i:D4}").ToArray();
        Assert.That(collected, Is.EqualTo(expected),
            "a reverse range scan must honour [startInclusive, endExclusive) and descend");
    }
}


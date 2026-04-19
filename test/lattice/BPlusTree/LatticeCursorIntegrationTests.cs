using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for stateful cursors (F-033): key/entry enumeration and
/// resumable range delete exposed on <see cref="ILattice"/>.
/// </summary>
[TestFixture]
public class LatticeCursorIntegrationTests
{
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
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<ILattice> SeedTreeAsync(string treeId, IEnumerable<string> keys)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        foreach (var k in keys) await tree.SetAsync(k, Bytes(k));
        return tree;
    }

    // --- Key cursor ---

    [Test]
    public async Task Key_cursor_streams_all_keys_across_multiple_pages()
    {
        var tree = await SeedTreeAsync($"cur-keys-{Guid.NewGuid():N}",
            new[] { "a", "b", "c", "d", "e", "f", "g" });

        var cursorId = await tree.OpenKeyCursorAsync();
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 3);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EqualTo(new[] { "a", "b", "c", "d", "e", "f", "g" }));
    }

    [Test]
    public async Task Key_cursor_respects_range_filter()
    {
        var tree = await SeedTreeAsync($"cur-range-{Guid.NewGuid():N}",
            new[] { "a", "b", "c", "d", "e", "f" });

        var cursorId = await tree.OpenKeyCursorAsync(startInclusive: "b", endExclusive: "e");
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 2);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EqualTo(new[] { "b", "c", "d" }));
    }

    [Test]
    public async Task Key_cursor_reverse_returns_descending()
    {
        var tree = await SeedTreeAsync($"cur-rev-{Guid.NewGuid():N}",
            new[] { "a", "b", "c", "d" });

        var cursorId = await tree.OpenKeyCursorAsync(reverse: true);
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 2);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EqualTo(new[] { "d", "c", "b", "a" }));
    }

    [Test]
    public async Task Key_cursor_on_empty_tree_returns_empty_page()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"cur-empty-{Guid.NewGuid():N}");
        var cursorId = await tree.OpenKeyCursorAsync();

        var page = await tree.NextKeysAsync(cursorId, 10);

        Assert.That(page.Keys, Is.Empty);
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task Key_cursor_throws_on_null_cursor_id()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"cur-null-{Guid.NewGuid():N}");
        Assert.ThrowsAsync<ArgumentNullException>(() => tree.NextKeysAsync(null!, 10));
        Assert.ThrowsAsync<ArgumentNullException>(() => tree.NextEntriesAsync(null!, 10));
        Assert.ThrowsAsync<ArgumentNullException>(() => tree.DeleteRangeStepAsync(null!, 10));
        Assert.ThrowsAsync<ArgumentNullException>(() => tree.CloseCursorAsync(null!));
    }

    // --- Entry cursor ---

    [Test]
    public async Task Entry_cursor_streams_entries_in_order()
    {
        var tree = await SeedTreeAsync($"cur-ent-{Guid.NewGuid():N}",
            new[] { "a", "b", "c", "d", "e" });

        var cursorId = await tree.OpenEntryCursorAsync();
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 2);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected.Select(kv => kv.Key), Is.EqualTo(new[] { "a", "b", "c", "d", "e" }));
        Assert.That(collected.All(kv => Encoding.UTF8.GetString(kv.Value) == kv.Key), Is.True);
    }

    // --- Delete range cursor ---

    [Test]
    public async Task DeleteRange_cursor_deletes_range_across_multiple_steps()
    {
        var treeId = $"cur-del-{Guid.NewGuid():N}";
        var tree = await SeedTreeAsync(treeId,
            new[] { "a", "b", "c", "d", "e", "f", "g", "h" });

        var cursorId = await tree.OpenDeleteRangeCursorAsync("b", "g");
        var totalDeleted = 0;
        while (true)
        {
            var progress = await tree.DeleteRangeStepAsync(cursorId, 2);
            totalDeleted = progress.DeletedTotal;
            if (progress.IsComplete) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(totalDeleted, Is.EqualTo(5)); // b,c,d,e,f

        var remaining = new List<string>();
        await foreach (var k in tree.KeysAsync()) remaining.Add(k);
        Assert.That(remaining, Is.EqualTo(new[] { "a", "g", "h" }));
    }

    [Test]
    public async Task DeleteRange_cursor_completes_immediately_on_empty_range()
    {
        var tree = await SeedTreeAsync($"cur-del-empty-{Guid.NewGuid():N}",
            new[] { "a", "b" });

        var cursorId = await tree.OpenDeleteRangeCursorAsync("m", "z");
        var progress = await tree.DeleteRangeStepAsync(cursorId, 10);

        Assert.That(progress.IsComplete, Is.True);
        Assert.That(progress.DeletedTotal, Is.EqualTo(0));
    }

    [Test]
    public async Task DeleteRange_cursor_open_throws_on_null_bounds()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"cur-del-null-{Guid.NewGuid():N}");
        Assert.ThrowsAsync<ArgumentNullException>(() => tree.OpenDeleteRangeCursorAsync(null!, "z"));
        Assert.ThrowsAsync<ArgumentNullException>(() => tree.OpenDeleteRangeCursorAsync("a", null!));
    }

    [Test]
    public async Task Cursor_kind_mismatch_throws_invalid_operation()
    {
        var tree = await SeedTreeAsync($"cur-mism-{Guid.NewGuid():N}", new[] { "a" });
        var cursorId = await tree.OpenKeyCursorAsync();

        Assert.ThrowsAsync<InvalidOperationException>(
            () => tree.NextEntriesAsync(cursorId, 10));
    }

    [Test]
    public async Task Cursor_survives_multiple_paging_iterations_exact_page_size()
    {
        // Edge case: items exactly divisible by pageSize. HasMore should become
        // false only after the final empty fetch — we accept that the cursor
        // may report HasMore=true on the last non-empty page.
        var tree = await SeedTreeAsync($"cur-exact-{Guid.NewGuid():N}",
            new[] { "a", "b", "c", "d" });
        var cursorId = await tree.OpenKeyCursorAsync();

        var collected = new List<string>();
        for (int i = 0; i < 10; i++)
        {
            var page = await tree.NextKeysAsync(cursorId, 2);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EqualTo(new[] { "a", "b", "c", "d" }));
    }
}

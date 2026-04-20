using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests that verify TTL (<c>LwwValue.ExpiresAtTicks</c>) is
/// preserved and filtered correctly across topology changes and on every
/// read path exposed by <see cref="ILattice"/>.
/// </summary>
[TestFixture]
public class TtlIntegrationTests
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
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    // --- KeysAsync / EntriesAsync ---

    [Test]
    public async Task KeysAsync_omits_expired_entries_after_elapsed_ttl()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"ttl-keys-{Guid.NewGuid():N}");

        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("2"), TimeSpan.FromMilliseconds(200));
        await tree.SetAsync("c", Bytes("3"));
        await tree.SetAsync("d", Bytes("4"), TimeSpan.FromMilliseconds(200));
        await tree.SetAsync("e", Bytes("5"));

        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync()) keys.Add(k);

        Assert.That(keys, Is.EqualTo(new[] { "a", "c", "e" }),
            "KeysAsync must skip entries whose TTL has elapsed.");
    }

    [Test]
    public async Task EntriesAsync_omits_expired_entries_after_elapsed_ttl()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"ttl-entries-{Guid.NewGuid():N}");

        await tree.SetAsync("a", Bytes("A"));
        await tree.SetAsync("b", Bytes("B"), TimeSpan.FromMilliseconds(200));
        await tree.SetAsync("c", Bytes("C"));

        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var pairs = new List<KeyValuePair<string, byte[]>>();
        await foreach (var kv in tree.EntriesAsync()) pairs.Add(kv);

        Assert.That(pairs.Select(kv => kv.Key), Is.EqualTo(new[] { "a", "c" }));
        Assert.That(pairs.Select(kv => Encoding.UTF8.GetString(kv.Value)),
            Is.EqualTo(new[] { "A", "C" }));
    }

    // --- Stateful cursors ---

    [Test]
    public async Task KeyCursor_omits_expired_entries()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"ttl-curkeys-{Guid.NewGuid():N}");

        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("2"), TimeSpan.FromMilliseconds(200));
        await tree.SetAsync("c", Bytes("3"));

        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var cursorId = await tree.OpenKeyCursorAsync();
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 10);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Is.EqualTo(new[] { "a", "c" }));
    }

    [Test]
    public async Task EntryCursor_omits_expired_entries()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"ttl-curentries-{Guid.NewGuid():N}");

        await tree.SetAsync("a", Bytes("A"));
        await tree.SetAsync("b", Bytes("B"), TimeSpan.FromMilliseconds(200));
        await tree.SetAsync("c", Bytes("C"));

        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var cursorId = await tree.OpenEntryCursorAsync();
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 10);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected.Select(kv => kv.Key), Is.EqualTo(new[] { "a", "c" }));
    }
}

/// <summary>
/// Multi-shard TTL tests that exercise <see cref="ILattice.CountAsync"/>,
/// <see cref="ILattice.CountPerShardAsync"/>, merge, and the drain path on a
/// four-shard cluster.
/// </summary>
[TestFixture]
public class TtlMultiShardIntegrationTests
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
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    // --- CountAsync / CountPerShardAsync ---

    [Test]
    public async Task CountAsync_excludes_expired_entries_across_shards()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"ttl-count-{Guid.NewGuid():N}");

        for (int i = 0; i < 20; i++)
            await tree.SetAsync($"live-{i:D3}", Bytes("v"));

        for (int i = 0; i < 10; i++)
            await tree.SetAsync($"ttl-{i:D3}", Bytes("t"), TimeSpan.FromMilliseconds(200));

        Assert.That(await tree.CountAsync(), Is.EqualTo(30),
            "Pre-expiry count must include both live and TTL'd entries.");

        await Task.Delay(TimeSpan.FromMilliseconds(500));

        Assert.That(await tree.CountAsync(), Is.EqualTo(20),
            "CountAsync must exclude entries whose TTL has elapsed.");
    }

    [Test]
    public async Task CountPerShardAsync_excludes_expired_entries()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"ttl-pershard-{Guid.NewGuid():N}");

        for (int i = 0; i < 40; i++)
            await tree.SetAsync($"live-{i:D3}", Bytes("v"));

        for (int i = 0; i < 20; i++)
            await tree.SetAsync($"ttl-{i:D3}", Bytes("t"), TimeSpan.FromMilliseconds(200));

        var pre = await tree.CountPerShardAsync();
        Assert.That(pre.Sum(), Is.EqualTo(60));

        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var post = await tree.CountPerShardAsync();
        Assert.That(post.Sum(), Is.EqualTo(40),
            "Per-shard counts must exclude expired entries in aggregate.");
    }

    // --- Merge preserves TTL ---

    [Test]
    public async Task MergeAsync_preserves_TTL_on_target_tree()
    {
        var sourceTreeId = $"ttl-merge-src-{Guid.NewGuid():N}";
        var targetTreeId = $"ttl-merge-tgt-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTreeId);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTreeId);

        await source.SetAsync("no-ttl", Bytes("N"));
        await source.SetAsync("long-ttl", Bytes("L"), TimeSpan.FromHours(1));
        await source.SetAsync("short-ttl", Bytes("S"), TimeSpan.FromMilliseconds(400));

        // Ensure target tree is registered.
        await target.SetAsync("_init", [0]);
        await target.DeleteAsync("_init");

        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTreeId);
        await merge.MergeAsync(sourceTreeId);
        await merge.RunMergePassAsync();

        // All three keys live on target immediately after merge.
        Assert.That(await target.GetAsync("no-ttl"), Is.Not.Null);
        Assert.That(await target.GetAsync("long-ttl"), Is.Not.Null);
        Assert.That(await target.GetAsync("short-ttl"), Is.Not.Null);

        // Wait past short TTL — it must disappear on the target, proving
        // the absolute expiry crossed the merge boundary verbatim.
        await Task.Delay(TimeSpan.FromMilliseconds(700));
        Assert.That(await target.GetAsync("short-ttl"), Is.Null,
            "Short-TTL entry should have expired on target after merge.");
        Assert.That(await target.GetAsync("no-ttl"), Is.Not.Null);
        Assert.That(await target.GetAsync("long-ttl"), Is.Not.Null);
    }

    // --- Adaptive split: drain phase preserves TTL ---

    [Test]
    public async Task Split_drain_preserves_TTL_on_target_shard()
    {
        // Distinct from the shadow-forward TTL test in ShardSplitIntegrationTests:
        // here we write TTL'd entries BEFORE SplitAsync is invoked. Those entries
        // are migrated to the target shard via the drain path
        // (TreeShardSplitGrain -> IBPlusLeafGrain.MergeManyAsync), not via
        // shadow-forward. ExpiresAtTicks must survive the copy verbatim.
        var treeId = $"ttl-drain-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Seed filler keys so shard 0 owns multiple virtual slots that will move.
        for (int i = 0; i < 200; i++)
            await tree.SetAsync($"seed-{i:D4}", Bytes($"v{i}"));

        // TTL'd entries written BEFORE split starts — these are drained.
        var ttl = TimeSpan.FromMilliseconds(900);
        var ttlKeys = new List<string>();
        for (int i = 0; i < 40; i++)
        {
            var k = $"ttl-{i:D3}";
            ttlKeys.Add(k);
            await tree.SetAsync(k, Bytes($"e{i}"), ttl);
        }

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsCompleteAsync(), Is.True);

        // Immediately after split: every TTL'd key is still live.
        foreach (var k in ttlKeys)
            Assert.That(await tree.GetAsync(k), Is.Not.Null,
                $"TTL'd key '{k}' should be live immediately after drain.");

        // Wait past the TTL. Every TTL'd key must now read null.
        await Task.Delay(ttl + TimeSpan.FromMilliseconds(500));

        var leaked = new List<string>();
        foreach (var k in ttlKeys)
        {
            if (await tree.GetAsync(k) is not null)
                leaked.Add(k);
        }

        Assert.That(leaked, Is.Empty,
            $"TTL must survive drain: {leaked.Count} key(s) remained live past expiry " +
            $"({string.Join(", ", leaked.Take(5))}).");
    }
}

/// <summary>
/// Resize-specific TTL preservation test. Uses the small-leaf fixture so the
/// pre-resize tree contains real B+ splits that must be re-laid on the
/// destination physical tree.
/// </summary>
[TestFixture]
public class TtlResizeIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ResizeAsync_preserves_TTL()
    {
        var treeId = $"ttl-resize-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        await tree.SetAsync("no-ttl", Encoding.UTF8.GetBytes("N"));
        await tree.SetAsync("long-ttl", Encoding.UTF8.GetBytes("L"), TimeSpan.FromHours(1));
        await tree.SetAsync("short-ttl", Encoding.UTF8.GetBytes("S"), TimeSpan.FromMilliseconds(500));

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(newMaxLeafKeys: 32, newMaxInternalChildren: 32);
        await resize.RunResizePassAsync();

        // Immediately after resize: all three keys are live under the new alias.
        Assert.That(await tree.GetAsync("no-ttl"), Is.Not.Null);
        Assert.That(await tree.GetAsync("long-ttl"), Is.Not.Null);
        Assert.That(await tree.GetAsync("short-ttl"), Is.Not.Null);

        // Wait past short TTL — must expire on the post-resize physical tree.
        await Task.Delay(TimeSpan.FromMilliseconds(800));
        Assert.That(await tree.GetAsync("short-ttl"), Is.Null,
            "Short-TTL entry should have expired after resize.");
        Assert.That(await tree.GetAsync("no-ttl"), Is.Not.Null);
        Assert.That(await tree.GetAsync("long-ttl"), Is.Not.Null);
    }
}

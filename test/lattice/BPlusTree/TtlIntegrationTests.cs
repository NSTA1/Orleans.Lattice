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
[Category("Integration")]
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
        await foreach (var kv in tree.ScanEntriesAsync()) pairs.Add(kv);

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
[Category("Integration")]
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

        // The no-TTL and long-TTL (1h) entries are live on the target
        // immediately after the merge. The long-TTL sentinel is the non-racy
        // proof that a TTL-bearing entry crosses the merge boundary with its
        // future absolute expiry intact. The short-TTL entry is deliberately
        // NOT asserted live here: with a 400 ms budget it races a merge saga
        // that legitimately runs longer than the TTL under load (see #2076),
        // so its only reliable assertion is that it is gone once we have
        // waited past its expiry (below).
        Assert.That(await target.GetAsync("no-ttl"), Is.Not.Null);
        Assert.That(await target.GetAsync("long-ttl"), Is.Not.Null);

        // Wait past short TTL - it must disappear on the target, proving the
        // absolute expiry crossed the merge boundary verbatim (a merge that
        // stripped or extended the TTL would leave it live here forever).
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

        // TTL'd entries written BEFORE split starts - these are drained.
        // A handful carry a long (1h) TTL: they are the non-racy proof that a
        // TTL-bearing entry is carried across the drain live with its future
        // absolute expiry intact. The rest carry a short 900 ms TTL and must be
        // gone once we wait past it. Splitting the same ttl-NNN family this way
        // keeps both cohorts under the identical shard-ownership distribution.
        var shortTtl = TimeSpan.FromMilliseconds(900);
        var longTtl = TimeSpan.FromHours(1);
        var shortTtlKeys = new List<string>();
        var longTtlKeys = new List<string>();
        for (int i = 0; i < 40; i++)
        {
            var k = $"ttl-{i:D3}";
            if (i % 8 == 0)
            {
                longTtlKeys.Add(k);
                await tree.SetAsync(k, Bytes($"e{i}"), longTtl);
            }
            else
            {
                shortTtlKeys.Add(k);
                await tree.SetAsync(k, Bytes($"e{i}"), shortTtl);
            }
        }

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);

        // The long-TTL sentinels crossed the drain live and keep their future
        // expiry - asserted immediately, which never races because 1h dwarfs
        // the split saga's duration on any runner.
        foreach (var k in longTtlKeys)
            Assert.That(await tree.GetAsync(k), Is.Not.Null,
                $"Long-TTL key '{k}' should be live after drain.");

        // Wait past the short TTL. Every short-TTL key must now read null - a
        // drain that stripped or extended their expiry would leave them live.
        await Task.Delay(shortTtl + TimeSpan.FromMilliseconds(500));

        var leaked = new List<string>();
        foreach (var k in shortTtlKeys)
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
[Category("Integration")]
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

        // Immediately after resize: the no-TTL and long-TTL (1h) entries are
        // live under the new alias. The long-TTL sentinel is the non-racy proof
        // that a TTL-bearing entry is re-laid on the destination physical tree
        // with its future absolute expiry intact. The short-TTL entry is
        // deliberately NOT asserted live here: with a 500 ms budget it races a
        // resize saga that legitimately runs longer than the TTL under load
        // (see #2076), so its only reliable assertion is that it is gone after
        // we wait past its expiry (below).
        Assert.That(await tree.GetAsync("no-ttl"), Is.Not.Null);
        Assert.That(await tree.GetAsync("long-ttl"), Is.Not.Null);

        // Wait past short TTL - must expire on the post-resize physical tree (a
        // resize that stripped or extended the TTL would leave it live here).
        await Task.Delay(TimeSpan.FromMilliseconds(800));
        Assert.That(await tree.GetAsync("short-ttl"), Is.Null,
            "Short-TTL entry should have expired after resize.");
        Assert.That(await tree.GetAsync("no-ttl"), Is.Not.Null);
        Assert.That(await tree.GetAsync("long-ttl"), Is.Not.Null);
    }

    /// <summary>
    /// Deterministic regression guard for #2076: the resize invariant is
    /// "absolute expiry crosses the alias flip verbatim", which must not depend
    /// on how long the saga takes. This spends a budget larger than the short
    /// TTL <em>before</em> the saga - standing in for a loaded runner - and
    /// still proves the long-TTL entry is live afterwards and the short-TTL
    /// entry expires. The pre-#2076 test raced exactly this budget and failed
    /// when the saga overran the short TTL.
    /// </summary>
    [Test]
    public async Task ResizeAsync_preserves_long_TTL_regardless_of_saga_duration()
    {
        var treeId = $"ttl-resize-dur-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        await tree.SetAsync("no-ttl", Encoding.UTF8.GetBytes("N"));
        await tree.SetAsync("long-ttl", Encoding.UTF8.GetBytes("L"), TimeSpan.FromHours(1));
        await tree.SetAsync("short-ttl", Encoding.UTF8.GetBytes("S"), TimeSpan.FromMilliseconds(500));

        // Stand in for a loaded runner: burn well past the short TTL before the
        // saga even starts. The shipped test used to spend this budget inside
        // the saga and then race a liveness assertion against it.
        await Task.Delay(TimeSpan.FromMilliseconds(700));

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(newMaxLeafKeys: 32, newMaxInternalChildren: 32);
        await resize.RunResizePassAsync();

        // Long-TTL and no-TTL entries survive the alias flip live, independent
        // of how much wall clock the saga consumed.
        Assert.That(await tree.GetAsync("no-ttl"), Is.Not.Null);
        Assert.That(await tree.GetAsync("long-ttl"), Is.Not.Null,
            "Long-TTL entry must cross the resize boundary live regardless of saga duration.");

        // The short-TTL entry's absolute expiry was already in the past by the
        // time the saga ran, so it must read null - never live.
        Assert.That(await tree.GetAsync("short-ttl"), Is.Null,
            "Short-TTL entry's absolute expiry must be honored across the resize.");
    }
}

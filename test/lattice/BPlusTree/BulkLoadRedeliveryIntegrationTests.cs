using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #5460: a streamed bulk load - chunks appended through
/// <see cref="ILattice.BulkAppendChunkAsync"/> - must leave a tree whose full
/// range scan, live-key count and diagnostics each report exactly the committed
/// keys, including when chunks are re-delivered the way a resumed bulk-load
/// stream re-sends its unacknowledged window.
/// </summary>
/// <remarks>
/// Two defects produced the same symptoms. Bulk-built leaves were born declaring
/// no key range, and the WAL materialiser replays into a reactivating leaf every
/// record its declared range covers, so each leaf that deactivated
/// re-materialised every later key of its shard. And each shard deduplicated a
/// re-driven chunk only by its last-completed operation id, so a chunk re-sent
/// after a later one had landed was grafted a second time below the shard's
/// right edge. Either way the tree held duplicate rows (the count and the
/// statistics over-reported) and a leaf chain whose keys regressed, so a range
/// scan suppressed the regressed leaves and omitted keys that point reads still
/// found. Runs on two silos and four shards.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class BulkLoadRedeliveryIntegrationTests
{
    private const int ShardCount = 4;
    private const int MaxLeafKeys = 32;
    private const int ChunkSize = 1_000;

    private FourShardClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task A_streamed_bulk_load_scans_counts_and_reports_exactly_the_committed_keys()
    {
        const int keyCount = 24_000;
        var tree = await CreateTreeAsync("bulk-exact");
        var op = Guid.NewGuid().ToString("N");
        var chunks = Chunks(keyCount);

        for (var c = 0; c < chunks.Count; c++)
        {
            await tree.BulkAppendChunkAsync($"{op}/{c}", chunks[c]);
        }

        // Straight after the last chunk, scan while the stream's last window is
        // re-delivered, as a resume after a lost acknowledgement re-sends it.
        var redelivery = Task.Run(async () =>
        {
            for (var c = chunks.Count - 4; c < chunks.Count; c++)
            {
                await tree.BulkAppendChunkAsync($"{op}/{c}", chunks[c]);
            }
        });
        var concurrentScan = ScanAllKeysAsync(tree);
        await Task.WhenAll(redelivery, concurrentScan);

        await AssertExactAsync(tree, keyCount, await concurrentScan);
    }

    [Test]
    public async Task Re_driving_a_superseded_window_after_every_chunk_grafts_nothing_twice()
    {
        const int keyCount = 8_000;
        var tree = await CreateTreeAsync("bulk-redrive");
        var op = Guid.NewGuid().ToString("N");
        var chunks = Chunks(keyCount);

        for (var c = 0; c < chunks.Count; c++)
        {
            await tree.BulkAppendChunkAsync($"{op}/{c}", chunks[c]);

            // A resumed stream re-sends its whole unacknowledged window, so a
            // chunk a later one has already superseded on every shard arrives
            // again under its own operation id.
            if (c > 0)
            {
                await tree.BulkAppendChunkAsync($"{op}/{c - 1}", chunks[c - 1]);
                await tree.BulkAppendChunkAsync($"{op}/{c}", chunks[c]);
            }
        }

        await AssertExactAsync(tree, keyCount, await ScanAllKeysAsync(tree));

        var found = await tree.GetManyAsync(chunks.SelectMany(chunk => chunk.Select(e => e.Key)).ToList());
        Assert.That(found, Has.Count.EqualTo(keyCount), "a point read of every key finds it");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Every_leaf_of_a_bulk_loaded_tree_declares_its_range_and_reactivates_holding_only_its_own_keys(bool streamed)
    {
        // The WAL materialiser replays into a reactivating leaf every record its
        // declared range covers. Bulk-built leaves used to declare no range, so
        // each one that deactivated re-materialised every later key of its shard:
        // the count and statistics grew with every reactivation, and the leaf
        // chain regressed so range scans suppressed whole leaves.
        const int keyCount = 8_000;
        var tree = await CreateTreeAsync(streamed ? "bulk-replay-streamed" : "bulk-replay-oneshot");
        var chunks = Chunks(keyCount);
        if (streamed)
        {
            var op = Guid.NewGuid().ToString("N");
            for (var c = 0; c < chunks.Count; c++)
            {
                await tree.BulkAppendChunkAsync($"{op}/{c}", chunks[c]);
            }
        }
        else
        {
            await tree.BulkLoadAsync(chunks.SelectMany(chunk => chunk).ToList());
        }

        var leaves = new List<IBPlusLeafGrain>();
        for (var shardIndex = 0; shardIndex < ShardCount; shardIndex++)
        {
            var shard = _fixture.Cluster.GrainFactory.GetGrain<IShardRootGrain>($"{tree.GetPrimaryKeyString()}/{shardIndex}");
            var ranges = new List<LeafKeyRange>();
            for (var cursor = await shard.GetLeftmostLeafIdAsync(); cursor is { } id;)
            {
                var leaf = _fixture.Cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id);
                var link = await leaf.GetKeyRangeAndNextSiblingAsync();
                ranges.Add(link.KeyRange);
                leaves.Add(leaf);
                cursor = link.NextSibling;
            }

            Assert.That(ranges, Has.Count.GreaterThan(1), $"precondition: shard {shardIndex} holds a multi-leaf chain");
            Assert.Multiple(() =>
            {
                Assert.That(ranges[0].LowKeyInclusive, Is.Null, $"shard {shardIndex}: the first leaf is open below");
                Assert.That(ranges[^1].HighKeyExclusive, Is.Null, $"shard {shardIndex}: the last leaf is open above");
                for (var i = 1; i < ranges.Count; i++)
                {
                    Assert.That(ranges[i].LowKeyInclusive, Is.Not.Null.And.EqualTo(ranges[i - 1].HighKeyExclusive),
                        $"shard {shardIndex}: leaf {i} begins where leaf {i - 1} ends");
                }
            });
        }

        foreach (var leaf in leaves)
        {
            await leaf.ForceDeactivateAsync();
        }

        await AssertExactAsync(tree, keyCount, await ScanAllKeysAsync(tree));
    }

    [Test]
    public async Task Tree_statistics_read_while_a_streamed_load_runs_never_count_a_key_twice()
    {
        // The statistics walk yields every 64 leaves and re-descends to resume. A
        // resume that lands on a leaf an earlier batch already counted - as it
        // does while a graft has linked appended leaves into the chain but not yet
        // into their parent - used to count that leaf again (issue #5460). No
        // reading may then exceed the keys appended by the time it returned. This
        // is a coarse guard, since the window is narrow; the interleaving itself
        // is pinned deterministically by ShardRootGrainDiagnosticsResumeTests.
        const int keyCount = 24_000;
        var tree = await CreateTreeAsync("bulk-stats-race");
        var stats = _fixture.Cluster.Client.GetGrain<ILatticeStats>(tree.GetPrimaryKeyString());
        var op = Guid.NewGuid().ToString("N");
        var chunks = Chunks(keyCount);
        var appended = 0;

        var load = Task.Run(async () =>
        {
            for (var c = 0; c < chunks.Count; c++)
            {
                await tree.BulkAppendChunkAsync($"{op}/{c}", chunks[c]);
                Volatile.Write(ref appended, Volatile.Read(ref appended) + chunks[c].Count);
            }
        });

        var overCounts = new List<string>();
        var readings = 0;
        while (!load.IsCompleted)
        {
            await stats.InvalidateAsync();
            var report = await tree.DiagnoseAsync(deep: readings % 2 == 1);

            // Chunks are appended one at a time, so at most one is in flight and
            // partly visible when the reading returns.
            var bound = Volatile.Read(ref appended) + ChunkSize;
            readings++;
            if (report.TotalLiveKeys > bound)
            {
                overCounts.Add($"{report.TotalLiveKeys} live keys reported when at most {bound} could have been appended");
            }
        }

        await load;
        await stats.InvalidateAsync();
        var final = await tree.DiagnoseAsync(deep: false);

        Assert.Multiple(() =>
        {
            Assert.That(readings, Is.GreaterThan(1), "precondition: the statistics were read while the load ran");
            Assert.That(overCounts, Is.Empty, "no reading may count a key twice");
            Assert.That(final.TotalLiveKeys, Is.EqualTo(keyCount));
        });
    }

    [Test]
    public async Task A_chunk_below_a_shards_right_edge_that_was_never_loaded_is_refused()
    {
        var tree = await CreateTreeAsync("bulk-out-of-order");
        var op = Guid.NewGuid().ToString("N");
        await tree.BulkAppendChunkAsync($"{op}/0", Chunks(ChunkSize)[0]);

        var earlier = Enumerable.Range(0, 64)
            .Select(i => KeyValuePair.Create($"a/{i:D8}", Encoding.UTF8.GetBytes(i.ToString())))
            .ToList();

        Assert.That(async () => await tree.BulkAppendChunkAsync($"{op}/1", earlier),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(await tree.CountAsync(), Is.EqualTo(ChunkSize), "nothing out of order was grafted");
    }

    private async Task<ILattice> CreateTreeAsync(string prefix)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = MaxLeafKeys,
            ShardCount = ShardCount,
        });
        return _fixture.Cluster.Client.GetGrain<ILattice>(treeId);
    }

    private static List<List<KeyValuePair<string, byte[]>>> Chunks(int keyCount)
    {
        var chunks = new List<List<KeyValuePair<string, byte[]>>>();
        for (var start = 0; start < keyCount; start += ChunkSize)
        {
            chunks.Add(Enumerable.Range(start, Math.Min(ChunkSize, keyCount - start))
                .Select(i => KeyValuePair.Create(Key(i), Encoding.UTF8.GetBytes(i.ToString())))
                .ToList());
        }

        return chunks;
    }

    private static string Key(int i) => $"bulk/{i:D8}";

    /// <summary>Reads every key through the entry cursor, a page of 1,000 at a time, as a paged range read does.</summary>
    private static async Task<List<string>> ScanAllKeysAsync(ILattice tree)
    {
        var keys = new List<string>();
        var cursor = await tree.OpenEntryCursorAsync();
        try
        {
            while (true)
            {
                var page = await tree.NextEntriesAsync(cursor, ChunkSize);
                foreach (var entry in page.Entries)
                {
                    keys.Add(entry.Key);
                }

                if (!page.HasMore)
                {
                    break;
                }
            }
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }

        return keys;
    }

    private static async Task AssertExactAsync(ILattice tree, int keyCount, List<string> firstScan)
    {
        var expected = Enumerable.Range(0, keyCount).Select(Key).ToList();
        var scanned = await ScanAllKeysAsync(tree);
        var count = await tree.CountAsync();
        var shallow = await tree.DiagnoseAsync(deep: false);
        var deep = await tree.DiagnoseAsync(deep: true);

        Assert.Multiple(() =>
        {
            Assert.That(firstScan, Is.EqualTo(expected), "the first scan after the load returns every key once, in order");
            Assert.That(scanned, Is.EqualTo(expected), "a full scan returns every key once, in order");
            Assert.That(count, Is.EqualTo(keyCount), "the live-key count is exact");
            Assert.That(shallow.TotalLiveKeys, Is.EqualTo(keyCount), "the tree statistics are exact");
            Assert.That(deep.TotalLiveKeys, Is.EqualTo(keyCount), "the deep tree statistics are exact");
            Assert.That(deep.TotalTombstones, Is.Zero);
        });
    }
}

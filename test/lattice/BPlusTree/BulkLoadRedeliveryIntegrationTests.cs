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
/// Each shard deduplicated a re-driven chunk only by its last-completed
/// operation id, so a chunk re-sent after a later one had landed was grafted a
/// second time below the shard's right edge. The tree then held duplicate rows
/// (the count and the statistics over-reported) and a leaf chain whose keys
/// regressed, so a range scan suppressed the regressed leaves and omitted keys
/// that point reads still found. Runs on two silos and four shards.
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

using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #6160: after #6155 and #6157 the tree-size bulk seed still replayed the WAL
/// cold, so every chunk cost more than the last. A leaf a bulk operation creates must
/// start reading the WAL at the head the operation found, whatever the shard held
/// before: on a run of appends, on a load into a written shard, and again after a
/// reactivation.
/// </summary>
public sealed partial class LiveCheckpointAdvanceIntegrationTests
{
    private const string BulkChunksTree = "live-advance-bulk-chunks";
    private const string BulkReloadTree = "live-advance-bulk-reload";
    private const int ResidualChunks = 5;

    [Test]
    public async Task Every_chunk_of_a_bulk_append_run_starts_its_leaves_at_its_own_head()
    {
        var sibling = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkChunksTree}/1");
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkChunksTree}/0");
        for (var i = 0; i < 5; i++)
        {
            await shard.SetAsync($"a{i}", [(byte)i]);
        }

        await WarmSiblingAsync(sibling);
        var reasons = new List<string>();
        var headBefore = new long[ResidualChunks];
        var lagging = new List<string>();
        var chunksWithLeaves = 0;
        using (ListenForDriverDeclines(BulkChunksTree, reasons))
        {
            for (var c = 0; c < ResidualChunks; c++)
            {
                // Every chunk finds more WAL below it than the last, as a large seed
                // does: a leaf that starts at the log's start, or at an earlier chunk's
                // position, pays for all of it.
                for (var i = 0; i < 10; i++)
                {
                    await sibling.SetAsync($"s{c}-{i}", [(byte)i]);
                }

                headBefore[c] = await HeadAsync(BulkChunksTree);
                var chunk = Enumerable.Range(0, 300).Select(i => KeyValuePair.Create($"z{c}{i:D4}", new byte[] { 1 })).ToList();
                await shard.BulkAppendAsync($"bulk-chunks-{c}", chunk);

                // Read as soon as the chunk returns, before the live advance can catch
                // a cold leaf up and hide that it started below the chunk's head.
                var born = await BornCheckpointsAsync(shard, $"z{c}");
                chunksWithLeaves += born.Count > 0 ? 1 : 0;
                foreach (var (low, checkpoint) in born)
                {
                    TestContext.Out.WriteLine($"chunk {c} head {headBefore[c]} leaf {low} checkpoint {checkpoint}");
                    if (checkpoint < headBefore[c] - 1)
                    {
                        lagging.Add($"{low}@{checkpoint}<{headBefore[c] - 1}");
                    }
                }
            }

            await WaitForBulkSwitchClosedAsync(BulkChunksTree);
        }

        Assert.Multiple(() =>
        {
            Assert.That(chunksWithLeaves, Is.EqualTo(ResidualChunks), "precondition: every chunk created leaves");
            Assert.That(lagging, Is.Empty,
                "a chunk's leaves read from the head the chunk found, not from an earlier chunk's position or the log's start");
            Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Value),
                "no leaf of the run took the starved arm");
        });

        await AssertReactivationsWarmAsync(
            BulkChunksTree,
            (await ChainAsync(shard)).Where((_, i) => i > 0).ToList(),
            $"z{ResidualChunks - 1}");
    }

    [Test]
    public async Task A_bulk_load_into_a_written_shard_starts_its_leaves_at_the_head()
    {
        var sibling = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkReloadTree}/1");
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkReloadTree}/0");
        for (var i = 0; i < 5; i++)
        {
            await sibling.SetAsync($"s{i}", [(byte)i]);
        }

        await WarmSiblingAsync(sibling);

        // A read seeds the shard's empty root leaf, so the load replaces a published
        // root: the switched path, which closes its switch at once (closeNow).
        Assert.That(await shard.GetAsync("absent"), Is.Null);
        Assert.That(await shard.GetLeftmostLeafIdAsync(), Is.Not.Null, "precondition: the shard has a published, empty root");
        var head = await HeadAsync(BulkReloadTree);
        var clock = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks };
        var entries = Enumerable.Range(0, 300)
            .Select(i => new LwwEntry($"r{i:D4}", LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), clock = HybridLogicalClock.Tick(clock))))
            .ToList();
        var reasons = new List<string>();
        List<(string Low, long Checkpoint)> born;
        using (ListenForDriverDeclines(BulkReloadTree, reasons))
        {
            await shard.BulkLoadRawAsync("bulk-reload-1", entries);
            born = await BornCheckpointsAsync(shard, "r");
            await WaitForBulkSwitchClosedAsync(BulkReloadTree);
        }

        foreach (var (low, checkpoint) in born)
        {
            TestContext.Out.WriteLine($"head {head} leaf {low} checkpoint {checkpoint}");
        }

        Assert.Multiple(() =>
        {
            Assert.That(born, Has.Count.GreaterThan(1), "precondition: the load created several leaves");
            Assert.That(born.Where(b => b.Checkpoint < head - 1).Select(b => $"{b.Low}@{b.Checkpoint}"), Is.Empty,
                "a leaf the load creates reads from the head the load found, not from the log's start");
            Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Value),
                "no loaded leaf took the starved arm");
        });

        await AssertReactivationsWarmAsync(BulkReloadTree, await ChainAsync(shard), "r");
    }

    /// <summary>
    /// Writes <paramref name="sibling"/>'s shard and waits until its leaf has banked a
    /// checkpoint, so the starved-arm decline the tests watch for, which is tagged by
    /// tree only, can come only from a leaf a bulk operation created.
    /// </summary>
    private async Task WarmSiblingAsync(IShardRootGrain sibling)
    {
        await sibling.SetAsync("warm", [1]);
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            var leaves = await ChainAsync(sibling);
            var checkpointed = true;
            foreach (var leaf in leaves)
            {
                var page = await _cluster.Client.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).GetProjectionCheckpointAndNextSiblingAsync();
                checkpointed &= page.CheckpointOffset > 0;
            }

            if (leaves.Count > 0 && checkpointed)
            {
                return;
            }

            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "precondition: the sibling shard's leaf checkpoints");
            await Task.Delay(100);
        }
    }

    /// <summary>
    /// The low bound and current projection checkpoint of every leaf of
    /// <paramref name="shard"/> whose low bound starts with <paramref name="prefix"/>.
    /// </summary>
    private async Task<List<(string Low, long Checkpoint)>> BornCheckpointsAsync(IShardRootGrain shard, string prefix)
    {
        var born = new List<(string Low, long Checkpoint)>();
        foreach (var leaf in await ChainAsync(shard))
        {
            var page = await _cluster.Client.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).GetProjectionCheckpointAndNextSiblingAsync();
            if (page.Link.KeyRange.LowKeyInclusive is { } low && low.StartsWith(prefix, StringComparison.Ordinal))
            {
                born.Add((low, page.CheckpointOffset));
            }
        }

        return born;
    }
}

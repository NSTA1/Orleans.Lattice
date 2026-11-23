using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #6152: a leaf a bulk operation seeds must bank a durable anchor for its seed,
/// so that a later reactivation resumes above it rather than re-reading its whole WAL
/// partition. Before the fix a bulk-born leaf reactivated cold every time, and on a
/// large bulk-loaded tree those full replays queued on the replay gate until it
/// saturated.
/// </summary>
public sealed partial class LiveCheckpointAdvanceIntegrationTests
{
    private const string BulkCheckpointAppendTree = "live-advance-bulk-ckpt-append";
    private const string BulkCheckpointRawTree = "live-advance-bulk-ckpt-raw";

    [Test]
    public async Task A_bulk_appended_leaf_reactivates_warm_above_its_seed()
    {
        var tree = _cluster.Client.GetGrain<ILattice>(BulkCheckpointAppendTree);
        for (var i = 0; i < 5; i++)
        {
            await tree.SetAsync($"a{i}", [(byte)i]);
        }

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkCheckpointAppendTree}/0");
        var appended = Enumerable.Range(0, 300).Select(i => KeyValuePair.Create($"z{i:D4}", new byte[] { 1 })).ToList();
        await shard.BulkAppendAsync("bulk-ckpt-append-1", appended);
        await WaitForBulkSwitchClosedAsync(BulkCheckpointAppendTree);

        var born = new List<GrainId>();
        foreach (var leaf in await ChainAsync(shard))
        {
            var link = await _cluster.Client.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).GetKeyRangeAndNextSiblingAsync();
            if (link.KeyRange.LowKeyInclusive is { } low && low.StartsWith('z'))
            {
                born.Add(leaf);
            }
        }

        Assert.That(born, Has.Count.GreaterThan(1), "precondition: the append created several leaves");
        await AssertReactivationsWarmAsync(BulkCheckpointAppendTree, born, "z");
    }

    [Test]
    public async Task A_raw_bulk_loaded_leaf_reactivates_warm_above_its_seed()
    {
        // The sibling shard writes first, so the shared WAL partition holds rows below
        // the loaded shard's birth: a cold reactivation would have to re-read them.
        var sibling = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkCheckpointRawTree}/1");
        for (var i = 0; i < 5; i++)
        {
            await sibling.SetAsync($"s{i}", [(byte)i]);
        }

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkCheckpointRawTree}/0");
        var clock = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks };
        var entries = Enumerable.Range(0, 300)
            .Select(i => new LwwEntry($"r{i:D4}", LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), clock = HybridLogicalClock.Tick(clock))))
            .ToList();
        await shard.BulkLoadRawAsync("bulk-ckpt-raw-1", entries);

        var leaves = await ChainAsync(shard);
        Assert.That(leaves, Has.Count.GreaterThan(1), "precondition: the load created several leaves");
        await AssertReactivationsWarmAsync(BulkCheckpointRawTree, leaves, "r");
    }

    /// <summary>
    /// Deactivates each of <paramref name="leaves"/> twice and asserts that no
    /// reactivation, the first included, was cold: each resumed above a durable anchor
    /// covering its seed.
    /// </summary>
    private async Task AssertReactivationsWarmAsync(string treeId, IReadOnlyList<GrainId> leaves, string keyPrefix)
    {
        var cold = 0;
        var warm = 0;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafActivationReplays,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var matchesTree = false;
                bool? isCold = null;
                foreach (var t in tags)
                {
                    if (t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
                    {
                        matchesTree = true;
                    }
                    else if (t.Key == LatticeMetrics.ActivationTemperatureCold.Key)
                    {
                        isCold = Equals(t.Value, LatticeMetrics.ActivationTemperatureCold.Value);
                    }
                }

                if (matchesTree && isCold is { } c)
                {
                    if (c)
                    {
                        Interlocked.Add(ref cold, (int)value);
                    }
                    else
                    {
                        Interlocked.Add(ref warm, (int)value);
                    }
                }
            }));

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        for (var cycle = 0; cycle < 2; cycle++)
        {
            foreach (var leaf in leaves)
            {
                await _cluster.Client.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).ForceDeactivateAsync();
            }

            await Task.Delay(500);

            foreach (var leaf in leaves)
            {
                await _cluster.Client.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).GetKeyRangeAndNextSiblingAsync();
            }
        }

        TestContext.Out.WriteLine($"activations: cold={Volatile.Read(ref cold)} warm={Volatile.Read(ref warm)}");
        Assert.Multiple(async () =>
        {
            Assert.That(Volatile.Read(ref cold), Is.Zero,
                "a bulk-born leaf reactivates above a durable anchor for its seed, not by replaying its whole WAL partition");
            Assert.That(await shard.GetAsync($"{keyPrefix}0299"), Is.Not.Null, "the seed is still readable");
        });
    }
}

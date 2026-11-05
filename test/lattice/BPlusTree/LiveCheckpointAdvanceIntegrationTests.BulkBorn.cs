using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #5812, member B: a leaf a bulk operation creates is seeded before the
/// operation publishes it. Seeding claims no append epoch: the seed is covered by the
/// bulk switch's close fence, or, on a never-written shard, is appended unstamped and
/// reported. The leaf's first claim at a partition is made by its first live write,
/// once it is reachable, so that write is stamped and the leaf arms. Before this, the
/// seed claimed, failed the reachability proof (#5709) before publication, and latched
/// the leaf unstamped for its activation, so it never armed. A leaf that claimed holds
/// a durable append-epoch row; one that did not has none.
/// </summary>
public sealed partial class LiveCheckpointAdvanceIntegrationTests
{
    private const string BulkRawTree = "live-advance-bulk-raw";
    private const string BulkGraftTree = "live-advance-bulk-graft";

    [Test]
    public async Task A_raw_bulk_loaded_leaf_claims_nothing_when_seeded_and_its_first_live_write_is_stamped_and_banked()
    {
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkRawTree}/0");
        var clock = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks };
        var entries = Enumerable.Range(0, 300)
            .Select(i => new LwwEntry($"r{i:D4}", LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), clock = HybridLogicalClock.Tick(clock))))
            .ToList();
        await shard.BulkLoadRawAsync("bulk-born-raw-1", entries);

        var leaves = await ChainAsync(shard);
        Assert.That(leaves, Has.Count.GreaterThan(1), "precondition: the load created several leaves");
        Assert.Multiple(async () =>
        {
            Assert.That(await ClaimedAsync(leaves), Is.Empty, "seeding claims no append epoch");

            // B2: the shard had no root, so the load opened no switch and ran no fence.
            Assert.That((await HandOffMarkerRows.ReadShardAsync(Storage, BulkRawTree, 0)).RecordExists, Is.False,
                "a never-written shard's load writes no hand-off marker");
            Assert.That((await WalAppendFenceRows.ReadGenerationAsync(Storage, BulkRawTree, 0, WalRoutingInitiator.ShardRoot(0))).RecordExists,
                Is.False, "and claims no routing generation");
        });

        var tree = _cluster.Client.GetGrain<ILattice>(BulkRawTree);
        var last = leaves[^1];
        await tree.SetAsync("r9999", [9]);
        await AssertFirstLiveWriteOfTheModeAsync(last, BulkRawTree, "r9999");
    }

    [Test]
    public async Task A_bulk_appended_leaf_claims_nothing_when_seeded_and_its_first_live_write_is_stamped_and_banked()
    {
        var tree = _cluster.Client.GetGrain<ILattice>(BulkGraftTree);
        for (var i = 0; i < 5; i++)
        {
            await tree.SetAsync($"a{i}", [(byte)i]);
        }

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkGraftTree}/0");
        var appended = Enumerable.Range(0, 300).Select(i => KeyValuePair.Create($"z{i:D4}", new byte[] { 1 })).ToList();
        await shard.BulkAppendAsync("bulk-born-graft-1", appended);
        await WaitForBulkSwitchClosedAsync(BulkGraftTree);

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
        Assert.That(await ClaimedAsync(born), Is.Empty, "seeding under the bulk switch claims no append epoch");

        var last = born[^1];
        await tree.SetAsync("z9999", [9]);
        await AssertFirstLiveWriteOfTheModeAsync(last, BulkGraftTree, "z9999");
    }

    /// <summary>
    /// A bulk-born leaf's first live write. Eager mode: the write claims the leaf's
    /// append epoch, is stamped, and the leaf arms and banks it. Pre-arm mode (#5833): the
    /// write claims nothing and is appended pre-arm; the leaf arms only on a pin's demand,
    /// and the write is banked either way.
    /// </summary>
    private async Task AssertFirstLiveWriteOfTheModeAsync(GrainId leaf, string tree, string key)
    {
        var snapshot = _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(leaf.GetGuidKey());
        if (eagerClaims)
        {
            Assert.Multiple(async () =>
            {
                Assert.That(await ClaimedAsync([leaf]), Has.Count.EqualTo(1),
                    "the bulk-born leaf's first live write claims its append epoch and is stamped");
                Assert.That(await WaitForCoverageAsync(snapshot, await HeadAsync(tree) - 1, key), Is.Not.Null,
                    "the bulk-born leaf arms and banks its live write");
            });
            return;
        }

        Assert.That(await WaitForCoverageAsync(snapshot, await HeadAsync(tree) - 1, key), Is.Not.Null,
            "the bulk-born leaf's pre-arm live write is banked");
    }

    private async Task<List<GrainId>> ChainAsync(IShardRootGrain shard)
    {
        var leaves = new List<GrainId>();
        GrainId? id = await shard.GetLeftmostLeafIdAsync();
        while (id is { } current && leaves.Count < 1000)
        {
            leaves.Add(current);
            id = (await _cluster.Client.GetGrain<IBPlusLeafGrain>(current.GetGuidKey()).GetKeyRangeAndNextSiblingAsync()).NextSibling;
        }

        return leaves;
    }

    /// <summary>The leaves of <paramref name="leaves"/> holding a durable append-epoch claim.</summary>
    private async Task<List<GrainId>> ClaimedAsync(IReadOnlyList<GrainId> leaves)
    {
        var claimed = new List<GrainId>();
        foreach (var leaf in leaves)
        {
            var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf.GetGuidKey());
            if (row.RecordExists && row.State.Epoch > 0)
            {
                claimed.Add(leaf);
            }
        }

        return claimed;
    }
}

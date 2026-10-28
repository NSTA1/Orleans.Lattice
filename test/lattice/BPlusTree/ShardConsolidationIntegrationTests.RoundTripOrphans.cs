using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A fold that returns a slot to the shard a split took it from must not
/// resurrect a key deleted, and reaped, while the slot lived elsewhere.
/// <para>
/// A split leaves its source's rows for the moved slots in place, sealed, so a
/// fold back onto that source exposes them again when it reclaims the slot. The
/// fold's drain covers a key the donor still holds, live or as a tombstone, by
/// last-writer-wins; a tombstone compaction already reaped leaves nothing for
/// the drain to carry, and the survivor's sealed pre-split row came back as the
/// key's value.
/// </para>
/// </summary>
public partial class ShardConsolidationIntegrationTests
{
    [Test]
    public async Task A_key_updated_after_a_split_keeps_its_new_value_when_the_slot_folds_back()
    {
        var (treeId, tree, newShard, victim) = await SplitAndPickMovedKeyAsync("cons-orphan-upd");
        var updated = Encoding.UTF8.GetBytes("updated-after-split");
        await tree.SetAsync(victim, updated);

        await FoldEveryShardBackOntoZeroAsync(treeId, newShard);

        var value = await tree.GetAsync(victim);
        Assert.That(value is null ? null : Encoding.UTF8.GetString(value), Is.EqualTo("updated-after-split"),
            $"Key '{victim}' must keep the value written after the split once its slot folds back onto the shard it was split from.");
    }

    [Test]
    public async Task A_key_deleted_after_a_split_stays_deleted_when_the_slot_folds_back()
    {
        var (treeId, tree, newShard, victim) = await SplitAndPickMovedKeyAsync("cons-orphan-del");
        Assert.That(await tree.DeleteAsync(victim), Is.True);

        await FoldEveryShardBackOntoZeroAsync(treeId, newShard);

        var value = await tree.GetAsync(victim);
        Assert.That(value is null ? null : Encoding.UTF8.GetString(value), Is.Null,
            $"Key '{victim}' deleted after the split must stay deleted once its slot folds back onto the shard it was split from.");
    }

    private async Task<(string TreeId, ILattice Tree, int NewShard, string Victim)> SplitAndPickMovedKeyAsync(string prefix)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "orphan", 250);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);

        var map = await GetMapAsync(treeId);
        var newShard = map.GetPhysicalShardIndices().Max();
        return (treeId, tree, newShard, expected.Keys.First(k => map.Resolve(k) == newShard));
    }

    private async Task FoldEveryShardBackOntoZeroAsync(string treeId, int highestShard)
    {
        for (var donor = highestShard; donor > 0; donor--)
            await RunFoldAsync(treeId, donor, donor - 1);
        Assert.That(await PhysicalShardCountAsync(treeId), Is.EqualTo(1),
            "precondition: every slot folded back onto shard 0");
    }

    [Test]
    public async Task A_key_deleted_and_reaped_after_a_split_stays_deleted_when_the_slot_folds_back()
    {
        var treeId = $"cons-orphan-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "orphan", 250);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);

        // A key the split moved off shard 0, onto the shard it allocated.
        var afterSplit = await GetMapAsync(treeId);
        var newShard = afterSplit.GetPhysicalShardIndices().Max();
        var victim = expected.Keys.First(k => afterSplit.Resolve(k) == newShard);

        Assert.That(await tree.DeleteAsync(victim), Is.True);

        // Grace elapsed: reap the tombstone on the shard that now owns the key.
        var owner = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{newShard}");
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var leafId = await owner.GetLeafIdForKeyAsync(victim);
            Assert.That(leafId, Is.Not.Null);
            var reaped = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value)
                .CompactTombstonesAsync(TimeSpan.Zero);
            Assert.That(reaped.EntriesRemoved, Is.GreaterThanOrEqualTo(1),
                "precondition: compaction reaped the delete's tombstone");
        }

        Assert.That(await tree.GetAsync(victim), Is.Null, "precondition: the key reads deleted before any fold");

        // Fold every slot back onto shard 0, the split's source.
        for (var donor = newShard; donor > 0; donor--)
            await RunFoldAsync(treeId, donor, donor - 1);
        Assert.That(await PhysicalShardCountAsync(treeId), Is.EqualTo(1),
            "precondition: every slot folded back onto shard 0");

        var value = await tree.GetAsync(victim);
        Assert.That(value is null ? null : Encoding.UTF8.GetString(value), Is.Null,
            $"Deleted key '{victim}' must stay deleted once its slot folds back onto the shard it was split from.");

        expected.Remove(victim);
        Assert.That(await tree.CountAsync(), Is.EqualTo(expected.Count),
            "The fold must not bring the deleted key back into the tree's count.");
    }
}

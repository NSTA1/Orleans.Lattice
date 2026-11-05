using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5839 / H's gap register: a tree range delete's walk is stamped under the tree's
/// range-delete generation, not the shard root's routing generation a deferred split
/// close raises, so its records are bounded by the range delete's own shard-marker
/// window (<c>HandOffMarkerCore.HeadAt</c> over the shard marker's close heads), never
/// by the deferred close. A receiver with both pending is held until both have closed.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_range_delete_walk_during_a_pending_deferred_split_close_holds_the_live_advance_until_both_close()
    {
        // Own offsets 0..2. The split's close is deferred on the leaf's own marker; the range
        // delete's walk wrote offset 1 under its own generation, and its window closed on the
        // shard marker with head 2.
        var (grain, rig) = await ArmedLeafWithWritesAsync("a", "b", "c");
        await rig.SetLeafMarkerAsync(m =>
        {
            HandOffMarkerCore.Open(m, "split:donor", null, null);
            return HandOffMarkerCore.CloseDeferred(m, "split:donor", rig.Partitions, shardIndex: 0, generation: 1);
        });
        rig.ShardMarker.Closes.Add(new HandOffClose { Token = "rd:walk", Heads = [2], Sequence = 1 });

        var heldWhileBothPending = !await grain.TryAdvanceLiveCheckpointAsync();

        // The deferred close resolves below the checkpoint - its arm re-claimed at a head the
        // leaf has already read - so only the walk's window still bears on the leaf.
        await rig.SetLeafMarkerAsync(m =>
        {
            HandOffMarkerCore.ResolveDeferred(m, partition: 0, shardIndex: 0, generation: 1, head: 0);
            return true;
        });
        var deferredResolved = (await rig.LeafMarkerAsync()).Closes.All(c => c.Heads.All(h => h != HandOffMarkerCore.DeferredHead));
        var heldByTheWalkAlone = !await grain.TryAdvanceLiveCheckpointAsync();

        // A replay reads past the walk's close head: both have closed.
        await grain.SetCheckpointOffsetHintsAsync([1]);
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(heldWhileBothPending, Is.True, "the advance passed a pending deferred split close and the walk's window");
            Assert.That(deferredResolved, Is.True, "precondition: the deferred close resolved");
            Assert.That(heldByTheWalkAlone, Is.True,
                "the walk's record (offset 1) is bounded by the range delete's own shard-marker close head, not by the deferred split close");
            Assert.That(advanced, Is.True, "once both have closed the leaf advances");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(2L));
        });
    }
}

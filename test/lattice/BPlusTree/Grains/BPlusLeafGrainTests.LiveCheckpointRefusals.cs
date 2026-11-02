using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5708: a routine refusal that allocated nothing must not turn the live
/// checkpoint advance off for the activation. Each case below - the member 2 self-fence,
/// a stale routing generation and an absorbed clock-floor refusal - is retried, and the
/// leaf advances live again once it has read to its (re-)claim head.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_self_fenced_append_releases_its_count_and_the_advance_resumes_after_the_re_claim_head()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");

        // A split's close re-claims at head 2, above a foreign record at 1; a write
        // admitted before the close is self-fenced and re-admitted by its seam.
        rig.Writer.ForeignAppend();
        rig.ClaimHead = 2;
        await grain.AppendFenceForTesting!.BumpAndReclaimAsync(_ => rig.Shard, rig.Partitions, CancellationToken.None);
        rig.Writer.ThrowNext = LeafRetiredException.ForSupersededActivation(rig.LeafId, 1);
        Assert.ThrowsAsync<LeafRetiredException>(async () => await SetStampedAsync(grain, "b", "2"));
        await SetStampedAsync(grain, "b", "2");

        Assert.That(grain.LiveAppendsUnappliedForTest, Is.Zero, "the refused append allocated nothing, so its count is released");
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False, "not yet read to the re-claim head");

        await grain.SetCheckpointOffsetHintsAsync([1]);
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(2L));
    }

    [Test]
    public async Task A_stale_generation_refusal_releases_its_count_and_the_advance_resumes()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");

        // An adaptive split, fold or range-delete close re-claimed the shard root's
        // generation; the shard root re-admits the write on the same leaf.
        rig.Writer.ThrowNext = new WalRoutingGenerationStaleException(LiveAdvanceRig.Routing.Initiator, 1, 2);
        Assert.ThrowsAsync<WalRoutingGenerationStaleException>(async () => await SetStampedAsync(grain, "b", "2"));
        await SetStampedAsync(grain, "b", "2");

        Assert.That(grain.LiveAppendsUnappliedForTest, Is.Zero);
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(1L));
    }

    [Test]
    public async Task An_absorbed_clock_floor_refusal_releases_its_count_and_the_advance_resumes()
    {
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");

        // A replicated tree's partition refuses a stamp below its clock floor; the leaf
        // absorbs it and re-runs the commit, so the write is counted twice.
        rig.Writer.ThrowNext = new WalStampBelowFloorException(
            "tree", 0, HybridLogicalClock.Zero, new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks });
        await SetStampedAsync(grain, "b", "2");

        Assert.That(grain.LiveAppendsUnappliedForTest, Is.Zero, "counted twice, released once for the apply and once for the refusal");
        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.True);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(1L));
    }
}

using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The fence's per-partition carriage of a witnessed donor close (a5e2e2ef's G4, A3 and A6
/// on #5866): a close resolved at one partition by the sibling's arm is dropped from that
/// partition's stamps only, and a witnessed bound no longer counts as pending.
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    [Test]
    public async Task A_carried_close_resolved_at_one_partition_is_dropped_from_its_stamps_only()
    {
        var fence = PreArmFence();
        fence.SetCarriedCloses([(CloseA, new[] { true, false })], boundPending: false);
        var shard = Substitute.For<IWalShardGrain>();

        var resolved = (await fence.StampForAsync(shard, partition: 0, CancellationToken.None))!.Value;
        var carried = (await fence.StampForAsync(shard, partition: 1, CancellationToken.None))!.Value;

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Close1.IsSet, Is.False, "a partition the arm resolved still carried the close");
            Assert.That(carried.Close1, Is.EqualTo(CloseA), "a partition the arm did not resolve dropped the close");
            Assert.That(fence.IsCarriedAtForTesting(0), Is.False);
            Assert.That(fence.IsCarriedAtForTesting(1), Is.True);
            Assert.That(fence.HasPendingCloses, Is.False, "a witnessed bound counted as pending");
        });
    }
}

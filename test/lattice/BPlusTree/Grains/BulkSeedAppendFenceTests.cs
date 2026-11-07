using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5812, member B: a bulk seed claims nothing. The fence it is appended through
/// stamps the seed with the bulk-seed class under an open switch, or not at all, and
/// never consults the leaf's own fence or asks the partition for an epoch, so the
/// bulk-born leaf's first claim is made by its first live write.
/// </summary>
[TestFixture]
public sealed class BulkSeedAppendFenceTests
{
    [Test]
    public async Task A_seed_under_an_open_switch_is_stamped_with_the_seed_class_and_claims_nothing()
    {
        var owner = Substitute.For<ILeafAppendFence>();
        var shard = Substitute.For<IWalShardGrain>();
        var fence = new BulkSeedAppendFence(Guid.NewGuid(), WalRoutingStamp.ForShardRoot(3, 7), owner);

        var stamp = await fence.StampForAsync(shard, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stamp?.IsBulkSeed, Is.True, "the seed names no leaf");
            Assert.That(stamp?.Initiator, Is.EqualTo(WalRoutingInitiator.ShardRoot(3)));
            Assert.That(stamp?.Generation, Is.EqualTo(7));
            Assert.That(owner.ReceivedCalls(), Is.Empty, "the leaf's own fence is not asked to claim");
            Assert.That(shard.ReceivedCalls(), Is.Empty, "no epoch is claimed at the partition");
        });
    }

    [Test]
    public async Task A_seed_with_no_switch_is_unstamped_and_claims_nothing()
    {
        var owner = Substitute.For<ILeafAppendFence>();
        var shard = Substitute.For<IWalShardGrain>();
        var fence = new BulkSeedAppendFence(Guid.NewGuid(), WalRoutingStamp.Unstamped, owner);

        var stamp = await fence.StampForAsync(shard, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stamp, Is.Null, "a never-written shard's seed is appended unstamped");
            Assert.That(owner.ReceivedCalls(), Is.Empty, "the leaf's own fence is not asked to claim");
            Assert.That(shard.ReceivedCalls(), Is.Empty, "no epoch is claimed at the partition");
        });
    }
}

using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for <see cref="WalRoutingStamp"/> (epic #5605, member 2): the routing
/// part of a stamped append, combined with the leaf's own epoch stamp, and the
/// forced-unstamped marker the existing leaf methods use (S3).
/// </summary>
[TestFixture]
public sealed class WalRoutingStampTests
{
    private static readonly Guid Leaf = Guid.NewGuid();

    [Test]
    public void A_set_routing_stamp_extends_the_leaf_stamp_with_its_initiator_and_generation()
    {
        var routing = WalRoutingStamp.ForShardRoot(3, 7);

        var stamp = routing.Apply(WalAppendStamp.ForLeaf(Leaf, 2));

        Assert.Multiple(() =>
        {
            Assert.That(stamp.LeafId, Is.EqualTo(Leaf));
            Assert.That(stamp.Epoch, Is.EqualTo(2));
            Assert.That(stamp.Initiator, Is.EqualTo(WalRoutingInitiator.ShardRoot(3)));
            Assert.That(stamp.Generation, Is.EqualTo(7));
        });
    }

    [Test]
    public void No_routing_and_forced_unstamped_leave_the_leaf_stamp_alone()
    {
        var leafStamp = WalAppendStamp.ForLeaf(Leaf, 2);

        Assert.Multiple(() =>
        {
            Assert.That(WalRoutingStamp.None.Apply(leafStamp), Is.EqualTo(leafStamp));
            Assert.That(WalRoutingStamp.Unstamped.Apply(leafStamp), Is.EqualTo(leafStamp));
            Assert.That(WalRoutingStamp.None.IsSet, Is.False);
            Assert.That(WalRoutingStamp.Unstamped.IsSet, Is.False);
            Assert.That(WalRoutingStamp.Unstamped.ForcesUnstamped, Is.True);
            Assert.That(WalRoutingStamp.None.ForcesUnstamped, Is.False);
        });
    }

    [Test]
    public void A_tree_range_delete_stamp_names_the_tree_initiator()
    {
        var routing = WalRoutingStamp.ForTreeRangeDelete(4);

        Assert.Multiple(() =>
        {
            Assert.That(routing.IsSet, Is.True);
            Assert.That(routing.Initiator, Is.EqualTo(WalRoutingInitiator.TreeRangeDelete));
            Assert.That(() => WalRoutingStamp.ForTreeRangeDelete(0), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public async Task A_stamped_leaf_call_falls_back_to_the_existing_method_only_on_method_unknown()
    {
        var plainCalls = 0;
        var fallback = await WalRoutedLeafCall.StampedOrPlainAsync(
            WalRoutingStamp.ForShardRoot(0, 1),
            () => Task.FromException<int>(new TypeLoadException("Unable to resolve type alias 'x'.")),
            () => { plainCalls++; return Task.FromResult(2); });
        var forced = await WalRoutedLeafCall.StampedOrPlainAsync(
            WalRoutingStamp.Unstamped,
            () => Task.FromResult(1),
            () => { plainCalls++; return Task.FromResult(3); });

        Assert.Multiple(() =>
        {
            Assert.That(fallback, Is.EqualTo(2), "a definite method-unknown failure re-issues through the existing method");
            Assert.That(forced, Is.EqualTo(3), "a forced-unstamped write goes straight to the existing method");
            Assert.That(plainCalls, Is.EqualTo(2));
        });
        Assert.ThrowsAsync<TimeoutException>(() => WalRoutedLeafCall.StampedOrPlainAsync(
            WalRoutingStamp.ForShardRoot(0, 1),
            () => Task.FromException<int>(new TimeoutException()),
            () => Task.FromResult(4)),
            "any other failure may have been allocated, so it propagates unchanged");
    }
}

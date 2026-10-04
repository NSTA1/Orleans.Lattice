using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Unit tests for the durable move-fence slot of <see cref="WalPlacementPin"/>
/// (issue #4525): holding and releasing a fence leaves the placement version
/// alone, and the placement change that ends a move drops exactly the moved
/// partitions' fences.
/// </summary>
[TestFixture]
public sealed class WalPlacementPinFenceTests
{
    private static WalMoveFence Fence(string moveId) =>
        new() { MoveId = moveId, SourceProviderKey = "default", LeaseExpiresUtcTicks = 42 };

    [Test]
    public void WithFence_holds_a_fence_without_changing_the_version()
    {
        var pin = WalPlacementPin.Create().WithFence(1, Fence("m1"));

        Assert.Multiple(() =>
        {
            Assert.That(pin.Version, Is.EqualTo(0));
            Assert.That(pin.ResolveFence(1), Is.EqualTo(Fence("m1")));
            Assert.That(pin.ResolveFence(0), Is.Null);
        });
    }

    [Test]
    public void WithoutFence_releases_one_partition_and_leaves_the_rest()
    {
        var pin = WalPlacementPin.Create().WithFence(0, Fence("m1")).WithFence(1, Fence("m1"));

        var released = pin.WithoutFence(0);
        var empty = released.WithoutFence(1);

        Assert.Multiple(() =>
        {
            Assert.That(released.ResolveFence(0), Is.Null);
            Assert.That(released.ResolveFence(1), Is.EqualTo(Fence("m1")));
            Assert.That(empty.Fences, Is.Null, "the slot stays minimal once no fence is held");
            Assert.That(empty.WithoutFence(3), Is.SameAs(empty), "releasing an absent fence is a no-op");
        });
    }

    [Test]
    public void WithPartition_drops_the_moved_partitions_fence_only()
    {
        var pin = WalPlacementPin.Create().WithFence(0, Fence("m1")).WithFence(1, Fence("m2"));

        var moved = pin.WithPartition(0, "secondary", 1);

        Assert.Multiple(() =>
        {
            Assert.That(moved.ResolveFence(0), Is.Null);
            Assert.That(moved.ResolveFence(1), Is.EqualTo(Fence("m2")));
            Assert.That(pin.ResolveFence(0), Is.EqualTo(Fence("m1")), "the source pin is not mutated");
        });
    }

    [Test]
    public void WithPartitions_drops_every_moved_partitions_fence()
    {
        var pin = WalPlacementPin.Create().WithFence(0, Fence("m1")).WithFence(1, Fence("m1")).WithFence(2, Fence("m2"));

        var moved = pin.WithPartitions([(0, "secondary"), (1, "secondary")], 1);

        Assert.Multiple(() =>
        {
            Assert.That(moved.ResolveFence(0), Is.Null);
            Assert.That(moved.ResolveFence(1), Is.Null);
            Assert.That(moved.ResolveFence(2), Is.EqualTo(Fence("m2")));
        });
    }
}

using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class WalStampBelowFloorExceptionTests
{
    [Test]
    public void The_production_constructor_names_the_partition_stamp_and_floor()
    {
        var stamp = new HybridLogicalClock { WallClockTicks = 10 };
        var floor = new HybridLogicalClock { WallClockTicks = 20 };

        var ex = new WalStampBelowFloorException("tree", 3, stamp, floor);

        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("tree"));
            Assert.That(ex.Partition, Is.EqualTo(3));
            Assert.That(ex.Timestamp, Is.EqualTo(stamp));
            Assert.That(ex.Floor, Is.EqualTo(floor));
            Assert.That(ex.Message, Does.Contain("tree/3"));
        });
    }

    [Test]
    public void The_framework_constructors_leave_an_empty_tree_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new WalStampBelowFloorException().TreeId, Is.Empty);
            Assert.That(new WalStampBelowFloorException("refused").Message, Is.EqualTo("refused"));
        });
    }
}

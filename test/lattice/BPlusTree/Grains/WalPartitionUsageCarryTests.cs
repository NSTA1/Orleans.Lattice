using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="WalPartitionUsageCarry"/>, the per-activation
/// memory of each WAL partition's last measured figures (issue #6017).
/// </summary>
[TestFixture]
public sealed class WalPartitionUsageCarryTests
{
    private static readonly DateTimeOffset Start = new(2026, 11, 8, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public void Resolve_under_two_consecutive_unreadable_pins_never_carries()
    {
        var carry = new WalPartitionUsageCarry();
        var unreadable = WalPartitionUsageCarry.PartitionIdentity(pin: null, partition: 0);
        carry.Begin("tree|-", partitionCount: 1);

        var measured = carry.Resolve(0, unreadable, retained: 500, physical: 500, Start);
        var refused = carry.Resolve(0, unreadable, retained: -1, physical: -1, Start.AddSeconds(15));

        Assert.Multiple(() =>
        {
            Assert.That(measured.Retained, Is.EqualTo(500L), "a fresh figure is still reported");
            Assert.That(refused.Retained, Is.EqualTo(-1L), "nothing measured under an unreadable pin is carried");
            Assert.That(refused.CarriedFrom, Is.Null);
        });
    }

    [Test]
    public void Resolve_under_an_unreadable_pin_leaves_the_readable_figure_in_place()
    {
        var carry = new WalPartitionUsageCarry();
        var readable = WalPartitionUsageCarry.PartitionIdentity(WalPlacementPin.Create(), partition: 0);
        var unreadable = WalPartitionUsageCarry.PartitionIdentity(pin: null, partition: 0);
        carry.Begin("tree|-", partitionCount: 1);

        carry.Resolve(0, readable, retained: 500, physical: 500, Start);
        carry.Resolve(0, unreadable, retained: -1, physical: -1, Start.AddSeconds(15));
        var later = carry.Resolve(0, readable, retained: -1, physical: -1, Start.AddSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(later.Retained, Is.EqualTo(500L), "the readable figure still stands in once the pin reads again");
            Assert.That(later.CarriedFrom, Is.EqualTo(Start));
        });
    }
}

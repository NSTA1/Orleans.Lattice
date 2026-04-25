using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplogShardEntryTests
{
    [Test]
    public void Default_value_has_zero_sequence_and_default_entry()
    {
        var def = default(ReplogShardEntry);

        Assert.Multiple(() =>
        {
            Assert.That(def.Sequence, Is.EqualTo(0L));
            Assert.That(def.Entry, Is.EqualTo(default(ReplogEntry)));
        });
    }

    [Test]
    public void With_initialiser_sets_properties()
    {
        var inner = new ReplogEntry { TreeId = "t", Op = ReplogOp.Set, Key = "k" };
        var sut = new ReplogShardEntry { Sequence = 7, Entry = inner };

        Assert.Multiple(() =>
        {
            Assert.That(sut.Sequence, Is.EqualTo(7L));
            Assert.That(sut.Entry, Is.EqualTo(inner));
        });
    }

    [Test]
    public void Records_with_same_values_are_equal()
    {
        var inner = new ReplogEntry { TreeId = "t", Op = ReplogOp.Set, Key = "k" };
        var a = new ReplogShardEntry { Sequence = 1, Entry = inner };
        var b = new ReplogShardEntry { Sequence = 1, Entry = inner };

        Assert.That(a, Is.EqualTo(b));
    }
}

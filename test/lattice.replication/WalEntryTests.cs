using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class WalEntryTests
{
    [Test]
    public void Default_value_has_zero_offset_and_default_entry()
    {
        var def = default(WalEntry);

        Assert.Multiple(() =>
        {
            Assert.That(def.Offset, Is.EqualTo(0L));
            Assert.That(def.Entry, Is.EqualTo(default(ReplogEntry)));
        });
    }

    [Test]
    public void With_initialiser_sets_properties()
    {
        var inner = new ReplogEntry { TreeId = "t", Op = ReplogOp.Set, Key = "k" };
        var sut = new WalEntry { Offset = 7, Entry = inner };

        Assert.Multiple(() =>
        {
            Assert.That(sut.Offset, Is.EqualTo(7L));
            Assert.That(sut.Entry, Is.EqualTo(inner));
        });
    }

    [Test]
    public void Records_with_same_values_are_equal()
    {
        var inner = new ReplogEntry { TreeId = "t", Op = ReplogOp.Set, Key = "k" };
        var a = new WalEntry { Offset = 1, Entry = inner };
        var b = new WalEntry { Offset = 1, Entry = inner };

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Records_with_different_offsets_are_not_equal()
    {
        var inner = new ReplogEntry { TreeId = "t", Op = ReplogOp.Set, Key = "k" };
        var a = new WalEntry { Offset = 1, Entry = inner };
        var b = new WalEntry { Offset = 2, Entry = inner };

        Assert.That(a, Is.Not.EqualTo(b));
    }
}

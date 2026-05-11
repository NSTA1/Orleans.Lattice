using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class WalShardSequencedEntryTests
{
    [Test]
    public void Default_value_has_zero_sequence_and_default_entry()
    {
        var def = default(WalShardSequencedEntry);

        Assert.Multiple(() =>
        {
            Assert.That(def.Sequence, Is.EqualTo(0L));
            Assert.That(def.Entry, Is.EqualTo(default(WalRecord)));
        });
    }

    [Test]
    public void With_initialiser_sets_properties()
    {
        var inner = new WalRecord { TreeId = "t", Op = MutationKind.Set, Key = "k" };
        var sut = new WalShardSequencedEntry { Sequence = 7, Entry = inner };

        Assert.Multiple(() =>
        {
            Assert.That(sut.Sequence, Is.EqualTo(7L));
            Assert.That(sut.Entry, Is.EqualTo(inner));
        });
    }

    [Test]
    public void Records_with_same_values_are_equal()
    {
        var inner = new WalRecord { TreeId = "t", Op = MutationKind.Set, Key = "k" };
        var a = new WalShardSequencedEntry { Sequence = 1, Entry = inner };
        var b = new WalShardSequencedEntry { Sequence = 1, Entry = inner };

        Assert.That(a, Is.EqualTo(b));
    }
}

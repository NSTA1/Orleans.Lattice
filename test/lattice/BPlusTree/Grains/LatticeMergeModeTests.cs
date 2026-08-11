using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class LatticeMergeModeTests
{
    [Test]
    public void LwwRegister_is_the_default_value() =>
        Assert.That(default(LatticeMergeMode), Is.EqualTo(LatticeMergeMode.LwwRegister));

    [Test]
    public void Underlying_values_are_stable_for_wire_format()
    {
        // The numeric values are part of the persisted wire format because
        // WalRecord.Mode is serialized as the underlying integer. Pinning
        // the values here guards against an accidental reorder of the enum
        // members breaking on-disk replog state.
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeMergeMode.LwwRegister, Is.EqualTo(0));
            Assert.That((int)LatticeMergeMode.OrSet, Is.EqualTo(1));
            Assert.That((int)LatticeMergeMode.PnCounter, Is.EqualTo(2));
            Assert.That((int)LatticeMergeMode.VersionVector, Is.EqualTo(3));
            Assert.That((int)LatticeMergeMode.MvRegister, Is.EqualTo(4));
            Assert.That((int)LatticeMergeMode.OrMap, Is.EqualTo(5));
            Assert.That((int)LatticeMergeMode.Sequence, Is.EqualTo(6));
            Assert.That((int)LatticeMergeMode.OrFlag, Is.EqualTo(7));
            Assert.That((int)LatticeMergeMode.RwFlag, Is.EqualTo(8));
            Assert.That((int)LatticeMergeMode.GCounter, Is.EqualTo(9));
            Assert.That((int)LatticeMergeMode.GSet, Is.EqualTo(10));
            Assert.That((int)LatticeMergeMode.RwSet, Is.EqualTo(11));
            Assert.That((int)LatticeMergeMode.MaxRegister, Is.EqualTo(12));
            Assert.That((int)LatticeMergeMode.MinRegister, Is.EqualTo(13));
        });
    }
}

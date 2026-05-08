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
        });
    }
}

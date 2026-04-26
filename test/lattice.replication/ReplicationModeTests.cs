using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationModeTests
{
    [Test]
    public void LwwRegister_is_the_default_value() =>
        Assert.That(default(ReplicationMode), Is.EqualTo(ReplicationMode.LwwRegister));

    [Test]
    public void Underlying_values_are_stable_for_wire_format()
    {
        // The numeric values are part of the persisted wire format because
        // ReplogEntry.Mode is serialized as the underlying integer. Pinning
        // the values here guards against an accidental reorder of the enum
        // members breaking on-disk replog state.
        Assert.Multiple(() =>
        {
            Assert.That((int)ReplicationMode.LwwRegister, Is.EqualTo(0));
            Assert.That((int)ReplicationMode.OrSet, Is.EqualTo(1));
            Assert.That((int)ReplicationMode.PnCounter, Is.EqualTo(2));
            Assert.That((int)ReplicationMode.VersionVector, Is.EqualTo(3));
        });
    }
}

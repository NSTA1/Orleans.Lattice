using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorIndexLoadModeTests
{
    [Test]
    public void The_default_load_mode_is_the_fully_resident_one()
    {
        Assert.That(default(VectorIndexLoadMode), Is.EqualTo(VectorIndexLoadMode.Full));
    }
}

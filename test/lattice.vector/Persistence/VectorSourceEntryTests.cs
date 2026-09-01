using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorSourceEntryTests
{
    [Test]
    public void An_entry_carries_its_identifier_and_vector()
    {
        var vector = new float[] { 1f, 2f };
        var entry = new VectorSourceEntry("doc", vector);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Id, Is.EqualTo("doc"));
            Assert.That(entry.Vector.ToArray(), Is.EqualTo(vector));
        });
    }
}

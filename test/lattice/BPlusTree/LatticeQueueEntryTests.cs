namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class LatticeQueueEntryTests
{
    [Test]
    public void Constructor_exposes_id_and_value()
    {
        var entry = new LatticeQueueEntry<string>(42, "payload");

        Assert.Multiple(() =>
        {
            Assert.That(entry.EntryId, Is.EqualTo(42L));
            Assert.That(entry.Value, Is.EqualTo("payload"));
        });
    }

    [Test]
    public void Equality_is_value_based()
    {
        var a = new LatticeQueueEntry<int>(1, 7);
        var b = new LatticeQueueEntry<int>(1, 7);
        var c = new LatticeQueueEntry<int>(2, 7);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(c));
        });
    }
}

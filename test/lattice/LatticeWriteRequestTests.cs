namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeWriteRequest"/>: constructor validation and
/// the value-carrying request shape (single key, incoming bytes, operation, and
/// optional TTL) an interceptor observes.
/// </summary>
[TestFixture]
public class LatticeWriteRequestTests
{
    [Test]
    public void Constructor_populates_every_member()
    {
        var value = new byte[] { 1, 2, 3 };
        var ttl = TimeSpan.FromMinutes(5);

        var request = new LatticeWriteRequest("orders", "k1", value, LatticeOperation.Write, ttl);

        Assert.Multiple(() =>
        {
            Assert.That(request.TreeId, Is.EqualTo("orders"));
            Assert.That(request.Key, Is.EqualTo("k1"));
            Assert.That(request.Value, Is.SameAs(value));
            Assert.That(request.Operation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(request.Ttl, Is.EqualTo(ttl));
        });
    }

    [Test]
    public void Ttl_defaults_to_null()
    {
        var request = new LatticeWriteRequest("orders", "k1", [1], LatticeOperation.Write);

        Assert.That(request.Ttl, Is.Null);
    }

    [Test]
    public void Constructor_rejects_a_null_tree_id()
    {
        Assert.That(() => new LatticeWriteRequest(null!, "k1", [1], LatticeOperation.Write),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Constructor_rejects_an_empty_tree_id()
    {
        Assert.That(() => new LatticeWriteRequest(string.Empty, "k1", [1], LatticeOperation.Write),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_rejects_a_null_key()
    {
        Assert.That(() => new LatticeWriteRequest("orders", null!, [1], LatticeOperation.Write),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_a_null_value()
    {
        Assert.That(() => new LatticeWriteRequest("orders", "k1", null!, LatticeOperation.Write),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_accepts_an_empty_value()
    {
        var request = new LatticeWriteRequest("orders", "k1", [], LatticeOperation.Write);

        Assert.That(request.Value, Is.Empty);
    }
}

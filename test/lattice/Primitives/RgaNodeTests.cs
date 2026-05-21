using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class RgaNodeTests
{
    [Test]
    public void New_node_defaults_are_safe()
    {
        var n = new RgaNode();
        Assert.That(n.ReplicaId, Is.EqualTo(string.Empty));
        Assert.That(n.Counter, Is.EqualTo(0));
        Assert.That(n.ParentDot, Is.EqualTo(default(OrSetDot)));
        Assert.That(n.Value, Is.Not.Null);
        Assert.That(n.Value, Is.Empty);
        Assert.That(n.IsTombstone, Is.False);
    }

    [Test]
    public void Dot_returns_replicaId_and_counter()
    {
        var n = new RgaNode { ReplicaId = "r1", Counter = 7 };
        Assert.That(n.Dot.ReplicaId, Is.EqualTo("r1"));
        Assert.That(n.Dot.Counter, Is.EqualTo(7));
    }

    [Test]
    public void Properties_are_settable()
    {
        var n = new RgaNode
        {
            ReplicaId = "r2",
            Counter = 3,
            ParentDot = new OrSetDot { ReplicaId = "r1", Counter = 1 },
            Value = new byte[] { 1, 2, 3 },
            IsTombstone = true,
        };
        Assert.That(n.ReplicaId, Is.EqualTo("r2"));
        Assert.That(n.Counter, Is.EqualTo(3));
        Assert.That(n.ParentDot.ReplicaId, Is.EqualTo("r1"));
        Assert.That(n.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(n.IsTombstone, Is.True);
    }
}
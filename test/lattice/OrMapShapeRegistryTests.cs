using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Tests;

[TestFixture]
public class OrMapShapeRegistryTests
{
    [Test]
    public void Register_throws_on_null_or_empty_tree()
    {
        var r = new OrMapShapeRegistry();
        var s = OrMapShape.For<string, PnCounter>();
        Assert.That(() => r.Register(null!, s), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => r.Register("", s), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Register_throws_on_null_shape()
    {
        var r = new OrMapShapeRegistry();
        Assert.That(() => r.Register("t", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGet_returns_null_when_unregistered()
    {
        var r = new OrMapShapeRegistry();
        Assert.That(r.TryGet("t"), Is.Null);
    }

    [Test]
    public void Register_then_TryGet_returns_the_registered_shape()
    {
        var r = new OrMapShapeRegistry();
        var s = OrMapShape.For<string, PnCounter>();
        r.Register("t", s);
        Assert.That(r.TryGet("t"), Is.SameAs(s));
    }

    [Test]
    public void Register_same_instance_twice_is_idempotent()
    {
        var r = new OrMapShapeRegistry();
        var s = OrMapShape.For<string, PnCounter>();
        r.Register("t", s);
        Assert.That(() => r.Register("t", s), Throws.Nothing);
    }

    [Test]
    public void Register_different_instance_for_same_tree_throws()
    {
        var r = new OrMapShapeRegistry();
        r.Register("t", OrMapShape.For<string, PnCounter>());
        var other = OrMapShape.For<string, PnCounter>();
        Assert.That(() => r.Register("t", other), Throws.InvalidOperationException);
    }

    [Test]
    public void Shape_For_roundtrips_state_and_merges_delta()
    {
        var shape = OrMapShape.For<string, PnCounter>();
        var map = (OrMap<string, PnCounter>)shape.CreateEmpty();
        var pc = new PnCounter(); pc.Increment("r", 7);
        var delta = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r", Counter = 1, Value = pc } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };
        var bytes = shape.SerializeState(map);
        var roundtripped = (OrMap<string, PnCounter>)shape.DeserializeState(bytes);
        shape.MergeDelta(roundtripped, delta);
        Assert.That(roundtripped.Get("k")!.Value, Is.EqualTo(7));
    }

    [Test]
    public void Shape_constructor_throws_on_null_delegates()
    {
        Func<byte[], object> a = _ => new object();
        Action<object, object> b = (_, _) => { };
        Func<object> c = () => new object();
        Func<object, byte[]> d = _ => Array.Empty<byte>();
        Assert.Multiple(() =>
        {
            Assert.That(() => new OrMapShape(null!, a, b, c, d), Throws.ArgumentNullException);
            Assert.That(() => new OrMapShape(a, null!, b, c, d), Throws.ArgumentNullException);
            Assert.That(() => new OrMapShape(a, a, null!, c, d), Throws.ArgumentNullException);
            Assert.That(() => new OrMapShape(a, a, b, null!, d), Throws.ArgumentNullException);
            Assert.That(() => new OrMapShape(a, a, b, c, null!), Throws.ArgumentNullException);
        });
    }
}
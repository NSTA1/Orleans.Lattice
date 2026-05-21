using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class CrdtLatticeExtensionsTests
{
    [Test]
    public void OrSet_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.OrSet("k1");
        Assert.That(accessor.Lattice, Is.SameAs(lattice));
        Assert.That(accessor.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void PnCounter_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.PnCounter("k1");
        Assert.That(accessor.Lattice, Is.SameAs(lattice));
        Assert.That(accessor.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void VersionVector_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.VersionVector("k1");
        Assert.That(accessor.Lattice, Is.SameAs(lattice));
        Assert.That(accessor.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void OrSet_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.OrSet("k"), Throws.ArgumentNullException);
    }

    [Test]
    public void PnCounter_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.PnCounter("k"), Throws.ArgumentNullException);
    }

    [Test]
    public void VersionVector_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.VersionVector("k"), Throws.ArgumentNullException);
    }

    [Test]
    public void OrSet_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.OrSet(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => lattice.OrSet(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void PnCounter_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.PnCounter(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => lattice.PnCounter(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void VersionVector_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.VersionVector(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => lattice.VersionVector(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Default_OrSetAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(OrSetAccessor);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Default_PnCounterAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(PnCounterAccessor);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Default_VersionVectorAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(VersionVectorAccessor);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void MvRegister_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.MvRegister<string>("k1");
        Assert.That(accessor.Lattice, Is.SameAs(lattice));
        Assert.That(accessor.Key, Is.EqualTo("k1"));
        Assert.That(accessor.Serializer, Is.Not.Null);
    }

    [Test]
    public void MvRegister_uses_supplied_serializer_when_provided()
    {
        var lattice = Substitute.For<ILattice>();
        var serializer = JsonLatticeSerializer<int>.Default;
        var accessor = lattice.MvRegister<int>("k1", serializer);
        Assert.That(accessor.Serializer, Is.SameAs(serializer));
    }

    [Test]
    public void MvRegister_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.MvRegister<string>("k"), Throws.ArgumentNullException);
    }

    [Test]
    public void MvRegister_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.MvRegister<string>(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => lattice.MvRegister<string>(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Default_MvRegisterAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(MvRegisterAccessor<string>);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void OrMap_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.OrMap<string, OrSet>("k1");
        Assert.That(accessor.Lattice, Is.SameAs(lattice));
        Assert.That(accessor.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void OrMap_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.OrMap<string, OrSet>("k"), Throws.ArgumentNullException);
    }

    [Test]
    public void OrMap_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.OrMap<string, OrSet>(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => lattice.OrMap<string, OrSet>(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Default_OrMapAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(OrMapAccessor<string, OrSet>);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Sequence_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.Sequence<string>("k1");
        Assert.That(accessor.Lattice, Is.SameAs(lattice));
        Assert.That(accessor.Key, Is.EqualTo("k1"));
        Assert.That(accessor.Serializer, Is.Not.Null);
    }

    [Test]
    public void Sequence_uses_supplied_serializer_when_provided()
    {
        var lattice = Substitute.For<ILattice>();
        var serializer = JsonLatticeSerializer<int>.Default;
        var accessor = lattice.Sequence<int>("k1", serializer);
        Assert.That(accessor.Serializer, Is.SameAs(serializer));
    }

    [Test]
    public void Sequence_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.Sequence<string>("k"), Throws.ArgumentNullException);
    }

    [Test]
    public void Sequence_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.Sequence<string>(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => lattice.Sequence<string>(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Default_RgaAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(RgaAccessor<string>);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }
}

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

    [Test]
    public void OrFlag_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.OrFlag("k1");
        Assert.That(accessor.Lattice, Is.SameAs(lattice));
        Assert.That(accessor.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void OrFlag_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.OrFlag("k"), Throws.ArgumentNullException);
    }

    [Test]
    public void OrFlag_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.OrFlag(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => lattice.OrFlag(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Default_OrFlagAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(OrFlagAccessor);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void RwFlag_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.RwFlag("k1");
        Assert.That(accessor.Lattice, Is.SameAs(lattice));
        Assert.That(accessor.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void RwFlag_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.RwFlag("k"), Throws.ArgumentNullException);
    }

    [Test]
    public void RwFlag_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.RwFlag(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => lattice.RwFlag(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Default_RwFlagAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(RwFlagAccessor);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    // ---- MaxRegister / MinRegister ----

    [Test]
    public void MaxRegister_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        Func<int, byte[]> selector = static v => BitConverter.GetBytes(v);
        var accessor = lattice.MaxRegister<int>("k1", selector);
        Assert.Multiple(() =>
        {
            Assert.That(accessor.Lattice, Is.SameAs(lattice));
            Assert.That(accessor.Key, Is.EqualTo("k1"));
            Assert.That(accessor.Serializer, Is.Not.Null);
            Assert.That(accessor.OrderKeySelector, Is.SameAs(selector));
        });
    }

    [Test]
    public void MaxRegister_uses_supplied_serializer_when_provided()
    {
        var lattice = Substitute.For<ILattice>();
        var serializer = JsonLatticeSerializer<int>.Default;
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v), serializer);
        Assert.That(accessor.Serializer, Is.SameAs(serializer));
    }

    [Test]
    public void MaxRegister_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.MaxRegister<int>("k", static v => BitConverter.GetBytes(v)), Throws.ArgumentNullException);
    }

    [Test]
    public void MaxRegister_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.Multiple(() =>
        {
            Assert.That(() => lattice.MaxRegister<int>("", static v => BitConverter.GetBytes(v)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => lattice.MaxRegister<int>(null!, static v => BitConverter.GetBytes(v)), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void MaxRegister_throws_on_null_order_key_selector()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.MaxRegister<int>("k", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Default_MaxRegisterAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(MaxRegisterAccessor<int>);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void MinRegister_returns_accessor_bound_to_lattice_and_key()
    {
        var lattice = Substitute.For<ILattice>();
        Func<int, byte[]> selector = static v => BitConverter.GetBytes(v);
        var accessor = lattice.MinRegister<int>("k1", selector);
        Assert.Multiple(() =>
        {
            Assert.That(accessor.Lattice, Is.SameAs(lattice));
            Assert.That(accessor.Key, Is.EqualTo("k1"));
            Assert.That(accessor.OrderKeySelector, Is.SameAs(selector));
        });
    }

    [Test]
    public void MinRegister_throws_on_null_lattice()
    {
        ILattice lattice = null!;
        Assert.That(() => lattice.MinRegister<int>("k", static v => BitConverter.GetBytes(v)), Throws.ArgumentNullException);
    }

    [Test]
    public void MinRegister_throws_on_empty_key()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.Multiple(() =>
        {
            Assert.That(() => lattice.MinRegister<int>("", static v => BitConverter.GetBytes(v)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => lattice.MinRegister<int>(null!, static v => BitConverter.GetBytes(v)), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void MinRegister_throws_on_null_order_key_selector()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.That(() => lattice.MinRegister<int>("k", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Default_MinRegisterAccessor_throws_InvalidOperationException_on_use()
    {
        var accessor = default(MinRegisterAccessor<int>);
        Assert.That(async () => await accessor.GetAsync(), Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task MaxRegister_SetAsync_writes_max_register_delta()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.ApplyCrdtDeltaAsync(Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v));

        await accessor.SetAsync(5);

        await lattice.Received(1).ApplyCrdtDeltaAsync("k1", LatticeMergeMode.MaxRegister, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task MinRegister_SetAsync_writes_min_register_delta()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.ApplyCrdtDeltaAsync(Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);
        var accessor = lattice.MinRegister<int>("k1", static v => BitConverter.GetBytes(v));

        await accessor.SetAsync(5);

        await lattice.Received(1).ApplyCrdtDeltaAsync("k1", LatticeMergeMode.MinRegister, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void MaxRegister_SetAsync_throws_on_non_positive_max_attempts()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v));
        Assert.That(async () => await accessor.SetAsync(1, default, 0), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task MaxRegister_GetAsync_returns_default_when_key_absent()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync("k1", Arg.Any<CancellationToken>()).Returns((byte[]?)null);
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v));

        var value = await accessor.GetAsync();

        Assert.That(value, Is.EqualTo(default(int)));
    }

    [Test]
    public async Task MaxRegister_GetAsync_returns_stored_value()
    {
        var lattice = Substitute.For<ILattice>();
        var register = BoundedRegister.CreateEmpty(isMin: false);
        var serializer = JsonLatticeSerializer<int>.Default;
        register.Set(serializer.Serialize(42), BitConverter.GetBytes(42));
        var stateBytes = JsonLatticeSerializer<BoundedRegister>.Default.Serialize(register);
        lattice.GetAsync("k1", Arg.Any<CancellationToken>()).Returns(stateBytes);
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v), serializer);

        var value = await accessor.GetAsync();

        Assert.That(value, Is.EqualTo(42));
    }

    [Test]
    public async Task MaxRegister_HasValueAsync_reflects_written_state()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync("k1", Arg.Any<CancellationToken>()).Returns((byte[]?)null);
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v));

        Assert.That(await accessor.HasValueAsync(), Is.False);
    }

    [Test]
    public async Task MaxRegister_GetRegisterAsync_returns_empty_max_register_when_absent()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync("k1", Arg.Any<CancellationToken>()).Returns((byte[]?)null);
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v));

        var register = await accessor.GetRegisterAsync();

        Assert.Multiple(() =>
        {
            Assert.That(register.IsBottom, Is.True);
            Assert.That(register.IsMin, Is.False);
        });
    }

    [Test]
    public async Task MinRegister_GetRegisterAsync_returns_empty_min_register_when_absent()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync("k1", Arg.Any<CancellationToken>()).Returns((byte[]?)null);
        var accessor = lattice.MinRegister<int>("k1", static v => BitConverter.GetBytes(v));

        var register = await accessor.GetRegisterAsync();

        Assert.That(register.IsMin, Is.True);
    }

    [Test]
    public async Task MaxRegister_MergeAsync_bottom_other_is_a_no_op()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v));

        await accessor.MergeAsync(BoundedRegister.CreateEmpty(isMin: false));

        await lattice.DidNotReceive().ApplyCrdtDeltaAsync(Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task MaxRegister_MergeAsync_written_other_applies_delta()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.ApplyCrdtDeltaAsync(Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);
        var other = BoundedRegister.CreateEmpty(isMin: false);
        other.Set(new byte[] { 1 }, new byte[] { 1 });
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v));

        await accessor.MergeAsync(other);

        await lattice.Received(1).ApplyCrdtDeltaAsync("k1", LatticeMergeMode.MaxRegister, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void MaxRegister_MergeAsync_throws_on_null_other()
    {
        var lattice = Substitute.For<ILattice>();
        var accessor = lattice.MaxRegister<int>("k1", static v => BitConverter.GetBytes(v));
        Assert.That(async () => await accessor.MergeAsync(null!), Throws.ArgumentNullException);
    }
}

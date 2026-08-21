using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for <see cref="MinRegisterAccessor{T}"/>: the monotone
/// low-water-mark register value surface over an <see cref="ILattice"/> key.
/// Exercises the blind-candidate writes (with and without a TTL), the reads
/// (empty and seeded), the out-of-band merge, and the argument / initialisation
/// guards - all against a mocked lattice so no cluster is needed.
/// </summary>
[TestFixture]
public class MinRegisterAccessorTests
{
    private static readonly Func<int, byte[]> OrderKey = v => BitConverter.GetBytes(v);

    private static ILattice Empty(string key)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(key, Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));
        return lattice;
    }

    private static ILattice Seeded(string key, int value)
    {
        var lattice = Substitute.For<ILattice>();
        var register = new BoundedRegister(isMin: true);
        register.Set(JsonLatticeSerializer<int>.Default.Serialize(value), OrderKey(value));
        var bytes = JsonLatticeSerializer<BoundedRegister>.Default.Serialize(register);
        lattice.GetAsync(key, Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(bytes));
        return lattice;
    }

    private static MinRegisterAccessor<int> Accessor(ILattice lattice, string key) =>
        lattice.MinRegister(key, OrderKey, JsonLatticeSerializer<int>.Default);

    [Test]
    public async Task SetAsync_applies_min_register_candidate_delta()
    {
        var lattice = Empty("k");
        byte[]? applied = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.MinRegister, Arg.Do<byte[]>(b => applied = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        await Accessor(lattice, "k").SetAsync(7);

        Assert.That(applied, Is.Not.Null);
        var delta = JsonLatticeSerializer<BoundedRegisterDelta>.Default.Deserialize(applied!);
        Assert.Multiple(() =>
        {
            Assert.That(delta.HasValue, Is.True);
            Assert.That(delta.OrderKey, Is.EqualTo(OrderKey(7)));
        });
    }

    [Test]
    public async Task SetAsync_with_ttl_applies_delta_on_ttl_overload()
    {
        var lattice = Empty("k");
        var called = false;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.MinRegister, Arg.Any<byte[]>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero)
            .AndDoes(_ => called = true);

        await Accessor(lattice, "k").SetAsync(3, TimeSpan.FromMinutes(5));

        Assert.That(called, Is.True);
    }

    [Test]
    public void SetAsync_rejects_maxAttempts_below_one()
    {
        var lattice = Empty("k");
        Assert.That(async () => await Accessor(lattice, "k").SetAsync(1, maxAttempts: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void SetAsync_ttl_overload_rejects_maxAttempts_below_one()
    {
        var lattice = Empty("k");
        Assert.That(async () => await Accessor(lattice, "k").SetAsync(1, TimeSpan.FromMinutes(1), maxAttempts: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task GetAsync_absent_key_returns_default()
    {
        var value = await Accessor(Empty("k"), "k").GetAsync();
        Assert.That(value, Is.EqualTo(0));
    }

    [Test]
    public async Task GetAsync_seeded_key_returns_stored_value()
    {
        var value = await Accessor(Seeded("k", 42), "k").GetAsync();
        Assert.That(value, Is.EqualTo(42));
    }

    [Test]
    public async Task HasValueAsync_reflects_presence()
    {
        Assert.That(await Accessor(Seeded("k", 5), "k").HasValueAsync(), Is.True);
        Assert.That(await Accessor(Empty("k"), "k").HasValueAsync(), Is.False);
    }

    [Test]
    public async Task GetRegisterAsync_absent_returns_empty_min_register()
    {
        var register = await Accessor(Empty("k"), "k").GetRegisterAsync();
        Assert.Multiple(() =>
        {
            Assert.That(register.HasValue, Is.False);
            Assert.That(register.IsMin, Is.True);
        });
    }

    [Test]
    public async Task MergeAsync_with_value_applies_delta()
    {
        var lattice = Empty("k");
        byte[]? applied = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.MinRegister, Arg.Do<byte[]>(b => applied = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        var other = new BoundedRegister(isMin: true);
        other.Set(JsonLatticeSerializer<int>.Default.Serialize(9), OrderKey(9));
        await Accessor(lattice, "k").MergeAsync(other);

        Assert.That(applied, Is.Not.Null);
        var delta = JsonLatticeSerializer<BoundedRegisterDelta>.Default.Deserialize(applied!);
        Assert.That(delta.HasValue, Is.True);
    }

    [Test]
    public async Task MergeAsync_empty_register_is_noop()
    {
        var lattice = Empty("k");
        await Accessor(lattice, "k").MergeAsync(BoundedRegister.CreateEmpty(isMin: true));

        await lattice.DidNotReceive().ApplyCrdtDeltaAsync(
            Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void MergeAsync_rejects_null_other()
    {
        var lattice = Empty("k");
        Assert.That(async () => await Accessor(lattice, "k").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MergeAsync_rejects_maxAttempts_below_one()
    {
        var lattice = Empty("k");
        var other = new BoundedRegister(isMin: true);
        other.Set(JsonLatticeSerializer<int>.Default.Serialize(1), OrderKey(1));
        Assert.That(async () => await Accessor(lattice, "k").MergeAsync(other, maxAttempts: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Default_accessor_is_uninitialised_and_throws()
    {
        Assert.That(async () => await default(MinRegisterAccessor<int>).GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Accessor_exposes_lattice_key_serializer_and_selector()
    {
        var lattice = Empty("kk");
        var acc = Accessor(lattice, "kk");

        Assert.Multiple(() =>
        {
            Assert.That(acc.Lattice, Is.SameAs(lattice));
            Assert.That(acc.Key, Is.EqualTo("kk"));
            Assert.That(acc.Serializer, Is.SameAs(JsonLatticeSerializer<int>.Default));
            Assert.That(acc.OrderKeySelector, Is.SameAs(OrderKey));
        });
    }

    [Test]
    public void SetAsync_honours_cancellation()
    {
        var lattice = Empty("k");
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.MinRegister, Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromCanceled<HybridLogicalClock>(cts.Token));

        Assert.That(async () => await Accessor(lattice, "k").SetAsync(1, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}

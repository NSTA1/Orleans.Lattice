using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for <see cref="GCounterAccessor"/>: the grow-only counter value
/// surface over an <see cref="ILattice"/> key. Exercises reads (empty and
/// seeded), the scalar value read, the increment path (with and without a TTL),
/// staging for a cross-tree atomic write, the out-of-band merge, and the
/// argument / initialisation guards - all against a mocked lattice.
/// </summary>
[TestFixture]
public class GCounterAccessorTests
{
    private static ILattice Empty(string key)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(key, Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));
        return lattice;
    }

    private static ILattice Seeded(string key, string replicaId, long amount)
    {
        var lattice = Substitute.For<ILattice>();
        var counter = new GCounter();
        counter.Increment(replicaId, amount);
        var bytes = JsonLatticeSerializer<GCounter>.Default.Serialize(counter);
        lattice.GetAsync(key, Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(bytes));
        return lattice;
    }

    [Test]
    public async Task GetAsync_absent_key_returns_empty_counter()
    {
        var counter = await Empty("k").GCounter("k").GetAsync();
        Assert.That(counter.Value, Is.EqualTo(0));
    }

    [Test]
    public async Task GetAsync_seeded_key_returns_stored_state()
    {
        var counter = await Seeded("k", "r", 5).GCounter("k").GetAsync();
        Assert.That(counter.Value, Is.EqualTo(5));
    }

    [Test]
    public async Task ValueAsync_returns_scalar_value()
    {
        var value = await Seeded("k", "r", 8).GCounter("k").ValueAsync();
        Assert.That(value, Is.EqualTo(8));
    }

    [Test]
    public async Task IncrementAsync_applies_gcounter_delta_for_replica()
    {
        var lattice = Empty("k");
        byte[]? applied = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, Arg.Do<byte[]>(b => applied = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        await lattice.GCounter("k").IncrementAsync("r", 3);

        Assert.That(applied, Is.Not.Null);
        var delta = JsonLatticeSerializer<GCounterDelta>.Default.Deserialize(applied!);
        Assert.That(delta.Increments["r"], Is.EqualTo(3));
    }

    [Test]
    public async Task IncrementAsync_with_ttl_applies_delta_on_ttl_overload()
    {
        var lattice = Empty("k");
        var called = false;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, Arg.Any<byte[]>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero)
            .AndDoes(_ => called = true);

        await lattice.GCounter("k").IncrementAsync("r", 2, TimeSpan.FromMinutes(5));

        Assert.That(called, Is.True);
    }

    [Test]
    public void IncrementAsync_rejects_empty_replica()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.GCounter("k").IncrementAsync(""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void IncrementAsync_rejects_negative_amount()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.GCounter("k").IncrementAsync("r", -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void IncrementAsync_rejects_maxAttempts_below_one()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.GCounter("k").IncrementAsync("r", 1, maxAttempts: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task StageIncrementAsync_mints_delta_without_applying()
    {
        var lattice = Empty("k");
        var staged = await lattice.GCounter("k").StageIncrementAsync("r", 4);

        Assert.That(staged.Key, Is.EqualTo("k"));
        var merged = JsonLatticeSerializer<GCounter>.Default.Deserialize(staged.Value);
        Assert.That(merged.Value, Is.EqualTo(4));
        await lattice.DidNotReceive().ApplyCrdtDeltaAsync(
            Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task MergeAsync_flattens_other_counter_into_delta()
    {
        var lattice = Empty("k");
        byte[]? applied = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, Arg.Do<byte[]>(b => applied = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        var other = new GCounter();
        other.Increment("x", 6);
        await lattice.GCounter("k").MergeAsync(other);

        Assert.That(applied, Is.Not.Null);
        var delta = JsonLatticeSerializer<GCounterDelta>.Default.Deserialize(applied!);
        Assert.That(delta.Increments["x"], Is.EqualTo(6));
    }

    [Test]
    public void MergeAsync_rejects_null_other()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.GCounter("k").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Default_accessor_is_uninitialised_and_throws()
    {
        Assert.That(async () => await default(GCounterAccessor).GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Accessor_exposes_lattice_and_key()
    {
        var lattice = Empty("kk");
        var acc = lattice.GCounter("kk");

        Assert.Multiple(() =>
        {
            Assert.That(acc.Lattice, Is.SameAs(lattice));
            Assert.That(acc.Key, Is.EqualTo("kk"));
        });
    }

    [Test]
    public void IncrementAsync_honours_cancellation()
    {
        var lattice = Empty("k");
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(async () => await lattice.GCounter("k").IncrementAsync("r", 1, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}

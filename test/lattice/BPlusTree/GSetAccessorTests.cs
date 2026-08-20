using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for <see cref="GSetAccessor"/>: the grow-only set value surface
/// over an <see cref="ILattice"/> key. Exercises reads (empty and seeded), the
/// idempotent add path (with and without a TTL), membership and enumeration, the
/// out-of-band merge, staging for a cross-tree atomic write, and the argument /
/// initialisation guards - all against a mocked lattice so no cluster is needed.
/// </summary>
[TestFixture]
public class GSetAccessorTests
{
    private static ILattice Empty(string key)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(key, Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));
        return lattice;
    }

    private static ILattice Seeded(string key, GSet state)
    {
        var lattice = Substitute.For<ILattice>();
        var bytes = JsonLatticeSerializer<GSet>.Default.Serialize(state);
        lattice.GetAsync(key, Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(bytes));
        return lattice;
    }

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task GetAsync_absent_key_returns_empty_set()
    {
        var set = await Empty("k").GSet("k").GetAsync();
        Assert.That(set.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task GetAsync_seeded_key_returns_stored_members()
    {
        var seed = new GSet();
        seed.Add(Bytes("apple"));
        var set = await Seeded("k", seed).GSet("k").GetAsync();

        Assert.That(set.Contains(Bytes("apple")), Is.True);
    }

    [Test]
    public void Accessor_exposes_lattice_and_key()
    {
        var lattice = Empty("kk");
        var acc = lattice.GSet("kk");

        Assert.Multiple(() =>
        {
            Assert.That(acc.Lattice, Is.SameAs(lattice));
            Assert.That(acc.Key, Is.EqualTo("kk"));
        });
    }

    [Test]
    public async Task AddAsync_applies_gset_delta_with_element()
    {
        var lattice = Empty("k");
        byte[]? applied = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GSet, Arg.Do<byte[]>(b => applied = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        await lattice.GSet("k").AddAsync(Bytes("apple"));

        Assert.That(applied, Is.Not.Null);
        var delta = JsonLatticeSerializer<GSetDelta>.Default.Deserialize(applied!);
        Assert.That(delta.Adds, Has.Count.EqualTo(1));
        Assert.That(delta.Adds[0], Is.EqualTo(Bytes("apple")));
    }

    [Test]
    public async Task AddAsync_with_ttl_applies_delta_on_ttl_overload()
    {
        var lattice = Empty("k");
        var called = false;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GSet, Arg.Any<byte[]>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero)
            .AndDoes(_ => called = true);

        await lattice.GSet("k").AddAsync(Bytes("apple"), TimeSpan.FromMinutes(5));

        Assert.That(called, Is.True);
    }

    [Test]
    public async Task ContainsAsync_returns_true_for_member_and_false_for_absent()
    {
        var seed = new GSet();
        seed.Add(Bytes("apple"));
        var acc = Seeded("k", seed).GSet("k");

        Assert.That(await acc.ContainsAsync(Bytes("apple")), Is.True);
        Assert.That(await acc.ContainsAsync(Bytes("pear")), Is.False);
    }

    [Test]
    public async Task ToListAsync_returns_members()
    {
        var seed = new GSet();
        seed.Add(Bytes("a"));
        seed.Add(Bytes("b"));
        var list = await Seeded("k", seed).GSet("k").ToListAsync();

        Assert.That(list, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task MergeAsync_flattens_other_set_into_adds()
    {
        var lattice = Empty("k");
        byte[]? applied = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GSet, Arg.Do<byte[]>(b => applied = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        var other = new GSet();
        other.Add(Bytes("x"));
        other.Add(Bytes("y"));
        await lattice.GSet("k").MergeAsync(other);

        Assert.That(applied, Is.Not.Null);
        var delta = JsonLatticeSerializer<GSetDelta>.Default.Deserialize(applied!);
        Assert.That(delta.Adds, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task MergeAsync_empty_set_produces_no_adds()
    {
        var lattice = Empty("k");
        byte[]? applied = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GSet, Arg.Do<byte[]>(b => applied = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        await lattice.GSet("k").MergeAsync(new GSet());

        var delta = JsonLatticeSerializer<GSetDelta>.Default.Deserialize(applied!);
        Assert.That(delta.Adds, Is.Empty);
    }

    [Test]
    public async Task StageAddAsync_mints_value_and_delta_without_applying()
    {
        var lattice = Empty("k");
        var staged = await lattice.GSet("k").StageAddAsync(Bytes("apple"));

        Assert.That(staged.Key, Is.EqualTo("k"));
        var merged = JsonLatticeSerializer<GSet>.Default.Deserialize(staged.Value);
        Assert.That(merged.Contains(Bytes("apple")), Is.True);
        await lattice.DidNotReceive().ApplyCrdtDeltaAsync(
            Arg.Any<string>(), Arg.Any<LatticeMergeMode>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void AddAsync_rejects_null_element()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.GSet("k").AddAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void AddAsync_rejects_maxAttempts_below_one()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.GSet("k").AddAsync(Bytes("a"), maxAttempts: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ContainsAsync_rejects_null_element()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.GSet("k").ContainsAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MergeAsync_rejects_null_other()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.GSet("k").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Default_accessor_is_uninitialised_and_throws()
    {
        Assert.That(async () => await default(GSetAccessor).GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void AddAsync_honours_cancellation()
    {
        var lattice = Empty("k");
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(async () => await lattice.GSet("k").AddAsync(Bytes("a"), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}

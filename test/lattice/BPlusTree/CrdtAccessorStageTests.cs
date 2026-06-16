using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for the CRDT accessor <c>Stage*</c> methods that mint a
/// <see cref="LatticeStagedCrdtWrite"/> for a cross-tree atomic write: the staged
/// (value, delta) round-trips through the primitive's <c>MergeDelta</c> to the
/// same state the live mutator yields, the mint advances the next monotonic dot,
/// and no <c>ApplyCrdtDeltaAsync</c> is issued on the stage path.
/// </summary>
[TestFixture]
public class CrdtAccessorStageTests
{
    private static ILattice Empty(string key)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(key).Returns(Task.FromResult<byte[]?>(null));
        return lattice;
    }

    private static ILattice Seeded<T>(string key, T state)
    {
        var lattice = Substitute.For<ILattice>();
        var bytes = JsonLatticeSerializer<T>.Default.Serialize(state);
        lattice.GetAsync(key).Returns(Task.FromResult<byte[]?>(bytes));
        return lattice;
    }

    // === OrFlag ===

    [Test]
    public async Task StageEnableAsync_orflag_mints_first_dot_and_enabled_value()
    {
        var lattice = Empty("k");
        var staged = await lattice.OrFlag("k").StageEnableAsync("r1");

        Assert.That(staged.Key, Is.EqualTo("k"));
        var delta = JsonLatticeSerializer<OrFlagDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Enables, Has.Count.EqualTo(1));
        Assert.That(delta.Enables[0].ReplicaId, Is.EqualTo("r1"));
        Assert.That(delta.Enables[0].Counter, Is.EqualTo(1));
        var merged = JsonLatticeSerializer<OrFlag>.Default.Deserialize(staged.Value);
        Assert.That(merged.IsEnabled, Is.True);
    }

    [Test]
    public async Task StageEnableAsync_orflag_advances_next_monotonic_dot()
    {
        var seed = new OrFlag();
        seed.Enable("r1", 5);
        var lattice = Seeded("k", seed);

        var staged = await lattice.OrFlag("k").StageEnableAsync("r1");

        var delta = JsonLatticeSerializer<OrFlagDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Enables[0].Counter, Is.EqualTo(6));
    }

    [Test]
    public async Task StageEnableAsync_orflag_matches_live_mutator_delta()
    {
        var lattice = Empty("k");
        byte[]? liveDelta = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrFlag, Arg.Do<byte[]>(b => liveDelta = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        var staged = await lattice.OrFlag("k").StageEnableAsync("r1");
        await lattice.OrFlag("k").EnableAsync("r1");

        Assert.That(liveDelta, Is.Not.Null);
        Assert.That(staged.Delta, Is.EqualTo(liveDelta));
    }

    [Test]
    public async Task StageDisableAsync_orflag_tombstones_observed_enable()
    {
        var seed = new OrFlag();
        seed.Enable("r1", 1);
        var lattice = Seeded("k", seed);

        var staged = await lattice.OrFlag("k").StageDisableAsync();

        var delta = JsonLatticeSerializer<OrFlagDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Disables, Has.Count.EqualTo(1));
        var merged = JsonLatticeSerializer<OrFlag>.Default.Deserialize(staged.Value);
        Assert.That(merged.IsEnabled, Is.False);
    }

    [Test]
    public void StageEnableAsync_orflag_rejects_empty_replicaId()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.OrFlag("k").StageEnableAsync(""),
            Throws.InstanceOf<ArgumentException>());
    }

    // === OrSet ===

    [Test]
    public async Task StageAddAsync_orset_mints_dot_and_membership_value()
    {
        var lattice = Empty("k");
        var staged = await lattice.OrSet("k").StageAddAsync(Bytes("apple"), "r1");

        var delta = JsonLatticeSerializer<OrSetDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Adds, Has.Count.EqualTo(1));
        Assert.That(delta.Adds[0].Counter, Is.EqualTo(1));
        var merged = JsonLatticeSerializer<OrSet>.Default.Deserialize(staged.Value);
        Assert.That(merged.Contains(Bytes("apple")), Is.True);
    }

    [Test]
    public async Task StageAddAsync_orset_matches_live_mutator_delta()
    {
        var lattice = Empty("k");
        byte[]? liveDelta = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, Arg.Do<byte[]>(b => liveDelta = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        var staged = await lattice.OrSet("k").StageAddAsync(Bytes("apple"), "r1");
        await lattice.OrSet("k").AddAsync(Bytes("apple"), "r1");

        Assert.That(staged.Delta, Is.EqualTo(liveDelta));
    }

    [Test]
    public async Task StageRemoveAsync_orset_tombstones_observed_dots()
    {
        var seed = new OrSet();
        seed.Add(Bytes("apple"), "r1", 1);
        var lattice = Seeded("k", seed);

        var staged = await lattice.OrSet("k").StageRemoveAsync(Bytes("apple"));

        var merged = JsonLatticeSerializer<OrSet>.Default.Deserialize(staged.Value);
        Assert.That(merged.Contains(Bytes("apple")), Is.False);
    }

    [Test]
    public void StageAddAsync_orset_rejects_null_element()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.OrSet("k").StageAddAsync(null!, "r1"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // === RwFlag ===

    [Test]
    public async Task StageEnableAsync_rwflag_enables_value()
    {
        var lattice = Empty("k");
        var staged = await lattice.RwFlag("k").StageEnableAsync("r1");

        var merged = JsonLatticeSerializer<RwFlag>.Default.Deserialize(staged.Value);
        Assert.That(merged.IsEnabled, Is.True);
    }

    [Test]
    public async Task StageDisableAsync_rwflag_mints_disable_dot()
    {
        var seed = new RwFlag();
        seed.Enable("r1", 1);
        var lattice = Seeded("k", seed);

        var staged = await lattice.RwFlag("k").StageDisableAsync("r2");

        var delta = JsonLatticeSerializer<RwFlagDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Disables, Has.Count.EqualTo(1));
        var merged = JsonLatticeSerializer<RwFlag>.Default.Deserialize(staged.Value);
        Assert.That(merged.IsEnabled, Is.False);
    }

    [Test]
    public void StageDisableAsync_rwflag_requires_replicaId()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwFlag("k").StageDisableAsync(""),
            Throws.InstanceOf<ArgumentException>());
    }

    // === PnCounter ===

    [Test]
    public async Task StageIncrementAsync_pncounter_mints_value_and_delta()
    {
        var lattice = Empty("k");
        var staged = await lattice.PnCounter("k").StageIncrementAsync("r1", 5);

        var delta = JsonLatticeSerializer<PnCounterDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Increments["r1"], Is.EqualTo(5));
        var merged = JsonLatticeSerializer<PnCounter>.Default.Deserialize(staged.Value);
        Assert.That(merged.Value, Is.EqualTo(5));
    }

    [Test]
    public async Task StageIncrementAsync_pncounter_advances_cumulative_component()
    {
        var seed = new PnCounter();
        seed.Increment("r1", 10);
        var lattice = Seeded("k", seed);

        var staged = await lattice.PnCounter("k").StageIncrementAsync("r1", 5);

        var delta = JsonLatticeSerializer<PnCounterDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Increments["r1"], Is.EqualTo(15));
        var merged = JsonLatticeSerializer<PnCounter>.Default.Deserialize(staged.Value);
        Assert.That(merged.Value, Is.EqualTo(15));
    }

    [Test]
    public async Task StageDecrementAsync_pncounter_mints_negative_component()
    {
        var lattice = Empty("k");
        var staged = await lattice.PnCounter("k").StageDecrementAsync("r1", 3);

        var merged = JsonLatticeSerializer<PnCounter>.Default.Deserialize(staged.Value);
        Assert.That(merged.Value, Is.EqualTo(-3));
    }

    [Test]
    public async Task StageIncrementAsync_pncounter_matches_live_mutator_delta()
    {
        var lattice = Empty("k");
        byte[]? liveDelta = null;
        lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.PnCounter, Arg.Do<byte[]>(b => liveDelta = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        var staged = await lattice.PnCounter("k").StageIncrementAsync("r1", 7);
        await lattice.PnCounter("k").IncrementAsync("r1", 7);

        Assert.That(staged.Delta, Is.EqualTo(liveDelta));
    }

    [Test]
    public void StageIncrementAsync_pncounter_rejects_negative_amount()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.PnCounter("k").StageIncrementAsync("r1", -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // === MvRegister ===

    [Test]
    public async Task StageSetAsync_mvregister_records_single_entry()
    {
        var lattice = Empty("k");
        var staged = await lattice.MvRegister<string>("k").StageSetAsync("hello", "r1");

        var merged = JsonLatticeSerializer<MvRegister>.Default.Deserialize(staged.Value);
        Assert.That(merged.Entries, Has.Count.EqualTo(1));
        var delta = JsonLatticeSerializer<MvRegisterDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public void StageSetAsync_mvregister_requires_replicaId()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.MvRegister<string>("k").StageSetAsync("v", ""),
            Throws.InstanceOf<ArgumentException>());
    }

    // === VersionVector ===

    [Test]
    public async Task StageTickAsync_versionvector_advances_entry()
    {
        var lattice = Empty("k");
        var staged = await lattice.VersionVector("k").StageTickAsync("r1");

        var merged = JsonLatticeSerializer<VersionVector>.Default.Deserialize(staged.Value);
        Assert.That(merged.GetClock("r1"), Is.Not.EqualTo(HybridLogicalClock.Zero));
        var delta = JsonLatticeSerializer<VersionVectorDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Entries.ContainsKey("r1"), Is.True);
    }

    // === Rga ===

    [Test]
    public async Task StageInsertAfterAsync_rga_appends_value()
    {
        var lattice = Empty("k");
        var staged = await lattice.Sequence<string>("k").StageInsertAfterAsync(Rga.Root, "r1", "a");

        var merged = JsonLatticeSerializer<Rga>.Default.Deserialize(staged.Value);
        var list = merged.ToList();
        Assert.That(list, Has.Count.EqualTo(1));
        Assert.That(JsonLatticeSerializer<string>.Default.Deserialize(list[0].Value), Is.EqualTo("a"));
    }

    [Test]
    public async Task StageInsertAtAsync_rga_inserts_at_index()
    {
        var lattice = Empty("k");
        var staged = await lattice.Sequence<string>("k").StageInsertAtAsync(0, "r1", "a");

        var merged = JsonLatticeSerializer<Rga>.Default.Deserialize(staged.Value);
        Assert.That(merged.ToList(), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task StageRemoveAtAsync_rga_tombstones_node()
    {
        var seed = new Rga();
        var dot = seed.InsertAfter(Rga.Root, "r1", JsonLatticeSerializer<string>.Default.Serialize("a"));
        var lattice = Seeded("k", seed);

        var staged = await lattice.Sequence<string>("k").StageRemoveAtAsync(0);

        var merged = JsonLatticeSerializer<Rga>.Default.Deserialize(staged.Value);
        Assert.That(merged.IsEmpty, Is.True);
        var delta = JsonLatticeSerializer<RgaDelta>.Default.Deserialize(staged.Delta);
        Assert.That(delta.Tombstones, Has.Count.EqualTo(1));
        Assert.That(delta.Tombstones[0], Is.EqualTo(dot));
    }

    [Test]
    public async Task StageRemoveAsync_rga_tombstones_by_dot()
    {
        var seed = new Rga();
        var dot = seed.InsertAfter(Rga.Root, "r1", JsonLatticeSerializer<string>.Default.Serialize("a"));
        var lattice = Seeded("k", seed);

        var staged = await lattice.Sequence<string>("k").StageRemoveAsync(dot);

        var merged = JsonLatticeSerializer<Rga>.Default.Deserialize(staged.Value);
        Assert.That(merged.IsEmpty, Is.True);
    }

    // === Builder.Set(staged) guards ===

    [Test]
    public void Builder_Set_staged_rejects_default_token()
    {
        var factory = Substitute.For<IGrainFactory>();
        var builder = factory.BeginAtomicWrite("op").ForTree("t");
        Assert.That(() => builder.Set(default(LatticeStagedCrdtWrite)),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task Builder_Set_staged_requires_a_selected_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Empty("k");
        var staged = await lattice.OrFlag("k").StageEnableAsync("r1");
        var builder = factory.BeginAtomicWrite("op");
        Assert.That(() => builder.Set(staged), Throws.InstanceOf<InvalidOperationException>());
    }

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);
}

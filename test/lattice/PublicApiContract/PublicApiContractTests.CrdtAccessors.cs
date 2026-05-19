using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── CrdtLatticeExtensions factory methods ───────────────────────────

    [Test]
    public void OrSet_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.OrSet(null!, "k"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task OrSet_with_empty_key_throws()
    {
        var tree = Tree("pac-crdt-orset-emptykey-" + Guid.NewGuid().ToString("N")[..8]);
        await tree.SetAsync("warmup", Bytes("v"));
        Assert.That(() => tree.OrSet(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void PnCounter_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.PnCounter(null!, "k"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void VersionVector_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.VersionVector(null!, "k"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ── OrSetAccessor ───────────────────────────────────────────────────

    [Test]
    public async Task OrSet_GetAsync_on_missing_key_returns_empty_set()
    {
        var treeId = "pac-crdt-orset-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var set = await tree.OrSet("absent").GetAsync();
        Assert.That(set, Is.Not.Null);
        Assert.That(set.IsEmpty, Is.True);
    }

    [Test]
    public async Task OrSet_AddAsync_makes_element_observable()
    {
        var treeId = "pac-crdt-orset-add-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.OrSet("set");
        await accessor.AddAsync(Bytes("alpha"), replicaId: "r1");

        Assert.That(await accessor.ContainsAsync(Bytes("alpha")), Is.True);
    }

    [Test]
    public async Task OrSet_RemoveAsync_makes_element_unobservable()
    {
        var treeId = "pac-crdt-orset-remove-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.OrSet("set");
        await accessor.AddAsync(Bytes("alpha"), replicaId: "r1");
        await accessor.RemoveAsync(Bytes("alpha"));

        Assert.That(await accessor.ContainsAsync(Bytes("alpha")), Is.False);
    }

    [Test]
    public async Task OrSet_RemoveAsync_on_missing_element_is_no_op()
    {
        var treeId = "pac-crdt-orset-remove-missing-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        // Removing an element that was never added is a successful no-op.
        Assert.That(
            async () => await tree.OrSet("set").RemoveAsync(Bytes("ghost")),
            Throws.Nothing);
    }

    [Test]
    public async Task OrSet_AddAsync_with_null_element_throws()
    {
        var treeId = "pac-crdt-orset-nullel-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.OrSet("set").AddAsync(null!, replicaId: "r1"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task OrSet_AddAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-orset-emptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.OrSet("set").AddAsync(Bytes("alpha"), replicaId: string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task OrSet_MergeAsync_with_null_other_throws()
    {
        var treeId = "pac-crdt-orset-mergenull-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.OrSet("set").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task OrSet_MergeAsync_unions_state()
    {
        var treeId = "pac-crdt-orset-merge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        var accessor = tree.OrSet("set");
        await accessor.AddAsync(Bytes("a"), replicaId: "r1");

        var other = new OrSet();
        other.Add(Bytes("b"), "r2", 1);
        await accessor.MergeAsync(other);

        Assert.That(await accessor.ContainsAsync(Bytes("a")), Is.True);
        Assert.That(await accessor.ContainsAsync(Bytes("b")), Is.True);
    }

    // ── PnCounterAccessor ───────────────────────────────────────────────

    [Test]
    public async Task PnCounter_ValueAsync_on_missing_key_returns_zero()
    {
        var treeId = "pac-crdt-pn-zero-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        Assert.That(await tree.PnCounter("absent").ValueAsync(), Is.EqualTo(0));
    }

    [Test]
    public async Task PnCounter_IncrementAsync_then_ValueAsync_returns_increment()
    {
        var treeId = "pac-crdt-pn-inc-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var counter = tree.PnCounter("c");
        await counter.IncrementAsync(replicaId: "r1", amount: 5);

        Assert.That(await counter.ValueAsync(), Is.EqualTo(5));
    }

    [Test]
    public async Task PnCounter_DecrementAsync_subtracts_from_value()
    {
        var treeId = "pac-crdt-pn-dec-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var counter = tree.PnCounter("c");
        await counter.IncrementAsync("r1", amount: 10);
        await counter.DecrementAsync("r1", amount: 3);

        Assert.That(await counter.ValueAsync(), Is.EqualTo(7));
    }

    [Test]
    public async Task PnCounter_IncrementAsync_with_negative_amount_throws()
    {
        var treeId = "pac-crdt-pn-neginc-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.PnCounter("c").IncrementAsync("r1", amount: -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task PnCounter_DecrementAsync_with_negative_amount_throws()
    {
        var treeId = "pac-crdt-pn-negdec-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.PnCounter("c").DecrementAsync("r1", amount: -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task PnCounter_MergeAsync_unions_per_replica_components()
    {
        var treeId = "pac-crdt-pn-merge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var counter = tree.PnCounter("c");
        await counter.IncrementAsync("r1", amount: 4);

        var other = new PnCounter();
        other.Increment("r2", 6);
        await counter.MergeAsync(other);

        Assert.That(await counter.ValueAsync(), Is.EqualTo(10));
    }

    [Test]
    public async Task PnCounter_GetAsync_returns_underlying_PnCounter()
    {
        var treeId = "pac-crdt-pn-get-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var counter = tree.PnCounter("c");
        await counter.IncrementAsync("r1", 3);

        var underlying = await counter.GetAsync();
        Assert.That(underlying, Is.Not.Null);
        Assert.That(underlying.Value, Is.EqualTo(3));
    }

    // ── VersionVectorAccessor ───────────────────────────────────────────

    [Test]
    public async Task VersionVector_GetAsync_on_missing_key_returns_empty_vector()
    {
        var treeId = "pac-crdt-vv-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var vv = await tree.VersionVector("absent").GetAsync();
        Assert.That(vv, Is.Not.Null);
        Assert.That(vv.Entries, Is.Empty);
    }

    [Test]
    public async Task VersionVector_TickAsync_advances_replica_entry()
    {
        var treeId = "pac-crdt-vv-tick-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var vv = tree.VersionVector("vec");
        await vv.TickAsync("r1");
        await vv.TickAsync("r1");

        var read = await vv.GetAsync();
        Assert.That(read.Entries.ContainsKey("r1"), Is.True);
    }

    [Test]
    public async Task VersionVector_TickAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-vv-emptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.VersionVector("vec").TickAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task VersionVector_MergeAsync_with_null_other_throws()
    {
        var treeId = "pac-crdt-vv-mergenull-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.VersionVector("vec").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task VersionVector_MergeAsync_pulls_in_remote_entries()
    {
        var treeId = "pac-crdt-vv-merge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var vv = tree.VersionVector("vec");
        await vv.TickAsync("r1");

        var remote = new VersionVector();
        remote.Tick("r2");
        await vv.MergeAsync(remote);

        var read = await vv.GetAsync();
        Assert.That(read.Entries.ContainsKey("r1"), Is.True);
        Assert.That(read.Entries.ContainsKey("r2"), Is.True);
    }

    // ── Default-constructed accessor guard ──────────────────────────────

    [Test]
    public void OrSetAccessor_default_throws_on_use()
    {
        var accessor = default(OrSetAccessor);
        Assert.That(
            async () => await accessor.GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void PnCounterAccessor_default_throws_on_use()
    {
        var accessor = default(PnCounterAccessor);
        Assert.That(
            async () => await accessor.ValueAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void VersionVectorAccessor_default_throws_on_use()
    {
        var accessor = default(VersionVectorAccessor);
        Assert.That(
            async () => await accessor.GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ── MvRegisterAccessor ──────────────────────────────────────────────

    [Test]
    public async Task MvRegister_GetAsync_on_missing_key_returns_empty_register()
    {
        var treeId = "pac-crdt-mv-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var register = await tree.MvRegister<string>("absent").GetAsync();
        Assert.That(register, Is.Not.Null);
        Assert.That(register.IsEmpty, Is.True);
    }

    [Test]
    public async Task MvRegister_SetAsync_stores_value()
    {
        var treeId = "pac-crdt-mv-set-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.MvRegister<string>("reg");
        await accessor.SetAsync("r1", "alpha");

        var values = await accessor.ValuesAsync();
        Assert.That(values, Is.EquivalentTo(new[] { "alpha" }));
    }

    [Test]
    public async Task MvRegister_MergeAsync_preserves_concurrent_values()
    {
        var treeId = "pac-crdt-mv-merge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.MvRegister<string>("reg");
        await accessor.SetAsync("r1", "alpha");

        var remote = new MvRegister();
        remote.Set("r2", JsonLatticeSerializer<string>.Default.Serialize("beta"));
        await accessor.MergeAsync(remote);

        var values = await accessor.ValuesAsync();
        Assert.That(values, Is.EquivalentTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public async Task MvRegister_SetAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-mv-emptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.MvRegister<string>("reg").SetAsync(string.Empty, "x"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task MvRegister_MergeAsync_with_null_other_throws()
    {
        var treeId = "pac-crdt-mv-mergenull-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.MvRegister<string>("reg").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MvRegisterAccessor_default_throws_on_use()
    {
        var accessor = default(MvRegisterAccessor<string>);
        Assert.That(
            async () => await accessor.GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }
}

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ?? CrdtLatticeExtensions factory methods ???????????????????????????

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

    // ?? OrSetAccessor ???????????????????????????????????????????????????

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

    // ?? PnCounterAccessor ???????????????????????????????????????????????

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

    // ?? VersionVectorAccessor ???????????????????????????????????????????

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

    // ?? Default-constructed accessor guard ??????????????????????????????

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

    // ?? MvRegisterAccessor ??????????????????????????????????????????????

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

    // ?? OrFlag factory + accessor ???????????????????????????????????????

    [Test]
    public void OrFlag_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.OrFlag(null!, "k"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task OrFlag_with_empty_key_throws()
    {
        var tree = await _fixture.CreateSmallTreeAsync(
            "pac-crdt-orflag-emptykey-" + Guid.NewGuid().ToString("N")[..8], shardCount: 1);
        Assert.That(() => tree.OrFlag(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task OrFlag_IsEnabledAsync_on_missing_key_returns_false()
    {
        var treeId = "pac-crdt-orflag-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        Assert.That(await tree.OrFlag("absent").IsEnabledAsync(), Is.False);
    }

    [Test]
    public async Task OrFlag_EnableAsync_makes_flag_enabled()
    {
        var treeId = "pac-crdt-orflag-enable-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.OrFlag("flag");
        await accessor.EnableAsync(replicaId: "r1");

        Assert.That(await accessor.IsEnabledAsync(), Is.True);
    }

    [Test]
    public async Task OrFlag_DisableAsync_makes_flag_disabled()
    {
        var treeId = "pac-crdt-orflag-disable-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.OrFlag("flag");
        await accessor.EnableAsync(replicaId: "r1");
        await accessor.DisableAsync();

        Assert.That(await accessor.IsEnabledAsync(), Is.False);
    }

    [Test]
    public async Task OrFlag_EnableAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-orflag-emptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.OrFlag("flag").EnableAsync(replicaId: string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task OrFlag_MergeAsync_with_null_other_throws()
    {
        var treeId = "pac-crdt-orflag-mergenull-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.OrFlag("flag").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task OrFlag_MergeAsync_unions_state_enable_wins()
    {
        var treeId = "pac-crdt-orflag-merge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        var accessor = tree.OrFlag("flag");
        await accessor.EnableAsync(replicaId: "r1");
        await accessor.DisableAsync();

        var other = new OrFlag();
        other.Enable("r2", 1);
        await accessor.MergeAsync(other);

        Assert.That(await accessor.IsEnabledAsync(), Is.True);
    }

    [Test]
    public void OrFlagAccessor_default_throws_on_use()
    {
        var accessor = default(OrFlagAccessor);
        Assert.That(
            async () => await accessor.GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ?? RwFlag factory + accessor ???????????????????????????????????????

    [Test]
    public void RwFlag_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.RwFlag(null!, "k"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task RwFlag_with_empty_key_throws()
    {
        var tree = await _fixture.CreateSmallTreeAsync(
            "pac-crdt-rwflag-emptykey-" + Guid.NewGuid().ToString("N")[..8], shardCount: 1);
        Assert.That(() => tree.RwFlag(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RwFlag_IsEnabledAsync_on_missing_key_returns_false()
    {
        var treeId = "pac-crdt-rwflag-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        Assert.That(await tree.RwFlag("absent").IsEnabledAsync(), Is.False);
    }

    [Test]
    public async Task RwFlag_EnableAsync_makes_flag_enabled()
    {
        var treeId = "pac-crdt-rwflag-enable-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.RwFlag("flag");
        await accessor.EnableAsync(replicaId: "r1");

        Assert.That(await accessor.IsEnabledAsync(), Is.True);
    }

    [Test]
    public async Task RwFlag_DisableAsync_makes_flag_disabled()
    {
        var treeId = "pac-crdt-rwflag-disable-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.RwFlag("flag");
        await accessor.EnableAsync(replicaId: "r1");
        await accessor.DisableAsync(replicaId: "r1");

        Assert.That(await accessor.IsEnabledAsync(), Is.False);
    }

    [Test]
    public async Task RwFlag_EnableAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-rwflag-emptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.RwFlag("flag").EnableAsync(replicaId: string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RwFlag_DisableAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-rwflag-disableemptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.RwFlag("flag").DisableAsync(replicaId: string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RwFlag_MergeAsync_with_null_other_throws()
    {
        var treeId = "pac-crdt-rwflag-mergenull-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.RwFlag("flag").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task RwFlag_MergeAsync_unions_state_remove_wins()
    {
        var treeId = "pac-crdt-rwflag-merge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        var accessor = tree.RwFlag("flag");
        await accessor.EnableAsync(replicaId: "r1");

        var other = new RwFlag();
        other.Disable("r2", 1);
        await accessor.MergeAsync(other);

        Assert.That(await accessor.IsEnabledAsync(), Is.False);
    }

    [Test]
    public void RwFlagAccessor_default_throws_on_use()
    {
        var accessor = default(RwFlagAccessor);
        Assert.That(
            async () => await accessor.GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ?? GCounter factory + accessor ?????????????????????????????????????

    [Test]
    public void GCounter_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.GCounter(null!, "k"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task GCounter_with_empty_key_throws()
    {
        var tree = await _fixture.CreateSmallTreeAsync(
            "pac-crdt-gcounter-emptykey-" + Guid.NewGuid().ToString("N")[..8], shardCount: 1);
        Assert.That(() => tree.GCounter(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task GCounter_ValueAsync_on_missing_key_returns_zero()
    {
        var treeId = "pac-crdt-gc-zero-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        Assert.That(await tree.GCounter("absent").ValueAsync(), Is.EqualTo(0));
    }

    [Test]
    public async Task GCounter_IncrementAsync_then_ValueAsync_returns_increment()
    {
        var treeId = "pac-crdt-gc-inc-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var counter = tree.GCounter("c");
        await counter.IncrementAsync(replicaId: "r1", amount: 5);

        Assert.That(await counter.ValueAsync(), Is.EqualTo(5));
    }

    [Test]
    public async Task GCounter_IncrementAsync_accumulates_across_calls()
    {
        var treeId = "pac-crdt-gc-acc-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var counter = tree.GCounter("c");
        await counter.IncrementAsync("r1", amount: 4);
        await counter.IncrementAsync("r1", amount: 6);

        Assert.That(await counter.ValueAsync(), Is.EqualTo(10));
    }

    [Test]
    public async Task GCounter_IncrementAsync_with_negative_amount_throws()
    {
        var treeId = "pac-crdt-gc-neginc-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.GCounter("c").IncrementAsync("r1", amount: -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task GCounter_IncrementAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-gc-emptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.GCounter("c").IncrementAsync(replicaId: string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task GCounter_MergeAsync_with_null_other_throws()
    {
        var treeId = "pac-crdt-gc-mergenull-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.GCounter("c").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task GCounter_MergeAsync_unions_per_replica_components()
    {
        var treeId = "pac-crdt-gc-merge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var counter = tree.GCounter("c");
        await counter.IncrementAsync("r1", amount: 4);

        var other = new GCounter();
        other.Increment("r2", 6);
        await counter.MergeAsync(other);

        Assert.That(await counter.ValueAsync(), Is.EqualTo(10));
    }

    [Test]
    public async Task GCounter_GetAsync_returns_underlying_GCounter()
    {
        var treeId = "pac-crdt-gc-get-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var counter = tree.GCounter("c");
        await counter.IncrementAsync("r1", 3);

        var underlying = await counter.GetAsync();
        Assert.That(underlying, Is.Not.Null);
        Assert.That(underlying.Value, Is.EqualTo(3));
    }

    [Test]
    public void GCounterAccessor_default_throws_on_use()
    {
        var accessor = default(GCounterAccessor);
        Assert.That(
            async () => await accessor.GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ?? RwSet factory + accessor ????????????????????????????????????????

    [Test]
    public void RwSet_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.RwSet(null!, "k"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task RwSet_with_empty_key_throws()
    {
        var tree = await _fixture.CreateSmallTreeAsync(
            "pac-crdt-rwset-emptykey-" + Guid.NewGuid().ToString("N")[..8], shardCount: 1);
        Assert.That(() => tree.RwSet(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RwSet_ContainsAsync_on_missing_key_returns_false()
    {
        var treeId = "pac-crdt-rwset-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        Assert.That(await tree.RwSet("absent").ContainsAsync(Bytes("x")), Is.False);
    }

    [Test]
    public async Task RwSet_AddAsync_makes_element_a_member()
    {
        var treeId = "pac-crdt-rwset-add-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.RwSet("set");
        await accessor.AddAsync(Bytes("x"), replicaId: "r1");

        Assert.That(await accessor.ContainsAsync(Bytes("x")), Is.True);
    }

    [Test]
    public async Task RwSet_RemoveAsync_removes_membership()
    {
        var treeId = "pac-crdt-rwset-remove-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.RwSet("set");
        await accessor.AddAsync(Bytes("x"), replicaId: "r1");
        await accessor.RemoveAsync(Bytes("x"), replicaId: "r1");

        Assert.That(await accessor.ContainsAsync(Bytes("x")), Is.False);
    }

    [Test]
    public async Task RwSet_ToListAsync_returns_live_members()
    {
        var treeId = "pac-crdt-rwset-tolist-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var accessor = tree.RwSet("set");
        await accessor.AddAsync(Bytes("a"), replicaId: "r1");
        await accessor.AddAsync(Bytes("b"), replicaId: "r1");
        await accessor.RemoveAsync(Bytes("a"), replicaId: "r1");

        var members = (await accessor.ToListAsync()).Select(b => System.Text.Encoding.UTF8.GetString(b));
        Assert.That(members, Is.EquivalentTo(new[] { "b" }));
    }

    [Test]
    public async Task RwSet_AddAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-rwset-emptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.RwSet("set").AddAsync(Bytes("x"), replicaId: string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RwSet_RemoveAsync_with_empty_replica_throws()
    {
        var treeId = "pac-crdt-rwset-removeemptyrep-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.RwSet("set").RemoveAsync(Bytes("x"), replicaId: string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RwSet_AddAsync_with_null_element_throws()
    {
        var treeId = "pac-crdt-rwset-addnull-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.RwSet("set").AddAsync(null!, replicaId: "r1"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task RwSet_MergeAsync_with_null_other_throws()
    {
        var treeId = "pac-crdt-rwset-mergenull-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        Assert.That(
            async () => await tree.RwSet("set").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task RwSet_MergeAsync_unions_state_remove_wins()
    {
        var treeId = "pac-crdt-rwset-merge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        var accessor = tree.RwSet("set");
        await accessor.AddAsync(Bytes("x"), replicaId: "r1");

        // A concurrent remove that our add never observed keeps x out.
        var other = new RwSet();
        other.Remove(Bytes("x"), "r2", 1);
        await accessor.MergeAsync(other);

        Assert.That(await accessor.ContainsAsync(Bytes("x")), Is.False);
    }

    [Test]
    public void RwSetAccessor_default_throws_on_use()
    {
        var accessor = default(RwSetAccessor);
        Assert.That(
            async () => await accessor.GetAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void MaxRegister_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.MaxRegister<int>(null!, "k", static v => BitConverter.GetBytes(v)),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MinRegister_with_null_lattice_throws()
    {
        Assert.That(
            () => CrdtLatticeExtensions.MinRegister<int>(null!, "k", static v => BitConverter.GetBytes(v)),
            Throws.InstanceOf<ArgumentNullException>());
    }
}

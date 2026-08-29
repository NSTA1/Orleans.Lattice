using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
[Category("Integration")]
public class TreeDeletionIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task DeleteTree_makes_tree_inaccessible()
    {
        var treeName = $"del-test-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        // Write some data.
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        Assert.That(await router.GetAsync("a"), Is.Not.Null);

        // Delete the tree.
        await router.DeleteTreeAsync();

        // All operations should throw.
        Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("a"));
        Assert.ThrowsAsync<InvalidOperationException>(() =>
            router.SetAsync("c", Encoding.UTF8.GetBytes("3")));
        Assert.ThrowsAsync<InvalidOperationException>(() => router.DeleteAsync("a"));
    }

    [Test]
    public async Task DeleteTree_is_idempotent()
    {
        var treeName = $"del-idem-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("x", Encoding.UTF8.GetBytes("val"));
        await router.DeleteTreeAsync();
        await router.DeleteTreeAsync(); // Should not throw.

        Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("x"));
    }

    [Test]
    public async Task DeleteTree_blocks_bulk_load()
    {
        var treeName = $"del-bulk-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.DeleteTreeAsync();

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            KeyValuePair.Create("a", Encoding.UTF8.GetBytes("1")),
        };
        Assert.ThrowsAsync<InvalidOperationException>(() =>
            router.BulkLoadAsync(entries));
    }

    [Test]
    public async Task DeleteTree_blocks_key_scan()
    {
        var treeName = $"del-keys-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in router.KeysAsync()) { }
        });
    }

    [Test]
    public async Task DeleteTree_on_empty_tree_succeeds()
    {
        var treeName = $"del-empty-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.DeleteTreeAsync();

        Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("x"));
    }

    [Test]
    public async Task IsDeleted_returns_correct_state()
    {
        var treeName = $"del-isdeleted-{Guid.NewGuid():N}";
        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName);

        Assert.That(await deletion.IsDeletedAsync(), Is.False);

        await deletion.DeleteTreeAsync();

        Assert.That(await deletion.IsDeletedAsync(), Is.True);
    }

    // --- RecoverTreeAsync ---

    [Test]
    public async Task RecoverTree_restores_access_to_data()
    {
        var treeName = $"rec-test-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await router.DeleteTreeAsync();

        // Tree is inaccessible.
        Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("a"));

        // Recover.
        await router.RecoverTreeAsync();

        // Data is accessible again.
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("a"))!), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("b"))!), Is.EqualTo("2"));
    }

    [Test]
    public async Task RecoverTree_allows_new_writes_after_recovery()
    {
        var treeName = $"rec-write-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("x", Encoding.UTF8.GetBytes("old"));
        await router.DeleteTreeAsync();
        await router.RecoverTreeAsync();

        await router.SetAsync("y", Encoding.UTF8.GetBytes("new"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("y"))!), Is.EqualTo("new"));
    }

    [Test]
    public async Task RecoverTree_throws_if_not_deleted()
    {
        var treeName = $"rec-notdel-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        Assert.ThrowsAsync<InvalidOperationException>(() => router.RecoverTreeAsync());
    }

    /// <summary>
    /// Issue #1744. A purge that dies part-way clears node state but leaves the
    /// owning shard root intact, and the shard root only seeds a node's tree id
    /// when it CREATES that node. Recovering such a tree used to leave a
    /// routable but unseeded root leaf: routing delivered the write, the leaf
    /// had no tree id to resolve a CrdtShape from, and every typed CRDT write to
    /// its key range failed permanently with
    /// <see cref="LatticeCrdtShapeNotRegisteredException"/>. Recovery now
    /// re-asserts the binding.
    /// </summary>
    [Test]
    public async Task RecoverTree_rebinds_a_root_leaf_left_unseeded_by_an_interrupted_purge()
    {
        var treeName = $"rec-partial-purge-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 1 });
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value);

        await router.DeleteTreeAsync();

        // Simulate a purge interrupted after its first node clear. The leftmost
        // leaf is unconditionally the first node ShardRootGrain.PurgeAsync
        // clears, and ClearGrainStateAsync is exactly the call it makes, so this
        // is the state a PurgeTreeAsync that blew the grain-call timeout leaves
        // behind: node state gone, shard root untouched, no purge flags set.
        await leaf.ClearGrainStateAsync();
        Assert.That(await leaf.GetTreeIdAsync(), Is.Null, "precondition: the simulated purge unbound the leaf");

        await router.RecoverTreeAsync();

        Assert.That(await leaf.GetTreeIdAsync(), Is.EqualTo(treeName));

        // The write path that used to fail permanently: a typed CRDT apply
        // routed to the re-bound leaf.
        await router.OrFlag("a").EnableAsync("replica-1");
        Assert.That(await router.OrFlag("a").IsEnabledAsync(), Is.True);
    }

    /// <summary>
    /// Issue #1744, deeper topology. When the shard root is an internal node the
    /// unseeded leaf is not the root, so the repair has to descend the internal
    /// nodes to reach it - the leaf sibling chain cannot be used, because
    /// clearing a leaf wipes its sibling pointers.
    /// </summary>
    [Test]
    public async Task RecoverTree_rebinds_a_non_root_leaf_left_unseeded_by_an_interrupted_purge()
    {
        var treeName = $"rec-partial-purge-deep-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
            ShardCount = 1,
        });

        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        for (var i = 0; i < 20; i++)
        {
            await router.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i:D2}"));
        }

        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var diagnostics = await shard.GetDiagnosticsAsync(deep: false);
        Assert.That(diagnostics.RootIsLeaf, Is.False, "precondition: the tree split into an internal root");

        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value);

        // A second leaf, so the repair has to rebind more than the one the
        // damage probe happens to look at.
        var siblingId = await leaf.GetNextSiblingAsync();
        Assert.That(siblingId, Is.Not.Null, "precondition: the tree has more than one leaf");
        var sibling = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(siblingId!.Value);

        await router.DeleteTreeAsync();
        await leaf.ClearGrainStateAsync();
        await sibling.ClearGrainStateAsync();
        Assert.That(await leaf.GetTreeIdAsync(), Is.Null, "precondition: the simulated purge unbound the leaf");
        Assert.That(await sibling.GetTreeIdAsync(), Is.Null, "precondition: the simulated purge unbound the sibling");

        await router.RecoverTreeAsync();

        Assert.That(await leaf.GetTreeIdAsync(), Is.EqualTo(treeName));
        Assert.That(await sibling.GetTreeIdAsync(), Is.EqualTo(treeName));
        await router.OrFlag("k00").EnableAsync("replica-1");
        Assert.That(await router.OrFlag("k00").IsEnabledAsync(), Is.True);
    }

    [Test]
    public async Task RecoverTree_leaves_a_healthy_tree_bound()
    {
        var treeName = $"rec-healthy-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 1 });
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value);

        await router.DeleteTreeAsync();
        await router.RecoverTreeAsync();

        Assert.That(await leaf.GetTreeIdAsync(), Is.EqualTo(treeName));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("a"))!), Is.EqualTo("1"));
    }

    [Test]
    public async Task RecoverTree_throws_after_purge()
    {
        var treeName = $"rec-purged-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();

        Assert.ThrowsAsync<InvalidOperationException>(() => router.RecoverTreeAsync());
    }

    // --- PurgeTreeAsync ---

    [Test]
    public async Task PurgeTree_immediately_destroys_data()
    {
        var treeName = $"purge-test-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();

        // Deletion grain should show purge complete.
        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName);
        Assert.That(await deletion.IsDeletedAsync(), Is.True);
    }

    [Test]
    public async Task PurgeTree_throws_if_not_deleted()
    {
        var treeName = $"purge-notdel-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        Assert.ThrowsAsync<InvalidOperationException>(() => router.PurgeTreeAsync());
    }

    [Test]
    public async Task PurgeTree_throws_if_already_purged()
    {
        var treeName = $"purge-twice-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();

        Assert.ThrowsAsync<InvalidOperationException>(() => router.PurgeTreeAsync());
    }
}

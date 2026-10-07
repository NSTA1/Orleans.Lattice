using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #1744. A leaf's owning-tree binding is only ever written when the node
/// is created, so a leaf that loses it stays unbound forever: routing keeps
/// delivering writes to it, but every typed CRDT write to its key range fails
/// with <see cref="LatticeCrdtShapeNotRegisteredException"/>, across process
/// restarts. These tests cover the write-path repair, which re-asserts the
/// binding from the shard root at the point the fault surfaces - the only path
/// that heals a deployment that is <em>already</em> in this state, since it
/// needs no operator action and no recover call.
/// </summary>
[TestFixture]
[Category("Integration")]
public class UnboundLeafSelfHealIntegrationTests
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
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    /// <summary>
    /// Registers a single-shard tree so a key's owning shard root is
    /// deterministic. The cluster default is
    /// <see cref="LatticeConstants.DefaultShardCount"/> shards, which would make
    /// "which shard owns this key" a hash outcome rather than a fixture fact.
    /// </summary>
    private async Task<ILattice> CreateSingleShardTreeAsync(string treeName, int? maxLeafKeys = null)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry
        {
            ShardCount = 1,
            MaxLeafKeys = maxLeafKeys ?? SmallLeafClusterFixture.SmallMaxLeafKeys,
        });
        return _cluster.GrainFactory.GetGrain<ILattice>(treeName);
    }

    /// <summary>
    /// The headline acceptance test for the self-heal: no delete, no purge, no
    /// recover, no restart. The leaf is simply unbound, exactly as a deployment
    /// found in the wild is, and the very next typed CRDT write both repairs the
    /// binding and succeeds. A repair driven only from
    /// <c>RecoverTreeAsync</c> cannot pass this test, because nothing here ever
    /// calls it.
    /// </summary>
    [Test]
    public async Task CrdtWrite_rebinds_an_unbound_leaf_with_no_recover_call()
    {
        var treeName = $"selfheal-{Guid.NewGuid():N}";
        var router = await CreateSingleShardTreeAsync(treeName);
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value);

        // The damaged state the self-heal covers: the leaf's row survives but
        // carries no tree id - the binding an unbound split donor hands its
        // sibling - and the shard root still routes to it. A leaf with no row at
        // all is not this case (issue #4654; see the test below).
        await UnbindLeafRowAsync(leaf);
        Assert.That(await leaf.GetTreeIdAsync(), Is.Null, "precondition: the leaf is unbound");

        await router.OrFlag("a").EnableAsync("replica-1");

        Assert.That(await leaf.GetTreeIdAsync(), Is.EqualTo(treeName), "the write path re-asserted the binding");
        Assert.That(await router.OrFlag("a").IsEnabledAsync(), Is.True, "and the write itself landed");
    }

    /// <summary>
    /// The shape the live incident actually took: the unbound leaf was not the
    /// root and not the leftmost node, but one created by a split deep in the
    /// key range. A repair that reasons from "purge clears the leftmost leaf
    /// first" would call this tree healthy, so the write-path repair has to work
    /// off the fault itself rather than off a probe.
    /// </summary>
    [Test]
    public async Task CrdtWrite_rebinds_an_unbound_non_leftmost_leaf()
    {
        var treeName = $"selfheal-deep-{Guid.NewGuid():N}";
        var router = await CreateSingleShardTreeAsync(treeName);
        for (var i = 0; i < 20; i++)
        {
            await router.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i:D2}"));
        }

        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var diagnostics = await shard.GetDiagnosticsAsync(deep: false);
        Assert.That(diagnostics.RootIsLeaf, Is.False, "precondition: the tree split into an internal root");

        var leftmostId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leftmostId, Is.Not.Null);
        var leftmost = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leftmostId!.Value);

        // A split-created leaf, i.e. one whose identity is a fresh Guid rather
        // than the shard's deterministic root-leaf id.
        var siblingId = await leftmost.GetNextSiblingAsync();
        Assert.That(siblingId, Is.Not.Null, "precondition: the tree has more than one leaf");
        var sibling = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(siblingId!.Value);

        var siblingKeys = await sibling.GetKeysAsync();
        Assert.That(siblingKeys, Is.Not.Empty, "precondition: the sibling owns a key range");
        var damagedKey = siblingKeys[0];

        await UnbindLeafRowAsync(sibling);
        Assert.That(await sibling.GetTreeIdAsync(), Is.Null, "precondition: the sibling is unbound");
        Assert.That(await leftmost.GetTreeIdAsync(), Is.EqualTo(treeName), "precondition: the leftmost leaf is untouched");

        await router.OrFlag(damagedKey).EnableAsync("replica-1");

        Assert.That(await sibling.GetTreeIdAsync(), Is.EqualTo(treeName), "the write path re-asserted the binding");
        Assert.That(await router.OrFlag(damagedKey).IsEnabledAsync(), Is.True, "and the write itself landed");
    }

    /// <summary>
    /// The repair must survive the full delete / interrupted purge / recover
    /// cycle from the issue even when recovery's own re-assert does not reach
    /// the node - the node is unbound again after recovery here, which is what a
    /// second interrupted purge or an unbound split donor produces.
    /// </summary>
    [Test]
    public async Task CrdtWrite_rebinds_an_unbound_leaf_after_delete_partial_purge_and_recover()
    {
        var treeName = $"selfheal-cycle-{Guid.NewGuid():N}";
        var router = await CreateSingleShardTreeAsync(treeName);
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value);

        await router.DeleteTreeAsync();
        await leaf.ClearGrainStateForPurgeAsync();
        await router.RecoverTreeAsync();

        // Re-damage after recovery: the leaf is unbound again but keeps its row,
        // so only the write path can heal this.
        await UnbindLeafRowAsync(leaf);
        Assert.That(await leaf.GetTreeIdAsync(), Is.Null, "precondition: the leaf is unbound after recovery");

        await router.OrFlag("a").EnableAsync("replica-1");

        Assert.That(await router.OrFlag("a").IsEnabledAsync(), Is.True);
    }

    /// <summary>
    /// Rewrites <paramref name="leaf"/>'s durable row with no tree id, the binding
    /// an unbound split donor hands its sibling, and deactivates the leaf so its
    /// next activation reads it.
    /// </summary>
    private static async Task UnbindLeafRowAsync(IBPlusLeafGrain leaf)
    {
        var services = Orleans.Lattice.Tests.Wal.SiloServiceProviderCaptureForWalTests.Captured
            ?? throw new InvalidOperationException("Silo IServiceProvider was not captured by the fixture.");
        await leaf.ForceDeactivateAsync();
        await Task.Delay(200);

        var storage = Microsoft.Extensions.DependencyInjection.ServiceProviderKeyedServiceExtensions
            .GetRequiredKeyedService<Orleans.Storage.IGrainStorage>(services, LatticeOptions.StorageProviderName);
        var row = new GrainState<LeafNodeState>();
        await storage.ReadStateAsync("leaf", leaf.GetGrainId(), row);
        Assert.That(row.RecordExists, Is.True, "precondition: the leaf has a row");
        row.State.TreeId = null;
        await storage.WriteStateAsync("leaf", leaf.GetGrainId(), row);
    }

    /// <summary>
    /// Issue #4654. A leaf with no state row at all - whether a purge cleared it or
    /// its row and row record were lost - cannot be told apart, so the write path
    /// does not re-create it: binding it would turn a lost leaf into an empty one.
    /// The typed CRDT write fails closed instead.
    /// </summary>
    [Test]
    public async Task CrdtWrite_to_a_routed_leaf_with_no_row_fails_closed()
    {
        var treeName = $"selfheal-rowless-{Guid.NewGuid():N}";
        var router = await CreateSingleShardTreeAsync(treeName);
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>((await shard.GetLeftmostLeafIdAsync())!.Value);
        await leaf.ClearGrainStateAsync();

        var write = Assert.CatchAsync(async () => await router.OrFlag("a").EnableAsync("replica-1"));

        Assert.That(write, Is.InstanceOf<ILatticeLeafUnavailable>(), $"got {write}");
        Assert.That(await leaf.GetTreeIdAsync(), Is.Null, "the write path must not bind the rowless leaf");
    }

    /// <summary>
    /// The repair is deliberately narrow. A tree that genuinely has no shape
    /// registered for its mode raises the same exception type but carries its
    /// tree id, and must still fail closed rather than be swallowed by a retry:
    /// re-binding an already-bound leaf would change nothing, so a retry would
    /// only double the work before failing anyway, and masking a real
    /// configuration fault is exactly what the issue asked us not to do.
    /// </summary>
    [Test]
    public async Task CrdtWrite_still_throws_for_a_genuinely_unregistered_shape()
    {
        var treeName = $"selfheal-noshape-{Guid.NewGuid():N}";
        var router = await CreateSingleShardTreeAsync(treeName);
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        // OrMap has no shape registered for this tree, so the leaf resolves its
        // binding fine and then fails to resolve a CrdtShape.
        var ex = Assert.ThrowsAsync<LatticeCrdtShapeNotRegisteredException>(() =>
            router.ApplyCrdtDeltaAsync("a", LatticeMergeMode.OrMap, Encoding.UTF8.GetBytes("{}")));

        Assert.That(ex!.TreeId, Is.EqualTo(treeName), "the fault carries the tree id, so the repair does not match it");
    }

    /// <summary>
    /// Fail-closed backstop. When the binding cannot be restored the write must
    /// still fault rather than silently succeed as an unbound write. Deleting
    /// the tree leaves the shard root unable to serve the repair, so the
    /// original fault reaches the caller.
    /// </summary>
    [Test]
    public async Task CrdtWrite_to_an_unbound_leaf_of_a_deleted_tree_still_fails_closed()
    {
        var treeName = $"selfheal-deleted-{Guid.NewGuid():N}";
        var router = await CreateSingleShardTreeAsync(treeName);
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value);

        await leaf.ClearGrainStateAsync();
        await router.DeleteTreeAsync();

        Assert.ThrowsAsync<InvalidOperationException>(() => router.OrFlag("a").EnableAsync("replica-1"));
    }
}

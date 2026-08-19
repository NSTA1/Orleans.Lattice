using NSubstitute;
using Orleans.Lattice.BPlusTree;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers the origin-side "single shape per replicated tree" write guard: a tree
/// declared for cross-cluster replication under one <see cref="LatticeMergeMode"/>
/// rejects any write whose shape differs from the declared mode with
/// <see cref="LatticeReplicationModeMismatchException"/> before it commits, so a
/// shape mismatch fails loudly at the origin instead of silently dead-lettering
/// on the receiver.
/// </summary>
public partial class LatticeGrainTests
{
    /// <summary>Builds a resolver that reports <paramref name="mode"/> for every tree id.</summary>
    private static ILatticeMergeModeResolver ResolverFor(LatticeMergeMode? mode)
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(mode);
        return resolver;
    }

    // --- LWW write surface rejected on a CRDT-declared tree ---

    [Test]
    public void SetAsync_throws_mode_mismatch_when_tree_declared_as_crdt()
    {
        var (grain, factory) = CreateGrain(treeId: "votes", mergeModeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        var shardRoot = SetupShardRoot(factory);

        var ex = Assert.ThrowsAsync<LatticeReplicationModeMismatchException>(
            () => grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1")));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo("votes"));
            Assert.That(ex.DeclaredMode, Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(ex.AttemptedMode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        });

        // The rejected write must never reach the shard root / WAL writer.
        shardRoot.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public void SetManyAsync_throws_mode_mismatch_when_tree_declared_as_crdt()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.OrSet));
        var shardRoot = SetupShardRoot(factory);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", Encoding.UTF8.GetBytes("v1")),
            new("k2", Encoding.UTF8.GetBytes("v2")),
        };

        var ex = Assert.ThrowsAsync<LatticeReplicationModeMismatchException>(
            () => grain.SetManyAsync(entries));
        Assert.That(ex!.DeclaredMode, Is.EqualTo(LatticeMergeMode.OrSet));

        shardRoot.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public void DeleteAsync_throws_mode_mismatch_when_tree_declared_as_crdt()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.OrSet));
        var shardRoot = SetupShardRoot(factory);

        Assert.ThrowsAsync<LatticeReplicationModeMismatchException>(
            () => grain.DeleteAsync("k1"));

        shardRoot.DidNotReceive().DeleteAsync(Arg.Any<string>());
    }

    [Test]
    public void DeleteRangeAsync_throws_mode_mismatch_when_tree_declared_as_crdt()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.OrSet));
        var shardRoot = SetupShardRoot(factory);

        Assert.ThrowsAsync<LatticeReplicationModeMismatchException>(
            () => grain.DeleteRangeAsync("a", "z"));

        shardRoot.DidNotReceive().DeleteRangeAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>());
    }

    [Test]
    public void BulkLoadAsync_throws_mode_mismatch_when_tree_declared_as_crdt()
    {
        var (grain, _) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.MvRegister));
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", Encoding.UTF8.GetBytes("v1")),
        };

        var ex = Assert.ThrowsAsync<LatticeReplicationModeMismatchException>(
            () => grain.BulkLoadAsync(entries));
        Assert.That(ex!.DeclaredMode, Is.EqualTo(LatticeMergeMode.MvRegister));
    }

    // --- CRDT write surface rejected under the wrong mode ---

    [Test]
    public void ApplyCrdtDeltaAsync_throws_mode_mismatch_when_mode_differs_from_declared()
    {
        var (grain, _) = CreateGrain(treeId: "votes", mergeModeResolver: ResolverFor(LatticeMergeMode.OrSet));

        var ex = Assert.ThrowsAsync<LatticeReplicationModeMismatchException>(
            () => grain.ApplyCrdtDeltaAsync("k1", LatticeMergeMode.PnCounter, new byte[] { 1, 2, 3 }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo("votes"));
            Assert.That(ex.DeclaredMode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(ex.AttemptedMode, Is.EqualTo(LatticeMergeMode.PnCounter));
        });
    }

    [Test]
    public void ApplyCrdtDeltaAsync_throws_mode_mismatch_when_tree_declared_lww()
    {
        var (grain, _) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.LwwRegister));

        var ex = Assert.ThrowsAsync<LatticeReplicationModeMismatchException>(
            () => grain.ApplyCrdtDeltaAsync("k1", LatticeMergeMode.OrSet, new byte[] { 1 }));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.DeclaredMode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(ex.AttemptedMode, Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }

    // --- Pass-through: matching or absent declaration is unaffected ---

    [Test]
    public async Task SetAsync_allowed_when_tree_declared_lww()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.LwwRegister));
        var shardRoot = SetupShardRoot(factory);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await shardRoot.Received(1).SetAsync("k1", Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAsync_allowed_when_tree_not_replicated()
    {
        // Resolver reports null (not replicated) - the guard is a no-op, exactly
        // as for a single-cluster host with no resolver registered at all.
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(null));
        var shardRoot = SetupShardRoot(factory);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await shardRoot.Received(1).SetAsync("k1", Arg.Any<byte[]>());
    }

    [Test]
    public async Task ApplyCrdtDeltaAsync_allowed_when_mode_matches_declared()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.OrSet));
        var shardRoot = SetupShardRoot(factory);

        await grain.ApplyCrdtDeltaAsync("k1", LatticeMergeMode.OrSet, new byte[] { 1 });

        await shardRoot.Received(1).ApplyCrdtDeltaAsync("k1", LatticeMergeMode.OrSet, Arg.Any<byte[]>(), 0L);
    }

    // --- Replication applies re-enter the seam under a foreign origin scope
    //     and must bypass the guard even when the shape disagrees with the
    //     locally-declared mode (the receiver carries the shipped entry's mode). ---

    [Test]
    public async Task SetAsync_allowed_under_foreign_origin_scope_on_crdt_declared_tree()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.OrSet));
        var shardRoot = SetupShardRoot(factory);

        using (LatticeOriginContext.With("peer-cluster"))
        {
            await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        }

        await shardRoot.Received(1).SetAsync("k1", Arg.Any<byte[]>());
    }

    [Test]
    public async Task ApplyCrdtDeltaAsync_allowed_under_foreign_origin_scope_when_mode_differs_from_declared()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.LwwRegister));
        var shardRoot = SetupShardRoot(factory);

        using (LatticeOriginContext.With("peer-cluster"))
        {
            await grain.ApplyCrdtDeltaAsync("k1", LatticeMergeMode.OrSet, new byte[] { 1 });
        }

        await shardRoot.Received(1).ApplyCrdtDeltaAsync("k1", LatticeMergeMode.OrSet, Arg.Any<byte[]>(), 0L);
    }

    // --- Atomic-write-saga commits re-enter the LWW seam under a prepare scope
    //     to flush an already-validated / shape-fixed batch; those internal
    //     flushes are not direct user writes and must bypass the guard. ---

    [Test]
    public async Task SetManyAsync_allowed_under_prepare_scope_on_crdt_declared_tree()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        var shardRoot = SetupShardRoot(factory);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", Encoding.UTF8.GetBytes("v1")),
        };

        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetManyAsync(entries);
        }

        await shardRoot.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task SetAsync_allowed_under_prepare_scope_on_crdt_declared_tree()
    {
        var (grain, factory) = CreateGrain(mergeModeResolver: ResolverFor(LatticeMergeMode.OrSet));
        var shardRoot = SetupShardRoot(factory);

        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        }

        await shardRoot.Received(1).SetAsync("k1", Arg.Any<byte[]>());
    }
}

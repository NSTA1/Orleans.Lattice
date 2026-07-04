using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for the internal-origin assertion on the internal
/// coordinator / saga grains: the atomic-write saga and the structural lifecycle
/// coordinators (delete, resize, merge, reshard, shard-split, snapshot). Access-gate
/// enforcement lives only on the <see cref="ILattice"/> facade; these coordinators
/// enforce no policy of their own and are keyed by ordinary strings, so a direct
/// external Orleans grain call to one would bypass the gate entirely. The
/// atomic-write saga is the worst case because it self-establishes system-origin
/// during execution, so a direct external call would run every downstream shard /
/// leaf mutation with the gate bypassed. These tests prove every mutating
/// coordinator entry point refuses a direct external client call, while a
/// facade-driven atomic write - which reaches the saga through a silo-sourced hop
/// stamped internal-origin - still succeeds.
/// </summary>
public sealed partial class InternalOriginGuardIntegrationTests
{
    private static List<KeyValuePair<string, byte[]>> Entry(string key) =>
        new() { new(key, Encoding.UTF8.GetBytes(key)) };

    // --- Atomic-write saga (the system-origin escalation surface) ---

    [Test]
    public void AtomicWrite_ExecuteAsync_direct_external_call_is_refused()
    {
        var saga = _cluster.GrainFactory.GetGrain<IAtomicWriteGrain>("coord-guard-aw-exec/op-1");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await saga.ExecuteAsync("coord-guard-aw-exec", Entry("k")));
    }

    [Test]
    public void AtomicWrite_ExecuteGuardedAsync_direct_external_call_is_refused()
    {
        var saga = _cluster.GrainFactory.GetGrain<IAtomicWriteGrain>("coord-guard-aw-guarded/op-1");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await saga.ExecuteGuardedAsync(
                "coord-guard-aw-guarded", Entry("k"), default(LatticePredicateNode)));
    }

    [Test]
    public void AtomicWrite_PrepareForCoordinatorAsync_direct_external_call_is_refused()
    {
        var saga = _cluster.GrainFactory.GetGrain<IAtomicWriteGrain>("coord-guard-aw-prepare/op-1");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await saga.PrepareForCoordinatorAsync(
                "coord-guard-aw-prepare", Entry("k"), predicate: null,
                coordinatorKey: "coord-1", participants: new[] { "coord-guard-aw-prepare" }));
    }

    [Test]
    public void AtomicWrite_FinalizeAsync_direct_external_call_is_refused()
    {
        var saga = _cluster.GrainFactory.GetGrain<IAtomicWriteGrain>("coord-guard-aw-finalize/op-1");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await saga.FinalizeAsync(commit: true));
    }

    // --- Tree deletion ---

    [Test]
    public void TreeDeletion_DeleteTreeAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>("coord-guard-delete");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.DeleteTreeAsync());
    }

    [Test]
    public void TreeDeletion_RecoverAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>("coord-guard-recover");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.RecoverAsync());
    }

    [Test]
    public void TreeDeletion_PurgeNowAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>("coord-guard-purge");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.PurgeNowAsync());
    }

    // --- Tree resize ---

    [Test]
    public void TreeResize_ResizeAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>("coord-guard-resize");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.ResizeAsync(newMaxLeafKeys: 8, newMaxInternalChildren: 8));
    }

    [Test]
    public void TreeResize_UndoResizeAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>("coord-guard-resize-undo");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.UndoResizeAsync());
    }

    // --- Tree merge ---

    [Test]
    public void TreeMerge_MergeAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>("coord-guard-merge-target");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.MergeAsync("coord-guard-merge-source"));
    }

    // --- Tree reshard ---

    [Test]
    public void TreeReshard_ReshardAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>("coord-guard-reshard");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.ReshardAsync(newShardCount: 4));
    }

    // --- Tree shard split ---

    [Test]
    public void TreeShardSplit_SplitAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>("coord-guard-split/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.SplitAsync(sourceShardIndex: 0));
    }

    // --- Tree snapshot ---

    [Test]
    public void TreeSnapshot_SnapshotAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>("coord-guard-snapshot");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.SnapshotAsync("coord-guard-snapshot-dest", SnapshotMode.Offline));
    }

    [Test]
    public void TreeSnapshot_SnapshotWithOperationIdAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>("coord-guard-snapshot-opid");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.SnapshotWithOperationIdAsync(
                "coord-guard-snapshot-opid-dest", SnapshotMode.Offline,
                maxLeafKeys: null, maxInternalChildren: null,
                operationId: "op-1", logicalTreeId: "coord-guard-snapshot-opid"));
    }

    [Test]
    public void TreeSnapshot_AbortAsync_direct_external_call_is_refused()
    {
        var grain = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>("coord-guard-snapshot-abort");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await grain.AbortAsync("op-1"));
    }

    // --- Non-breakage: the saga still runs through the facade ---

    // A facade-driven atomic write reaches the saga through a silo-sourced grain
    // hop, so the filter stamps the internal-origin marker and the saga's guard
    // passes. This proves the coordinator guards do not break the legitimate call
    // graph.
    [Test]
    public async Task Facade_atomic_write_that_drives_the_saga_still_succeeds()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("coord-guard-facade-saga");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("a")),
            new("b", Encoding.UTF8.GetBytes("b")),
        };

        await tree.SetManyAtomicAsync(entries);

        var readA = await tree.GetAsync("a");
        var readB = await tree.GetAsync("b");

        Assert.Multiple(() =>
        {
            Assert.That(readA, Is.EqualTo(Encoding.UTF8.GetBytes("a")));
            Assert.That(readB, Is.EqualTo(Encoding.UTF8.GetBytes("b")));
        });
    }
}

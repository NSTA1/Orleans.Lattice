using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // Regression coverage for the write-path sibling of issue 899
    // (https://github.com/NSTA1/Orleans.Lattice/issues/899). The first fix made
    // the sorted-scan surface re-descend a mis-typed node, but the mutation
    // traversals (TraverseForWriteAsync and friends, the bulk SetMany paths, and
    // the delete paths) and the point-read traversals still trusted the persisted
    // RootIsLeaf / ChildrenAreLeaves routing flags and blind-cast the resolved
    // node id to IBPlusLeafGrain. A baked-inconsistent topology - observed live as
    // a shard root whose persisted RootIsLeaf bit was true over an internal root -
    // therefore crashed every write that routed onto the mislabelled root with
    // InvalidCastException (BPlusInternalGrain -> IBPlusLeafGrain), and because the
    // corruption was persisted it kept crashing across silo restarts (it
    // crash-looped the sample's inventory seeder).
    //
    // This test deterministically injects the exact live fault: it builds a
    // genuine depth->=2 single-shard tree, flips the persisted shard-root
    // RootIsLeaf flag to true over its (internal) root via the process-scope
    // storage provider, and restarts the cluster so the corrupt flag is rehydrated
    // cold from storage. A subsequent SetAsync routes onto the internal root the
    // flag now claims is a leaf; it must re-descend to a real leaf and write
    // rather than throw, and the value must round-trip back through the (equally
    // guarded) read path.

    [Test]
    public async Task SetAsync_does_not_throw_when_a_corrupt_root_is_leaf_flag_routes_a_write_onto_an_internal_root()
    {
        var treeId = "pac-deepwrite-rootisleaf";
        var tree = await CreateDepthTwoTreeAsync(treeId);

        var routing = await tree.GetRoutingAsync();
        var shard = Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");

        var rootRef = await shard.GetRootNodeRefAsync();
        Assert.That(rootRef, Is.Not.Null, "Depth->=2 tree must have a root node.");
        Assert.That(rootRef!.Value.IsLeaf, Is.False, "Depth->=2 tree root must be an internal node.");

        // Corrupt the persisted shard-root state: flip RootIsLeaf to true while
        // RootNodeId still addresses the internal root. Pre-fix this makes every
        // write blind-cast the internal root to IBPlusLeafGrain.
        var corrupted = ProcessScopeMemoryGrainStorage.ForceRootIsLeafOverInternalRoot(rootRef.Value.NodeId);
        Assert.That(corrupted, Is.GreaterThanOrEqualTo(1),
            "Test setup must corrupt the persisted shard-root RootIsLeaf flag over the internal root.");

        // Restart so the shard root's routing cache starts cold and rehydrates
        // the corrupt RootIsLeaf bit from the process-scope grain storage.
        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(
            treeId,
            shardCount: 1,
            maxLeafKeys: DeepScanLeafKeys,
            maxInternalChildren: DeepScanInternalChildren);

        // A write must not blind-cast the mis-flagged internal root to a leaf; it
        // must re-descend by key to a real leaf and persist.
        var writeKey = DeepScanKey(0);
        InvalidCastException? castFailure = null;
        try
        {
            await rehydrated.SetAsync(writeKey, Bytes("rewritten"));
        }
        catch (InvalidCastException ex)
        {
            castFailure = ex;
        }

        Assert.That(castFailure, Is.Null,
            "SetAsync must re-descend a mis-flagged internal root to a real leaf instead of casting it to IBPlusLeafGrain.");

        // The write must have landed at the real leaf; the read path re-descends
        // the same mis-flagged root and returns the new value.
        var read = await rehydrated.GetAsync(writeKey);
        Assert.That(read, Is.Not.Null, "The guarded write must persist a value readable through the guarded read path.");
        Assert.That(Str(read!), Is.EqualTo("rewritten"));

        // The leaf-chain entry point must also be type-safe: GetLeftmostLeafIdAsync
        // is what the replication snapshot producer, compaction, merge and split
        // walkers call before blind-casting the id to IBPlusLeafGrain. Under the
        // corrupt RootIsLeaf flag the pre-fix path returned the internal root id;
        // the guard must return a real leaf, so a leaf-only call does not throw.
        var rehydratedShard = Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");
        var leftmostLeafId = await rehydratedShard.GetLeftmostLeafIdAsync();
        Assert.That(leftmostLeafId, Is.Not.Null, "The corrupt-root shard must still resolve a leftmost leaf.");

        var leftmostLeaf = Client.GetGrain<IBPlusLeafGrain>(leftmostLeafId!.Value);
        InvalidCastException? leftmostCastFailure = null;
        try
        {
            await leftmostLeaf.CountAsync();
        }
        catch (InvalidCastException ex)
        {
            leftmostCastFailure = ex;
        }

        Assert.That(leftmostCastFailure, Is.Null,
            "GetLeftmostLeafIdAsync must return a leaf-typed id so leaf-chain walkers (e.g. the replication snapshot producer) never cast an internal node to IBPlusLeafGrain.");
    }
}

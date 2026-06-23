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

    // Diagnostic, topology and replication-digest siblings of the issue 899
    // write-path fix. The same baked-inconsistent topology - a shard root whose
    // persisted RootIsLeaf bit is true over an internal root - was observed live
    // in the explorer as two distinct symptoms: a Topology panel that failed
    // with a gRPC "Internal" error (GetTopologySnapshotAsync blind-cast the
    // internal root to IBPlusLeafGrain and threw InvalidCastException) and a
    // Metrics "live keys" value of zero (GetDiagnosticsAsync addressed an empty
    // leaf grain by the internal root's guid and silently counted nothing, with
    // no throw). The first issue 899 fix did not touch these diagnostic paths.
    // This test injects the identical fault and asserts the diagnostic surface
    // re-routes by node TYPE instead of trusting the persisted flag.
    [Test]
    public async Task Diagnostic_and_topology_paths_stay_correct_when_a_corrupt_root_is_leaf_flag_sits_over_an_internal_root()
    {
        var treeId = "pac-deepwrite-rootisleaf-diag";
        var tree = await CreateDepthTwoTreeAsync(treeId);

        var routing = await tree.GetRoutingAsync();
        var shard = Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");

        var rootRef = await shard.GetRootNodeRefAsync();
        Assert.That(rootRef, Is.Not.Null, "Depth->=2 tree must have a root node.");
        Assert.That(rootRef!.Value.IsLeaf, Is.False, "Depth->=2 tree root must be an internal node.");

        var corrupted = ProcessScopeMemoryGrainStorage.ForceRootIsLeafOverInternalRoot(rootRef.Value.NodeId);
        Assert.That(corrupted, Is.GreaterThanOrEqualTo(1),
            "Test setup must corrupt the persisted shard-root RootIsLeaf flag over the internal root.");

        await _fixture.RestartClusterAsync();
        await _fixture.CreateSmallTreeAsync(
            treeId,
            shardCount: 1,
            maxLeafKeys: DeepScanLeafKeys,
            maxInternalChildren: DeepScanInternalChildren);

        var rehydratedShard = Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");

        // Topology snapshot (the live explorer "Internal" gRPC error): must not
        // blind-cast the mis-flagged internal root to a leaf, and must return a
        // real node describing the internal-rooted structure.
        ShardTopologyNode? topology = null;
        InvalidCastException? topologyFailure = null;
        try
        {
            topology = await rehydratedShard.GetTopologySnapshotAsync(8, CancellationToken.None);
        }
        catch (InvalidCastException ex)
        {
            topologyFailure = ex;
        }

        Assert.That(topologyFailure, Is.Null,
            "GetTopologySnapshotAsync must read the internal-rooted subtree instead of casting the mis-flagged internal root to IBPlusLeafGrain.");
        Assert.That(topology, Is.Not.Null, "The corrupt-root shard still has data, so its topology snapshot must be non-null.");

        // Diagnostics live-key count (the live explorer "live keys = 0" metric):
        // must descend the internal subtree and count every live key rather than
        // address an empty leaf grain by the internal root's guid (which returns
        // zero with no throw - a silent wrong answer pre-fix).
        ShardDiagnosticReport? diag = null;
        InvalidCastException? diagFailure = null;
        try
        {
            diag = await rehydratedShard.GetDiagnosticsAsync(deep: false);
        }
        catch (InvalidCastException ex)
        {
            diagFailure = ex;
        }

        Assert.That(diagFailure, Is.Null,
            "GetDiagnosticsAsync must not blind-cast the mis-flagged internal root to IBPlusLeafGrain.");
        Assert.That(diag, Is.Not.Null);
        Assert.That(diag!.Value.LiveKeys, Is.EqualTo(DeepScanKeyCount),
            "GetDiagnosticsAsync must walk the internal subtree and count every live key, not silently report zero from an empty leaf grain addressed by the internal root's guid.");

        // Replication anti-entropy digest folds the leaf chain through the same
        // root-cast gate; it must not throw on the mis-flagged internal root.
        InvalidCastException? digestFailure = null;
        try
        {
            await rehydratedShard.GetShardProjectionDigestAsync(CancellationToken.None);
        }
        catch (InvalidCastException ex)
        {
            digestFailure = ex;
        }

        Assert.That(digestFailure, Is.Null,
            "GetShardProjectionDigestAsync must fold the internal-rooted leaf chain instead of casting the mis-flagged internal root to IBPlusLeafGrain.");
    }
}

using System.Linq;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // Regression coverage for the depth->=2 full-scan InvalidCastException
    // (https://github.com/NSTA1/Orleans.Lattice/issues/899): once a single
    // shard's B+ tree grows a second level of internal nodes, the scan
    // surface (ScanKeysAsync / GetSortedKeysBatchAsync and the reverse /
    // entries siblings) resolves a start leaf via the leftmost / rightmost
    // traversal and then walks the leaf sibling chain. Each hop did
    // grainFactory.GetGrain<IBPlusLeafGrain>(nodeId) and invoked it
    // directly. If the resolved node id actually addressed an internal node
    // - because a baked-inconsistent topology left an internal node's
    // ChildrenAreLeaves bit true over internal children, or a leaf
    // next/prev sibling pointer crossed a node level - the first method
    // dispatch cast a BPlusInternalGrain reference to IBPlusLeafGrain and
    // threw InvalidCastException, and (because the corruption was
    // persisted) kept throwing across silo restarts.
    //
    // The first group below builds a genuine depth->=2 single-shard tree
    // (MaxLeafKeys = 4, MaxInternalChildren = 4, ShardCount = 1) through the
    // normal write path and asserts every scan variant returns the full,
    // correctly-sorted key / value set - guarding the steady-state scan.
    //
    // The final test deterministically injects the exact fault the issue
    // reports - a leaf next-sibling pointer that crosses onto an internal
    // node - and asserts the scan no longer throws InvalidCastException but
    // re-descends to a real leaf (the defensive guard in DescendToLeafAsync).

    private const int DeepScanLeafKeys = 4;
    private const int DeepScanInternalChildren = 4;

    // Enough keys into a single shard to force the shard's tree past one
    // internal level: with 4 keys/leaf and 4 children/internal a depth-1
    // tree tops out around 16 keys, so 256 guarantees several internal
    // levels (64 leaves -> 16 -> 4 -> 1 root).
    private const int DeepScanKeyCount = 256;

    private const int DeepScanWriteBatch = 32;

    private static string DeepScanKey(int i) => $"k{i:D5}";

    private async Task<ILattice> CreateDepthTwoTreeAsync(string treeId)
    {
        var tree = await _fixture.CreateSmallTreeAsync(
            treeId,
            shardCount: 1,
            maxLeafKeys: DeepScanLeafKeys,
            maxInternalChildren: DeepScanInternalChildren);

        // Write the keys in ascending order in modest batches. This drives
        // the leaf-split -> AcceptSplit -> PromoteRoot ladder that grows the
        // internal levels until the shard's tree is several levels deep.
        var batch = new List<KeyValuePair<string, byte[]>>(DeepScanWriteBatch);
        for (var i = 0; i < DeepScanKeyCount; i++)
        {
            batch.Add(Kvp(DeepScanKey(i), i.ToString()));
            if (batch.Count == DeepScanWriteBatch)
            {
                await tree.SetManyAsync(batch);
                batch = new List<KeyValuePair<string, byte[]>>(DeepScanWriteBatch);
            }
        }
        if (batch.Count > 0)
        {
            await tree.SetManyAsync(batch);
        }

        // Guard: prove the tree really is depth->=2 (the root's children are
        // themselves internal nodes). The diagnostics depth walk counts the
        // root level as 1 and each additional internal level adds one, so a
        // tree whose root's children are internal reports Depth >= 3.
        var report = await tree.DiagnoseAsync();
        var maxDepth = report.Shards.Length == 0 ? 0 : report.Shards.Max(s => s.Depth);
        Assert.That(maxDepth, Is.GreaterThanOrEqualTo(3),
            "Test setup must build a tree with at least two internal levels to exercise the depth->=2 scan path.");

        return tree;
    }

    [Test]
    public async Task ScanKeysAsync_returns_all_keys_when_single_shard_tree_is_depth_two()
    {
        var tree = await CreateDepthTwoTreeAsync("pac-deepscan-keys");

        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync())
        {
            keys.Add(k);
        }

        var expected = Enumerable.Range(0, DeepScanKeyCount).Select(DeepScanKey).ToArray();
        Assert.That(keys, Is.EqualTo(expected));
    }

    [Test]
    public async Task ScanKeysAsync_reverse_returns_all_keys_descending_when_single_shard_tree_is_depth_two()
    {
        var tree = await CreateDepthTwoTreeAsync("pac-deepscan-keys-reverse");

        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync(reverse: true))
        {
            keys.Add(k);
        }

        var expected = Enumerable.Range(0, DeepScanKeyCount)
            .Select(DeepScanKey)
            .Reverse()
            .ToArray();
        Assert.That(keys, Is.EqualTo(expected));
    }

    [Test]
    public async Task ScanEntriesAsync_returns_all_entries_when_single_shard_tree_is_depth_two()
    {
        var tree = await CreateDepthTwoTreeAsync("pac-deepscan-entries");

        var entries = new List<KeyValuePair<string, string>>();
        await foreach (var e in tree.ScanEntriesAsync())
        {
            entries.Add(new(e.Key, Str(e.Value)));
        }

        var expectedKeys = Enumerable.Range(0, DeepScanKeyCount).Select(DeepScanKey).ToArray();
        var expectedValues = Enumerable.Range(0, DeepScanKeyCount).Select(i => i.ToString()).ToArray();
        Assert.That(entries.Select(e => e.Key), Is.EqualTo(expectedKeys));
        Assert.That(entries.Select(e => e.Value), Is.EqualTo(expectedValues));
    }

    [Test]
    public async Task ScanEntriesAsync_reverse_returns_all_entries_descending_when_single_shard_tree_is_depth_two()
    {
        var tree = await CreateDepthTwoTreeAsync("pac-deepscan-entries-reverse");

        var keys = new List<string>();
        await foreach (var e in tree.ScanEntriesAsync(reverse: true))
        {
            keys.Add(e.Key);
        }

        var expected = Enumerable.Range(0, DeepScanKeyCount)
            .Select(DeepScanKey)
            .Reverse()
            .ToArray();
        Assert.That(keys, Is.EqualTo(expected));
    }

    [Test]
    public async Task ScanKeysAsync_returns_all_keys_when_depth_two_tree_is_scanned_with_cold_routing_cache()
    {
        var treeId = "pac-deepscan-cold-" + Guid.NewGuid().ToString("N")[..8];
        await CreateDepthTwoTreeAsync(treeId);

        // Restart the cluster so the shard-root activation (and its
        // _routingTableCache) starts cold and every internal / leaf grain
        // is freshly hydrated from the surviving WAL + grain storage.
        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(
            treeId,
            shardCount: 1,
            maxLeafKeys: DeepScanLeafKeys,
            maxInternalChildren: DeepScanInternalChildren);

        var keys = new List<string>();
        await foreach (var k in rehydrated.ScanKeysAsync())
        {
            keys.Add(k);
        }

        var expected = Enumerable.Range(0, DeepScanKeyCount).Select(DeepScanKey).ToArray();
        Assert.That(keys, Is.EqualTo(expected));
    }

    [Test]
    public async Task ScanKeysAsync_does_not_throw_when_a_leaf_next_sibling_pointer_crosses_onto_an_internal_node()
    {
        // Deterministic reproduction of issue 899's InvalidCastException
        // without relying on the (race-only) topology corruption that bakes
        // it in production. We build a real depth->=2 tree, then directly
        // corrupt the leftmost leaf's next-sibling pointer so it addresses
        // an INTERNAL node instead of the next leaf - the precise hazard the
        // issue flags as "does the leaf next-sibling chain stay strictly at
        // leaf level across internal-node boundaries?". Before the fix the
        // scan walked that pointer into
        // grainFactory.GetGrain<IBPlusLeafGrain>(internalId).GetKeysAsync()
        // and threw InvalidCastException (BPlusInternalGrain -> IBPlusLeafGrain).
        // After the fix the scan's DescendToLeafAsync guard re-descends the
        // internal node to a real leaf and continues, so the scan completes
        // and returns a sorted, de-duplicated key list that still includes
        // the leftmost leaf's keys.
        var treeId = "pac-deepscan-crosslevel-sibling";
        var tree = await CreateDepthTwoTreeAsync(treeId);

        var routing = await tree.GetRoutingAsync();
        var shard = Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");

        var rootRef = await shard.GetRootNodeRefAsync();
        Assert.That(rootRef, Is.Not.Null, "Depth->=2 tree must have a root node.");
        Assert.That(rootRef!.Value.IsLeaf, Is.False, "Depth->=2 tree root must be an internal node.");

        var leftmostLeaf = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leftmostLeaf, Is.Not.Null);

        // Pick a NON-leftmost child of the root. Because the tree is
        // depth->=2 the root's children are themselves internal nodes, so
        // this id addresses an internal grain - the exact mis-typed target
        // the bug walked into.
        var rootInternal = Client.GetGrain<IBPlusInternalGrain>(rootRef.Value.NodeId);
        var rootSnapshot = await rootInternal.GetRoutingTableAsync();
        Assert.That(rootSnapshot.ChildrenAreLeaves, Is.False,
            "Depth->=2 root's children must be internal nodes for this reproduction.");
        Assert.That(rootSnapshot.ChildIds.Length, Is.GreaterThanOrEqualTo(2));
        var internalSibling = rootSnapshot.ChildIds[^1];

        // Corrupt the leftmost leaf so its next-sibling crosses onto the
        // internal node.
        var leaf = Client.GetGrain<IBPlusLeafGrain>(leftmostLeaf!.Value);
        await leaf.SetNextSiblingAsync(internalSibling);

        // The scan must not throw InvalidCastException. Collect what it
        // returns and assert it degraded gracefully: a sorted, de-duplicated
        // key list that still includes the leftmost leaf's first key.
        var keys = new List<string>();
        InvalidCastException? castFailure = null;
        try
        {
            await foreach (var k in tree.ScanKeysAsync())
            {
                keys.Add(k);
            }
        }
        catch (InvalidCastException ex)
        {
            castFailure = ex;
        }

        Assert.That(castFailure, Is.Null,
            "ScanKeysAsync must not blind-cast an internal node id to IBPlusLeafGrain when a sibling pointer crosses a node level.");
        Assert.That(keys, Does.Contain(DeepScanKey(0)),
            "The scan should still return the leftmost leaf's keys after re-descending the cross-level sibling.");
        Assert.That(keys, Is.Ordered, "Returned keys must remain sorted.");
        Assert.That(keys.Distinct().Count(), Is.EqualTo(keys.Count), "Returned keys must be de-duplicated.");
    }
}

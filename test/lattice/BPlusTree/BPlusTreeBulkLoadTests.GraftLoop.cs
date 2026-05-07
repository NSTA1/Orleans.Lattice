using Orleans.Lattice.BPlusTree;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression tests targeting the <c>CompleteBulkGraftAsync</c> per-entry
/// loop refactor in cycle 16 (commit <c>e2d54b5</c>): the descent
/// <c>Stack&lt;GrainId&gt;</c> is hoisted out of the foreach and reused
/// via <c>Clear()</c>; the bubble loop tracks the pending promoted
/// <c>(key, child)</c> as locals instead of boxing a <c>SplitResult</c>
/// per iteration. These tests assert externally-visible correctness on
/// the code paths the refactor changed, so a future regression that
/// drops <c>path.Clear()</c>, swaps the bubble locals, or reorders the
/// <c>RootIsLeaf</c> early-continue would surface here rather than
/// being caught only by the integration suite.
/// </summary>
public partial class BPlusTreeBulkLoadTests
{
    /// <summary>
    /// Drives many leaf splits in a single <c>BulkAppendAsync</c> call.
    /// With <see cref="SmallLeafClusterFixture.SmallMaxLeafKeys"/>=4 and
    /// 80 entries, the foreach loop in <c>CompleteBulkGraftAsync</c>
    /// iterates ~20 times, each iteration filling the hoisted Stack and
    /// then draining it. If <c>path.Clear()</c> were missed between
    /// iterations, a stale ancestor <c>GrainId</c> from a prior entry
    /// would be popped during the bubble loop and the keyspace would
    /// fragment. The test verifies all keys are readable in sorted
    /// order after a single such call.
    /// </summary>
    [Test]
    public async Task BulkAppend_single_call_with_many_leaf_splits_keeps_tree_consistent()
    {
        await RegisterSingleShardAsync("graft-loop-many-splits");
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>(
            "graft-loop-many-splits/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(
            "graft-loop-many-splits");

        const int count = 80;
        var entries = Enumerable.Range(0, count)
            .Select(i => KeyValuePair.Create(
                $"k{i:D4}",
                Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

        await shard.BulkAppendAsync("op-many-splits", entries);

        // Every key readable.
        var missing = new List<string>();
        for (var i = 0; i < count; i++)
        {
            if (await tree.GetAsync($"k{i:D4}") is null)
            {
                missing.Add($"k{i:D4}");
            }
        }
        Assert.That(missing, Is.Empty, "all bulk-appended keys must be retrievable");

        // KeysAsync walks the leaf chain via the next-sibling pointers.
        // A stale-Stack regression would corrupt the bubble-up promoted
        // separators, breaking sort order on a subsequent leaf-chain walk.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
        {
            keys.Add(k);
        }
        var expected = Enumerable.Range(0, count)
            .Select(i => $"k{i:D4}")
            .ToList();
        Assert.That(keys, Is.EqualTo(expected),
            "leaf-chain key order must match the sorted bulk-loaded keyset");
    }

    /// <summary>
    /// Bulk-appends from a flat-tree shard (<c>RootIsLeaf=true</c>): the
    /// very first entry hits the rare <c>RootIsLeaf</c> early-continue
    /// branch where a <see cref="SplitResult"/> *is* allocated and passed
    /// to <c>PromoteRootAsync</c>; subsequent entries fall through to
    /// the steady-state branch where the locals replace the allocation.
    /// Verifies the transition does not lose the first key.
    /// </summary>
    [Test]
    public async Task BulkAppend_from_flat_tree_transitions_root_correctly()
    {
        await RegisterSingleShardAsync("graft-loop-root-is-leaf");
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>(
            "graft-loop-root-is-leaf/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(
            "graft-loop-root-is-leaf");

        // 12 entries with MaxLeafKeys=4 -> root must be promoted to an
        // internal node and at least 2 leaf splits will bubble through
        // the steady-state non-RootIsLeaf branch.
        var entries = Enumerable.Range(0, 12)
            .Select(i => KeyValuePair.Create(
                $"k{i:D4}",
                Encoding.UTF8.GetBytes("v")))
            .ToList();

        await shard.BulkAppendAsync("op-root-leaf", entries);

        // Critical: the *first* entry routed through the RootIsLeaf
        // branch (and its SplitResult was actually allocated). A bug
        // that mishandled the RootIsLeaf path would silently lose k0000.
        Assert.That(await tree.GetAsync("k0000"), Is.Not.Null,
            "first key (RootIsLeaf branch) must be retained");

        // The last entry was processed via the steady-state branch
        // after RootIsLeaf flipped to false.
        Assert.That(await tree.GetAsync("k0011"), Is.Not.Null,
            "last key (steady-state branch) must be retained");

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
        {
            keys.Add(k);
        }
        Assert.That(keys.Count, Is.EqualTo(12));
        Assert.That(keys, Is.Ordered);
    }

    /// <summary>
    /// Two consecutive <c>BulkAppendAsync</c> calls against the same
    /// shard activation. Each call enters <c>CompleteBulkGraftAsync</c>
    /// with a fresh hoisted Stack; the second call exercises an
    /// already-non-leaf root. Verifies cross-call state hygiene -
    /// notably that the steady-state branch's local-tracked
    /// <c>(pendingKey, pendingChild, pendingHasValue)</c> does not
    /// leak across calls and that the <c>InvalidateRoutingTable</c>
    /// invocation cycle still produces a correct routing table for
    /// reads that follow.
    /// </summary>
    [Test]
    public async Task BulkAppend_two_consecutive_calls_remain_consistent()
    {
        await RegisterSingleShardAsync("graft-loop-two-calls");
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>(
            "graft-loop-two-calls/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(
            "graft-loop-two-calls");

        var batch1 = Enumerable.Range(0, 16)
            .Select(i => KeyValuePair.Create(
                $"k{i:D4}",
                Encoding.UTF8.GetBytes("v")))
            .ToList();
        await shard.BulkAppendAsync("op-1", batch1);

        // Read between calls to force a routing table fetch with the
        // post-batch-1 tree shape.
        Assert.That(await tree.GetAsync("k0008"), Is.Not.Null);

        var batch2 = Enumerable.Range(16, 16)
            .Select(i => KeyValuePair.Create(
                $"k{i:D4}",
                Encoding.UTF8.GetBytes("v")))
            .ToList();
        await shard.BulkAppendAsync("op-2", batch2);

        // All 32 keys present, in sorted order.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
        {
            keys.Add(k);
        }
        var expected = Enumerable.Range(0, 32)
            .Select(i => $"k{i:D4}")
            .ToList();
        Assert.That(keys, Is.EqualTo(expected));
    }

    /// <summary>
    /// The bubble loop's locals carry <c>entry.SeparatorKey</c> and
    /// <c>entry.LeafId</c> from the foreach scope. A typo that swapped
    /// the roles or reused a stale value across iterations would
    /// promote the wrong separator on a later entry, fragmenting the
    /// keyspace. The test issues a bulk-append followed by per-key
    /// integrity probes: if any separator was promoted from a prior
    /// iteration's data, individual key reads would either miss or
    /// return a neighbour's value.
    /// </summary>
    [Test]
    public async Task BulkAppend_promoted_separators_match_per_entry_keys()
    {
        await RegisterSingleShardAsync("graft-loop-separators");
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>(
            "graft-loop-separators/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(
            "graft-loop-separators");

        const int count = 40;
        var entries = Enumerable.Range(0, count)
            .Select(i => KeyValuePair.Create(
                $"k{i:D4}-stable",
                Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

        await shard.BulkAppendAsync("op-separators", entries);

        // Probe every key. With MaxLeafKeys=4 the leaves split at
        // k0003/k0004, k0007/k0008, ...; a separator-mixup bug would
        // route boundary reads to the wrong leaf, returning either
        // null or a neighbour's value.
        for (var i = 0; i < count; i++)
        {
            var key = $"k{i:D4}-stable";
            var v = await tree.GetAsync(key);
            Assert.That(v, Is.Not.Null, $"key {key} must be retrievable");
            Assert.That(
                Encoding.UTF8.GetString(v!),
                Is.EqualTo($"v{i}"),
                $"key {key} must return its own value, not a neighbour's");
        }
    }
}
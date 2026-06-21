using System.Text;
using System.Threading;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration oracle for the push-up structural topology digest
/// (<see cref="IShardRootGrain.GetTopologySnapshotAsync"/>). The structural
/// fields that ride upward on every <see cref="ChildDigestSnapshot"/>
/// (per-subtree key range, live/tombstone split, entry count, depth and
/// fanout) must reconstruct - at the shard root, without fanning out to the
/// leaf chain - the same aggregate a fresh walk over every leaf produces.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeTopologySnapshotIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> NewTreeAsync(string prefix)
        => await _fixture.CreateTreeAsync($"{prefix}-{Guid.NewGuid():N}");

    private async Task<IShardRootGrain> ResolveShardAsync(ILattice tree, int shardIndex)
    {
        var grainFactory = _fixture.Cluster.GrainFactory;
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(tree.GetPrimaryKeyString());
        return grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
    }

    /// <summary>
    /// Walks the leaf chain of <paramref name="shardIndex"/> directly and
    /// folds each leaf's ground-truth <see cref="ShardTopologyNode"/>
    /// (computed from the live leaf cache) into the same shard-level
    /// aggregate the pushed-up structural digest is supposed to maintain:
    /// summed entry/live/tombstone counts and the spanned key range.
    /// </summary>
    private async Task<(long Entry, long Live, long Tomb, string? Low, string? High)> WalkLeafTopologyAsync(
        ILattice tree,
        int shardIndex)
    {
        var grainFactory = _fixture.Cluster.GrainFactory;
        var shard = await ResolveShardAsync(tree, shardIndex);

        long entry = 0;
        long live = 0;
        long tomb = 0;
        string? low = null;
        string? high = null;
        var highUnbounded = false;

        var leafId = await shard.GetLeftmostLeafIdAsync();
        while (leafId is { } id)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(id.GetGuidKey());
            var node = await leaf.GetTopologyNodeAsync();
            entry += node.EntryCount;
            live += node.LiveCount;
            tomb += node.TombstoneCount;
            if (node.LowKeyInclusive is not null &&
                (low is null || string.CompareOrdinal(node.LowKeyInclusive, low) < 0))
            {
                low = node.LowKeyInclusive;
            }
            if (node.HighKeyExclusive is null)
            {
                highUnbounded = true;
            }
            else if (high is null || string.CompareOrdinal(node.HighKeyExclusive, high) > 0)
            {
                high = node.HighKeyExclusive;
            }

            leafId = await leaf.GetNextSiblingAsync();
        }

        if (highUnbounded) high = null;
        return (entry, live, tomb, low, high);
    }

    private async Task AssertTopologyMatchesWalkAsync(
        ILattice tree,
        long expectedTotalEntries,
        int? shardCount = null)
    {
        var shards = shardCount ?? FourShardClusterFixture.TestShardCount;
        await LatticeDigestSettleHelpers.AwaitAllShardDigestsConvergeAsync(
            tree, shards, expectedTotalEntries);

        for (var shardIndex = 0; shardIndex < shards; shardIndex++)
        {
            var (entry, live, tomb, low, high) = await WalkLeafTopologyAsync(tree, shardIndex);
            var shard = await ResolveShardAsync(tree, shardIndex);
            var topology = await shard.GetTopologySnapshotAsync(16, CancellationToken.None);

            if (entry == 0 && topology is null)
            {
                // Empty shard: a leaf walk yields nothing and the snapshot is null.
                continue;
            }

            Assert.That(topology, Is.Not.Null, $"shard {shardIndex} must expose a topology snapshot");
            Assert.Multiple(() =>
            {
                Assert.That(topology!.EntryCount, Is.EqualTo(entry),
                    $"shard {shardIndex} subtree entry count must match a fresh leaf walk");
                Assert.That(topology.LiveCount, Is.EqualTo(live),
                    $"shard {shardIndex} subtree live count must match a fresh leaf walk");
                Assert.That(topology.TombstoneCount, Is.EqualTo(tomb),
                    $"shard {shardIndex} subtree tombstone count must match a fresh leaf walk");
                Assert.That(topology.LiveCount + topology.TombstoneCount, Is.EqualTo(topology.EntryCount),
                    $"shard {shardIndex} live + tombstone must reconcile with entry count");
                Assert.That(topology.LowKeyInclusive, Is.EqualTo(low),
                    $"shard {shardIndex} low key must match a fresh leaf walk");
                Assert.That(topology.HighKeyExclusive, Is.EqualTo(high),
                    $"shard {shardIndex} high key must match a fresh leaf walk");
            });
        }
    }

    [Test]
    public async Task Topology_aggregate_matches_leaf_walk_after_reshard()
    {
        // Grow the shard count online; the push-up structural digest must
        // re-converge so a shard-root snapshot still reconstructs the same
        // aggregate a fresh leaf walk produces, now across the new shard set.
        const int reshardTarget = 6;
        var treeId = $"topo-reshard-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        for (var i = 0; i < 50; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"val-{i}"));
        }
        await AssertTopologyMatchesWalkAsync(tree, expectedTotalEntries: 50);

        await tree.ReshardAsync(reshardTarget, CancellationToken.None);
        await DriveReshardToCompletionAsync(treeId, tree);

        // The logical data is preserved through routing dedup even though the
        // physical shard set may briefly retain moved rows in more than one shard.
        Assert.That(await tree.CountAsync(), Is.EqualTo(50), "reshard must preserve every live key");

        var registry = _fixture.Cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var map = await registry.GetShardMapAsync(treeId);
        Assert.That(map, Is.Not.Null, "reshard must persist a grown shard map");
        var shardIndices = map!.GetPhysicalShardIndices().ToArray();
        Assert.That(shardIndices, Has.Length.GreaterThanOrEqualTo(reshardTarget));

        await AssertEachShardTopologyReconcilesAsync(tree, shardIndices);
    }

    /// <summary>
    /// For every physical shard in <paramref name="shardIndices"/>, polls the
    /// shard root's pushed-up topology snapshot until it reconciles exactly with a
    /// fresh walk of that shard's own leaf chain (summed entry / live / tombstone
    /// counts and spanned key range). This is the digest-correctness invariant the
    /// push-up snapshot must restore after an online reshard: each shard root's
    /// aggregate equals the ground truth beneath it. Cross-shard sums are not
    /// asserted because a reshard can leave a moved row physically present in more
    /// than one shard's leaf chain until trimming completes (the routing layer
    /// dedups logically), so the per-shard aggregate, not a tree-wide sum, is the
    /// faithful oracle.
    /// </summary>
    private async Task AssertEachShardTopologyReconcilesAsync(ILattice tree, IReadOnlyList<int> shardIndices)
    {
        foreach (var shardIndex in shardIndices)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (true)
            {
                var (entry, live, tomb, low, high) = await WalkLeafTopologyAsync(tree, shardIndex);
                var shard = await ResolveShardAsync(tree, shardIndex);
                var topology = await shard.GetTopologySnapshotAsync(16, CancellationToken.None);

                var reconciled = (entry == 0 && topology is null)
                    || (topology is not null
                        && topology.EntryCount == entry
                        && topology.LiveCount == live
                        && topology.TombstoneCount == tomb
                        && topology.LowKeyInclusive == low
                        && topology.HighKeyExclusive == high);

                if (reconciled) break;
                if (DateTime.UtcNow > deadline)
                {
                    Assert.Fail(
                        $"shard {shardIndex} topology did not reconcile after reshard: " +
                        $"snapshot=({topology?.EntryCount},{topology?.LiveCount},{topology?.TombstoneCount}) " +
                        $"leafWalk=({entry},{live},{tomb})");
                }
                await Task.Delay(100);
            }
        }
    }

    /// <summary>
    /// Drives the asynchronous reshard coordinator and every dispatched
    /// per-shard split coordinator to completion synchronously, since the
    /// integration cluster's timers tick too slowly for the default test budget.
    /// </summary>
    private async Task DriveReshardToCompletionAsync(string treeId, ILattice tree)
    {
        var grainFactory = _fixture.Cluster.GrainFactory;
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(treeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        for (var i = 0; i < 100; i++)
        {
            if (await tree.IsReshardCompleteAsync() && await reshard.IsIdleAsync()) return;

            await reshard.RunReshardPassAsync();

            var map = await registry.GetShardMapAsync(treeId)
                ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, FourShardClusterFixture.TestShardCount);
            foreach (var idx in map.GetPhysicalShardIndices())
            {
                var split = grainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{idx}");
                if (!await split.IsIdleAsync())
                {
                    await split.RunSplitPassAsync();
                }
            }

            await Task.Delay(50);
        }

        Assert.Fail("Reshard did not converge within the allotted passes.");
    }

    [Test]
    public async Task Topology_aggregate_matches_leaf_walk_after_resize()
    {
        // Resize the structural fanout online (rebuilds the leaf/internal
        // shape). The pushed-up digest must re-converge against a fresh leaf
        // walk after the rebuild rather than retaining the pre-resize aggregate.
        var tree = await NewTreeAsync("topo-resize");
        for (var i = 0; i < 50; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"val-{i}"));
        }
        await AssertTopologyMatchesWalkAsync(tree, expectedTotalEntries: 50);

        await tree.ResizeAsync(newMaxLeafKeys: 16, newMaxInternalChildren: 16, CancellationToken.None);

        await AssertTopologyMatchesWalkAsync(tree, expectedTotalEntries: 50);
    }

    [Test]
    public async Task Topology_aggregate_matches_leaf_walk_after_small_writes()
    {
        var tree = await NewTreeAsync("topo-small");
        for (var i = 0; i < 8; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        await AssertTopologyMatchesWalkAsync(tree, expectedTotalEntries: 8);
    }

    [Test]
    public async Task Topology_aggregate_matches_leaf_walk_after_splits()
    {
        // MaxLeafKeys=4, so 50 keys force multi-level internal topology.
        var tree = await NewTreeAsync("topo-splits");
        for (var i = 0; i < 50; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"val-{i}"));
        }

        await AssertTopologyMatchesWalkAsync(tree, expectedTotalEntries: 50);

        // At least one shard must have grown past a single leaf, proving the
        // aggregate is reconstructed through internal nodes rather than a
        // flat single-leaf shortcut.
        var sawInternalRoot = false;
        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var shard = await ResolveShardAsync(tree, shardIndex);
            var topology = await shard.GetTopologySnapshotAsync(16, CancellationToken.None);
            if (topology is { IsLeaf: false })
            {
                sawInternalRoot = true;
                Assert.That(topology.SubtreeDepth, Is.GreaterThanOrEqualTo(2),
                    "an internal-rooted shard must report depth >= 2");
                Assert.That(topology.ChildFanout, Is.EqualTo(topology.Children.Count),
                    "a fully-expanded internal node must list every immediate child");
            }
        }

        Assert.That(sawInternalRoot, Is.True, "50 keys over MaxLeafKeys=4 must produce an internal-rooted shard");
    }

    [Test]
    public async Task Topology_aggregate_tracks_tombstones_after_deletes()
    {
        var tree = await NewTreeAsync("topo-delete");
        for (var i = 0; i < 24; i++)
        {
            await tree.SetAsync($"d{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var deleted = 0;
        for (var i = 0; i < 24; i += 3)
        {
            if (await tree.DeleteAsync($"d{i:D3}")) deleted++;
        }
        Assert.That(deleted, Is.GreaterThan(0), "test must delete at least one key");

        // Deletes leave tombstones (no compaction here), so the entry count
        // is unchanged while live drops and tombstones rise. The aggregate
        // must still reconcile against a fresh leaf walk.
        await AssertTopologyMatchesWalkAsync(tree, expectedTotalEntries: 24);

        long totalTombstones = 0;
        long totalLive = 0;
        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var shard = await ResolveShardAsync(tree, shardIndex);
            var topology = await shard.GetTopologySnapshotAsync(16, CancellationToken.None);
            if (topology is null) continue;
            totalTombstones += topology.TombstoneCount;
            totalLive += topology.LiveCount;
        }

        Assert.Multiple(() =>
        {
            Assert.That(totalTombstones, Is.EqualTo(deleted),
                "tree-wide tombstone count must equal the number of deleted keys");
            Assert.That(totalLive, Is.EqualTo(24 - deleted),
                "tree-wide live count must equal the surviving keys");
        });
    }

    [Test]
    public async Task Topology_depth_limit_truncates_without_visiting_leaves()
    {
        var tree = await NewTreeAsync("topo-depth");
        for (var i = 0; i < 60; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"val-{i}"));
        }
        await LatticeDigestSettleHelpers.AwaitAllShardDigestsConvergeAsync(
            tree, FourShardClusterFixture.TestShardCount, 60);

        var checkedInternal = false;
        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var shard = await ResolveShardAsync(tree, shardIndex);
            var deep = await shard.GetTopologySnapshotAsync(16, CancellationToken.None);
            if (deep is not { IsLeaf: false, ChildFanout: > 0 }) continue;
            checkedInternal = true;

            var shallow = await shard.GetTopologySnapshotAsync(0, CancellationToken.None);

            Assert.That(shallow, Is.Not.Null);
            Assert.Multiple(() =>
            {
                // Summary aggregates are complete even when the structure is truncated.
                Assert.That(shallow!.EntryCount, Is.EqualTo(deep.EntryCount),
                    "depth-limited root must report the same total entry count");
                Assert.That(shallow.LiveCount, Is.EqualTo(deep.LiveCount));
                Assert.That(shallow.TombstoneCount, Is.EqualTo(deep.TombstoneCount));
                Assert.That(shallow.ChildFanout, Is.EqualTo(deep.ChildFanout),
                    "depth-limited root must still list immediate children");
                Assert.That(shallow.Children, Has.Count.EqualTo(deep.ChildFanout));

                // No immediate child carries expanded grandchildren at depth 0;
                // internal children are flagged truncated, leaf children are not.
                foreach (var child in shallow.Children)
                {
                    Assert.That(child.Children, Is.Empty,
                        "a depth-0 read must not expand grandchildren");
                    if (!child.IsLeaf && child.ChildFanout > 0)
                    {
                        Assert.That(child.ChildrenTruncated, Is.True,
                            "an unexpanded internal child must be flagged truncated");
                    }
                }
            });
        }

        Assert.That(checkedInternal, Is.True, "expected at least one internal-rooted shard to depth-limit");
    }

    [Test]
    public async Task Topology_snapshot_is_idempotent_across_repeated_reads()
    {
        var tree = await NewTreeAsync("topo-idem");
        for (var i = 0; i < 40; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"val-{i}"));
        }
        await LatticeDigestSettleHelpers.AwaitAllShardDigestsConvergeAsync(
            tree, FourShardClusterFixture.TestShardCount, 40);

        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var shard = await ResolveShardAsync(tree, shardIndex);
            var first = await shard.GetTopologySnapshotAsync(16, CancellationToken.None);
            var second = await shard.GetTopologySnapshotAsync(16, CancellationToken.None);

            AssertStructurallyEqual(first, second, $"shard {shardIndex}");
        }
    }

    private static void AssertStructurallyEqual(ShardTopologyNode? expected, ShardTopologyNode? actual, string context)
    {
        if (expected is null || actual is null)
        {
            Assert.That(actual, Is.EqualTo(expected), $"{context}: nullability must match");
            return;
        }

        Assert.Multiple(() =>
        {
            Assert.That(actual.NodeId, Is.EqualTo(expected.NodeId), $"{context}: node id");
            Assert.That(actual.IsLeaf, Is.EqualTo(expected.IsLeaf), $"{context}: leaf flag");
            Assert.That(actual.SubtreeDepth, Is.EqualTo(expected.SubtreeDepth), $"{context}: depth");
            Assert.That(actual.LowKeyInclusive, Is.EqualTo(expected.LowKeyInclusive), $"{context}: low key");
            Assert.That(actual.HighKeyExclusive, Is.EqualTo(expected.HighKeyExclusive), $"{context}: high key");
            Assert.That(actual.EntryCount, Is.EqualTo(expected.EntryCount), $"{context}: entry count");
            Assert.That(actual.LiveCount, Is.EqualTo(expected.LiveCount), $"{context}: live count");
            Assert.That(actual.TombstoneCount, Is.EqualTo(expected.TombstoneCount), $"{context}: tombstone count");
            Assert.That(actual.ChildFanout, Is.EqualTo(expected.ChildFanout), $"{context}: fanout");
            Assert.That(actual.ChildrenTruncated, Is.EqualTo(expected.ChildrenTruncated), $"{context}: truncation");
            Assert.That(actual.Children, Has.Count.EqualTo(expected.Children.Count), $"{context}: child count");
        });

        for (var i = 0; i < expected.Children.Count && i < actual.Children.Count; i++)
        {
            AssertStructurallyEqual(expected.Children[i], actual.Children[i], $"{context}.child[{i}]");
        }
    }

    [Test]
    public async Task Topology_snapshot_is_null_for_empty_shard()
    {
        var tree = await NewTreeAsync("topo-empty");
        // A single key occupies exactly one shard, leaving the rest empty.
        await tree.SetAsync("only-key", Encoding.UTF8.GetBytes("v"));
        await LatticeDigestSettleHelpers.AwaitAllShardDigestsConvergeAsync(
            tree, FourShardClusterFixture.TestShardCount, 1);

        var nonEmptyShards = 0;
        var emptyShards = 0;
        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var shard = await ResolveShardAsync(tree, shardIndex);
            var topology = await shard.GetTopologySnapshotAsync(4, CancellationToken.None);

            // An untouched shard reports either a null snapshot (no root yet)
            // or a zero-entry root leaf once it has been activated.
            if (topology is null || topology.EntryCount == 0)
            {
                emptyShards++;
                continue;
            }

            nonEmptyShards++;
            Assert.That(topology.EntryCount, Is.EqualTo(1),
                "the only non-empty shard must report exactly one entry");
        }

        Assert.Multiple(() =>
        {
            Assert.That(nonEmptyShards, Is.EqualTo(1), "exactly one shard must hold the single key");
            Assert.That(emptyShards, Is.GreaterThan(0), "writing one key must leave at least one shard empty");
        });
    }
}

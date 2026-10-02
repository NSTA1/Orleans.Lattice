using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end regression for tree deletion and recovery on a tree an adaptive
/// shard split has grown past its pinned <c>ShardCount</c>. The split
/// allocates its target shard above the pin and routes the moved slots there
/// without changing the pin; the deletion walk used to stop at
/// <c>ShardCount - 1</c>, so every key the split moved stayed readable and
/// writable after <see cref="ILattice.DeleteTreeAsync"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShardSplitTreeDeletionIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task DeleteTree_blocks_keys_a_split_moved_above_the_pinned_shard_count_and_recover_restores_them()
    {
        var treeId = $"split-delete-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var expected = new Dictionary<string, string>();
        for (var i = 0; i < 200; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "Split should be complete after RunSplitPassAsync.");

        // The split target sits above the pinned ShardCount, and the pin is unchanged.
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = await registry.GetEntryAsync(treeId);
        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.ShardCount, Is.EqualTo(FourShardClusterFixture.TestShardCount));
        var splitShard = FourShardClusterFixture.TestShardCount;
        Assert.That(entry.ShardMap, Is.Not.Null, "Split must persist a custom shard map.");
        var movedKeys = expected.Keys.Where(k => entry.ShardMap!.Resolve(k) == splitShard).ToList();
        Assert.That(movedKeys, Is.Not.Empty,
            $"Precondition: the split must have routed at least one key to shard {splitShard}.");

        await tree.DeleteTreeAsync();

        foreach (var key in movedKeys)
        {
            Assert.ThrowsAsync<InvalidOperationException>(() => tree.GetAsync(key),
                $"Key '{key}' routed to split shard {splitShard} is still readable after DeleteTreeAsync.");
        }
        Assert.ThrowsAsync<InvalidOperationException>(
            () => tree.SetAsync(movedKeys[0], Encoding.UTF8.GetBytes("written-after-delete")),
            $"Key '{movedKeys[0]}' routed to split shard {splitShard} is still writable after DeleteTreeAsync.");

        await tree.RecoverTreeAsync();

        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' missing after RecoverTreeAsync.");
            Assert.That(Encoding.UTF8.GetString(actual!), Is.EqualTo(value), $"Wrong value for '{key}' after RecoverTreeAsync.");
        }
    }

    /// <summary>
    /// Issue #4234, case 1: an online reshard that re-pins an observably empty
    /// tree to a smaller count rewrites the pin and the map. Every shard the
    /// tree had before the re-pin must stay inside the delete, recover and
    /// purge walks, or a purge leaves those shards' state in storage.
    /// </summary>
    [Test]
    public async Task Empty_tree_downward_repin_keeps_the_dropped_shards_in_delete_recover_and_purge()
    {
        var treeId = $"repin-delete-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        const int originalShards = FourShardClusterFixture.TestShardCount;

        // Leave state on every shard, then delete the keys so the tree is
        // observably empty and the reshard takes its empty-tree fast path.
        for (var i = 0; i < 64; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"value-{i}"));
        }
        for (var i = 0; i < 64; i++)
        {
            await tree.DeleteAsync($"key-{i:D4}");
        }

        await _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId).ReshardAsync(2);

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = await registry.GetEntryAsync(treeId);
        Assert.That(entry!.ShardCount, Is.EqualTo(2), "Precondition: the empty-tree re-pin must lower the pinned count.");

        await tree.DeleteTreeAsync();
        await AssertShardsDeletedAsync(treeId, originalShards, expected: true, "after DeleteTreeAsync");

        await tree.RecoverTreeAsync();
        await AssertShardsDeletedAsync(treeId, originalShards, expected: false, "after RecoverTreeAsync");

        await tree.DeleteTreeAsync();
        await AssertShardsDeletedAsync(treeId, originalShards, expected: true, "after the second DeleteTreeAsync");
        await tree.PurgeTreeAsync();

        var status = await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).GetDeletionStatusAsync();
        Assert.That(status.PurgeComplete, Is.True, "Purge should complete within PurgeTreeAsync's wait budget.");
        Assert.That(status.PurgeShardCount, Is.GreaterThanOrEqualTo(originalShards), "Purge walk must cover every pre-re-pin shard.");
        // A purged shard root's state is cleared, so a shard the delete marked
        // and the purge reached reads as not deleted; one the purge skipped
        // keeps its deletion mark.
        await AssertShardsDeletedAsync(treeId, originalShards, expected: false, "after PurgeTreeAsync");
    }

    /// <summary>
    /// Issue #4234, case 2: a split the tree makes after a resize has aliased
    /// it records its allocation against the logical tree, while the shards
    /// live under the copy the alias targets. Delete, recover and purge of the
    /// aliased tree must still reach the shard the split added to that copy.
    /// </summary>
    [Test]
    public async Task Delete_recover_and_purge_of_an_aliased_tree_reach_a_shard_split_after_the_alias()
    {
        var treeId = $"alias-split-delete-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var expected = new Dictionary<string, string>();
        for (var i = 0; i < 200; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physical = await registry.ResolveAsync(treeId);
        Assert.That(physical, Is.Not.EqualTo(treeId), "Precondition: the resize must alias the tree to a new copy.");

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "Split should be complete after RunSplitPassAsync.");

        var entry = await registry.GetEntryAsync(treeId);
        Assert.That(entry?.ShardMap, Is.Not.Null, "Split must persist a custom shard map.");
        var indices = entry!.ShardMap!.GetPhysicalShardIndices();
        var splitShard = indices[indices.Count - 1];
        Assert.That(splitShard, Is.GreaterThanOrEqualTo(FourShardClusterFixture.TestShardCount),
            "Precondition: the split target must sit above the copy's original shards.");
        var movedKeys = expected.Keys.Where(k => entry.ShardMap.Resolve(k) == splitShard).ToList();
        Assert.That(movedKeys, Is.Not.Empty,
            $"Precondition: the split must have routed at least one key to shard {splitShard}.");

        var splitShardRoot = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{physical}/{splitShard}");

        await tree.DeleteTreeAsync();

        Assert.That(await splitShardRoot.IsDeletedAsync(), Is.True,
            $"Shard {splitShard} of the live copy was not marked deleted.");
        foreach (var key in movedKeys)
        {
            Assert.ThrowsAsync<InvalidOperationException>(() => tree.GetAsync(key),
                $"Key '{key}' routed to split shard {splitShard} is still readable after DeleteTreeAsync.");
        }
        Assert.ThrowsAsync<InvalidOperationException>(
            () => tree.SetAsync(movedKeys[0], Encoding.UTF8.GetBytes("written-after-delete")),
            $"Key '{movedKeys[0]}' routed to split shard {splitShard} is still writable after DeleteTreeAsync.");

        await tree.RecoverTreeAsync();

        Assert.That(await splitShardRoot.IsDeletedAsync(), Is.False,
            $"Shard {splitShard} of the live copy is still marked deleted after RecoverTreeAsync.");
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' missing after RecoverTreeAsync.");
            Assert.That(Encoding.UTF8.GetString(actual!), Is.EqualTo(value), $"Wrong value for '{key}' after RecoverTreeAsync.");
        }

        await tree.DeleteTreeAsync();
        Assert.That(await splitShardRoot.IsDeletedAsync(), Is.True,
            $"Shard {splitShard} of the live copy was not marked deleted by the second DeleteTreeAsync.");
        await tree.PurgeTreeAsync();

        var status = await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeId).GetDeletionStatusAsync();
        Assert.That(status.PurgeComplete, Is.True, "Purge should complete within PurgeTreeAsync's wait budget.");
        Assert.That(await splitShardRoot.IsDeletedAsync(), Is.False,
            $"Shard {splitShard} of the live copy kept its deletion mark, so the purge never reached it.");
    }

    private async Task AssertShardsDeletedAsync(string treeId, int shardCount, bool expected, string when)
    {
        for (var i = 0; i < shardCount; i++)
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{i}");
            Assert.That(await shard.IsDeletedAsync(), Is.EqualTo(expected),
                $"Shard {i} of '{treeId}' has the wrong deletion mark {when}.");
        }
    }
}

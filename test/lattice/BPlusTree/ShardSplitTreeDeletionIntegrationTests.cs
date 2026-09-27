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
}

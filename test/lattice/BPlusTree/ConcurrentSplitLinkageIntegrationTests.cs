using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression tests for issue #3523. Concurrent writes that race a split
/// used to orphan leaves in two ways:
/// <list type="bullet">
/// <item>
/// A leaf forwarded work to its sibling, the sibling split, and the
/// sibling's split was discarded, so its new leaf never got a separator.
/// </item>
/// <item>
/// A leaf split held its split gate while publishing its digest to the
/// parent. Meanwhile the parent held its own gate while seeding the child,
/// so the two waited on each other until the publish deadline broke the
/// cycle, and the write failed after its leaf had already been spliced in.
/// </item>
/// </list>
/// Each test drives concurrent single-entry writes - and, in one case, point
/// and range deletes - into a tree that splits under them. It then audits
/// every shard for orphaned leaves and a leaf chain that tiles the keyspace,
/// and reads every key back.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ConcurrentSplitLinkageIntegrationTests
{
    private const int SeedCount = 50;
    private const int ConcurrentWrites = 300;

    // Keys written before the race and then deleted, point-wise or by range,
    // while batch writes split the leaves they live on.
    private const int DoomedCount = 80;

    // The smallest internal fan-out the tree accepts. It makes internal nodes
    // split under the concurrent writes, so a separator can be delivered along
    // an ancestor path captured before an interleaved turn split one of its
    // nodes. The default fan-out never splits an internal node at this size.
    private const int SmallMaxInternalChildren = 3;

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

    [Repeat(3)]
    [TestCase(null)]
    [TestCase(SmallMaxInternalChildren)]
    public async Task Concurrent_SetManyAsync_leaves_no_orphaned_leaves_or_lost_keys(int? maxInternalChildren)
    {
        var (treeId, tree) = await CreateSeededTreeAsync("split-link-setmany", maxInternalChildren);

        var writes = new Task[ConcurrentWrites];
        for (var i = 0; i < ConcurrentWrites; i++)
        {
            writes[i] = tree.SetManyAsync(
            [
                new KeyValuePair<string, byte[]>(LiveKey(i), Encoding.UTF8.GetBytes($"live-{i}")),
            ]);
        }

        await Task.WhenAll(writes);

        await AssertNoOrphansAndNoLostKeysAsync(treeId, tree);
    }

    [Repeat(3)]
    [TestCase(null)]
    [TestCase(SmallMaxInternalChildren)]
    public async Task Concurrent_SetAsync_leaves_no_orphaned_leaves_or_lost_keys(int? maxInternalChildren)
    {
        var (treeId, tree) = await CreateSeededTreeAsync("split-link-set", maxInternalChildren);

        var writes = new Task[ConcurrentWrites];
        for (var i = 0; i < ConcurrentWrites; i++)
        {
            writes[i] = tree.SetAsync(LiveKey(i), Encoding.UTF8.GetBytes($"live-{i}"));
        }

        await Task.WhenAll(writes);

        await AssertNoOrphansAndNoLostKeysAsync(treeId, tree);
    }

    [Repeat(3)]
    [TestCase(null)]
    [TestCase(SmallMaxInternalChildren)]
    public async Task Interleaved_SetAsync_and_SetManyAsync_leave_no_orphaned_leaves_or_lost_keys(int? maxInternalChildren)
    {
        var (treeId, tree) = await CreateSeededTreeAsync("split-link-mixed", maxInternalChildren);

        // SetAsync is not interleaving and SetManyAsync is, so a single-key
        // write's split can be linked while a batch turn is mid-descent over
        // the same nodes.
        var writes = new Task[ConcurrentWrites];
        for (var i = 0; i < ConcurrentWrites; i++)
        {
            var value = Encoding.UTF8.GetBytes($"live-{i}");
            writes[i] = i % 2 == 0
                ? tree.SetAsync(LiveKey(i), value)
                : tree.SetManyAsync([new KeyValuePair<string, byte[]>(LiveKey(i), value)]);
        }

        await Task.WhenAll(writes);

        await AssertNoOrphansAndNoLostKeysAsync(treeId, tree);
    }

    [Repeat(3)]
    [TestCase(null)]
    [TestCase(SmallMaxInternalChildren)]
    public async Task Deletes_racing_splits_neither_resurrect_nor_lose_keys(int? maxInternalChildren)
    {
        var (treeId, tree) = await CreateSeededTreeAsync("split-link-delete", maxInternalChildren);
        for (var i = 0; i < DoomedCount; i++)
        {
            await tree.SetAsync(DoomedKey(i), Encoding.UTF8.GetBytes($"doomed-{i}"));
        }

        // Point deletes take the first half of the doomed keys and a range
        // delete takes the next quarter, both while batch writes split the
        // leaves under them. The last quarter is never deleted.
        var operations = new List<Task>(ConcurrentWrites + (DoomedCount / 2) + 1);
        for (var i = 0; i < ConcurrentWrites; i++)
        {
            operations.Add(tree.SetManyAsync(
            [
                new KeyValuePair<string, byte[]>(LiveKey(i), Encoding.UTF8.GetBytes($"live-{i}")),
            ]));

            if (i < DoomedCount / 2)
            {
                operations.Add(tree.DeleteAsync(DoomedKey(i)));
            }

            if (i == ConcurrentWrites / 2)
            {
                operations.Add(tree.DeleteRangeAsync(DoomedKey(DoomedCount / 2), DoomedKey(DoomedCount * 3 / 4)));
            }
        }

        await Task.WhenAll(operations);

        var resurrected = new List<string>();
        var survivorsLost = new List<string>();
        for (var i = 0; i < DoomedCount; i++)
        {
            var present = await tree.GetAsync(DoomedKey(i)) is not null;
            if (i < DoomedCount * 3 / 4 && present)
            {
                resurrected.Add(DoomedKey(i));
            }
            else if (i >= DoomedCount * 3 / 4 && !present)
            {
                survivorsLost.Add(DoomedKey(i));
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(resurrected, Is.Empty, "a deleted key must stay deleted across the splits it raced");
            Assert.That(survivorsLost, Is.Empty, "a key no delete covered must remain readable");
        });

        await AssertNoOrphansAndNoLostKeysAsync(treeId, tree);
    }

    private static string DoomedKey(int i) => $"doomed-{i:D3}";

    private static string LiveKey(int i) => $"live-{i:D3}";

    private async Task<(string TreeId, ILattice Tree)> CreateSeededTreeAsync(string prefix, int? maxInternalChildren)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = FourShardClusterFixture.SmallMaxLeafKeys,
            MaxInternalChildren = maxInternalChildren,
            ShardCount = FourShardClusterFixture.TestShardCount,
        });

        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        for (var i = 0; i < SeedCount; i++)
        {
            await tree.SetAsync($"seed-{i:D3}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        return (treeId, tree);
    }

    private async Task AssertNoOrphansAndNoLostKeysAsync(string treeId, ILattice tree)
    {
        var orphans = new List<string>();
        var tilingBreaks = new List<string>();
        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{shardIndex}");
            if (await shard.GetLeftmostLeafIdAsync() is not null)
            {
                foreach (var tilingBreak in await LeafChainTiling.FindBreaksAsync(_cluster.GrainFactory, shard))
                {
                    tilingBreaks.Add($"shard {shardIndex}: {tilingBreak}");
                }
            }

            string? resume = null;
            do
            {
                var page = await shard.SurveyOrphanedLeavesAsync(resume);
                foreach (var finding in page.Findings)
                {
                    orphans.Add($"shard {shardIndex}: {finding}");
                }

                resume = page.ResumeFromInclusive;
            }
            while (resume is not null);
        }

        var lost = new List<string>();
        for (var i = 0; i < ConcurrentWrites; i++)
        {
            if (await tree.GetAsync(LiveKey(i)) is null)
            {
                lost.Add(LiveKey(i));
            }
        }

        for (var i = 0; i < SeedCount; i++)
        {
            if (await tree.GetAsync($"seed-{i:D3}") is null)
            {
                lost.Add($"seed-{i:D3}");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(orphans, Is.Empty, "no leaf may be left spliced into the chain but unreachable by descent");
            Assert.That(tilingBreaks, Is.Empty, "every shard's leaf chain must tile the keyspace and agree with routing");
            Assert.That(lost, Is.Empty, "every acknowledged write must be readable");
        });
    }
}

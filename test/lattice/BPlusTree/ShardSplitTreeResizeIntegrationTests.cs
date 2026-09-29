using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end regression for online resize and snapshot of a tree an adaptive
/// shard split has grown past its pinned <c>ShardCount</c> (issue 3880). The
/// split allocates its target shard above the pin and routes the moved slots
/// there without changing the pin, while the shard that gave the slots up keeps
/// a sealed copy of every entry it moved. The copy used to walk only shards
/// <c>0</c> to <c>ShardCount - 1</c> into an identity-routed destination, so
/// every key the split moved was dropped, and the sealed copies resurfaced with
/// the values they held when the split moved them.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShardSplitTreeResizeIntegrationTests
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
    public async Task Resize_preserves_keys_a_split_moved_above_the_pinned_shard_count_with_their_current_values()
    {
        var treeId = $"split-resize-{Guid.NewGuid():N}";
        var (tree, expected, movedKeys) = await CreateSplitTreeAsync(treeId);

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();

        await AssertTreeHoldsExactlyAsync(tree, expected, "after resize");

        // The resized tree keeps routing through the split topology, and keeps
        // accepting writes to the keys the split moved.
        await tree.SetAsync(movedKeys[0], Encoding.UTF8.GetBytes("written-after-resize"));
        expected[movedKeys[0]] = "written-after-resize";
        await AssertTreeHoldsExactlyAsync(tree, expected, "after a post-resize write");
    }

    [Test]
    public async Task Undo_after_a_split_tree_resize_restores_every_key()
    {
        var treeId = $"split-resize-undo-{Guid.NewGuid():N}";
        var (tree, expected, _) = await CreateSplitTreeAsync(treeId);

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        await resize.UndoResizeAsync();

        await AssertTreeHoldsExactlyAsync(tree, expected, "after undoing the resize");
    }

    [Test]
    public async Task Second_resize_of_a_split_tree_preserves_every_key()
    {
        var treeId = $"split-resize-twice-{Guid.NewGuid():N}";
        var (tree, expected, _) = await CreateSplitTreeAsync(treeId);

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        await resize.ResizeAsync(96, 96);
        await resize.RunResizePassAsync();

        await AssertTreeHoldsExactlyAsync(tree, expected, "after a second resize");
    }

    [Test]
    public async Task Snapshot_of_a_split_tree_copies_every_key_with_its_current_value()
    {
        var treeId = $"split-snapshot-{Guid.NewGuid():N}";
        var (_, expected, _) = await CreateSplitTreeAsync(treeId);
        var destinationId = $"{treeId}-copy";

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(treeId);
        await snapshot.SnapshotAsync(destinationId, SnapshotMode.Online);
        await snapshot.RunSnapshotPassAsync();

        var copy = _cluster.GrainFactory.GetGrain<ILattice>(destinationId);
        await AssertTreeHoldsExactlyAsync(copy, expected, "in the online snapshot");
    }

    [Test]
    public async Task Offline_snapshot_of_a_split_tree_copies_every_key_with_its_current_value()
    {
        var treeId = $"split-snapshot-off-{Guid.NewGuid():N}";
        var (tree, expected, _) = await CreateSplitTreeAsync(treeId);
        var destinationId = $"{treeId}-copy";

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(treeId);
        await snapshot.SnapshotAsync(destinationId, SnapshotMode.Offline);
        await snapshot.RunSnapshotPassAsync();

        var copy = _cluster.GrainFactory.GetGrain<ILattice>(destinationId);
        await AssertTreeHoldsExactlyAsync(copy, expected, "in the offline snapshot");
        await AssertTreeHoldsExactlyAsync(tree, expected, "in the source after the offline snapshot");
    }

    /// <summary>
    /// Writes a tree, splits shard 0 onto a shard above the pinned count, then
    /// rewrites half the moved keys and deletes one, so the sealed copies the
    /// split left on shard 0 disagree with the live values on the split shard.
    /// </summary>
    private async Task<(ILattice Tree, Dictionary<string, string> Expected, List<string> MovedKeys)> CreateSplitTreeAsync(
        string treeId)
    {
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

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = await registry.GetEntryAsync(treeId);
        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.ShardCount, Is.EqualTo(FourShardClusterFixture.TestShardCount));
        Assert.That(entry.ShardMap, Is.Not.Null, "Split must persist a custom shard map.");
        var splitShard = FourShardClusterFixture.TestShardCount;
        var movedKeys = expected.Keys.Where(k => entry.ShardMap!.Resolve(k) == splitShard).ToList();
        Assert.That(movedKeys, Has.Count.GreaterThanOrEqualTo(2),
            $"Precondition: the split must have routed at least two keys to shard {splitShard}.");

        for (var i = 1; i < movedKeys.Count; i += 2)
        {
            var value = $"rewritten-after-split-{i}";
            await tree.SetAsync(movedKeys[i], Encoding.UTF8.GetBytes(value));
            expected[movedKeys[i]] = value;
        }

        var deleted = movedKeys[^1];
        await tree.DeleteAsync(deleted);
        expected.Remove(deleted);
        movedKeys.RemoveAt(movedKeys.Count - 1);

        await AssertTreeHoldsExactlyAsync(tree, expected, "before the operation under test");
        return (tree, expected, movedKeys);
    }

    private static async Task AssertTreeHoldsExactlyAsync(
        ILattice tree, Dictionary<string, string> expected, string when)
    {
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' missing {when}.");
            Assert.That(Encoding.UTF8.GetString(actual!), Is.EqualTo(value), $"Wrong value for '{key}' {when}.");
        }

        var scanned = new Dictionary<string, string>();
        await foreach (var (key, value) in tree.ScanEntriesAsync())
        {
            scanned[key] = Encoding.UTF8.GetString(value);
        }

        Assert.That(scanned, Is.EquivalentTo(expected), $"Scanned entries differ from the expected set {when}.");
    }
}

using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for the lifecycle of a tree after a resize has aliased its
/// logical id to a new physical tree. A tree's first resize retires a physical
/// copy whose id is the logical id itself, so every coordinator that treats that
/// id as "the physical tree" instead of resolving the alias acts on the wrong
/// state: the retirement purge used to unregister the live logical tree, the
/// alias swap used to discard the tree's own configuration overrides, and a
/// snapshot used to copy the retired shards instead of the live ones.
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class TreeResizeAliasLifecycleIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Purging_the_physical_copy_retired_by_a_first_resize_keeps_the_logical_tree_registered()
    {
        var treeName = $"resize-retire-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        for (int i = 0; i < 8; i++)
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        await ResizeAsync(treeName);

        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        var resizedPhysical = await registry.ResolveAsync(treeName);
        Assert.That(resizedPhysical, Is.Not.EqualTo(treeName), "the resize did not alias the tree");

        // Traffic during the soft-delete window meets the retired shards'
        // rejecting phase and moves the routing tier onto the resized copy.
        for (int i = 0; i < 8; i++)
            Assert.That(await tree.GetAsync($"key-{i:D4}"), Is.Not.Null, $"key-{i:D4} unreadable after the resize");

        // The resize soft-deleted the retired physical copy, whose id is the
        // logical id. Purge it now, exactly as the purge reminder does once
        // LatticeOptions.SoftDeleteDuration has elapsed.
        await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName).PurgePhysicalAsync();

        var status = await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName).GetDeletionStatusAsync();
        Assert.That(status.IsDeleted, Is.False, "retiring a physical copy must not delete its logical tree");
        Assert.That(await tree.TreeExistsAsync(), Is.True,
            "purging the retired physical copy unregistered the live logical tree");
        Assert.That(await registry.ResolveAsync(treeName), Is.EqualTo(resizedPhysical),
            "purging the retired physical copy discarded the logical tree's alias");
        var entry = await registry.GetEntryAsync(treeName);
        Assert.That(entry?.MaxLeafKeys, Is.EqualTo(64),
            "purging the retired physical copy discarded the logical tree's sizing");
        Assert.That(await tree.GetAllTreeIdsAsync(), Does.Contain(treeName));
        for (int i = 0; i < 8; i++)
        {
            var value = await tree.GetAsync($"key-{i:D4}");
            Assert.That(value, Is.Not.Null, $"key-{i:D4} unreadable after the retirement purge");
        }
    }

    [Test]
    public async Task Resize_preserves_the_trees_configuration_overrides()
    {
        var treeName = $"resize-config-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        await tree.SetAsync("key", Encoding.UTF8.GetBytes("v"));
        await tree.SetPublishEventsEnabledAsync(false);
        await tree.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, TimeSpan.FromHours(6));

        await ResizeAsync(treeName);

        var retention = await tree.GetHistoryRetentionAsync();
        Assert.That(retention.Mode, Is.EqualTo(HistoryRetentionMode.FullValue),
            "the resize discarded the tree's history retention mode");
        Assert.That(retention.Window, Is.EqualTo(TimeSpan.FromHours(6)),
            "the resize discarded the tree's history retention window");

        var entry = await _cluster.GrainFactory.GetLatticeRegistry().GetEntryAsync(treeName);
        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.PublishEvents, Is.False,
            "the resize discarded the tree's PublishEvents override");
        Assert.That(entry.MaxLeafKeys, Is.EqualTo(64));
        Assert.That(entry.MaxInternalChildren, Is.EqualTo(64));
        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        var physical = await registry.ResolveAsync(treeName);
        Assert.That((await registry.GetEntryAsync(physical))!.DerivedFrom, Is.EqualTo(treeName));
        Assert.That(await registry.GetAliasesTargetingAsync(physical), Is.EqualTo(new[] { treeName }));
    }

    [Test]
    [TestCase(SnapshotMode.Offline)]
    [TestCase(SnapshotMode.Online)]
    public async Task Snapshot_of_a_resized_tree_copies_the_live_physical_tree(SnapshotMode mode)
    {
        var treeName = $"resize-snap-{mode}-{Guid.NewGuid():N}".ToLowerInvariant();
        var destName = $"resize-snap-dest-{mode}-{Guid.NewGuid():N}".ToLowerInvariant();
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        var expected = new Dictionary<string, string>();
        for (int i = 0; i < 6; i++)
        {
            expected[$"before-{i:D4}"] = $"b{i}";
            await tree.SetAsync($"before-{i:D4}", Encoding.UTF8.GetBytes($"b{i}"));
        }

        await ResizeAsync(treeName);

        for (int i = 0; i < 6; i++)
        {
            expected[$"after-{i:D4}"] = $"a{i}";
            await tree.SetAsync($"after-{i:D4}", Encoding.UTF8.GetBytes($"a{i}"));
        }

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(treeName);
        await snapshot.SnapshotAsync(destName, mode);
        await snapshot.RunSnapshotPassAsync();

        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destName);
        foreach (var (key, value) in expected)
        {
            var result = await dest.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"'{key}' missing from the snapshot of the resized tree");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo(value));
        }

        var keys = new List<string>();
        await foreach (var key in dest.ScanKeysAsync())
            keys.Add(key);
        Assert.That(keys, Has.Count.EqualTo(expected.Count));

        // The source stays fully readable once the snapshot has finished.
        foreach (var key in expected.Keys)
            Assert.That(await tree.GetAsync(key), Is.Not.Null, $"'{key}' unreadable on the source after the snapshot");
    }

    private async Task ResizeAsync(string treeName)
    {
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
    }
}

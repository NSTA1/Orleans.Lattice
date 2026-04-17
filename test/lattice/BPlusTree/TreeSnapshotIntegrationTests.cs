using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class TreeSnapshotIntegrationTests
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
    public async Task Offline_snapshot_copies_all_data()
    {
        var sourceTree = $"snap-offline-{Guid.NewGuid():N}";
        var destTree = $"snap-offline-dest-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);

        var expected = new Dictionary<string, string>();
        for (int i = 0; i < 12; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        await snapshot.SnapshotAsync(destTree, SnapshotMode.Offline);
        await snapshot.RunSnapshotPassAsync();

        // Verify destination has all data.
        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destTree);
        foreach (var (key, value) in expected)
        {
            var result = await dest.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"Key '{key}' missing in snapshot");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo(value));
        }

        // Source tree should be accessible again after offline snapshot.
        foreach (var (key, value) in expected)
        {
            var result = await tree.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"Key '{key}' missing from source after snapshot");
        }
    }

    [Test]
    public async Task Online_snapshot_copies_all_data()
    {
        var sourceTree = $"snap-online-{Guid.NewGuid():N}";
        var destTree = $"snap-online-dest-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);

        var expected = new Dictionary<string, string>();
        for (int i = 0; i < 10; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        await snapshot.SnapshotAsync(destTree, SnapshotMode.Online);
        await snapshot.RunSnapshotPassAsync();

        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destTree);
        foreach (var (key, value) in expected)
        {
            var result = await dest.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"Key '{key}' missing in snapshot");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo(value));
        }
    }

    [Test]
    public async Task Snapshot_of_empty_tree_produces_empty_destination()
    {
        var sourceTree = $"snap-empty-{Guid.NewGuid():N}";
        var destTree = $"snap-empty-dest-{Guid.NewGuid():N}";

        // Write and delete to ensure the source tree exists but is empty of live data.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        await tree.SetAsync("temp", Encoding.UTF8.GetBytes("temp"));
        await tree.DeleteAsync("temp");

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        await snapshot.SnapshotAsync(destTree, SnapshotMode.Offline);
        await snapshot.RunSnapshotPassAsync();

        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destTree);
        var keys = new List<string>();
        await foreach (var key in dest.KeysAsync())
            keys.Add(key);

        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task Snapshot_destination_accepts_writes()
    {
        var sourceTree = $"snap-writes-{Guid.NewGuid():N}";
        var destTree = $"snap-writes-dest-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);

        for (int i = 0; i < 6; i++)
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        await snapshot.SnapshotAsync(destTree, SnapshotMode.Online);
        await snapshot.RunSnapshotPassAsync();

        // Write new data to the destination.
        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destTree);
        await dest.SetAsync("new-key", Encoding.UTF8.GetBytes("new-value"));

        var result = await dest.GetAsync("new-key");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("new-value"));

        // Original data still present.
        Assert.That(await dest.GetAsync("key-0000"), Is.Not.Null);
    }

    [Test]
    public async Task Snapshot_excludes_tombstoned_keys()
    {
        var sourceTree = $"snap-tombstones-{Guid.NewGuid():N}";
        var destTree = $"snap-tombstones-dest-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);

        for (int i = 0; i < 8; i++)
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        await tree.DeleteAsync("key-0002");
        await tree.DeleteAsync("key-0005");

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        await snapshot.SnapshotAsync(destTree, SnapshotMode.Offline);
        await snapshot.RunSnapshotPassAsync();

        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destTree);
        Assert.That(await dest.GetAsync("key-0002"), Is.Null);
        Assert.That(await dest.GetAsync("key-0005"), Is.Null);
        Assert.That(await dest.GetAsync("key-0000"), Is.Not.Null);
        Assert.That(await dest.GetAsync("key-0003"), Is.Not.Null);

        var keys = new List<string>();
        await foreach (var key in dest.KeysAsync())
            keys.Add(key);
        Assert.That(keys, Has.Count.EqualTo(6));
    }

    [Test]
    public void Snapshot_rejects_existing_destination()
    {
        var sourceTree = $"snap-exists-{Guid.NewGuid():N}";
        var destTree = sourceTree; // same tree

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        Assert.ThrowsAsync<ArgumentException>(
            () => snapshot.SnapshotAsync(destTree, SnapshotMode.Offline));
    }

    [Test]
    public async Task Snapshot_destination_is_registered()
    {
        var sourceTree = $"snap-reg-{Guid.NewGuid():N}";
        var destTree = $"snap-reg-dest-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);

        await tree.SetAsync("key", Encoding.UTF8.GetBytes("val"));

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        await snapshot.SnapshotAsync(destTree, SnapshotMode.Online);
        await snapshot.RunSnapshotPassAsync();

        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destTree);
        Assert.That(await dest.TreeExistsAsync(), Is.True);
    }
}

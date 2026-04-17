using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class TreeResizeIntegrationTests
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
    public async Task Resize_preserves_all_data_with_larger_leaf_size()
    {
        var treeName = $"resize-grow-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        // Write enough keys to trigger splits with SmallMaxLeafKeys=4.
        var expected = new Dictionary<string, string>();
        for (int i = 0; i < 20; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        // Verify all keys readable before resize.
        foreach (var (key, value) in expected)
        {
            var result = await tree.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"Key '{key}' missing before resize");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo(value));
        }

        // Resize to larger leaf size.
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();

        // Verify all keys still readable after resize (via alias to new physical tree).
        foreach (var (key, value) in expected)
        {
            var result = await tree.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"Key '{key}' missing after resize");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo(value));
        }

        // Verify key scan returns all keys in order.
        var keys = new List<string>();
        await foreach (var key in tree.KeysAsync())
            keys.Add(key);

        Assert.That(keys, Has.Count.EqualTo(expected.Count));
        var sortedExpected = expected.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.That(keys, Is.EqualTo(sortedExpected));
    }

    [Test]
    public async Task Resize_preserves_all_data_with_smaller_leaf_size()
    {
        var treeName = $"resize-shrink-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        var expected = new Dictionary<string, string>();
        for (int i = 0; i < 10; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        // Resize to smaller leaf size (2 — minimum is >1).
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(2, 3);
        await resize.RunResizePassAsync();

        foreach (var (key, value) in expected)
        {
            var result = await tree.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"Key '{key}' missing after resize");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo(value));
        }
    }

    [Test]
    public async Task Resize_handles_empty_tree()
    {
        var treeName = $"resize-empty-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(256, 128);
        await resize.RunResizePassAsync();

        // Tree should still work after resize.
        await tree.SetAsync("after-resize", Encoding.UTF8.GetBytes("works"));
        var result = await tree.GetAsync("after-resize");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("works"));
    }

    [Test]
    public async Task Resize_preserves_data_with_tombstones()
    {
        var treeName = $"resize-tombstones-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        for (int i = 0; i < 10; i++)
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"value-{i}"));

        // Delete some keys.
        await tree.DeleteAsync("key-0002");
        await tree.DeleteAsync("key-0005");
        await tree.DeleteAsync("key-0008");

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();

        // Deleted keys should still be absent.
        Assert.That(await tree.GetAsync("key-0002"), Is.Null);
        Assert.That(await tree.GetAsync("key-0005"), Is.Null);
        Assert.That(await tree.GetAsync("key-0008"), Is.Null);

        // Live keys should still exist.
        Assert.That(await tree.GetAsync("key-0000"), Is.Not.Null);
        Assert.That(await tree.GetAsync("key-0001"), Is.Not.Null);
        Assert.That(await tree.GetAsync("key-0003"), Is.Not.Null);

        var keys = new List<string>();
        await foreach (var key in tree.KeysAsync())
            keys.Add(key);
        Assert.That(keys, Has.Count.EqualTo(7));
    }

    [Test]
    public async Task Resize_idempotent_with_same_parameters()
    {
        var treeName = $"resize-idempotent-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await tree.SetAsync("key-1", Encoding.UTF8.GetBytes("value-1"));

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();

        // Second resize with different params after completion should work.
        await resize.ResizeAsync(32, 32);
        await resize.RunResizePassAsync();

        var result = await tree.GetAsync("key-1");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("value-1"));
    }

    [Test]
    public async Task Resize_tree_accepts_writes_after_completion()
    {
        var treeName = $"resize-writes-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        for (int i = 0; i < 8; i++)
            await tree.SetAsync($"before-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();

        // Write new data after resize.
        for (int i = 0; i < 8; i++)
            await tree.SetAsync($"after-{i:D4}", Encoding.UTF8.GetBytes($"new-v{i}"));

        var keys = new List<string>();
        await foreach (var key in tree.KeysAsync())
            keys.Add(key);

        Assert.That(keys, Has.Count.EqualTo(16));
    }

    [Test]
    public async Task UndoResize_restores_original_data()
    {
        var treeName = $"resize-undo-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        var expected = new Dictionary<string, string>();
        for (int i = 0; i < 10; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        // Resize.
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();

        // Undo the resize.
        await resize.UndoResizeAsync();

        // All data should still be readable from the recovered original tree.
        foreach (var (key, value) in expected)
        {
            var result = await tree.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"Key '{key}' missing after undo");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo(value));
        }
    }
}

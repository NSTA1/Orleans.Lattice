using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class TreeMergeIntegrationTests
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
    public async Task Merge_copies_all_data_from_source_to_target()
    {
        var sourceTree = $"merge-src-{Guid.NewGuid():N}";
        var targetTree = $"merge-tgt-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTree);

        // Populate source tree.
        var expected = new Dictionary<string, string>();
        for (int i = 0; i < 12; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await source.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        // Ensure target tree is registered (write a dummy key then delete it).
        await target.SetAsync("_init", [0]);
        await target.DeleteAsync("_init");

        // Merge source into target.
        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTree);
        await merge.MergeAsync(sourceTree);
        await merge.RunMergePassAsync();

        // Verify target has all source data.
        foreach (var (key, value) in expected)
        {
            var result = await target.GetAsync(key);
            Assert.That(result, Is.Not.Null, $"Key '{key}' missing in target after merge");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo(value));
        }
    }

    [Test]
    public async Task Merge_preserves_existing_target_data()
    {
        var sourceTree = $"merge-src-preserve-{Guid.NewGuid():N}";
        var targetTree = $"merge-tgt-preserve-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTree);

        // Populate target with existing data.
        await target.SetAsync("existing-key", Encoding.UTF8.GetBytes("existing-value"));

        // Populate source with different data.
        await source.SetAsync("new-key", Encoding.UTF8.GetBytes("new-value"));

        // Merge.
        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTree);
        await merge.MergeAsync(sourceTree);
        await merge.RunMergePassAsync();

        // Both keys should be present.
        var existingResult = await target.GetAsync("existing-key");
        Assert.That(existingResult, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(existingResult!), Is.EqualTo("existing-value"));

        var newResult = await target.GetAsync("new-key");
        Assert.That(newResult, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(newResult!), Is.EqualTo("new-value"));
    }

    [Test]
    public async Task Merge_lww_resolves_conflicts_by_timestamp()
    {
        var sourceTree = $"merge-lww-src-{Guid.NewGuid():N}";
        var targetTree = $"merge-lww-tgt-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTree);

        // Write to target first (older timestamp).
        await target.SetAsync("conflict-key", Encoding.UTF8.GetBytes("target-value"));

        // Wait briefly to ensure source timestamp is newer.
        await Task.Delay(10);

        // Write to source (newer timestamp).
        await source.SetAsync("conflict-key", Encoding.UTF8.GetBytes("source-value"));

        // Merge — source should win because it has a newer timestamp.
        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTree);
        await merge.MergeAsync(sourceTree);
        await merge.RunMergePassAsync();

        var result = await target.GetAsync("conflict-key");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("source-value"));
    }

    [Test]
    public async Task Merge_target_wins_when_it_has_newer_timestamp()
    {
        var sourceTree = $"merge-tgt-wins-src-{Guid.NewGuid():N}";
        var targetTree = $"merge-tgt-wins-tgt-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTree);

        // Write to source first (older timestamp).
        await source.SetAsync("conflict-key", Encoding.UTF8.GetBytes("source-value"));

        // Wait briefly to ensure target timestamp is newer.
        await Task.Delay(10);

        // Write to target (newer timestamp).
        await target.SetAsync("conflict-key", Encoding.UTF8.GetBytes("target-value"));

        // Merge — target should win because it has a newer timestamp.
        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTree);
        await merge.MergeAsync(sourceTree);
        await merge.RunMergePassAsync();

        var result = await target.GetAsync("conflict-key");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("target-value"));
    }

    [Test]
    public async Task Merge_empty_source_is_noop()
    {
        var sourceTree = $"merge-empty-src-{Guid.NewGuid():N}";
        var targetTree = $"merge-empty-tgt-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTree);

        // Ensure both trees exist.
        await source.SetAsync("_init", [0]);
        await source.DeleteAsync("_init");
        await target.SetAsync("existing", Encoding.UTF8.GetBytes("value"));

        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTree);
        await merge.MergeAsync(sourceTree);
        await merge.RunMergePassAsync();

        // Target data should be unchanged.
        var result = await target.GetAsync("existing");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("value"));
    }

    [Test]
    public async Task Merge_into_empty_target()
    {
        var sourceTree = $"merge-to-empty-src-{Guid.NewGuid():N}";
        var targetTree = $"merge-to-empty-tgt-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTree);

        // Populate source.
        await source.SetAsync("key-a", Encoding.UTF8.GetBytes("val-a"));
        await source.SetAsync("key-b", Encoding.UTF8.GetBytes("val-b"));

        // Ensure target exists.
        await target.SetAsync("_init", [0]);
        await target.DeleteAsync("_init");

        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTree);
        await merge.MergeAsync(sourceTree);
        await merge.RunMergePassAsync();

        Assert.That(await target.GetAsync("key-a"), Is.Not.Null);
        Assert.That(await target.GetAsync("key-b"), Is.Not.Null);
    }

    [Test]
    public async Task MergeAsync_via_ILattice_works()
    {
        var sourceTree = $"merge-api-src-{Guid.NewGuid():N}";
        var targetTree = $"merge-api-tgt-{Guid.NewGuid():N}";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetTree);

        await source.SetAsync("key-1", Encoding.UTF8.GetBytes("value-1"));

        // Ensure target exists.
        await target.SetAsync("_init", [0]);
        await target.DeleteAsync("_init");

        // Use the ILattice API.
        await target.MergeAsync(sourceTree);

        // Run the merge pass manually since the timer may not fire in time.
        var merge = _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetTree);
        await merge.RunMergePassAsync();

        var result = await target.GetAsync("key-1");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("value-1"));
    }
}

using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class TombstoneCompactionIntegrationTests
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
    public async Task Compaction_removes_tombstones_and_preserves_live_keys()
    {
        var treeName = $"{SmallLeafClusterFixture.CompactionTreeName}-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        // Write some keys.
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await router.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await router.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        // Delete two of them (creates tombstones).
        await router.DeleteAsync("b");
        await router.DeleteAsync("d");

        // Verify deletes are visible.
        Assert.That(await router.GetAsync("b"), Is.Null);
        Assert.That(await router.GetAsync("d"), Is.Null);

        // Run compaction (grace period = 0 → all tombstones eligible).
        var compaction = _cluster.GrainFactory
            .GetGrain<ITombstoneCompactionGrain>(treeName);
        await compaction.RunCompactionPassAsync();

        // Live keys still readable.
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("a"))!), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("c"))!), Is.EqualTo("3"));

        // Deleted keys still absent.
        Assert.That(await router.GetAsync("b"), Is.Null);
        Assert.That(await router.GetAsync("d"), Is.Null);

        // Verify tombstones are physically gone: the key enumeration should
        // return only live keys, and a second compaction should be a no-op
        // (no version change → LastCompactionVersion fast-path).
        var keys = new List<string>();
        await foreach (var key in router.KeysAsync())
        {
            keys.Add(key);
        }
        Assert.That(keys.Count, Is.EqualTo(2));
        Assert.That(keys, Does.Contain("a"));
        Assert.That(keys, Does.Contain("c"));
    }

    [Test]
    public async Task Compaction_is_idempotent_and_handles_incremental_deletes()
    {
        var treeName = $"{SmallLeafClusterFixture.CompactionTreeName}-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var compaction = _cluster.GrainFactory
            .GetGrain<ITombstoneCompactionGrain>(treeName);

        // --- First wave: write + delete + compact ---
        await router.SetAsync("w1-a", Encoding.UTF8.GetBytes("1"));
        await router.SetAsync("w1-b", Encoding.UTF8.GetBytes("2"));
        await router.DeleteAsync("w1-b");

        await compaction.RunCompactionPassAsync();

        // w1-b tombstone should be gone; a second pass is a fast no-op.
        await compaction.RunCompactionPassAsync();

        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("w1-a"))!), Is.EqualTo("1"));
        Assert.That(await router.GetAsync("w1-b"), Is.Null);

        // --- Second wave: more writes + deletes + compact ---
        await router.SetAsync("w2-a", Encoding.UTF8.GetBytes("3"));
        await router.SetAsync("w2-b", Encoding.UTF8.GetBytes("4"));
        await router.DeleteAsync("w2-a");

        await compaction.RunCompactionPassAsync();

        // First-wave survivor still intact.
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("w1-a"))!), Is.EqualTo("1"));
        // Second-wave delete applied.
        Assert.That(await router.GetAsync("w2-a"), Is.Null);
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("w2-b"))!), Is.EqualTo("4"));

        // Full key enumeration shows only live keys.
        var keys = new List<string>();
        await foreach (var key in router.KeysAsync())
        {
            keys.Add(key);
        }
        Assert.That(keys.Count, Is.EqualTo(2));
        Assert.That(keys, Does.Contain("w1-a"));
        Assert.That(keys, Does.Contain("w2-b"));
    }
}

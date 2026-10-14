using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
[Category("Integration")]
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
        await foreach (var key in router.ScanKeysAsync())
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
        await foreach (var key in router.ScanKeysAsync())
        {
            keys.Add(key);
        }
        Assert.That(keys.Count, Is.EqualTo(2));
        Assert.That(keys, Does.Contain("w1-a"));
        Assert.That(keys, Does.Contain("w2-b"));
    }

    [Test]
    public async Task Compaction_discards_a_persisted_unrouted_nomination_without_admitting_the_rowless_leaf()
    {
        var treeName = $"{SmallLeafClusterFixture.CompactionTreeName}-stale-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry
        {
            ShardCount = 1,
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
        });
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        for (var i = 0; i < 30; i++)
            await router.SetAsync($"k{i:D3}", [(byte)i]);

        var routing = await router.GetRoutingAsync();
        Assert.That((await router.DiagnoseAsync()).Shards.Single().RootIsLeaf, Is.False,
            "the recovery proof must traverse real internal routing, not just a single-leaf root");
        var shardIndex = routing.Map.GetPhysicalShardIndices()[0];
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}");
        var rowless = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        var rowlessId = rowless.GetGrainId();
        await shard.RetainDirtyLeafAsync(rowlessId, default);
        Assert.That((await shard.GetDirtyLeavesSinceLastCompactionAsync()).DirtyLeaves, Does.Contain(rowlessId));

        var compaction = _cluster.GrainFactory.GetGrain<ITombstoneCompactionGrain>(treeName);
        await compaction.RunCompactionPassAsync();

        Assert.That((await shard.GetDirtyLeavesSinceLastCompactionAsync()).DirtyLeaves, Does.Not.Contain(rowlessId));
        await compaction.RunCompactionPassAsync();
        Assert.That((await shard.GetDirtyLeavesSinceLastCompactionAsync()).DirtyLeaves, Does.Not.Contain(rowlessId));
        Assert.ThrowsAsync<LeafStateRowLostException>(() => rowless.GetAsync("k000"));
        for (var i = 0; i < 30; i++)
            Assert.That(await router.GetAsync($"k{i:D3}"), Is.EqualTo(new byte[] { (byte)i }));
    }
}

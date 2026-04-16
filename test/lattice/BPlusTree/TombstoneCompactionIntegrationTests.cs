using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[Collection(SmallLeafClusterCollection.Name)]
public class TombstoneCompactionIntegrationTests(SmallLeafClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
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
        Assert.Null(await router.GetAsync("b"));
        Assert.Null(await router.GetAsync("d"));

        // Run compaction (grace period = 0 → all tombstones eligible).
        var compaction = _cluster.GrainFactory
            .GetGrain<ITombstoneCompactionGrain>(treeName);
        await compaction.RunCompactionPassAsync();

        // Live keys still readable.
        Assert.Equal("1", Encoding.UTF8.GetString((await router.GetAsync("a"))!));
        Assert.Equal("3", Encoding.UTF8.GetString((await router.GetAsync("c"))!));

        // Deleted keys still absent.
        Assert.Null(await router.GetAsync("b"));
        Assert.Null(await router.GetAsync("d"));

        // Verify tombstones are physically gone: the key enumeration should
        // return only live keys, and a second compaction should be a no-op
        // (no version change → LastCompactionVersion fast-path).
        var keys = new List<string>();
        await foreach (var key in router.KeysAsync())
        {
            keys.Add(key);
        }
        Assert.Equal(2, keys.Count);
        Assert.Contains("a", keys);
        Assert.Contains("c", keys);
    }

    [Fact]
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

        Assert.Equal("1", Encoding.UTF8.GetString((await router.GetAsync("w1-a"))!));
        Assert.Null(await router.GetAsync("w1-b"));

        // --- Second wave: more writes + deletes + compact ---
        await router.SetAsync("w2-a", Encoding.UTF8.GetBytes("3"));
        await router.SetAsync("w2-b", Encoding.UTF8.GetBytes("4"));
        await router.DeleteAsync("w2-a");

        await compaction.RunCompactionPassAsync();

        // First-wave survivor still intact.
        Assert.Equal("1", Encoding.UTF8.GetString((await router.GetAsync("w1-a"))!));
        // Second-wave delete applied.
        Assert.Null(await router.GetAsync("w2-a"));
        Assert.Equal("4", Encoding.UTF8.GetString((await router.GetAsync("w2-b"))!));

        // Full key enumeration shows only live keys.
        var keys = new List<string>();
        await foreach (var key in router.KeysAsync())
        {
            keys.Add(key);
        }
        Assert.Equal(2, keys.Count);
        Assert.Contains("w1-a", keys);
        Assert.Contains("w2-b", keys);
    }
}

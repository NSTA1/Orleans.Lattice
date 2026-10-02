using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The bulk-load emptiness check (issue #4251): a shard whose root is only the
/// empty leaf a read seeded holds no data and must accept a bulk load, while a
/// shard that has held data - even data since deleted - must still refuse.
/// </summary>
public partial class BPlusTreeBulkLoadTests
{
    private static List<KeyValuePair<string, byte[]>> EmptyShardEntries(int count) =>
        Enumerable.Range(0, count)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

    private async Task RegisterShardsAsync(string treeId, int shardCount)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
            ShardCount = shardCount,
        });
    }

    private static async Task<List<string>> ScanAllKeysAsync(ILattice tree)
    {
        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync())
            keys.Add(k);
        return keys;
    }

    [Test]
    public async Task BulkLoad_after_an_empty_tree_reshard_loads_every_entry()
    {
        const string treeId = "bulk-after-empty-reshard";
        await RegisterShardsAsync(treeId, 2);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var entries = EmptyShardEntries(40);

        // The reshard's emptiness probe reads every shard, which seeds each
        // shard root with an empty leaf.
        await tree.ReshardAsync(4);
        await tree.BulkLoadAsync(entries);

        var keys = await ScanAllKeysAsync(tree);
        Assert.That(keys, Is.EqualTo(entries.Select(e => e.Key).ToList()));
        Assert.That(await tree.CountAsync(), Is.EqualTo(entries.Count));
    }

    [Test]
    public async Task BulkLoad_after_a_read_of_an_empty_tree_loads_every_entry()
    {
        const string treeId = "bulk-after-empty-read";
        await RegisterShardsAsync(treeId, 1);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var entries = EmptyShardEntries(20);

        Assert.That(await tree.GetAsync("absent"), Is.Null);
        var seededLeaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(SeededRootLeafId($"{treeId}/0"));
        Assert.That(await seededLeaf.GetTreeIdAsync(), Is.EqualTo(treeId), "the read seeded the shard's root leaf");

        await tree.BulkLoadAsync(entries);

        Assert.That(await ScanAllKeysAsync(tree), Is.EqualTo(entries.Select(e => e.Key).ToList()));
        Assert.That(await tree.GetAsync("k0007"), Is.EqualTo(Encoding.UTF8.GetBytes("v7")));
        Assert.That(await seededLeaf.GetTreeIdAsync(), Is.Null,
            "the replaced seeded leaf is retired rather than left unreachable with its pins");
    }

    // Mirrors ShardRootGrain.DeterministicGuid over the shard key, the id
    // EnsureRootAsync seeds a shard's first root leaf under.
    private static Guid SeededRootLeafId(string shardKey) =>
        new(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(shardKey)).AsSpan(0, 16));

    [Test]
    public async Task BulkLoadRaw_after_a_read_of_an_empty_shard_loads_every_entry()
    {
        const string treeId = "bulk-raw-after-empty-read";
        await RegisterShardsAsync(treeId, 1);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/0");
        var clock = HybridLogicalClock.Zero;
        var raw = EmptyShardEntries(12)
            .Select(e =>
            {
                clock = HybridLogicalClock.Tick(clock);
                return new LwwEntry(e.Key, LwwValue<byte[]>.Create(e.Value, clock));
            })
            .ToList();

        Assert.That(await tree.GetAsync("absent"), Is.Null);
        await shard.BulkLoadRawAsync("raw-op", raw);

        Assert.That(await ScanAllKeysAsync(tree), Is.EqualTo(raw.Select(e => e.Key).ToList()));
    }

    [Test]
    public async Task BulkLoad_after_a_delete_emptied_the_tree_still_refuses()
    {
        const string treeId = "bulk-after-delete";
        await RegisterShardsAsync(treeId, 1);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        await tree.SetAsync("gone", Encoding.UTF8.GetBytes("v"));
        await tree.DeleteAsync("gone");

        // The shard holds a tombstone, so it has held data: a bulk load stamps
        // its entries from a zero clock and must not be offered such a shard.
        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => tree.BulkLoadAsync(EmptyShardEntries(5)));
        Assert.That(ex!.Message, Does.Contain("requires an empty shard"));
    }
}

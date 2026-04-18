using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests that verify the streaming <see cref="LatticeExtensions.BulkLoadAsync"/>
/// honours a custom <see cref="ShardMap"/> persisted in the registry (F-030).
/// A multi-shard fixture is required: under a single-shard layout, any map
/// pinned to shard 0 is a no-op and the routing assertion is vacuous.
/// </summary>
[TestFixture]
public class BulkLoadShardMapRoutingTests
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
    public async Task BulkLoadAsync_streaming_routes_via_persisted_shard_map()
    {
        // Exercises the full F-030 path: GetRoutingAsync resolves a custom
        // shard map persisted in the registry, and entries are routed accordingly.
        // IMPORTANT: must not touch the tree before writing the custom map,
        // because LatticeGrain caches _shardMap on first access. SetShardMapAsync
        // upserts the registry entry, so no bootstrap call is needed.
        //
        // We pin every virtual slot onto physical shard 3 (not 0). Under the
        // fixture's default hashed map with 4 shards, 25 entries would spread
        // across all four shards, so observing all 25 on shard 3 proves the
        // custom map was honoured end-to-end through streaming bulk-load.
        const string treeId = "bulk-custom-map";
        const int pinnedShard = 3;

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pinnedSlots = new int[LatticeOptions.DefaultVirtualShardCount];
        Array.Fill(pinnedSlots, pinnedShard);
        await registry.SetShardMapAsync(treeId, new ShardMap { Slots = pinnedSlots });

        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        const int count = 25;
        async IAsyncEnumerable<KeyValuePair<string, byte[]>> GenerateEntries()
        {
            for (int i = 0; i < count; i++)
            {
                yield return KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
                await Task.Yield();
            }
        }

        await tree.BulkLoadAsync(GenerateEntries(), _cluster.GrainFactory, chunkSize: 5);

        // All entries are retrievable.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);
        Assert.That(keys, Has.Count.EqualTo(count));

        // Every entry landed on the pinned shard — proves the custom map was
        // used for routing during streaming bulk-load. Under the default 4-shard
        // hashed map, CountPerShardAsync would return length 4 with values
        // spread across all shards. Under the pinned map, GetPhysicalShardIndices
        // collapses to a single physical shard, so:
        //   - CountPerShardAsync returns exactly one entry, and
        //   - that entry holds every live key.
        var perShard = await tree.CountPerShardAsync();
        Assert.That(perShard, Has.Count.EqualTo(1),
            $"ShardMap pins every virtual slot to shard {pinnedShard}, so " +
            "GetPhysicalShardIndices() must collapse to a single physical shard. " +
            "Under default hashed routing this would be 4, proving the custom map was ignored.");
        Assert.That(perShard[0], Is.EqualTo(count),
            $"All {count} entries must route to pinned shard {pinnedShard}.");
    }
}


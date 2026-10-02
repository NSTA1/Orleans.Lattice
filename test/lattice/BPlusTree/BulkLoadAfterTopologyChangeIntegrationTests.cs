using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4206: the tree's <c>[StatelessWorker]</c> grain caches its routing per
/// activation, and neither a reshard nor an alias swap invalidates it. The bulk-load
/// paths partition their entries by that routing and graft them straight onto shard
/// roots, which check no slot ownership, so routing cached before the change puts
/// keys on shards, or on a physical tree, that the live routing no longer sends
/// reads to.
/// </summary>
/// <remarks>
/// Each test warms the routing of the tree's only activation (a single silo, calls
/// made one at a time) while the tree is empty, then either re-pins it from
/// <see cref="InitialShards"/> to <see cref="GrownShards"/> through the reshard
/// fast path or swaps its alias, bulk-loads through the warmed activation, and reads
/// every key under freshly resolved routing.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class BulkLoadAfterTopologyChangeIntegrationTests
{
    private const int InitialShards = 2;
    private const int GrownShards = 4;
    private const int KeyCount = 64;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private ILatticeRegistry Registry => _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task BulkLoadAsync_after_an_alias_swap_writes_to_the_live_physical_tree()
    {
        // A grow would not do here: the reshard's emptiness probe activates the
        // tree's shard roots, and BulkLoadAsync refuses a shard that has a root. An
        // alias swap onto a fresh physical tree leaves every shard untouched.
        var treeId = $"bulk-load-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = InitialShards });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var warmed = await tree.GetRoutingAsync();
        Assert.That(warmed.PhysicalTreeId, Is.EqualTo(treeId), "precondition: the warmed alias");

        var physical = $"{treeId}-copy";
        await Registry.RegisterAsync(physical, new TreeRegistryEntry { ShardCount = InitialShards, DerivedFrom = treeId });
        await Registry.SetAliasAsync(treeId, physical);

        await tree.BulkLoadAsync(Entries());

        await AssertEveryKeyReadableAsync(tree);
    }

    [Test]
    public async Task BulkAppendChunkAsync_after_a_reshard_partitions_by_the_live_map()
    {
        var tree = await WarmThenGrowEmptyTreeAsync("bulk-append");

        var accepted = await tree.BulkAppendChunkAsync($"op-{Guid.NewGuid():N}", Entries());

        Assert.That(accepted, Is.EqualTo(KeyCount));
        await AssertEveryKeyReadableAsync(tree);
    }

    [Test]
    public async Task Streaming_BulkLoadAsync_after_a_reshard_partitions_by_the_live_map()
    {
        var tree = await WarmThenGrowEmptyTreeAsync("bulk-stream");

        await tree.BulkLoadAsync(StreamAsync(Entries()), _cluster.Client, chunkSize: 8);

        await AssertEveryKeyReadableAsync(tree);
    }

    private static List<KeyValuePair<string, byte[]>> Entries() =>
        Enumerable.Range(0, KeyCount)
            .Select(i => new KeyValuePair<string, byte[]>($"k-{i:D4}", [(byte)i]))
            .ToList();

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> StreamAsync(
        IEnumerable<KeyValuePair<string, byte[]>> entries)
    {
        foreach (var entry in entries)
        {
            yield return entry;
            await Task.Yield();
        }
    }

    /// <summary>
    /// Registers an empty tree, warms its activation's routing at
    /// <see cref="InitialShards"/>, and re-pins it to <see cref="GrownShards"/>.
    /// </summary>
    private async Task<ILattice> WarmThenGrowEmptyTreeAsync(string prefix)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = InitialShards });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        var warmed = await tree.GetRoutingAsync();
        Assert.That(warmed.Map.GetPhysicalShardIndices(), Has.Count.EqualTo(InitialShards), "precondition: the warmed map");

        await tree.ReshardAsync(GrownShards);
        var persisted = await Registry.GetShardMapAsync(treeId);
        Assert.That(persisted?.GetPhysicalShardIndices(), Has.Count.EqualTo(GrownShards), "precondition: the empty tree was re-pinned");
        return tree;
    }

    private static async Task AssertEveryKeyReadableAsync(ILattice tree)
    {
        // Resolve the live map before reading, so a key is looked up on the shard the
        // registry routes it to rather than on the one the warmed map named.
        _ = await tree.GetRoutingAsync(forceRefresh: true);

        var missing = new List<string>();
        foreach (var entry in Entries())
        {
            if (await tree.GetAsync(entry.Key) is null)
            {
                missing.Add(entry.Key);
            }
        }

        Assert.That(missing, Is.Empty, "every bulk-loaded key must be on the shard the live map routes it to");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

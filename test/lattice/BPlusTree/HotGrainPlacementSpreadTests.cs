using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression guard for the multi-silo placement pile-up behind #3348: a tree's
/// shard roots and WAL shard grains are activated in one burst from whichever
/// silo hosts the caller's <c>[StatelessWorker]</c> <see cref="LatticeGrain"/>.
/// Under the Orleans default (resource-optimised placement, which prefers the
/// local silo while every silo still reports the same pre-load statistics) the
/// whole burst lands on that one silo, and a long-lived grain never moves once
/// placed. On an eight-silo Layer 3 cohort that put all 64 shard roots and all
/// 16 WAL partitions on one pinned 4-core silo while the other seven idled.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class HotGrainPlacementSpreadTests
{
    private const int ShardCount = 64;
    private const int WalPartitions = 32;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
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

    [Test]
    public async Task Burst_activated_shard_roots_and_wal_shards_spread_across_silos()
    {
        const string treeId = "placement-spread";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = ShardCount });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        // 4096 uniform keys touch every shard and every WAL partition with
        // overwhelming probability, and they all enter through one gateway's
        // LatticeGrain - the same single-front-door burst the rig produces.
        var entries = Enumerable.Range(0, 4096)
            .Select(_ => KeyValuePair.Create(Guid.NewGuid().ToString("N"), new byte[] { 1 }))
            .ToList();
        await tree.SetManyAsync(entries);

        var management = _cluster.Client.GetGrain<IManagementGrain>(0);
        var stats = await management.GetDetailedGrainStatistics();
        var silos = _cluster.GetActiveSilos().Select(s => s.SiloAddress).ToArray();

        AssertSpread(stats, typeof(ShardRootGrain).FullName!, treeId, silos, ShardCount);
        AssertSpread(stats, typeof(WalShardGrain).FullName!, treeId, silos, WalPartitions);
    }

    private static void AssertSpread(
        DetailedGrainStatistic[] stats, string grainClass, string treeId, SiloAddress[] silos, int expectedTotal)
    {
        var perSilo = stats
            .Where(s => s.GrainType.StartsWith(grainClass + ",", StringComparison.Ordinal)
                && s.GrainId.Key.ToString()!.StartsWith(treeId + "/", StringComparison.Ordinal))
            .GroupBy(s => s.SiloAddress)
            .ToDictionary(g => g.Key, g => g.Count());

        Assert.That(perSilo.Values.Sum(), Is.EqualTo(expectedTotal), $"{grainClass} activations for the tree");

        // Random placement leaves one of two silos empty with probability 2^(1-n):
        // 2^-63 for the shard roots and 2^-31 for the WAL shards.
        foreach (var silo in silos)
        {
            Assert.That(
                perSilo.GetValueOrDefault(silo),
                Is.GreaterThan(0),
                $"{grainClass} activations on {silo}; distribution was "
                + string.Join(", ", perSilo.Select(kv => $"{kv.Key}={kv.Value}")));
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.WalPartitions = WalPartitions);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

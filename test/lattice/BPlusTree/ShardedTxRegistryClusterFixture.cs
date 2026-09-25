using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture for the sharded saga decision registry (issue #3501). It pins a
/// deliberately small per-shard <see cref="LatticeOptions.TxRegistryAdmissionBudgetBytes"/>
/// and a retention long enough that no tombstone ages out during a test, so one
/// registry shard admits exactly <see cref="TombstonesPerShard"/> retained decisions.
/// A tree can then complete more sagas than one shard admits only if the sagas
/// spread across <see cref="ShardCount"/> shards.
/// </summary>
public sealed class ShardedTxRegistryClusterFixture
{
    /// <summary>The registry shard count every silo runs with.</summary>
    public const int ShardCount = 8;

    /// <summary>The retained tombstones one registry shard admits under <see cref="AdmissionBudgetBytes"/>.</summary>
    public const int TombstonesPerShard = 128;

    /// <summary>
    /// The per-shard admission budget: the estimate's fixed base plus
    /// <see cref="TombstonesPerShard"/> tombstones, each weighted as one retained
    /// decision plus its forgotten-at stamp.
    /// </summary>
    public const long AdmissionBudgetBytes = TxRegistryGrain.AdmissionEstimateBaseBytes
        + (TombstonesPerShard * (TxRegistryGrain.AdmissionEstimateDecisionBytes + TxRegistryGrain.AdmissionEstimateForgottenAtBytes));

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.TxRegistryShardCount = ShardCount;
                o.TxRegistryAdmissionBudgetBytes = AdmissionBudgetBytes;
                o.TxDecisionRetention = TimeSpan.FromMinutes(30);
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

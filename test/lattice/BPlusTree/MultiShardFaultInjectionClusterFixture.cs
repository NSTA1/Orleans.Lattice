using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture that wires <see cref="FaultInjectionGrainStorage"/> for
/// multi-shard chaos testing. Uses 4 shards and small leaf/internal limits to
/// force splits quickly under concurrent load.
/// </summary>
public sealed class MultiShardFaultInjectionClusterFixture
{
    public const string TreeName = "i-multi-tree";
    public const int TestShardCount = 4;
    public const int SmallMaxLeafKeys = 4;
    public const int SmallMaxInternalChildren = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();

        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(TreeName, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            MaxInternalChildren = SmallMaxInternalChildren,
            ShardCount = TestShardCount,
        });
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
            siloBuilder.AddLattice((_, name) =>
                siloBuilder.Services.AddFaultInjectionMemoryStorage(
                    name,
                    (MemoryGrainStorageOptions _) => { },
                    (FaultInjectionGrainStorageOptions _) => { }));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

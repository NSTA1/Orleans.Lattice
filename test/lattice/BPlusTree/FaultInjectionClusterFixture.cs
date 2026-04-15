using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture that uses <see cref="FaultInjectionGrainStorage"/> so that
/// tests can inject read/write/clear faults per grain via <see cref="IStorageFaultGrain"/>.
/// Small leaf and internal limits are used to force splits quickly.
/// </summary>
public sealed class FaultInjectionClusterFixture : IAsyncLifetime
{
    public const string TreeName = "fi-tree";
    public const int SmallMaxLeafKeys = 4;
    public const int SmallMaxInternalChildren = 4;

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
            siloBuilder.Services.AddFaultInjectionMemoryStorage(
                "bplustree",
                (Orleans.Configuration.MemoryGrainStorageOptions _) => { },
                (FaultInjectionGrainStorageOptions _) => { });
            siloBuilder.ConfigureLattice(TreeName, o =>
            {
                o.MaxLeafKeys = SmallMaxLeafKeys;
                o.MaxInternalChildren = SmallMaxInternalChildren;
                o.ShardCount = 1;
            });
        }
    }
}

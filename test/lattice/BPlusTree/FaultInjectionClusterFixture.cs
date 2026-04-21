using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture that uses <see cref="FaultInjectionGrainStorage"/> so that
/// tests can inject read/write/clear faults per grain via <see cref="IStorageFaultGrain"/>.
/// Small leaf and internal limits are used to force splits quickly.
/// </summary>
public sealed class FaultInjectionClusterFixture
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

        // F-019c: structural sizing is pinned in the registry. Pre-register
        // the tree with the desired structural pin before any writes so the
        // resolver-backed grains see our custom sizing instead of the
        // LatticeConstants defaults.
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(TreeName, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            MaxInternalChildren = SmallMaxInternalChildren,
            ShardCount = 1,
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
                    (Orleans.Configuration.MemoryGrainStorageOptions _) => { },
                    (FaultInjectionGrainStorageOptions _) => { }));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

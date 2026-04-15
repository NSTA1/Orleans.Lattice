using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed class FourShardClusterFixture : IAsyncLifetime
{
    public const string TreeName = "four-shard-tree";
    public const int TestShardCount = 4;
    public const int SmallMaxLeafKeys = 4;

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
            siloBuilder.AddMemoryGrainStorage("bplustree");
            siloBuilder.ConfigureLattice(o =>
            {
                o.MaxLeafKeys = SmallMaxLeafKeys;
                o.ShardCount = TestShardCount;
            });
        }
    }
}

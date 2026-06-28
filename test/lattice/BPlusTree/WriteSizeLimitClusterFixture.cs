using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture that pins the optional write-size bounds
/// (<see cref="LatticeOptions.MaxKeyLength"/> /
/// <see cref="LatticeOptions.MaxValueSizeBytes"/>) so the integration tests can
/// prove the <see cref="ILattice"/> write surface rejects oversized keys and
/// values at the public boundary.
/// </summary>
public sealed class WriteSizeLimitClusterFixture
{
    public const int MaxKeyLength = 16;
    public const int MaxValueSizeBytes = 32;

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
                o.MaxKeyLength = MaxKeyLength;
                o.MaxValueSizeBytes = MaxValueSizeBytes;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

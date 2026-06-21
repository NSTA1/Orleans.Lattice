using System.Linq;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Single-silo <see cref="TestCluster"/> fixture that registers the core
/// lattice and the state API add-on, used by the integration-level
/// registration tests.
/// </summary>
internal sealed class LatticeStateApiClusterFixture
{
    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeStateApi();
        }
    }
}

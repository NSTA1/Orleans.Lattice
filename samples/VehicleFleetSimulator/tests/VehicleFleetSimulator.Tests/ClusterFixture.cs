using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains;
using VehicleFleetSimulator.Grains.Cities;
using VehicleFleetSimulator.Grains.Telemetry;

// xUnit serializes tests within a [Collection], but runs different collections (and uncollected
// classes) in parallel by default. Several of the simulator's pure-unit-test classes carry no
// [Collection] attribute and therefore race the cluster-fixture tests for CPU, starving the
// shared TestCluster's reminder/timer cadence. Empirically, that races StreamSubscriberOrderTests
// against its 15s telemetry-collection budget. Disabling assembly-level collection parallelism
// keeps the whole suite single-threaded — the suite runs in ~3s end-to-end so we lose nothing.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace VehicleFleetSimulator.Tests;

/// <summary>
/// Shared Orleans <see cref="TestCluster"/> fixture for grain-level tests. Configures memory grain
/// storage and a static <see cref="ICityGraphProvider"/> backed by <see cref="TestGraph.BuildSimple"/>.
/// </summary>
public sealed class ClusterFixture : IAsyncLifetime
{
    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            Cluster.Dispose();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder silo)
        {
            silo.AddMemoryGrainStorage("Default");
            silo.ConfigureServices(services =>
            {
                services.AddSingleton<ICityGraphProvider>(new StaticCityGraphProvider(TestGraph.BuildSimple()));
                services.AddSingleton<ITelemetrySink, FanOutTelemetrySink>();
                services.AddSingleton<SimulationRuntimeState>();
            });
        }
    }

    private sealed class ClientConfigurator : Orleans.TestingHost.IClientBuilderConfigurator
    {
        public void Configure(Microsoft.Extensions.Configuration.IConfiguration configuration, IClientBuilder clientBuilder)
        {
        }
    }
}

[CollectionDefinition(Name)]
public sealed class ClusterCollection : ICollectionFixture<ClusterFixture>
{
    public const string Name = "Orleans cluster";
}

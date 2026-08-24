using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed class ClusterFixture
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
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>
    /// Response timeout for both the client and the silos in the test cluster.
    /// The default is 30s, but a contended <c>ILatticeLockGrain.AcquireAsync</c>
    /// parks server-side for up to its <c>MaxWait</c> (lock integration tests use
    /// values up to 60s), and under a saturated CI run an in-activation lease
    /// timer can be scheduled late. A 30s ceiling therefore surfaces a legitimate
    /// long wait - or a starved-but-correct grant - as a false response timeout.
    /// Raising it comfortably above the largest test <c>MaxWait</c> removes that
    /// race without weakening any assertion (no test asserts on the ceiling).
    /// </summary>
    private static readonly TimeSpan ClusterResponseTimeout = TimeSpan.FromMinutes(2);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.Configure<SiloMessagingOptions>(o =>
            {
                o.ResponseTimeout = ClusterResponseTimeout;
                o.SystemResponseTimeout = ClusterResponseTimeout;
            });
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.Services.Configure<ClientMessagingOptions>(
                o => o.ResponseTimeout = ClusterResponseTimeout);
        }
    }
}

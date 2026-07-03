using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice and the
/// authorization policy-store add-on, over in-memory grain storage. Shared by the
/// policy-store integration tests; no network or external store is involved.
/// </summary>
public sealed class AuthClusterFixture
{
    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider (source of the silo-side authorization services).</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side authorization policy store.</summary>
    public ILatticeAuthorizationPolicyStore Store =>
        SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
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
            siloBuilder.AddLatticeAuth();
        }
    }
}

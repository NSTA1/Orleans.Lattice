using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// membership and auth add-ons the tenancy registry hard-depends on, and the
/// tenancy add-on itself. Shared by the tenant-registry integration tests, which
/// exercise the real dogfooded <c>sys-tenant-*</c> trees end to end.
/// </summary>
public sealed class TenancyClusterFixture
{
    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side tenant registry under test.</summary>
    public ITenantRegistry Registry => SiloServices.GetRequiredService<ITenantRegistry>();

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
            siloBuilder.AddLatticeMembership();
            siloBuilder.AddLatticeAuth();
            siloBuilder.AddLatticeTenancy();
        }
    }
}

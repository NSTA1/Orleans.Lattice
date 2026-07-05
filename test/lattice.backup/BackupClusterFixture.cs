using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the view
/// infrastructure, and the backup add-on - so the default in-cluster sink and the
/// catalog store are live over in-memory grain storage. No network or external
/// store is involved. Shared by the sink and catalog integration tests.
/// </summary>
public sealed class BackupClusterFixture
{
    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider (source of the silo-side backup services).</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side default in-cluster backup sink.</summary>
    public ILatticeBackupSink Sink => SiloServices.GetRequiredService<ILatticeBackupSink>();

    /// <summary>The silo-side backup catalog store.</summary>
    public ILatticeBackupCatalogStore Catalog => SiloServices.GetRequiredService<ILatticeBackupCatalogStore>();

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
            siloBuilder.AddLatticeBackup();
        }
    }
}

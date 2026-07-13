using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// backup engine, and the backup control-API add-on, exposing the live
/// <see cref="ILatticeBackupControl"/> facade plus the engine's catalog, sink,
/// and restore seams so the facade can be driven end to end. Also exposes a
/// factory that builds a control facade over a caller-supplied
/// <see cref="BackupAccessAuthorizer"/> so the fail-closed permission path can be
/// driven with a denying gate.
/// </summary>
public sealed class ApiBackupClusterFixture
{
    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The client-side grain factory used to seed and read trees.</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>The silo-side backup control facade under test.</summary>
    internal ILatticeBackupControl Control =>
        SiloServices.GetRequiredService<ILatticeBackupControl>();

    /// <summary>The silo-side backup catalog store.</summary>
    public ILatticeBackupCatalogStore Catalog =>
        SiloServices.GetRequiredService<ILatticeBackupCatalogStore>();

    /// <summary>The silo-side default in-cluster backup sink.</summary>
    public ILatticeBackupSink Sink => SiloServices.GetRequiredService<ILatticeBackupSink>();

    /// <summary>The silo-side on-demand backup scheduler facade.</summary>
    public ILatticeBackupScheduler Scheduler =>
        SiloServices.GetRequiredService<ILatticeBackupScheduler>();

    /// <summary>Deploys the single-silo cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>
    /// Builds a control facade wired to the fixture's live engine seams but over
    /// the supplied authorizer, so a denying gate can drive the fail-closed path.
    /// </summary>
    internal ILatticeBackupControl CreateControlWith(BackupAccessAuthorizer authorizer) =>
        new LatticeBackupControl(
            SiloServices.GetRequiredService<ILatticeBackupCaptureService>(),
            SiloServices.GetRequiredService<ILatticeBackupIncrementalCaptureService>(),
            Catalog,
            SiloServices.GetRequiredService<ILatticeBackupCatalogRebuildService>(),
            SiloServices.GetRequiredService<ILatticeBackupCatalogScrubService>(),
            Sink,
            SiloServices.GetRequiredService<ILatticeBackupRestoreService>(),
            SiloServices.GetRequiredService<ILatticeBackupHealthService>(),
            SiloServices.GetRequiredService<ILatticeBackupHealthStore>(),
            authorizer,
            SiloServices.GetRequiredService<IGrainFactory>(),
            SiloServices.GetRequiredService<BackupInventoryRegistry>(),
            Options.Create(new LatticeApiBackupOptions()),
            SiloServices);

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
            siloBuilder.AddLatticeBackupApi();
        }
    }
}

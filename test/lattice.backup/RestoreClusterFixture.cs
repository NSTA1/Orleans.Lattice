using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice and the
/// backup add-on, exposing the live capture and restore services, the sink, the
/// catalog, and the silo-side serializer so a backup can be captured and then
/// restored end to end. Also exposes a factory that builds a restore service over
/// a caller-supplied <see cref="BackupAccessAuthorizer"/> so the fail-closed
/// permission path can be driven with a denying gate.
/// </summary>
public sealed class RestoreClusterFixture
{
    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The client-side grain factory used to seed and read trees.</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>The silo-side capture service, used to author the backups under restore.</summary>
    public ILatticeBackupCaptureService Capture =>
        SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

    /// <summary>The silo-side incremental-capture service, used to author increment chains under restore.</summary>
    public ILatticeBackupIncrementalCaptureService Incremental =>
        SiloServices.GetRequiredService<ILatticeBackupIncrementalCaptureService>();

    /// <summary>The silo-side restore service under test.</summary>
    public ILatticeBackupRestoreService Restore =>
        SiloServices.GetRequiredService<ILatticeBackupRestoreService>();

    /// <summary>The silo-side cold, catalog-free disaster-restore service under test.</summary>
    public ILatticeBackupColdRestoreService ColdRestore =>
        SiloServices.GetRequiredService<ILatticeBackupColdRestoreService>();

    /// <summary>The silo-side backup catalog store.</summary>
    public ILatticeBackupCatalogStore Catalog =>
        SiloServices.GetRequiredService<ILatticeBackupCatalogStore>();

    /// <summary>The silo-side default in-cluster backup sink.</summary>
    public ILatticeBackupSink Sink => SiloServices.GetRequiredService<ILatticeBackupSink>();

    /// <summary>The silo-side Orleans serializer, used to decode stored artifact chunks.</summary>
    public Serializer Serializer => SiloServices.GetRequiredService<Serializer>();

    /// <summary>Deploys the single-silo cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>
    /// Builds a restore service wired to the fixture's live sink / catalog /
    /// serializer / grain factory but over the supplied authorizer, so a denying
    /// gate can drive the fail-closed path.
    /// </summary>
    internal ILatticeBackupRestoreService CreateRestoreServiceWith(BackupAccessAuthorizer authorizer) =>
        new LatticeBackupRestoreService(
            GrainFactory,
            Sink,
            Catalog,
            authorizer,
            Serializer,
            SiloServices.GetRequiredService<ITagIndexReconcileTrigger>(),
            SiloServices,
            SiloServices.GetRequiredService<ILoggerFactory>().CreateLogger<LatticeBackupRestoreService>());

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

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Wal;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice and the
/// backup add-on, exposing the live capture service, the sink, the catalog, and
/// the silo-side serializer so a capture can be driven end to end and its stored
/// artifact decoded back to raw entries. The per-shard snapshot replay budget is
/// configurable so the fail-fast size gate can be exercised.
/// </summary>
public sealed class CaptureClusterFixture
{
    private static long s_maxSnapshotReplayEntries = 10_000_000L;

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The client-side grain factory used to seed and read trees.</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>The silo-side capture service under test.</summary>
    public ILatticeBackupCaptureService Capture =>
        SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

    /// <summary>The silo-side incremental-capture service under test.</summary>
    public ILatticeBackupIncrementalCaptureService Incremental =>
        SiloServices.GetRequiredService<ILatticeBackupIncrementalCaptureService>();

    /// <summary>The silo-side backup catalog store.</summary>
    public ILatticeBackupCatalogStore Catalog =>
        SiloServices.GetRequiredService<ILatticeBackupCatalogStore>();

    /// <summary>The silo-side default in-cluster backup sink.</summary>
    public ILatticeBackupSink Sink => SiloServices.GetRequiredService<ILatticeBackupSink>();

    /// <summary>The silo-side Orleans serializer, used to decode stored artifact chunks.</summary>
    public Serializer Serializer => SiloServices.GetRequiredService<Serializer>();

    /// <summary>The local cluster id the capture engine stamps onto every manifest.</summary>
    public string LocalClusterId =>
        SiloServices.GetRequiredService<IOptions<Orleans.Configuration.ClusterOptions>>().Value.ClusterId;

    /// <summary>Deploys the cluster with the given per-shard snapshot replay budget.</summary>
    public async Task InitializeAsync(long maxSnapshotReplayEntries = 10_000_000L)
    {
        s_maxSnapshotReplayEntries = maxSnapshotReplayEntries;
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
            siloBuilder.ConfigureLattice(o => o.MaxSnapshotReplayEntries = s_maxSnapshotReplayEntries);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeBackup(o =>
            {
                // Generous fence budget so a brief concurrent cross-tree write
                // never exhausts the retry/drain allowance in CI.
                o.MaxCrossTreeFenceAttempts = 50;
                o.CrossTreeFenceDrainTimeout = TimeSpan.FromSeconds(15);
                o.CrossTreeFencePollInterval = TimeSpan.FromMilliseconds(10);
            });
        }
    }

    /// <summary>
    /// Builds a capture service wired to the fixture's live silo services but over
    /// the supplied authorizer, so a denying gate can drive the fail-closed
    /// capture path.
    /// </summary>
    internal ILatticeBackupCaptureService CreateCaptureServiceWith(BackupAccessAuthorizer authorizer) =>
        new LatticeBackupCaptureService(
            SiloServices.GetRequiredService<IGrainFactory>(),
            Sink,
            Catalog,
            authorizer,
            SiloServices.GetRequiredService<IOptionsMonitor<LatticeOptions>>(),
            SiloServices.GetRequiredService<IOptions<LatticeBackupOptions>>(),
            SiloServices.GetRequiredService<ILatticeMergeModeResolver>(),
            Serializer,
            SiloServices.GetRequiredService<ICommitLogReader>(),
            SiloServices.GetRequiredService<IWalSubscriber>(),
            SiloServices.GetRequiredService<LatticeOptionsResolver>(),
            SiloServices.GetRequiredService<IWalCursorRegistry>(),
            SiloServices.GetRequiredService<IOptions<Orleans.Configuration.ClusterOptions>>(),
            SiloServices.GetRequiredService<ILoggerFactory>().CreateLogger<LatticeBackupCaptureService>());
}

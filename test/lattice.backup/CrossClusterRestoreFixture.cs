using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A two-cluster disaster-recovery harness. It stands up two genuinely
/// independent single-silo <see cref="TestCluster"/> instances - a capturing
/// cluster A and a fresh restoring cluster B - whose <b>only</b> shared state is a
/// single <see cref="SharedBackupStore"/> handed to both through a shared
/// <see cref="SharedInMemoryBackupSink"/>. Each cluster has its own in-memory
/// grain storage and a distinct cluster id, so B's reserved
/// <c>sys-backup-catalog</c> and <c>sys-backup-*</c> trees start empty and must be
/// rebuilt from the sink during a cold restore.
/// <para>
/// The framework instantiates the silo configurator and it cannot take
/// constructor arguments, so the shared store is handed to it through a static
/// registry keyed by each cluster's id (both ids map to the one store instance),
/// mirroring how the replication multi-site fixture routes per-cluster state via
/// <see cref="ClusterOptions.ClusterId"/>.
/// </para>
/// </summary>
internal sealed class CrossClusterRestoreFixture
{
    private static readonly ConcurrentDictionary<string, SharedBackupStore> Stores = new(StringComparer.Ordinal);

    private readonly string _clusterIdA = "dr-a-" + Guid.NewGuid().ToString("N");
    private readonly string _clusterIdB = "dr-b-" + Guid.NewGuid().ToString("N");

    /// <summary>The shared payload store both clusters read and write - the sole shared state.</summary>
    public SharedBackupStore Store { get; } = new();

    /// <summary>The capturing cluster (source of the backups under restore).</summary>
    public TestCluster ClusterA { get; private set; } = null!;

    /// <summary>The fresh restoring cluster (independent grain storage, empty catalog).</summary>
    public TestCluster ClusterB { get; private set; } = null!;

    /// <summary>Cluster A's client-side grain factory, used to seed the source tree.</summary>
    public IGrainFactory GrainFactoryA => ClusterA.GrainFactory;

    /// <summary>Cluster B's client-side grain factory, used to read the restored tree.</summary>
    public IGrainFactory GrainFactoryB => ClusterB.GrainFactory;

    /// <summary>Cluster A's silo-side capture service, used to author the backups under restore.</summary>
    public ILatticeBackupCaptureService CaptureA =>
        ServicesOf(ClusterA).GetRequiredService<ILatticeBackupCaptureService>();

    /// <summary>Cluster A's silo-side incremental-capture service, used to author increment chains.</summary>
    public ILatticeBackupIncrementalCaptureService IncrementalA =>
        ServicesOf(ClusterA).GetRequiredService<ILatticeBackupIncrementalCaptureService>();

    /// <summary>Cluster A's silo-side Orleans serializer, used to decode source artifacts.</summary>
    public Serializer SerializerA => ServicesOf(ClusterA).GetRequiredService<Serializer>();

    /// <summary>Cluster A's view over the shared sink.</summary>
    public ILatticeBackupSink SinkA => ServicesOf(ClusterA).GetRequiredService<ILatticeBackupSink>();

    /// <summary>Cluster B's silo-side capture service, used to re-capture the restored tree for causal inspection.</summary>
    public ILatticeBackupCaptureService CaptureB =>
        ServicesOf(ClusterB).GetRequiredService<ILatticeBackupCaptureService>();

    /// <summary>Cluster B's silo-side cold, catalog-free disaster-restore service under test.</summary>
    public ILatticeBackupColdRestoreService ColdRestoreB =>
        ServicesOf(ClusterB).GetRequiredService<ILatticeBackupColdRestoreService>();

    /// <summary>Cluster B's silo-side backup catalog store, expected to start empty.</summary>
    public ILatticeBackupCatalogStore CatalogB =>
        ServicesOf(ClusterB).GetRequiredService<ILatticeBackupCatalogStore>();

    /// <summary>Cluster B's silo-side Orleans serializer, used to decode restored artifacts.</summary>
    public Serializer SerializerB => ServicesOf(ClusterB).GetRequiredService<Serializer>();

    /// <summary>Cluster B's view over the shared sink.</summary>
    public ILatticeBackupSink SinkB => ServicesOf(ClusterB).GetRequiredService<ILatticeBackupSink>();

    /// <summary>Stands up both independent clusters over the one shared sink store.</summary>
    public async Task InitializeAsync()
    {
        // Both distinct cluster ids resolve to the same store instance, so the two
        // otherwise-independent clusters share the sink payload and nothing else.
        Stores[_clusterIdA] = Store;
        Stores[_clusterIdB] = Store;

        ClusterA = await BuildClusterAsync(_clusterIdA);
        ClusterB = await BuildClusterAsync(_clusterIdB);
    }

    /// <summary>Stops and disposes both clusters and drops the shared-store registrations.</summary>
    public async Task DisposeAsync()
    {
        if (ClusterA is not null)
        {
            await ClusterA.StopAllSilosAsync();
            await ClusterA.DisposeAsync();
        }

        if (ClusterB is not null)
        {
            await ClusterB.StopAllSilosAsync();
            await ClusterB.DisposeAsync();
        }

        Stores.TryRemove(_clusterIdA, out _);
        Stores.TryRemove(_clusterIdB, out _);
    }

    private static async Task<TestCluster> BuildClusterAsync(string clusterId)
    {
        var builder = new TestClusterBuilder(1);
        builder.Options.ClusterId = clusterId;
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static IServiceProvider ServicesOf(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();

            // Register the shared sink BEFORE AddLatticeBackup so its TryAdd keeps
            // ours: the store is resolved by this cluster's id, and both cluster
            // ids map to the one shared store, so both clusters read and write the
            // same sink payload - the only state a cross-cluster restore may depend
            // on.
            siloBuilder.Services.AddSingleton<ILatticeBackupSink>(sp =>
            {
                var clusterId = sp.GetRequiredService<IOptions<ClusterOptions>>().Value.ClusterId;
                return new SharedInMemoryBackupSink(Stores[clusterId], sp.GetRequiredService<Serializer>());
            });

            siloBuilder.AddLatticeBackup();
        }
    }
}

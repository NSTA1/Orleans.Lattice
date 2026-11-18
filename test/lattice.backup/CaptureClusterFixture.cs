using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Wal;
using Orleans.Runtime;
using Orleans.Runtime.Hosting;
using Orleans.Storage;
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
    private static string? s_localOriginClusterId;
    private static long s_pinnedEntryBudget = 1_000_000;

    /// <summary>
    /// When set, a grain-state write whose state name and state the predicate
    /// matches throws, so a test can fault one specific write (for example the
    /// one a set schedule's retention makes) without faulting the others. Reset by
    /// <see cref="InitializeAsync"/>.
    /// </summary>
    internal static Func<string, object?, bool>? StateWriteFault { get; set; }

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

    /// <summary>
    /// Deploys the cluster with the given per-shard snapshot replay budget. A
    /// non-null <paramref name="localOriginClusterId"/> replaces the core's empty
    /// origin resolver with one that returns that id for every tree, so own
    /// writes reach the WAL stamped with it, as on a replicated host.
    /// </summary>
    public async Task InitializeAsync(
        long maxSnapshotReplayEntries = 10_000_000L,
        string? localOriginClusterId = null,
        long pinnedEntryBudget = 1_000_000)
    {
        s_maxSnapshotReplayEntries = maxSnapshotReplayEntries;
        s_pinnedEntryBudget = pinnedEntryBudget;
        s_localOriginClusterId = localOriginClusterId;
        StateWriteFault = null;
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
            siloBuilder.AddLattice((silo, name) =>
            {
                var inner = name + ".inner";
                silo.AddMemoryGrainStorage(inner);
                silo.Services.AddGrainStorage(name, (sp, _) =>
                    new FaultableGrainStorage(sp.GetRequiredKeyedService<IGrainStorage>(inner)));
            });
            siloBuilder.ConfigureLattice(o => o.MaxSnapshotReplayEntries = s_maxSnapshotReplayEntries);
            if (s_localOriginClusterId is { } localOrigin)
            {
                siloBuilder.Services.AddSingleton<ILatticeOriginClusterIdResolver>(new FixedOriginClusterIdResolver(localOrigin));
            }

            siloBuilder.UseInMemoryReminderService();

            siloBuilder.AddLatticeBackup(o =>
            {
                // Generous fence budget so a brief concurrent cross-tree write
                // never exhausts the retry/drain allowance in CI.
                o.MaxCrossTreeFenceAttempts = 50;
                o.CrossTreeFenceDrainTimeout = TimeSpan.FromSeconds(15);
                o.CrossTreeFencePollInterval = TimeSpan.FromMilliseconds(10);
                o.CrossTreeSetPinnedEntryBudget = s_pinnedEntryBudget;
                o.SetScheduleFenceRetryDelay = TimeSpan.Zero;
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

    private sealed class FaultableGrainStorage(IGrainStorage inner) : IGrainStorage
    {
        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ReadStateAsync(stateName, grainId, grainState);

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            StateWriteFault?.Invoke(stateName, grainState.State) == true
                ? throw new IOException($"Injected fault writing '{stateName}'.")
                : inner.WriteStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ClearStateAsync(stateName, grainId, grainState);
    }

    private sealed class FixedOriginClusterIdResolver(string originClusterId) : ILatticeOriginClusterIdResolver
    {
        public string Resolve(string treeId) => originClusterId;
    }
}

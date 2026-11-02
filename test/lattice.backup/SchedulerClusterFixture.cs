using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice and the
/// backup add-on and configured for scheduling / retention, exposing the
/// per-scope scheduler grain, the catalog, the sink, and (optionally) a gated
/// capture double so the overlap guard can be exercised deterministically.
/// </summary>
public sealed class SchedulerClusterFixture
{
    private static Action<LatticeBackupScheduleOptions>? s_configure;
    private static bool s_gated;
    private static bool s_intercepted;
    private static Action<LatticeBackupOptions>? s_configureBackup;
    private static bool s_recordGate;
    private static TimeSpan? s_responseTimeout;

    /// <summary>
    /// A mutable toggle a test can flip to change the incremental-schedule setting
    /// the schedule-options configurator reads, so an options change can be
    /// simulated within one deployment by flipping this and calling
    /// <see cref="ClearOptionsCache"/>.
    /// </summary>
    public static bool IncrementalScheduleEnabled { get; set; }

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The client-side grain factory used to seed trees and resolve grains.</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>The silo-side backup catalog store.</summary>
    public ILatticeBackupCatalogStore Catalog =>
        SiloServices.GetRequiredService<ILatticeBackupCatalogStore>();

    /// <summary>The silo-side default in-cluster backup sink.</summary>
    public ILatticeBackupSink Sink => SiloServices.GetRequiredService<ILatticeBackupSink>();

    /// <summary>The silo-side Orleans serializer.</summary>
    public Serializer Serializer => SiloServices.GetRequiredService<Serializer>();

    /// <summary>The gated capture double (only meaningful when the fixture was initialized gated).</summary>
    internal GatedBackupCaptureService Gate =>
        (GatedBackupCaptureService)SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

    /// <summary>Resolves the per-scope scheduler grain for <paramref name="scope"/>.</summary>
    internal ILatticeBackupSchedulerGrain Scheduler(BackupScopeSelector scope) =>
        GrainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));

    /// <summary>The silo-side full capture service.</summary>
    public ILatticeBackupCaptureService CaptureService =>
        SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

    /// <summary>The silo-side incremental capture service.</summary>
    public ILatticeBackupIncrementalCaptureService IncrementalService =>
        SiloServices.GetRequiredService<ILatticeBackupIncrementalCaptureService>();

    /// <summary>
    /// Deploys the cluster with the given schedule / retention configuration.
    /// When <paramref name="intercepted"/> is set, the sink is an
    /// <see cref="InterceptingBackupSink"/> over the real in-cluster sink.
    /// <paramref name="configureBackup"/> adjusts the backup options, and
    /// <paramref name="recordGate"/> fronts the publication gate with a
    /// <see cref="RecordingPublicationGate"/>. <paramref name="responseTimeout"/>
    /// shortens the silo's grain response timeout.
    /// </summary>
    public async Task InitializeAsync(
        Action<LatticeBackupScheduleOptions>? configure = null,
        bool gated = false,
        bool intercepted = false,
        Action<LatticeBackupOptions>? configureBackup = null,
        bool recordGate = false,
        TimeSpan? responseTimeout = null)
    {
        s_configure = configure;
        s_gated = gated;
        s_intercepted = intercepted;
        s_configureBackup = configureBackup;
        s_recordGate = recordGate;
        s_responseTimeout = responseTimeout;
        InterceptingBackupSink.Reset();
        RecordingPublicationGate.Reset();
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

    /// <summary>
    /// Clears the silo-side options-monitor cache for
    /// <see cref="LatticeBackupScheduleOptions"/> so the next resolution re-runs the
    /// configurator and picks up a flipped <see cref="IncrementalScheduleEnabled"/>.
    /// </summary>
    public void ClearOptionsCache() =>
        SiloServices.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitorCache<LatticeBackupScheduleOptions>>()
            .Clear();

    /// <summary>Counts the manifests the catalog holds for <paramref name="scope"/>.</summary>
    public async Task<List<BackupManifest>> ListScopeAsync(BackupScopeSelector scope)    {
        var manifests = new List<BackupManifest>();
        await foreach (var manifest in Catalog.ListAsync())
        {
            if (manifest.Scope.Kind == scope.Kind
                && manifest.Scope.TreeId == scope.TreeId
                && manifest.Scope.KeyOrPrefix == scope.KeyOrPrefix)
            {
                manifests.Add(manifest);
            }
        }

        return manifests;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeBackup();

            if (s_configure is not null)
            {
                siloBuilder.ConfigureLatticeBackupSchedule(s_configure);
            }

            if (s_intercepted)
            {
                siloBuilder.Services.AddSingleton<InClusterLatticeBackupSink>();
                siloBuilder.Services.Replace(
                    ServiceDescriptor.Singleton<ILatticeBackupSink, InterceptingBackupSink>());
            }

            if (s_responseTimeout is { } timeout)
            {
                siloBuilder.Services.Configure<Orleans.Configuration.SiloMessagingOptions>(o => o.ResponseTimeout = timeout);
            }

            if (s_configureBackup is not null)
            {
                siloBuilder.Services.Configure(s_configureBackup);
            }

            if (s_recordGate)
            {
                siloBuilder.Services.Replace(ServiceDescriptor.Singleton<IBackupPublicationGate>(sp =>
                    new RecordingPublicationGate(new SchedulerBackupPublicationGate(
                        sp.GetRequiredService<IGrainFactory>(),
                        sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<LatticeBackupOptions>>(),
                        sp.GetRequiredService<BackupPublisher>()))));
            }

            if (s_gated)
            {
                // Front the real capture engine with a gate so a capture can be
                // held mid-flight while a second, overlapping trigger is issued.
                siloBuilder.Services.AddSingleton<LatticeBackupCaptureService>();
                siloBuilder.Services.Replace(
                    ServiceDescriptor.Singleton<ILatticeBackupCaptureService, GatedBackupCaptureService>());
            }
        }
    }
}

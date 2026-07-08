using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// replication add-on, and the backup add-on, so the coordinated-restore path can
/// be driven end to end over the <b>real</b> restore engine, the <b>real</b>
/// durable <see cref="ISagaWriteFenceGrain"/>, and the <b>real</b>
/// <see cref="RestoreParticipant"/> - not fakes. Two logical tree ids stand in for
/// two replicating clusters' copies of the same tree, so the union re-advance
/// defect (#1169) can be reproduced in-process without a cross-cluster shipping
/// transport (which the replication test project does not host).
/// <para>
/// The <see cref="ISagaCompletionSource"/> is replaced with a controllable
/// <see cref="FakeSagaCompletionSource"/> so a test can hold global completion
/// (modelling a laggard participant) and then release it, exercising the fence's
/// globally-gated shipping resume (#1173) deterministically. The fixture is not
/// safe to initialize concurrently with another instance; the coordinated-restore
/// integration tests run sequentially, which is the NUnit default.
/// </para>
/// </summary>
internal sealed class CoordinatedRestoreClusterFixture
{
    /// <summary>Cluster id assigned to the single silo.</summary>
    public const string ClusterId = "site-home";

    /// <summary>
    /// The completion source the next silo build should adopt. Set immediately
    /// before <see cref="TestClusterBuilder.Build"/> and read by the in-process
    /// silo configurator; safe because fixtures initialize one at a time.
    /// </summary>
    private static FakeSagaCompletionSource? _pendingCompletion;

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The client-side grain factory used to seed and read trees.</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>The silo-side backup capture service, used to author the backups under restore.</summary>
    public ILatticeBackupCaptureService Capture =>
        SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

    /// <summary>The silo-side coordinated restore engine (build / commit / delete / probe).</summary>
    public ILatticeCoordinatedRestoreEngine Engine =>
        SiloServices.GetRequiredService<ILatticeCoordinatedRestoreEngine>();

    /// <summary>The silo-side real restore participant that maps the engine onto the saga.</summary>
    public RestoreParticipant Participant =>
        SiloServices.GetRequiredService<RestoreParticipant>();

    /// <summary>The controllable global-completion source gating the fence's shipping resume.</summary>
    public FakeSagaCompletionSource Completion { get; private set; } = null!;

    /// <summary>Gets the durable write-fence grain for <paramref name="sagaId"/>.</summary>
    public ISagaWriteFenceGrain Fence(string sagaId) =>
        GrainFactory.GetGrain<ISagaWriteFenceGrain>(sagaId);

    /// <summary>Deploys the single-silo cluster.</summary>
    public async Task InitializeAsync()
    {
        Completion = new FakeSagaCompletionSource();
        _pendingCompletion = Completion;

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
            var completion = _pendingCompletion
                ?? throw new InvalidOperationException("No pending completion source was staged for the silo build.");

            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();

            // Register the controllable completion source BEFORE AddLatticeReplication,
            // so the package's TryAddSingleton default (the coordinator-dialling source)
            // does not replace it. This lets a test hold and release global completion.
            siloBuilder.Services.AddSingleton<ISagaCompletionSource>(completion);

            // The saga control channel is normally supplied by the gRPC package; this
            // test drives the restore participants directly (never the dispatcher), so a
            // no-op stub satisfies the RestoreSagaDispatcher's constructor for container
            // validation without standing up a real cross-cluster transport.
            siloBuilder.Services.AddSingleton<ISagaControlChannel, UnusedSagaControlChannel>();

            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            siloBuilder.AddLatticeBackup();

            // Ad-hoc tree ids: opt every tree in to LwwRegister so the merge-mode
            // resolver does not need each id enumerated on the configurator.
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// Inert <see cref="ISagaControlChannel"/> that satisfies the dispatcher's
    /// constructor for DI validation. This fixture never invokes the dispatcher (the
    /// tests drive the restore participants directly), so no method is ever called.
    /// </summary>
    private sealed class UnusedSagaControlChannel : ISagaControlChannel
    {
        public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("The coordinated-restore fixture does not use the saga control channel.");

        public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("The coordinated-restore fixture does not use the saga control channel.");

        public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("The coordinated-restore fixture does not use the saga control channel.");

        public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("The coordinated-restore fixture does not use the saga control channel.");
    }
}

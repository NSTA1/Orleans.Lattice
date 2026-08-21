using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
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

    /// <summary>
    /// The loopback control channel the next silo build should adopt, or
    /// <c>null</c> to keep the inert <see cref="UnusedSagaControlChannel"/>. Staged
    /// like <see cref="_pendingCompletion"/>; safe because fixtures initialize one
    /// at a time.
    /// </summary>
    private static LoopbackSagaControlChannel? _pendingChannel;

    /// <summary>The member trees the next silo build reports as replicated to the dispatcher.</summary>
    private static IReadOnlyCollection<string>? _pendingReplicatedTrees;

    /// <summary>When set, the next silo build wires a capacity probe that refuses every target.</summary>
    private static bool _pendingRefuseCapacity;

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

    /// <summary>
    /// The in-process loopback saga control channel wired when the fixture is
    /// initialized to drive a restore through the real dispatcher and coordinator
    /// grain (see the <c>driveThroughDispatcher</c> parameter of
    /// <see cref="InitializeAsync"/>). Records every control request routed to the
    /// local participant grain so a test can assert the saga carried the set id.
    /// <c>null</c> when the fixture drives the participant directly.
    /// </summary>
    public LoopbackSagaControlChannel? ControlChannel { get; private set; }

    /// <summary>The silo-side public backup restore service (the set-restore entry point).</summary>
    public Backup.ILatticeBackupRestoreService RestoreService =>
        SiloServices.GetRequiredService<Backup.ILatticeBackupRestoreService>();

    /// <summary>Gets the durable write-fence grain for <paramref name="sagaId"/>.</summary>
    public ISagaWriteFenceGrain Fence(string sagaId) =>
        GrainFactory.GetGrain<ISagaWriteFenceGrain>(sagaId);

    /// <summary>Deploys the single-silo cluster.</summary>
    /// <param name="driveThroughDispatcher">
    /// When <c>true</c>, wires the in-process <see cref="LoopbackSagaControlChannel"/>
    /// (exposed on <see cref="ControlChannel"/>) so a restore can be driven through the
    /// real dispatcher and coordinator grain to the local participant grain, and
    /// overrides the replicated-tree membership seam so the dispatcher promotes
    /// <paramref name="replicatedTrees"/> to a saga. When <c>false</c> (the default),
    /// keeps the inert control channel and drives the participant directly.
    /// </param>
    /// <param name="replicatedTrees">
    /// The member trees the membership seam reports replicated to the dispatcher when
    /// <paramref name="driveThroughDispatcher"/> is <c>true</c>. Ignored otherwise.
    /// </param>
    /// <param name="refuseCapacity">
    /// When <c>true</c>, wires a capacity probe that refuses every target so a
    /// dispatcher-driven set restore votes abort and the coordinated abort path can be
    /// exercised end to end.
    /// </param>
    public async Task InitializeAsync(
        bool driveThroughDispatcher = false,
        IReadOnlyCollection<string>? replicatedTrees = null,
        bool refuseCapacity = false)
    {
        Completion = new FakeSagaCompletionSource();
        _pendingCompletion = Completion;

        if (driveThroughDispatcher)
        {
            ControlChannel = new LoopbackSagaControlChannel();
            _pendingChannel = ControlChannel;
            _pendingReplicatedTrees = replicatedTrees ?? Array.Empty<string>();
            _pendingRefuseCapacity = refuseCapacity;
        }
        else
        {
            ControlChannel = null;
            _pendingChannel = null;
            _pendingReplicatedTrees = null;
            _pendingRefuseCapacity = false;
        }

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
            var channel = _pendingChannel;
            var replicated = _pendingReplicatedTrees;
            var refuseCapacity = _pendingRefuseCapacity;

            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();

            // Register the controllable completion source BEFORE AddLatticeReplication,
            // so the package's TryAddSingleton default (the coordinator-dialling source)
            // does not replace it. This lets a test hold and release global completion.
            siloBuilder.Services.AddSingleton<ISagaCompletionSource>(completion);

            if (channel is null)
            {
                // The saga control channel is normally supplied by the gRPC package; a
                // participant-direct test drives the restore participants directly (never
                // the dispatcher), so a no-op stub satisfies the RestoreSagaDispatcher's
                // constructor for container validation without a real transport.
                siloBuilder.Services.AddSingleton<ISagaControlChannel, UnusedSagaControlChannel>();
            }
            else
            {
                // A dispatcher-driven test loops every control request back to the local
                // per-saga participant grain, so the real dispatcher and real coordinator
                // grain drive the real participant in-process without a cross-cluster
                // transport. Bound to the silo grain factory at resolution time.
                siloBuilder.Services.AddSingleton<ISagaControlChannel>(sp =>
                {
                    channel.Bind(sp.GetRequiredService<IGrainFactory>());
                    return channel;
                });
            }

            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            siloBuilder.AddLatticeBackup();

            // Ad-hoc data-tree ids: opt every non-view tree in to LwwRegister so the
            // resolver does not need each id enumerated on the configurator. Internal
            // backup views remain local, matching DeriveLocally topology.
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowDataTreesLwwRegisterResolver>();

            if (channel is not null)
            {
                // Dispatcher-driven set tests only. Override the membership seam AFTER the
                // add-ons (AddSingleton, last wins) so the dispatcher promotes the configured
                // member trees to a saga. The seam reports nothing to the backup shared-sink
                // startup guard (its ReplicatedTrees is empty): the single-silo harness has no
                // external cross-cluster sink to validate, so the guard stays a no-op while
                // IsReplicated still drives the dispatch decision.
                siloBuilder.Services.AddSingleton<Backup.IReplicatedTreeMembership>(
                    new FixedReplicatedTreeMembership(replicated ?? Array.Empty<string>()));

                if (refuseCapacity)
                {
                    siloBuilder.Services.AddSingleton<IRestoreCapacityProbe, RefusingRestoreCapacityProbe>();
                }
            }
        }
    }

    private sealed class AllowDataTreesLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith(LatticeConstants.ViewTreePrefix, StringComparison.Ordinal)
                ? null
                : LatticeMergeMode.LwwRegister;
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

    /// <summary>
    /// In-process <see cref="ISagaControlChannel"/> that loops every RPC back to the
    /// local per-saga <see cref="ICrossClusterSagaParticipantGrain"/> (keyed by the
    /// request's saga id), so the real dispatcher and real coordinator grain drive the
    /// real participant in-process. Records every prepare / commit / abort request so a
    /// test can assert the saga carried the set id on the wire.
    /// </summary>
    internal sealed class LoopbackSagaControlChannel : ISagaControlChannel
    {
        private readonly object _gate = new();
        private readonly List<SagaControlRequest> _prepared = [];
        private readonly List<SagaControlRequest> _committed = [];
        private readonly List<SagaControlRequest> _aborted = [];
        private IGrainFactory? _grainFactory;

        /// <summary>The prepare requests routed to the local participant, in order.</summary>
        public IReadOnlyList<SagaControlRequest> Prepared
        {
            get { lock (_gate) { return [.. _prepared]; } }
        }

        /// <summary>The commit requests routed to the local participant, in order.</summary>
        public IReadOnlyList<SagaControlRequest> Committed
        {
            get { lock (_gate) { return [.. _committed]; } }
        }

        /// <summary>The abort requests routed to the local participant, in order.</summary>
        public IReadOnlyList<SagaControlRequest> Aborted
        {
            get { lock (_gate) { return [.. _aborted]; } }
        }

        /// <summary>Binds the silo grain factory used to resolve the participant grain.</summary>
        public void Bind(IGrainFactory grainFactory) => _grainFactory = grainFactory;

        private ICrossClusterSagaParticipantGrain Participant(SagaControlRequest request) =>
            (_grainFactory ?? throw new InvalidOperationException("The loopback control channel was not bound to a grain factory."))
                .GetGrain<ICrossClusterSagaParticipantGrain>(request.SagaId);

        /// <inheritdoc />
        public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            lock (_gate) { _prepared.Add(request); }
            return Participant(request).PrepareAsync(request);
        }

        /// <inheritdoc />
        public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            lock (_gate) { _committed.Add(request); }
            return Participant(request).CommitAsync(request);
        }

        /// <inheritdoc />
        public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            lock (_gate) { _aborted.Add(request); }
            return Participant(request).AbortAsync(request);
        }

        /// <inheritdoc />
        public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            Participant(request).GetStatusAsync(request);
    }

    /// <summary>
    /// Test <see cref="Backup.IReplicatedTreeMembership"/> that reports a fixed set of
    /// member trees as replicated to the dispatcher's dispatch decision, while reporting
    /// an empty replicated set to the backup shared-sink startup guard. The single-silo
    /// harness hosts every tree on the sole (coordinator) silo and has no external
    /// cross-cluster sink, so the guard has nothing to validate; keeping its view empty
    /// lets the in-cluster sink stay the default while the dispatcher still promotes the
    /// configured members to a saga.
    /// </summary>
    private sealed class FixedReplicatedTreeMembership(IReadOnlyCollection<string> replicated)
        : Backup.IReplicatedTreeMembership
    {
        private readonly HashSet<string> _replicated = new(replicated, StringComparer.Ordinal);

        public IReadOnlyCollection<string> ReplicatedTrees => Array.Empty<string>();

        public bool IsReplicated(string treeId)
        {
            ArgumentNullException.ThrowIfNull(treeId);
            return _replicated.Contains(treeId);
        }
    }

    /// <summary>
    /// Capacity probe that refuses every target, so a dispatcher-driven set restore
    /// votes abort at prepare and the coordinated compensation path runs end to end.
    /// </summary>
    private sealed class RefusingRestoreCapacityProbe : IRestoreCapacityProbe
    {
        public Task<bool> CanHostAsync(Backup.RestoreAdmissionReport report, CancellationToken cancellationToken = default) =>
            Task.FromResult(false);
    }
}

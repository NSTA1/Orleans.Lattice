using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="IHostedService"/> that activates one
/// <see cref="IReplicationShipperGrain"/> per
/// <c>(replicated tree, peer cluster)</c> pair and one
/// <see cref="IReplicationMaintenanceGrain"/> per replicated tree on
/// silo startup. Orleans' single-activation guarantee per grain key
/// means the activation calls are cluster-singleton even though every
/// silo in the cluster runs this hosted service: the second activation
/// request for an already-active grain simply routes to the existing
/// activation. Once each grain's keepalive reminder is registered (via
/// <see cref="IReplicationShipperGrain.EnsureActiveAsync"/> /
/// <see cref="IReplicationMaintenanceGrain.EnsureActiveAsync"/>) Orleans
/// keeps the activation alive even after the activating silo shuts
/// down.
/// <para>
/// The replicated-tree set is sourced from
/// <see cref="IReplicatedTreeMembership"/> rather than read directly
/// from <see cref="LatticeReplicationOptions.ReplicatedTrees"/>: on a
/// host that opted into runtime replication configuration that seam is
/// the snapshot-backed union of the static options map <b>and</b> the
/// trees enabled at runtime through the config CRDT, so a tree enabled
/// via <c>lattice_replication_enable</c> is enrolled here too. When the
/// runtime config snapshot maintainer is wired, a lightweight poll keyed
/// on <see cref="CompiledReplicationConfigSnapshotMaintainer.CurrentEpoch"/>
/// enrols the shipper, maintenance, and (opt-in) digest-probe grains for
/// any tree enabled after startup, without restarting the silo. Enrolment
/// is additive only: disabling a tree at runtime does not tear the grains
/// down (the shipper stops shipping on its own because the merge-mode
/// resolver returns null once the tree is disabled), mirroring the
/// peer-removed policy below.
/// </para>
/// <para>
/// Peer membership is sourced from <see cref="IReplicationTopology"/>
/// rather than read once from
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/>: the
/// initial snapshot drives the startup activation pass, and a
/// long-lived <see cref="IReplicationTopology.Subscribe"/> subscription
/// activates one shipper per <b>currently enrolled</b> replicated tree
/// for every peer added at runtime. <see cref="PeerChangeKind.Removed"/>
/// events do not trigger any teardown - the shipper grain stays
/// activated to drain its remaining backlog, and the producer-side
/// doorbell ring stops firing for the removed peer automatically because
/// <c>ShardedReplogSink</c> reads
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/> per WAL
/// append.
/// </para>
/// <para>
/// <see cref="IHostedService.StartAsync"/> ordering is not guaranteed
/// across hosted services: the host queues every registered service's
/// <see cref="BackgroundService.ExecuteAsync"/> on a tracked task in
/// registration order but does not await them. The Orleans silo is
/// itself an <see cref="IHostedService"/>, so the first attempts to
/// activate grains here can race ahead of the silo becoming
/// dispatch-ready and fail with transient errors (cluster client not
/// connected, no active silo for grain placement, etc.). To make the
/// driver activation reliable rather than flaky, this service uses a
/// retry-with-backoff loop: every grain that fails to activate is
/// kept in the pending set and retried on the next pass, with
/// exponential backoff between passes (250ms initial, doubling, 30s
/// cap, reset on any per-item success). The loop only exits when the
/// pending set is empty or <c>stoppingToken</c> is
/// cancelled.
/// </para>
/// <para>
/// The <see cref="ReplicationPeerStats"/> dependency is taken purely
/// to force eager DI activation of the singleton at silo Start: the
/// type's constructor is what registers the four observable peer
/// gauges (<c>peer.entries_behind</c>, <c>peer.bytes_behind</c>,
/// <c>peer.consecutive_errors</c>, <c>peer.last_contact_seconds</c>)
/// on the shared meter, and a <c>TryAddSingleton</c> with no consumer
/// would otherwise leave the singleton lazy - the gauges would never
/// be observed because <c>_current</c> stays null until the first
/// resolution. Holding a reference here makes the DI graph resolve
/// the singleton during <see cref="IHostedService.StartAsync"/>, so
/// the gauges are observable from the silo's first scrape onward.
/// </para>
/// </summary>
internal sealed class ReplicationDriverActivationService : BackgroundService
{
    private readonly IGrainFactory _grainFactory;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor;
    private readonly IReplicationTopology _topology;
    private readonly IReplicatedTreeMembership _membership;
    private readonly CompiledReplicationConfigSnapshotMaintainer? _configMaintainer;
    private readonly TimeSpan _runtimeConfigPollInterval;
    private readonly ILogger<ReplicationDriverActivationService> _logger;
    private readonly object _gate = new();
    private readonly HashSet<string> _enrolledTrees = new(StringComparer.Ordinal);
    private IDisposable? _topologySubscription;

    /// <summary>
    /// Initialises the service. The <paramref name="peerStats"/> dependency
    /// is taken purely to force eager DI activation of the
    /// <see cref="ReplicationPeerStats"/> singleton at silo Start: that
    /// type's constructor registers the four observable peer gauges
    /// (<c>peer.entries_behind</c>, <c>peer.bytes_behind</c>,
    /// <c>peer.consecutive_errors</c>, <c>peer.last_contact_seconds</c>)
    /// on the shared meter, and a <c>TryAddSingleton</c> with no consumer
    /// would otherwise leave the singleton lazy - the gauges would never
    /// fire because <c>_current</c> stays null until first resolution.
    /// Resolving the parameter (even though we never read it inside this
    /// class) wires the gauge registration into the silo's first scrape.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to resolve driver grains.</param>
    /// <param name="optionsMonitor">The replication options monitor (per-tree digest-probe gate).</param>
    /// <param name="topology">The peer-membership topology.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="peerStats">Resolved purely to force eager gauge registration; not stored.</param>
    /// <param name="membership">
    /// The replicated-tree membership seam - the union of the static
    /// options map and (when runtime config is wired) the trees enabled at
    /// runtime through the config CRDT.
    /// </param>
    /// <param name="configMaintainer">
    /// The runtime replication-config snapshot maintainer, present only
    /// when the host opted into runtime configuration. When supplied, its
    /// monotonic epoch drives the live enrolment poll; when null, the
    /// service behaves exactly as a static-only host (no poll loop).
    /// </param>
    /// <param name="runtimeConfigPollInterval">
    /// Optional override for the live-enrolment poll interval. Defaults to
    /// <see cref="DefaultRuntimeConfigPollInterval"/>; exposed for tests.
    /// </param>
    public ReplicationDriverActivationService(
        IGrainFactory grainFactory,
        IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
        IReplicationTopology topology,
        ILogger<ReplicationDriverActivationService> logger,
        ReplicationPeerStats peerStats,
        IReplicatedTreeMembership membership,
        CompiledReplicationConfigSnapshotMaintainer? configMaintainer = null,
        TimeSpan? runtimeConfigPollInterval = null)
    {
        _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
        _optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _membership = membership ?? throw new ArgumentNullException(nameof(membership));
        _configMaintainer = configMaintainer;
        _runtimeConfigPollInterval = runtimeConfigPollInterval ?? DefaultRuntimeConfigPollInterval;
        ArgumentNullException.ThrowIfNull(peerStats);
        // peerStats is intentionally not stored: the DI container roots
        // the singleton; resolving it here is sufficient to trigger the
        // constructor-side gauge registration on silo Start.
    }

    /// <summary>
    /// Initial delay between retry passes when at least one
    /// activation is still pending. Tuned for fast-silo startup -
    /// the silo is typically ready within a few hundred ms after
    /// <see cref="BackgroundService.ExecuteAsync"/> begins running.
    /// </summary>
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>Upper bound on the per-pass retry delay.</summary>
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Default interval for the runtime-config live-enrolment poll. Runtime
    /// enable is a low-frequency admin action, so a few seconds of latency
    /// before a newly-enabled tree starts shipping is acceptable; the poll
    /// only does real work when the snapshot epoch advances.
    /// </summary>
    internal static readonly TimeSpan DefaultRuntimeConfigPollInterval = TimeSpan.FromSeconds(2);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // The tree set is the replicated-tree membership union (static
        // options + runtime-enabled trees), not the raw options map, so a
        // tree enabled purely at runtime is enrolled here too. Peer
        // membership flows through IReplicationTopology.
        var runtimeConfigured = _configMaintainer is not null;
        var initialTrees = Materialize(_membership.ReplicatedTrees);

        if (!runtimeConfigured && initialTrees.Count == 0)
        {
            _logger.LogInformation("No replicated trees configured; skipping replication driver activation.");
            return;
        }

        // Subscribe to the topology BEFORE snapshotting the initial peer
        // set so a peer added between the snapshot read and the subscribe
        // call is not lost. The subscription stays alive for the lifetime
        // of this service so runtime-added peers get their shippers
        // activated (for every currently enrolled tree) without restarting
        // the silo; it is disposed in Dispose() below.
        _topologySubscription = _topology.Subscribe(change => OnPeerChange(change, stoppingToken));

        var initialPeers = _topology.CurrentPeers;

        // Startup enrolment pass over the initial membership. Records each
        // tree in the enrolled set (so the topology callback covers it) and
        // builds the maintenance / digest-probe / shipper activations.
        var pending = BuildActivationsForNewTrees(initialTrees, initialPeers);
        if (pending.Count > 0)
        {
            await DrainPendingAsync(pending, stoppingToken).ConfigureAwait(false);
        }

        // When runtime config is wired, keep watching the snapshot for
        // trees enabled after startup and enrol them live. A static-only
        // host has no runtime config seam, so it simply returns here.
        if (runtimeConfigured)
        {
            await PollRuntimeEnrolmentsAsync(stoppingToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Fire-and-forget activation of one shipper per currently enrolled
    /// tree for a newly-added peer. Invoked off the topology subscription
    /// callback so the subscriber stays non-blocking.
    /// </summary>
    private void OnPeerChange(PeerChanged change, CancellationToken stoppingToken)
    {
        if (change.Kind != PeerChangeKind.Added)
        {
            // Removed events do not trigger any teardown - the shipper
            // grain stays activated to drain its remaining backlog, and
            // the doorbell ring on ShardedReplogSink already keys off the
            // live ReplicationPeers snapshot.
            return;
        }

        string[] trees;
        lock (_gate)
        {
            trees = [.. _enrolledTrees];
        }

        foreach (var treeName in trees)
        {
            if (string.IsNullOrEmpty(treeName))
            {
                continue;
            }

            var capturedTree = treeName;
            var capturedPeer = change.PeerClusterId;
            _ = Task.Run(
                () => ActivateWithRetryAsync(
                    kind: "shipper",
                    label: $"({capturedTree}, {capturedPeer})",
                    activate: ct => _grainFactory
                        .GetGrain<IReplicationShipperGrain>($"{capturedTree}/{capturedPeer}")
                        .EnsureActiveAsync(ct),
                    stoppingToken),
                stoppingToken);
        }
    }

    /// <summary>
    /// Polls the runtime replication-config snapshot for trees enabled
    /// after startup and enrols their driver grains. Keyed on the
    /// maintainer's monotonic epoch so a poll tick does real work only when
    /// the snapshot actually rebuilt; the membership diff and grain
    /// activations are otherwise skipped. Runs until
    /// <paramref name="stoppingToken"/> is cancelled.
    /// </summary>
    private async Task PollRuntimeEnrolmentsAsync(CancellationToken stoppingToken)
    {
        // Sentinel forces the first tick to diff regardless of the current
        // epoch, closing the race where a tree is enabled between the
        // startup membership read and the first poll.
        var lastEpoch = -1L;

        // PeriodicTimer over Task.Delay: this loop runs for the life of the
        // silo, so the steady-state (no config change) tick must not
        // allocate. PeriodicTimer reuses its internal state across ticks,
        // whereas Task.Delay allocates a fresh Timer + Task every interval.
        using var timer = new PeriodicTimer(_runtimeConfigPollInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                var epoch = _configMaintainer!.CurrentEpoch;
                if (epoch == lastEpoch)
                {
                    continue;
                }

                lastEpoch = epoch;

                var pending = BuildActivationsForNewTrees(_membership.ReplicatedTrees, _topology.CurrentPeers);
                if (pending.Count == 0)
                {
                    continue;
                }

                await DrainPendingAsync(pending, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    /// <summary>
    /// Records every not-yet-enrolled tree in <paramref name="candidateTrees"/>
    /// into the enrolled set and builds its activation work items
    /// (maintenance, opt-in digest-probe, and one shipper per peer). A tree
    /// already enrolled by a prior pass is skipped, so the returned list
    /// only covers genuinely new enrolments.
    /// </summary>
    private List<ActivationWorkItem> BuildActivationsForNewTrees(
        IReadOnlyCollection<string> candidateTrees,
        IReadOnlyCollection<string> peers)
    {
        var pending = new List<ActivationWorkItem>();
        lock (_gate)
        {
            foreach (var treeName in candidateTrees)
            {
                if (string.IsNullOrEmpty(treeName))
                {
                    continue;
                }

                if (!_enrolledTrees.Add(treeName))
                {
                    // Already enrolled on a prior pass.
                    continue;
                }

                var capturedTree = treeName;
                pending.Add(new ActivationWorkItem(
                    Kind: "maintenance",
                    Label: $"tree '{capturedTree}'",
                    Activate: ct => _grainFactory
                        .GetGrain<IReplicationMaintenanceGrain>(capturedTree)
                        .EnsureActiveAsync(ct)));

                // Anti-entropy digest-probe scheduler, one per replicated
                // tree. Activated only when the host has opted into the
                // detection feature; default-off so an un-opted host never
                // pays the activation. The per-tree options resolution uses
                // the default instance here for the activation gate; the
                // grain itself re-resolves per-tree on each phase tick.
                if (_optionsMonitor.Get(capturedTree).DigestProbeEnabled)
                {
                    pending.Add(new ActivationWorkItem(
                        Kind: "digest-probe",
                        Label: $"tree '{capturedTree}'",
                        Activate: ct => _grainFactory
                            .GetGrain<IReplicationDigestProbeGrain>(capturedTree)
                            .EnsureActiveAsync(ct)));
                }

                foreach (var peer in peers)
                {
                    if (string.IsNullOrEmpty(peer))
                    {
                        continue;
                    }

                    var capturedPeer = peer;
                    pending.Add(new ActivationWorkItem(
                        Kind: "shipper",
                        Label: $"({capturedTree}, {capturedPeer})",
                        Activate: ct => _grainFactory
                            .GetGrain<IReplicationShipperGrain>($"{capturedTree}/{capturedPeer}")
                            .EnsureActiveAsync(ct)));
                }
            }
        }

        return pending;
    }

    /// <summary>
    /// Runs the retry-with-backoff drain over <paramref name="pending"/>.
    /// Each pass tries every remaining item once; successful items are
    /// removed in-place and any per-item success resets the inter-pass
    /// delay back to the initial value. Returns when the pending set is
    /// empty; throws <see cref="OperationCanceledException"/> on host
    /// shutdown.
    /// </summary>
    private async Task DrainPendingAsync(List<ActivationWorkItem> pending, CancellationToken stoppingToken)
    {
        var delay = InitialRetryDelay;
        var pass = 0;
        while (pending.Count > 0)
        {
            stoppingToken.ThrowIfCancellationRequested();
            pass++;
            var anySuccess = false;
            for (var i = pending.Count - 1; i >= 0; i--)
            {
                stoppingToken.ThrowIfCancellationRequested();
                var item = pending[i];
                try
                {
                    await item.Activate(stoppingToken).ConfigureAwait(false);
                    pending.RemoveAt(i);
                    anySuccess = true;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Host shutdown - propagate.
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "Replication driver activation failed for {Kind} {Label} on pass {Pass}; will retry",
                        item.Kind, item.Label, pass);
                }
            }

            if (pending.Count == 0)
            {
                return;
            }

            if (anySuccess)
            {
                // The cluster is making progress; reset the inter-pass
                // delay so the remaining items don't inherit a long
                // backoff caused by the earliest failure.
                delay = InitialRetryDelay;
            }

            try
            {
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }

            // Exponential backoff with a hard cap. Doubling without
            // jitter is fine here because there is exactly one
            // activation service per silo and at most a small handful
            // of pending items - there is no thundering-herd seam.
            var nextTicks = Math.Min(MaxRetryDelay.Ticks, delay.Ticks * 2);
            delay = TimeSpan.FromTicks(nextTicks);
        }
    }

    /// <summary>
    /// Materialises a membership snapshot into a stable list so the
    /// downstream passes iterate a fixed set (the seam may recompute its
    /// backing collection on every read).
    /// </summary>
    private static List<string> Materialize(IReadOnlyCollection<string> trees) => [.. trees];

    /// <summary>
    /// Runs <paramref name="activate"/> with the same retry-with-backoff
    /// loop the initial activation pass uses. Invoked off the
    /// <see cref="IReplicationTopology.Subscribe"/> callback for
    /// runtime-added peers so the topology subscriber stays
    /// non-blocking. Cancellation via <paramref name="stoppingToken"/>
    /// is the only graceful termination path; the loop swallows
    /// transient errors and retries until activation succeeds.
    /// </summary>
    private async Task ActivateWithRetryAsync(
        string kind,
        string label,
        Func<CancellationToken, Task> activate,
        CancellationToken stoppingToken)
    {
        var delay = InitialRetryDelay;
        var pass = 0;
        while (true)
        {
            if (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            pass++;
            try
            {
                await activate(stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Replication driver runtime activation failed for {Kind} {Label} on pass {Pass}; will retry",
                    kind, label, pass);
            }

            try
            {
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            var nextTicks = Math.Min(MaxRetryDelay.Ticks, delay.Ticks * 2);
            delay = TimeSpan.FromTicks(nextTicks);
        }
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        // Release the topology subscription before base.Dispose so
        // no Added callback fires after the underlying CTS is gone.
        _topologySubscription?.Dispose();
        _topologySubscription = null;
        base.Dispose();
    }

    /// <summary>
    /// Work item describing one grain activation that must succeed
    /// at least once before the driver activation service is
    /// considered done. The <see cref="Activate"/> delegate is
    /// invoked on every retry pass until it returns without throwing.
    /// </summary>
    private sealed record ActivationWorkItem(
        string Kind,
        string Label,
        Func<CancellationToken, Task> Activate);
}

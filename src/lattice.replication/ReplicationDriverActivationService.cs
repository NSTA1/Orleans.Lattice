using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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
/// Peer membership is sourced from <see cref="IReplicationTopology"/>
/// rather than read once from
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/>: the
/// initial snapshot drives the startup activation pass, and a
/// long-lived <see cref="IReplicationTopology.Subscribe"/> subscription
/// activates one shipper per replicated tree for every peer added at
/// runtime. <see cref="PeerChangeKind.Removed"/> events do not trigger
/// any teardown - the shipper grain stays activated to drain its
/// remaining backlog, and the producer-side doorbell ring stops firing
/// for the removed peer automatically because
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
/// pending set is empty or <paramref name="stoppingToken"/> is
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
    private readonly ILogger<ReplicationDriverActivationService> _logger;
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
    public ReplicationDriverActivationService(
        IGrainFactory grainFactory,
        IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
        IReplicationTopology topology,
        ILogger<ReplicationDriverActivationService> logger,
        ReplicationPeerStats peerStats)
    {
        _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
        _optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
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

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Resolve the unnamed (default) options instance. Per-tree
        // overrides apply when each grain calls IOptionsMonitor.Get;
        // the activation service only needs the replicated-tree
        // membership (peer membership now flows through
        // IReplicationTopology).
        var options = _optionsMonitor.CurrentValue;

        if (options.ReplicatedTrees is not { } trees || trees.Count == 0)
        {
            _logger.LogInformation("No replicated trees configured; skipping replication driver activation.");
            return;
        }

        // Subscribe to the topology BEFORE snapshotting the initial
        // peer set so a peer added between the snapshot read and the
        // subscribe call is not lost. The subscription stays alive
        // for the lifetime of this service so runtime-added peers
        // get their shippers activated without restarting the silo;
        // it is disposed in Dispose() below.
        var stableTrees = trees;
        _topologySubscription = _topology.Subscribe(change =>
        {
            if (change.Kind != PeerChangeKind.Added)
            {
                // Removed events do not trigger any teardown - the
                // shipper grain stays activated to drain its
                // remaining backlog, and the doorbell ring on
                // ShardedReplogSink already keys off the live
                // ReplicationPeers snapshot.
                return;
            }

            // Fire-and-forget activation per (tree, newly-added peer).
            // Bounded by stoppingToken so a host shutdown cancels
            // pending retries cleanly.
            foreach (var (treeName, _) in stableTrees)
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
        });

        var initialPeers = _topology.CurrentPeers;

        // Build the pending work list once. Each work item carries a
        // closure that performs the activation call, plus a
        // human-readable label for diagnostics. The closure captures
        // the IGrainFactory and the grain key - re-issuing it after
        // the silo finishes startup is safe because Orleans grain
        // proxies are cheap and the second EnsureActiveAsync after a
        // first success is a no-op (RegisterOrUpdateReminder is
        // idempotent, the phase-timer guard short-circuits, etc.).
        var pending = new List<ActivationWorkItem>();
        foreach (var (treeName, _) in trees)
        {
            if (string.IsNullOrEmpty(treeName))
            {
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

            foreach (var peer in initialPeers)
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

        if (pending.Count == 0)
        {
            return;
        }

        // Retry-with-backoff outer loop. The loop body tries every
        // pending item once; successful items are removed in-place
        // and any per-item success resets the inter-pass delay back
        // to the initial value (a transient failure followed by a
        // success means the cluster has finished coming up - start
        // the next failure from a fresh budget).
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

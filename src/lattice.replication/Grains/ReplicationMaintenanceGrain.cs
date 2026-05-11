using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Default <see cref="IReplicationMaintenanceGrain"/> implementation.
/// Schedules WAL garbage collection, per-peer fall-off-the-log
/// probes, and atomic-batch buffer orphan sweeps for a single
/// replicated tree using the shared
/// <see cref="CoordinatorGrain{TSelf}"/> reminder + phase-timer
/// scaffold.
/// </summary>
internal sealed class ReplicationMaintenanceGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILogger<ReplicationMaintenanceGrain> logger,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    ILatticeWalGc gc,
    ILatticeFallOffLogDetector fallOffDetector,
    ILatticeWalIntrospection walIntrospection,
    IGrainFactory grainFactory,
    [PersistentState("replication-maintenance", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationMaintenanceState> state)
    : CoordinatorGrain<ReplicationMaintenanceGrain>(context, reminderRegistry, logger),
      IReplicationMaintenanceGrain
{
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ILatticeWalGc _gc =
        gc ?? throw new ArgumentNullException(nameof(gc));
    private readonly ILatticeFallOffLogDetector _fallOffDetector =
        fallOffDetector ?? throw new ArgumentNullException(nameof(fallOffDetector));
    private readonly ILatticeWalIntrospection _walIntrospection =
        walIntrospection ?? throw new ArgumentNullException(nameof(walIntrospection));
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <summary>
    /// Cached per-activation tree name. The maintenance grain key is
    /// the tree name verbatim, so resolution is a single
    /// <see cref="GrainId.Key"/> read on first phase tick. Cached to
    /// avoid the per-tick <c>ToString()</c> allocation that the
    /// <see cref="LogContext"/> getter would otherwise pay.
    /// </summary>
    private string _treeName = "";
    private bool _treeNameResolved;

    private string TreeName
    {
        get
        {
            if (!_treeNameResolved)
            {
                _treeName = Context.GrainId.Key.ToString() ?? "";
                _treeNameResolved = true;
            }
            return _treeName;
        }
    }

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "maintenance-keepalive";

    /// <inheritdoc />
    protected override TimeSpan KeepaliveReminderPeriod => TimeSpan.FromSeconds(60);

    /// <inheritdoc />
    protected override TimeSpan PhaseTimerPeriod => TimeSpan.FromSeconds(5);

    /// <inheritdoc />
    protected override bool InProgress => true; // Always running.

    /// <inheritdoc />
    protected override string LogContext => $"maintenance {TreeName}";

    /// <inheritdoc />
    public async Task EnsureActiveAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrEmpty(TreeName))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationMaintenanceGrain)} activation key is empty; expected the replicated tree name.");
        }
        // Idempotent: RegisterOrUpdateReminder + StartPhaseTimer's
        // _phaseTimer ??= make repeated calls safe.
        await StartCoordinatorAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        var options = _optionsMonitor.Get(TreeName);
        var nowTicks = DateTime.UtcNow.Ticks;

        // GC pass — independent cadence. The cadence stamp advances
        // only on a successful pass so a thrown GC retries on the
        // next phase tick rather than waiting a full cadence; the
        // backstop is the keepalive reminder so a deterministically-
        // failing GC cannot stall the activation indefinitely.
        if (ShouldRunCadence(nowTicks, state.State.LastGcTicks, options.MaintenanceGcInterval))
        {
            try
            {
                await _gc.RunOnceAsync(TreeName, CancellationToken.None).ConfigureAwait(true);
                state.State.LastGcTicks = nowTicks;
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex,
                    "WAL garbage-collection pass failed for {Context}; will retry on next phase tick",
                    LogContext);
            }
        }

        // Atomic-batch buffer orphan sweep — half-cadence relative to
        // the GC pass. The two cadences share a clock-budget tick:
        // running orphan-sweep at every other GC tick lets the GC
        // pick up the buffer's freed blocked-floor pin promptly
        // without doubling the maintenance grain's wake-up rate. The
        // cadence stamp advances only on a clean sweep so a thrown
        // sweep retries on the next phase tick.
        var orphanCadence = TimeSpan.FromTicks(Math.Max(1L, options.MaintenanceGcInterval.Ticks / 2));
        if (ShouldRunCadence(nowTicks, state.State.LastOrphanSweepTicks, orphanCadence))
        {
            // Reserved orphan-sweep cadence stamp - retained so the
            // durable maintenance state's [Id(2)] LastOrphanSweepTicks
            // slot continues to advance under existing tooling. The
            // receiver-side atomic-batch staging buffer that this
            // sweep formerly drove was retired with the prepared-
            // mutation isolation model; the cadence is preserved as
            // a future hook so the public maintenance state shape
            // does not have to change again.
            state.State.LastOrphanSweepTicks = nowTicks;
            await state.WriteStateAsync().ConfigureAwait(true);
        }

        // Fall-off-the-log probe — independent cadence. Same retry
        // contract: the cadence stamp advances only on a clean
        // probe pass.
        if (ShouldRunCadence(nowTicks, state.State.LastFallOffCheckTicks, options.MaintenanceFallOffCheckInterval))
        {
            try
            {
                await ProbeFallOffAsync(options).ConfigureAwait(true);
                state.State.LastFallOffCheckTicks = nowTicks;
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex,
                    "Fall-off-log probe pass failed for {Context}; will retry on next phase tick",
                    LogContext);
            }
        }
    }

    private async Task ProbeFallOffAsync(LatticeReplicationOptions options)
    {
        if (options.ReplicationPeers is not { } peers || peers.Count == 0)
        {
            return;
        }

        var oldestHlc = await _walIntrospection
            .GetOldestAvailableHlcAsync(TreeName, CancellationToken.None)
            .ConfigureAwait(true);
        if (oldestHlc is null)
        {
            // WAL is empty for this tree — there's nothing a peer
            // could fall off of. Probing is a no-op until the first
            // entry lands.
            return;
        }

        foreach (var peer in peers)
        {
            if (string.IsNullOrEmpty(peer))
            {
                continue;
            }
            try
            {
                _ = await _fallOffDetector
                    .CheckAndTriggerAsync(TreeName, peer, oldestHlc.Value, CancellationToken.None)
                    .ConfigureAwait(true);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex,
                    "Fall-off-log probe for peer {Peer} failed for {Context}; will retry on next cadence",
                    peer, LogContext);
            }
        }
    }

    private static bool ShouldRunCadence(long nowTicks, long lastTicks, TimeSpan interval)
    {
        if (lastTicks == 0)
        {
            return true; // Never run before — fire on first tick.
        }
        return nowTicks - lastTicks >= interval.Ticks;
    }
}

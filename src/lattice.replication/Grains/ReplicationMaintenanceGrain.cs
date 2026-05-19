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
/// <para>
/// The peer list driving the fall-off probe is sourced from
/// <see cref="IReplicationTopology.CurrentPeers"/>, not from
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/> directly.
/// In the default configuration the two are equivalent because the
/// registered <see cref="OptionsReplicationTopology"/> projects the
/// same <see cref="IOptionsMonitor{TOptions}"/> instance; hosts that
/// register a custom <see cref="IReplicationTopology"/> singleton
/// have that topology drive which peers get walked each cadence,
/// closing the only correctness asymmetry that remained after the
/// initial activation-only topology seam (an unprotected peer can
/// no longer silently fall off the WAL retention window when the
/// custom topology and options diverge).
/// </para>
/// </summary>
internal sealed class ReplicationMaintenanceGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILogger<ReplicationMaintenanceGrain> logger,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    ILatticeWalGc gc,
    ILatticeFallOffLogDetector fallOffDetector,
    ILatticeWalIntrospection walIntrospection,
    IReplicationTopology topology,
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
    private readonly IReplicationTopology _topology =
        topology ?? throw new ArgumentNullException(nameof(topology));
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

        // GC pass - independent cadence. The cadence stamp advances
        // only on a successful pass so a thrown GC retries on the
        // next phase tick rather than waiting a full cadence; the
        // backstop is the keepalive reminder so a deterministically-
        // failing GC cannot stall the activation indefinitely.
        if (ShouldRunCadence(nowTicks, state.State.LastGcTicks, options.MaintenanceGcInterval))
        {
            var prevGcTicks = state.State.LastGcTicks;
            try
            {
                await _gc.RunOnceAsync(TreeName, CancellationToken.None).ConfigureAwait(true);
                state.State.LastGcTicks = nowTicks;
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch (Exception ex)
            {
                // Restore the in-memory cadence stamp so the next phase
                // tick's ShouldRunCadence guard sees the pre-attempt
                // value and correctly fires the retry the comment above
                // promises. Without this, a transient WriteStateAsync
                // failure latches the dirty in-memory stamp at nowTicks
                // and the GC pass is skipped for the full
                // MaintenanceGcInterval (default many minutes).
                state.State.LastGcTicks = prevGcTicks;
                Logger.LogWarning(ex,
                    "WAL garbage-collection pass failed for {Context}; will retry on next phase tick",
                    LogContext);
            }
        }

        // Atomic-batch buffer orphan sweep - half-cadence relative to
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
            var prevOrphanTicks = state.State.LastOrphanSweepTicks;
            state.State.LastOrphanSweepTicks = nowTicks;
            try
            {
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch
            {
                // Snapshot/restore: a transient persist failure must
                // not latch the in-memory cadence stamp - the next
                // tick must retry. The throw propagates as before;
                // existing callers / tests rely on the orphan-sweep
                // block surfacing storage failures.
                state.State.LastOrphanSweepTicks = prevOrphanTicks;
                throw;
            }
        }

        // Fall-off-the-log probe - independent cadence. Same retry
        // contract: the cadence stamp advances only on a clean
        // probe pass.
        if (ShouldRunCadence(nowTicks, state.State.LastFallOffCheckTicks, options.MaintenanceFallOffCheckInterval))
        {
            var prevFallOffTicks = state.State.LastFallOffCheckTicks;
            try
            {
                await ProbeFallOffAsync().ConfigureAwait(true);
                state.State.LastFallOffCheckTicks = nowTicks;
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch (Exception ex)
            {
                // Restore so the next phase tick retries (mirrors the
                // GC block; see comment there for the full rationale).
                state.State.LastFallOffCheckTicks = prevFallOffTicks;
                Logger.LogWarning(ex,
                    "Fall-off-log probe pass failed for {Context}; will retry on next phase tick",
                    LogContext);
            }
        }
    }

    private async Task ProbeFallOffAsync()
    {
        // Snapshot the topology once per cadence so a runtime peer-
        // membership change mid-pass is observed atomically: the
        // probe walks either the pre-change or the post-change set,
        // never a torn one.
        var peers = _topology.CurrentPeers;
        if (peers.Count == 0)
        {
            return;
        }

        var oldestHlc = await _walIntrospection
            .GetOldestAvailableHlcAsync(TreeName, CancellationToken.None)
            .ConfigureAwait(true);
        if (oldestHlc is null)
        {
            // WAL is empty for this tree - there's nothing a peer
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
            return true; // Never run before - fire on first tick.
        }
        return nowTicks - lastTicks >= interval.Ticks;
    }
}

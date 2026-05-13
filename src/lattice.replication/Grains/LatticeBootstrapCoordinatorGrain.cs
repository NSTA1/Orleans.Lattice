using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Default <see cref="ILatticeBootstrapCoordinatorGrain"/>
/// implementation. Hosts the receiver-side bootstrap state machine
/// for a single tree using the same reminder-anchored work-pump
/// pattern as <c>TreeResizeGrain</c> (see
/// <see cref="CoordinatorGrain{TSelf}"/>).
/// <para>
/// Bootstrap is a long-running operation that drains an entire
/// snapshot of the tree from a source cluster and applies every
/// entry through the local apply seam. The grain therefore exposes
/// <see cref="BootstrapAsync"/> as an idempotent kickoff: it
/// persists intent, schedules background work, and returns. Callers
/// poll <see cref="GetStateAsync"/> for progress.
/// </para>
/// <para>
/// Cluster-wide single-activation per tree id provides cross-silo
/// mutual exclusion: a concurrent
/// <see cref]="ILatticeBootstrapCoordinator.BootstrapAsync"/> from
/// another silo routes to the same activation, observes
/// <see cref="BootstrapCoordinatorState.InProgress"/> on persistent
/// state, and either no-ops (same source cluster) or throws
/// (different source cluster). After a silo crash, Orleans
/// reactivates the grain on a surviving silo within the keepalive
/// reminder period; the work-pump resumes from the persisted
/// <see cref="BootstrapCoordinatorState.Phase"/> and re-opens the
/// snapshot stream at
/// <see cref="BootstrapCoordinatorState.LastAppliedHlc"/> rather
/// than from <see cref="HybridLogicalClock.Zero"/>.
/// </para>
/// </summary>
internal sealed class LatticeBootstrapCoordinatorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    ISnapshotProvider snapshotProvider,
    IReminderRegistry reminderRegistry,
    ILogger<LatticeBootstrapCoordinatorGrain> logger,
    [PersistentState("bootstrap-coordinator", LatticeOptions.StorageProviderName)]
    IPersistentState<BootstrapCoordinatorState> state)
    : CoordinatorGrain<LatticeBootstrapCoordinatorGrain>(context, reminderRegistry, logger),
      ILatticeBootstrapCoordinatorGrain
{
    /// <summary>
    /// Number of snapshot entries applied between
    /// <see cref="IPersistentState{TState}.WriteStateAsync"/> calls
    /// during the <see cref="LatticeBootstrapState.ApplyingSnapshot"/>
    /// phase. A silo crash may cost up to this many re-applied entries
    /// on resume; the per-origin HWM dedupe makes the replay
    /// idempotent so the cost is bandwidth, not correctness.
    /// </summary>
    private const int CursorPersistEntryInterval = 100;

    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ISnapshotProvider _snapshotProvider =
        snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));

    private string TreeName => Context.GrainId.Key.ToString() ?? "";

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "bootstrap-keepalive";

    /// <inheritdoc />
    protected override bool InProgress => state.State.InProgress;

    /// <inheritdoc />
    protected override string LogContext => $"tree {TreeName}";

    /// <inheritdoc />
    public Task<LatticeBootstrapState> GetStateAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(state.State.Phase);
    }

    /// <inheritdoc />
    public async Task BootstrapAsync(string sourceClusterId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        if (await TryInitiateBootstrapAsync(sourceClusterId).ConfigureAwait(true))
        {
            await StartCoordinatorAsync().ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Persists kickoff intent and returns whether the caller should
    /// register the keepalive reminder + phase timer. Returns
    /// <see langword="false"/> on the idempotent "already in progress
    /// from the same source cluster" path so tests (and idempotent
    /// retries) don't double-register the coordinator. Exposed as
    /// <c>internal</c> for unit testing the persistence shape without
    /// touching <see cref="CoordinatorGrain{TSelf}.StartCoordinatorAsync"/>,
    /// which requires a real grain scheduler.
    /// </summary>
    internal async Task<bool> TryInitiateBootstrapAsync(string sourceClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);

        var treeName = TreeName;
        if (string.IsNullOrEmpty(treeName))
        {
            throw new InvalidOperationException(
                $"{nameof(LatticeBootstrapCoordinatorGrain)} activation key is empty; expected the replicated tree name.");
        }

        if (state.State.InProgress)
        {
            // Idempotent: same source cluster - caller is retrying the
            // kickoff, the in-flight work continues unchanged.
            if (string.Equals(state.State.SourceClusterId, sourceClusterId, StringComparison.Ordinal))
            {
                return false;
            }

            throw new InvalidOperationException(
                $"A bootstrap is already in progress for tree '{treeName}' from source cluster " +
                $"'{state.State.SourceClusterId}'; cannot start a new bootstrap from '{sourceClusterId}'.");
        }

        // Persist intent BEFORE any external side effects. The phase
        // timer's first tick (scheduled by StartCoordinatorAsync) will
        // observe RequestingSnapshot and call ExportAsync.
        //
        // Snapshot every field we're about to mutate so a failed
        // WriteStateAsync rolls in-memory state back to the pre-call
        // values. Without the revert, the `if (state.State.InProgress)`
        // guard above short-circuits every same-source kickoff retry
        // from the same activation, silently dropping the bootstrap.
        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevSourceClusterId = state.State.SourceClusterId;
        var prevOperationId = state.State.OperationId;
        var prevLastAppliedHlc = state.State.LastAppliedHlc;
        var prevSnapshotAsOfHlc = state.State.SnapshotAsOfHlc;
        var prevCausalStableFrontier = state.State.CausalStableFrontier;

        state.State.InProgress = true;
        state.State.Phase = LatticeBootstrapState.RequestingSnapshot;
        state.State.SourceClusterId = sourceClusterId;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.LastAppliedHlc = HybridLogicalClock.Zero;
        state.State.SnapshotAsOfHlc = HybridLogicalClock.Zero;
        state.State.CausalStableFrontier = new VersionVector();
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.SourceClusterId = prevSourceClusterId;
            state.State.OperationId = prevOperationId;
            state.State.LastAppliedHlc = prevLastAppliedHlc;
            state.State.SnapshotAsOfHlc = prevSnapshotAsOfHlc;
            state.State.CausalStableFrontier = prevCausalStableFrontier;
            throw;
        }
        return true;
    }

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!state.State.InProgress) return;

        try
        {
            switch (state.State.Phase)
            {
                case LatticeBootstrapState.RequestingSnapshot:
                case LatticeBootstrapState.ApplyingSnapshot:
                    await DrainSnapshotAsync().ConfigureAwait(true);
                    break;

                case LatticeBootstrapState.IncrementalHandoff:
                    await PinAndCompleteAsync().ConfigureAwait(true);
                    break;

                case LatticeBootstrapState.Idle:
                case LatticeBootstrapState.LiveIncremental:
                case LatticeBootstrapState.Failed:
                default:
                    // Terminal / unexpected - stop the pump.
                    state.State.InProgress = false;
                    await state.WriteStateAsync().ConfigureAwait(true);
                    await CompleteCoordinatorAsync().ConfigureAwait(true);
                    break;
            }
        }
        catch (Exception ex)
        {
            // Mark the bootstrap failed and tear down the work-pump.
            // The next BootstrapAsync call restarts the cycle from
            // RequestingSnapshot. The base class also catches and logs
            // tick failures, but persisting Failed here makes the state
            // observable to GetStateAsync callers.
            Logger.LogWarning(ex,
                "Bootstrap phase {Phase} failed for {Context}",
                state.State.Phase, LogContext);

            // Snapshot the fields we're about to mutate. If the
            // catch-handler persist below also throws, the L207
            // "leave keepalive armed for retry" branch deliberately
            // keeps the coordinator running so the next tick can
            // retry the Failed pivot. Without the revert, the next
            // tick would observe dirty in-memory InProgress=false
            // (set just below) and short-circuit at the
            // `if (!state.State.InProgress) return;` guard in
            // ProcessNextPhaseAsync - silently breaking the documented
            // retry intent and stranding the activation until it
            // recycles.
            var prevPhase = state.State.Phase;
            var prevInProgress = state.State.InProgress;

            state.State.Phase = LatticeBootstrapState.Failed;
            state.State.InProgress = false;
            bool persisted;
            try
            {
                await state.WriteStateAsync().ConfigureAwait(true);
                persisted = true;
            }
            catch (Exception writeEx)
            {
                Logger.LogError(writeEx,
                    "Failed to persist Failed phase for {Context}; leaving keepalive reminder armed so the next tick can retry",
                    LogContext);
                state.State.Phase = prevPhase;
                state.State.InProgress = prevInProgress;
                persisted = false;
            }

            // Only tear down the keepalive reminder + phase timer when
            // the Failed transition actually made it to durable storage.
            // Otherwise the next reactivation would observe stale
            // persisted state (Phase=ApplyingSnapshot, InProgress=true)
            // with no driver attached - a "looks in-progress but nothing
            // is running" zombie. Leaving the coordinator armed lets the
            // next tick retry the persist.
            if (persisted)
            {
                await CompleteCoordinatorAsync().ConfigureAwait(true);
            }
            throw;
        }
    }

    /// <summary>
    /// Opens (or re-opens, after a crash) the snapshot stream from
    /// <see cref="BootstrapCoordinatorState.LastAppliedHlc"/>, drains
    /// every entry through the local apply seam, and transitions to
    /// <see cref="LatticeBootstrapState.IncrementalHandoff"/> when the
    /// stream is exhausted. Persists the cursor every
    /// <see cref="CursorPersistEntryInterval"/> entries so a mid-drain
    /// crash re-applies at most that many entries on resume.
    /// </summary>
    private async Task DrainSnapshotAsync()
    {
        var treeName = TreeName;
        var snapshot = await _snapshotProvider
            .ExportAsync(treeName, state.State.LastAppliedHlc, CancellationToken.None)
            .ConfigureAwait(true);

        // Update the durable handoff metadata to whatever the latest
        // export reports. On crash recovery this overwrites the prior
        // export's metadata - safe because the receiver will have
        // applied every entry up through the new export's AsOfHlc by
        // the time it reaches IncrementalHandoff, and the per-origin
        // HWM dedupe makes any overlap a no-op.
        state.State.SnapshotAsOfHlc = snapshot.AsOfHlc;
        state.State.CausalStableFrontier = snapshot.CausalStableFrontier;
        if (state.State.Phase != LatticeBootstrapState.ApplyingSnapshot)
        {
            state.State.Phase = LatticeBootstrapState.ApplyingSnapshot;
        }
        await state.WriteStateAsync().ConfigureAwait(true);

        var apply = _grainFactory.GetGrain<IReplicationApplyGrain>(treeName);
        var sourceClusterId = state.State.SourceClusterId;
        int sinceLastPersist = 0;

        await foreach (var entry in snapshot.Entries.ConfigureAwait(true))
        {
            if (entry.Value is null)
            {
                // Tombstones are not emitted by the default provider,
                // but defend against custom providers that might
                // surface them.
                continue;
            }

            await apply.ApplySetAsync(
                entry.Key,
                entry.Value,
                entry.Timestamp,
                sourceClusterId,
                sourceVectorClock: null,
                expiresAtTicks: 0).ConfigureAwait(true);

            // Track the highest source HLC observed so a resume can
            // re-export from this point. The per-origin HWM dedupe
            // tolerates a stale cursor, so persisting in batches is
            // safe.
            if (entry.Timestamp.CompareTo(state.State.LastAppliedHlc) > 0)
            {
                state.State.LastAppliedHlc = entry.Timestamp;
            }

            if (++sinceLastPersist >= CursorPersistEntryInterval)
            {
                await state.WriteStateAsync().ConfigureAwait(true);
                sinceLastPersist = 0;
            }
        }

        state.State.Phase = LatticeBootstrapState.IncrementalHandoff;
        await state.WriteStateAsync().ConfigureAwait(true);
    }

    /// <summary>
    /// Pins the snapshot's as-of HLC and causal-stable frontier on
    /// the per-tree <see cref="IReplicationHighWaterMarkGrain"/> and
    /// completes the bootstrap. The HWM pin is the snapshot/incremental
    /// handoff seam: the per-origin HWM dedupe makes any incremental
    /// entry whose timestamp is at or below the pinned frontier a
    /// no-op, so the boundary is exactly-once regardless of overlap.
    /// </summary>
    private async Task PinAndCompleteAsync()
    {
        var treeName = TreeName;
        var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
        // Idempotent: PinSnapshotAsync is a monotonic max + frontier
        // merge, so a crash between this call and the WriteStateAsync
        // below replays safely on reactivation - the second pin with
        // identical (asOfHlc, frontier) is a no-op.
        await hwm
            .PinSnapshotAsync(state.State.SnapshotAsOfHlc, state.State.CausalStableFrontier, CancellationToken.None)
            .ConfigureAwait(true);

        state.State.Phase = LatticeBootstrapState.LiveIncremental;
        state.State.InProgress = false;
        await state.WriteStateAsync().ConfigureAwait(true);
        await CompleteCoordinatorAsync().ConfigureAwait(true);
    }
}

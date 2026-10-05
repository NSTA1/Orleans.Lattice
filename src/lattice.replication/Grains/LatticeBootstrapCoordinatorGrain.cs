using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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
/// <see cref="Orleans.Lattice.Replication.ILatticeBootstrapCoordinator.BootstrapAsync(System.String,System.String,System.Threading.CancellationToken)"/> from
/// another silo routes to the same activation, observes
/// <see cref="BootstrapCoordinatorState.InProgress"/> on persistent
/// state, and either no-ops (same source cluster) or throws
/// (different source cluster). After a silo crash, Orleans
/// reactivates the grain on a surviving silo within the keepalive
/// reminder period; the work-pump resumes from the persisted
/// <see cref="BootstrapCoordinatorState.Phase"/> and re-opens the
/// snapshot stream with no upper bound
/// (<see cref="HybridLogicalClock.Zero"/>), because the export treats a
/// non-zero <c>asOfHlc</c> as a strict upper bound and would drop every
/// not-yet-applied entry stamped above
/// <see cref="BootstrapCoordinatorState.LastAppliedHlc"/>.
/// </para>
/// </summary>
internal sealed partial class LatticeBootstrapCoordinatorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IBootstrapSnapshotSource snapshotProvider,
    IReplicationApplier replicationApplier,
    IReminderRegistry reminderRegistry,
    ILatticeMergeModeResolver mergeModeResolver,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    ILatticeWalIntrospection walIntrospection,
    ILogger<LatticeBootstrapCoordinatorGrain> logger,
    [PersistentState("bootstrap-coordinator", LatticeOptions.StorageProviderName)]
    IPersistentState<BootstrapCoordinatorState> state,
    IBootstrapReadFence? readFence = null)
    : CoordinatorGrain<LatticeBootstrapCoordinatorGrain>(context, reminderRegistry, logger),
      ILatticeBootstrapCoordinatorGrain
{
    private static readonly IReadOnlyDictionary<string, HybridLogicalClock> EmptyFrontierWatermarks =
        new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);

    private static readonly IReadOnlyDictionary<string, HybridLogicalClock[]> EmptyFrontierHeld =
        new Dictionary<string, HybridLogicalClock[]>(StringComparer.Ordinal);

    /// <summary>
    /// Number of snapshot entries applied between
    /// <c>WriteStateAsync</c> calls
    /// during the <see cref="LatticeBootstrapState.ApplyingSnapshot"/>
    /// phase. A silo crash may cost up to this many re-applied entries
    /// on resume; recent exact-identity dedupe and
    /// per-key LWW idempotency make the replay safe so the cost is bandwidth, not correctness.
    /// </summary>
    private const int CursorPersistEntryInterval = 100;

    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ISnapshotProvider _snapshotProvider =
        snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));
    private readonly IReplicationApplier _replicationApplier =
        replicationApplier ?? throw new ArgumentNullException(nameof(replicationApplier));
    private readonly ILatticeMergeModeResolver _mergeModeResolver =
        mergeModeResolver ?? throw new ArgumentNullException(nameof(mergeModeResolver));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ILatticeWalIntrospection _walIntrospection =
        walIntrospection ?? throw new ArgumentNullException(nameof(walIntrospection));

    // Resolved from the activation's services when the constructor did not
    // supply one. Never defaulted to a no-op: a drain with no fence to arm is
    // refused (fails closed) rather than run unfenced (issue #4526).
    private IBootstrapReadFence ReadFence =>
        _readFence ??= context.ActivationServices?.GetService(typeof(IBootstrapReadFence)) as IBootstrapReadFence
            ?? throw new InvalidOperationException(
                $"No {nameof(IBootstrapReadFence)} is registered; a snapshot bootstrap cannot drain into tree '{TreeName}' without a read fence. Register replication with AddLatticeReplication.");

    private IBootstrapReadFence? _readFence = readFence;

    /// <summary>The first automatic re-drive delay of a failed, read-fenced bootstrap.</summary>
    internal static readonly TimeSpan RedriveInitialDelay = TimeSpan.FromSeconds(5);

    /// <summary>The cap on the automatic re-drive delay of a failed, read-fenced bootstrap.</summary>
    internal static readonly TimeSpan RedriveMaxDelay = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Per-activation stopwatch timestamp captured when the coordinator
    /// first observes an in-flight bootstrap. <see langword="null"/>
    /// when the activation has not yet driven a drain. Reset to
    /// <see langword="null"/> after the terminal
    /// <see cref="LatticeReplicationMetrics.BootstrapDuration"/>
    /// histogram emit so a subsequent re-bootstrap on the same
    /// activation gets a fresh start anchor. Held in memory (not
    /// persistent state) because a silo failover should report
    /// "duration since most recent reactivation" - the per-entry
    /// counters carry cross-failover progress.
    /// </summary>
    private long? _drainStartTimestamp;

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
    public Task<BootstrapCoordinatorStatus> GetStatusAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Project an empty SourceClusterId to null so the caller does
        // not have to know about the persistent state's empty-string
        // sentinel. A finished or never-started bootstrap also reports
        // null even if the persisted source string survived a prior
        // run, because InProgress is the authoritative liveness gate.
        var source = state.State.InProgress && !string.IsNullOrEmpty(state.State.SourceClusterId)
            ? state.State.SourceClusterId
            : null;
        return Task.FromResult(new BootstrapCoordinatorStatus(state.State.Phase, source)
        {
            ReadFenced = state.State.ReadFenceArmed,
            EntriesApplied = state.State.EntriesApplied,
            RedriveAttempts = state.State.RedriveAttempts,
        });
    }

    /// <inheritdoc />
    public Task<long?> GetCompletedExportEpochAsync(string sourceClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        return Task.FromResult<long?>(
            state.State.CompletedExportEpochs.TryGetValue(sourceClusterId, out var epoch) ? epoch : null);
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

    /// <summary>First delay between owed delete-reconcile retries (issue #4537).</summary>
    internal static readonly TimeSpan OwedRetryInitialDelay = TimeSpan.FromMinutes(1);

    /// <summary>Cap on the delay between owed delete-reconcile retries (issue #4537).</summary>
    internal static readonly TimeSpan OwedRetryMaxDelay = TimeSpan.FromHours(6);

    /// <inheritdoc />
    public async Task RetryOwedReconcileAsync(string sourceClusterId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        if (!state.State.ReconcileOwedBySource.TryGetValue(sourceClusterId, out var owed) || !owed)
        {
            return;
        }

        if (state.State.InProgress)
        {
            return;
        }

        var now = DateTime.UtcNow.Ticks;
        if (state.State.OwedRetryNotBeforeTicksBySource.TryGetValue(sourceClusterId, out var notBefore) && now < notBefore)
        {
            return;
        }

        // Bounded exponential backoff: an unstable generation clears quickly, but a
        // sender that never reports one must not re-bootstrap on every tick.
        var attempts = state.State.OwedRetryAttemptsBySource.GetValueOrDefault(sourceClusterId);
        var delay = TimeSpan.FromTicks(Math.Min(
            OwedRetryMaxDelay.Ticks,
            OwedRetryInitialDelay.Ticks << Math.Min(attempts, 16)));
        state.State.OwedRetryAttemptsBySource[sourceClusterId] = attempts + 1;
        state.State.OwedRetryNotBeforeTicksBySource[sourceClusterId] = now + delay.Ticks;
        await state.WriteStateAsync().ConfigureAwait(true);

        await BootstrapAsync(sourceClusterId, cancellationToken).ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task<bool> IsReseedPendingAsync(string sourceClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        return Task.FromResult(state.State.ReseedAfterEpochs.ContainsKey(sourceClusterId));
    }

    /// <inheritdoc />
    public Task<ReplicationDrainedLineage?> GetDrainedLineageAsync(string sourceClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        return Task.FromResult<ReplicationDrainedLineage?>(
            state.State.DrainedLineageBySource.TryGetValue(sourceClusterId, out var lineage)
                ? new ReplicationDrainedLineage(
                    lineage,
                    state.State.DrainedFrontierEpochBySource.TryGetValue(sourceClusterId, out var epoch) ? epoch : Guid.Empty)
                : null);
    }

    /// <summary>
    /// Records the source lineage a whole-tree export opened under, with the
    /// tree frontier epoch the drain began in (issue #4673). An export from a
    /// sender that reports no lineage records nothing, and leaves any earlier
    /// record in place.
    /// </summary>
    private void RecordDrainedLineage(string sourceClusterId, SnapshotSourceGeneration? openGeneration)
    {
        if (openGeneration?.Lineage is not { } lineage)
        {
            return;
        }

        state.State.DrainedLineageBySource[sourceClusterId] = lineage;
        state.State.DrainedFrontierEpochBySource[sourceClusterId] = state.State.FrontierEpoch;
    }

    /// <inheritdoc />
    public async Task BootstrapForReseedAsync(string sourceClusterId, long reseedAfterEpoch, bool start, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        if (state.State.InProgress
            && !string.Equals(state.State.SourceClusterId, sourceClusterId, StringComparison.Ordinal)
            && state.State.Phase != LatticeBootstrapState.Failed)
        {
            throw new InvalidOperationException(
                $"A bootstrap is already in progress for tree '{TreeName}' from source cluster "
                + $"'{state.State.SourceClusterId}'; cannot re-seed from '{sourceClusterId}'.");
        }

        var requests = state.State.ReseedAfterEpochs;
        var had = requests.TryGetValue(sourceClusterId, out var recorded);
        if (!had || recorded < reseedAfterEpoch)
        {
            requests[sourceClusterId] = reseedAfterEpoch;
            try
            {
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch
            {
                if (had)
                {
                    requests[sourceClusterId] = recorded;
                }
                else
                {
                    requests.Remove(sourceClusterId);
                }

                throw;
            }
        }

        if (start)
        {
            await BootstrapAsync(sourceClusterId, cancellationToken).ConfigureAwait(true);
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
            // kickoff, the in-flight work continues unchanged. That includes
            // a failed bootstrap whose partial import keeps the tree
            // read-fenced: its automatic re-drive is already scheduled.
            if (string.Equals(state.State.SourceClusterId, sourceClusterId, StringComparison.Ordinal))
            {
                return false;
            }

            // A failed bootstrap held only for its automatic re-drive (issue
            // #4526) may be taken over by a different source: the new
            // bootstrap re-drains the whole tree, which is what lifts the
            // fence. Anything still running refuses.
            if (state.State.Phase != LatticeBootstrapState.Failed)
            {
                throw new InvalidOperationException(
                    $"A bootstrap is already in progress for tree '{treeName}' from source cluster " +
                    $"'{state.State.SourceClusterId}'; cannot start a new bootstrap from '{sourceClusterId}'.");
            }
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
        var prevEntriesApplied = state.State.EntriesApplied;
        var prevRedriveAttempts = state.State.RedriveAttempts;
        var prevNextRedriveAt = state.State.NextRedriveAtUtcTicks;
        var prevPoisonSettleOrigin = state.State.PoisonSettleOriginClusterId;
        var prevPoisonSettleTxids = state.State.PoisonSettleTransactionIds;

        state.State.InProgress = true;
        state.State.Phase = LatticeBootstrapState.RequestingSnapshot;
        state.State.SourceClusterId = sourceClusterId;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.LastAppliedHlc = HybridLogicalClock.Zero;
        state.State.SnapshotAsOfHlc = HybridLogicalClock.Zero;
        state.State.CausalStableFrontier = new VersionVector();
        // The read-fence slots (ReadFenceArmed, FencedPhysicalTreeId,
        // FencedShardIndices, ImportApplied) are deliberately carried over: a
        // partial import left by an earlier failed bootstrap stays fenced until
        // this one completes (issue #4526).
        state.State.EntriesApplied = 0;
        state.State.RedriveAttempts = 0;
        state.State.NextRedriveAtUtcTicks = 0;
        state.State.PoisonSettleOriginClusterId = "";
        state.State.PoisonSettleTransactionIds = new List<Guid>();
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
            state.State.EntriesApplied = prevEntriesApplied;
            state.State.RedriveAttempts = prevRedriveAttempts;
            state.State.NextRedriveAtUtcTicks = prevNextRedriveAt;
            state.State.PoisonSettleOriginClusterId = prevPoisonSettleOrigin;
            state.State.PoisonSettleTransactionIds = prevPoisonSettleTxids;
            throw;
        }

        // Anchor the duration timer at the moment the kickoff is
        // durable. A reactivation after a silo crash skips this path
        // (the persisted Phase != Idle), so DrainSnapshotAsync lazy-
        // initialises the anchor on resume.
        _drainStartTimestamp = Stopwatch.GetTimestamp();

        Logger.LogInformation(
            "Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': Idle -> RequestingSnapshot (LastAppliedHlc={LastAppliedHlc})",
            treeName, sourceClusterId, state.State.LastAppliedHlc);

        return true;
    }

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!state.State.InProgress) return;

        // A failed bootstrap that left a partial import behind keeps the tree
        // read-fenced and stays in progress only to be re-driven (issue #4526).
        if (state.State.Phase == LatticeBootstrapState.Failed && state.State.ReadFenceArmed)
        {
            await RedriveIfDueAsync().ConfigureAwait(true);
            return;
        }

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
            var prevNextRedriveAt = state.State.NextRedriveAtUtcTicks;

            // Issue #4526. A drain that applied part of an import leaves the
            // tree read-fenced: lifting the fence would expose the partial
            // import, so the bootstrap stays in progress, keeps the fence, and
            // is re-driven automatically with backoff until a drain completes.
            // A fence armed before any entry was applied hides nothing and is
            // lifted; if that lift fails the fence is kept and re-driven too,
            // so no fence is ever left up without a coordinator to lift it.
            var keepFence = state.State.ReadFenceArmed && state.State.ImportApplied;
            if (state.State.ReadFenceArmed && !keepFence)
            {
                keepFence = !await TryLiftReadFenceAsync().ConfigureAwait(true);
            }

            state.State.Phase = LatticeBootstrapState.Failed;
            state.State.InProgress = keepFence;
            if (keepFence)
            {
                state.State.NextRedriveAtUtcTicks =
                    DateTime.UtcNow.Ticks + ComputeBackoff(state.State.RedriveAttempts + 1, RedriveInitialDelay, RedriveMaxDelay).Ticks;
            }
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
                state.State.NextRedriveAtUtcTicks = prevNextRedriveAt;
                persisted = false;
            }

            // Only tear down the keepalive reminder + phase timer when
            // the Failed transition actually made it to durable storage.
            // Otherwise the next reactivation would observe stale
            // persisted state (Phase=ApplyingSnapshot, InProgress=true)
            // with no driver attached - a "looks in-progress but nothing
            // is running" zombie. Leaving the coordinator armed lets the
            // next tick retry the persist. A failure that keeps the read
            // fence also keeps the coordinator armed, to re-drive it.
            if (persisted)
            {
                // Terminal duration recording: outcome=failed. Emit
                // before the structured log so a log-tail consumer who
                // joins on (treeName, sourceClusterId) sees the metric
                // and the log in the canonical order.
                RecordBootstrapDuration(TreeName, state.State.SourceClusterId, LatticeReplicationMetrics.BootstrapOutcomeFailed);

                Logger.LogInformation(
                    "Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': {PreviousPhase} -> Failed (LastAppliedHlc={LastAppliedHlc})",
                    TreeName, state.State.SourceClusterId, prevPhase, state.State.LastAppliedHlc);

                if (keepFence)
                {
                    Logger.LogWarning(
                        "Bootstrap of tree '{TreeName}' from source '{SourceClusterId}' failed after applying part of the snapshot; the tree stays read-fenced (reads throw LatticeTreeBootstrappingException) and the bootstrap is re-driven automatically at {NextRedriveAtUtc:o}",
                        TreeName, state.State.SourceClusterId, new DateTime(state.State.NextRedriveAtUtcTicks, DateTimeKind.Utc));
                }
                else
                {
                    await CompleteCoordinatorAsync().ConfigureAwait(true);
                }
            }
            throw;
        }
    }

    /// <summary>
    /// Re-drives a failed bootstrap that left a partial import read-fenced
    /// (issue #4526) once its backoff has elapsed: the next tick re-exports and
    /// re-drains the whole snapshot, and the fence lifts when that completes.
    /// </summary>
    private async Task RedriveIfDueAsync()
    {
        if (DateTime.UtcNow.Ticks < state.State.NextRedriveAtUtcTicks)
        {
            return;
        }

        var prevAttempts = state.State.RedriveAttempts;
        state.State.RedriveAttempts = prevAttempts + 1;
        state.State.Phase = LatticeBootstrapState.RequestingSnapshot;
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            state.State.RedriveAttempts = prevAttempts;
            state.State.Phase = LatticeBootstrapState.Failed;
            throw;
        }

        _drainStartTimestamp ??= Stopwatch.GetTimestamp();
        Logger.LogWarning(
            "Re-driving bootstrap of read-fenced tree '{TreeName}' from source '{SourceClusterId}' (attempt {Attempt})",
            TreeName, state.State.SourceClusterId, state.State.RedriveAttempts);
    }

    /// <summary>
    /// Lifts the read fence on the shards it was armed on and clears the fence
    /// slots, without persisting them. Returns whether every shard was lifted;
    /// on a fault the slots are left recording the fence as armed.
    /// </summary>
    private async Task<bool> TryLiftReadFenceAsync()
    {
        try
        {
            await LiftReadFenceAsync().ConfigureAwait(true);
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Could not lift the bootstrap read fence on tree '{TreeName}'; it stays armed and the bootstrap stays in progress to lift it",
                TreeName);
            return false;
        }
    }

    /// <summary>
    /// Lifts the read fence on the recorded shards and clears the fence slots
    /// in memory. The caller persists. A fault propagates with the slots intact.
    /// </summary>
    private async Task LiftReadFenceAsync()
    {
        if (state.State.FencedPhysicalTreeId is { } physical && state.State.FencedShardIndices is { } indices)
        {
            await ReadFence.SetAsync(new TreeBootstrapReadFence.Shards(physical, indices), fenced: false).ConfigureAwait(true);
        }

        state.State.ReadFenceArmed = false;
        state.State.ImportApplied = false;
        state.State.FencedPhysicalTreeId = null;
        state.State.FencedShardIndices = null;
    }

    /// <summary>
    /// Arms the read fence on every shard of the copy the tree routes to, then
    /// checks nothing holds the drain (issue #4526). Returns
    /// <see langword="false"/> when a split, consolidation, resize or undo is in
    /// progress: the drain waits for a later tick, and a fence that hides no
    /// partial import is lifted meanwhile. The fence slots are persisted before
    /// any shard is armed, so a crash part-way through arming still lifts it.
    /// </summary>
    private async Task<bool> ArmReadFenceAsync()
    {
        var shards = await ReadFence.ResolveAsync(TreeName).ConfigureAwait(true);

        // Fold in a set armed by an earlier attempt, so every shard ever armed
        // is lifted. The interlock holds migrations and resizes while the fence
        // is up, so the set only changes across a window in which it was down.
        if (state.State.ReadFenceArmed
            && state.State.FencedPhysicalTreeId is { } priorPhysical
            && state.State.FencedShardIndices is { } priorIndices)
        {
            if (string.Equals(priorPhysical, shards.PhysicalTreeId, StringComparison.Ordinal))
            {
                shards = shards with
                {
                    ShardIndices = shards.ShardIndices.Union(priorIndices).Order().ToArray(),
                };
            }
            else
            {
                await ReadFence.SetAsync(new TreeBootstrapReadFence.Shards(priorPhysical, priorIndices), fenced: false)
                    .ConfigureAwait(true);
            }
        }

        var prevArmed = state.State.ReadFenceArmed;
        var prevPhysical = state.State.FencedPhysicalTreeId;
        var prevIndices = state.State.FencedShardIndices;
        state.State.ReadFenceArmed = true;
        state.State.FencedPhysicalTreeId = shards.PhysicalTreeId;
        state.State.FencedShardIndices = shards.ShardIndices;
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            state.State.ReadFenceArmed = prevArmed;
            state.State.FencedPhysicalTreeId = prevPhysical;
            state.State.FencedShardIndices = prevIndices;
            throw;
        }

        await ReadFence.SetAsync(shards, fenced: true).ConfigureAwait(true);

        var blocker = await ReadFence.FindBlockerAsync(TreeName, shards).ConfigureAwait(true);
        if (blocker is null)
        {
            return true;
        }

        if (!state.State.ImportApplied)
        {
            await LiftReadFenceAsync().ConfigureAwait(true);
            await state.WriteStateAsync().ConfigureAwait(true);
        }

        Logger.LogInformation(
            "Bootstrap of tree '{TreeName}' from source '{SourceClusterId}' is waiting: {Blocker}. It retries on the next tick.",
            TreeName, state.State.SourceClusterId, blocker);
        return false;
    }

    /// <inheritdoc />
    public async Task<bool> ForceLiftReadFenceAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!state.State.ReadFenceArmed)
        {
            return false;
        }

        if (state.State.InProgress && state.State.Phase != LatticeBootstrapState.Failed)
        {
            throw new InvalidOperationException(
                $"A snapshot bootstrap of tree '{TreeName}' is running ({state.State.Phase}); its read fence lifts when it completes and cannot be force-lifted while it runs.");
        }

        if (state.State.FencedPhysicalTreeId is null || state.State.FencedShardIndices is null)
        {
            var shards = await ReadFence.ResolveAsync(TreeName).ConfigureAwait(true);
            state.State.FencedPhysicalTreeId = shards.PhysicalTreeId;
            state.State.FencedShardIndices = shards.ShardIndices;
        }

        await LiftReadFenceAsync().ConfigureAwait(true);
        var wasRedriving = state.State.InProgress;
        state.State.InProgress = false;
        await state.WriteStateAsync().ConfigureAwait(true);
        if (wasRedriving)
        {
            await CompleteCoordinatorAsync().ConfigureAwait(true);
        }

        return true;
    }

    /// <summary>
    /// Opens (or re-opens, after a crash) the full snapshot stream,
    /// drains every entry through the local apply seam, and transitions to
    /// <see cref="LatticeBootstrapState.IncrementalHandoff"/> when the
    /// stream is exhausted. Persists the
    /// <see cref="BootstrapCoordinatorState.LastAppliedHlc"/> cursor every
    /// <see cref="CursorPersistEntryInterval"/> entries for the handoff
    /// seal; it is never used to narrow a resumed export.
    /// <para>
    /// Wraps the export + apply loop in a bounded transient-retry
    /// policy
    /// (<see cref="LatticeReplicationOptions.BootstrapTransientRetry"/>).
    /// A classified-transient fault (e.g. a gRPC
    /// <c>StatusCode.Unavailable</c> from
    /// <c>RemoteSnapshotProvider</c>) consumes one retry slot and
    /// re-opens the full snapshot; per-key LWW reconciliation makes
    /// re-applying the entries the failed attempt already applied a no-op.
    /// Non-transient faults pivot to <see cref="LatticeBootstrapState.Failed"/>
    /// on the first failure via the catch block in
    /// <see cref="ProcessNextPhaseAsync"/>. Budget exhaustion re-throws
    /// the final classified-transient exception verbatim so the
    /// same catch block records the failure outcome.
    /// </para>
    /// <para>
    /// An entry the applier defers (<see cref="ApplyResult.Deferred"/>) ends
    /// the attempt with a <see cref="LatticeBootstrapEntryDeferredException"/>,
    /// which consumes a slot of the same budget whatever the host's classifier
    /// says (issue #4604). Once the budget is spent the bootstrap fails with the
    /// import already started, so the read fence stays up and the bootstrap is
    /// re-driven until a drain applies every entry.
    /// </para>
    /// </summary>
    private async Task DrainSnapshotAsync()
    {
        // Hand-rolled retry loop instead of delegating to
        // BoundedExponentialRetryPolicy.ExecuteAsync. The shared policy
        // internally uses ConfigureAwait(false), which strips the
        // Orleans single-threaded grain scheduler on the retry hop.
        // Subsequent state.WriteStateAsync() / grain calls inside
        // DrainSnapshotOnceAsync would then run off-grain and surface
        // as a hard failure (Orleans rejects grain-state writes from
        // foreign schedulers), defeating the entire purpose of the
        // retry. The grain-local loop below preserves
        // TaskScheduler.Current across every awaiter via the existing
        // ConfigureAwait(true) convention used throughout this grain.
        var (maxAttempts, initial, max, classifier) = ResolveRetryPolicy();

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await DrainSnapshotOnceAsync(CancellationToken.None).ConfigureAwait(true);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts
                && (ex is LatticeBootstrapEntryDeferredException || classifier(ex)))
            {
                var delay = ComputeBackoff(attempt, initial, max);
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay).ConfigureAwait(true);
                }
            }
        }
    }

    /// <summary>
    /// Resolves the retry policy parameters from
    /// <see cref="LatticeReplicationOptions.BootstrapTransientRetry"/>
    /// (falling back to the public default constants) and wraps the
    /// host-supplied (or default) classifier with the metric +
    /// structured-log emit. The classifier returns
    /// <see langword="true"/> for a classified-transient exception
    /// so the caller's retry loop consumes one slot; for any other
    /// shape the loop re-throws verbatim and
    /// <see cref="ProcessNextPhaseAsync"/>'s catch block pivots to
    /// <see cref="LatticeBootstrapState.Failed"/>.
    /// </summary>
    private (int MaxAttempts, TimeSpan InitialDelay, TimeSpan MaxDelay, Func<Exception, bool> Classifier)
        ResolveRetryPolicy()
    {
        var options = _optionsMonitor.Get(TreeName);
        var configured = options.BootstrapTransientRetry;

        var maxAttempts = configured?.MaxAttempts ?? LatticeReplicationOptions.DefaultBootstrapMaxAttempts;
        var initial = configured?.InitialDelay ?? LatticeReplicationOptions.DefaultBootstrapInitialRetryDelay;
        var max = configured?.MaxDelay ?? LatticeReplicationOptions.DefaultBootstrapMaxRetryDelay;
        var hostClassifier = configured?.RetryableExceptionClassifier
            ?? LatticeBootstrapTransientFaultClassifier.IsTransient;

        var treeName = TreeName;
        var sourceClusterId = state.State.SourceClusterId;
        bool ClassifyAndCount(Exception ex)
        {
            if (!hostClassifier(ex))
            {
                return false;
            }

            // Count classified-transient retries so a sustained
            // non-zero rate is visible on dashboards regardless of
            // whether the budget eventually exhausts. The counter
            // fires before the policy's Task.Delay, so each tick
            // matches one consumed retry slot.
            LatticeReplicationMetrics.BootstrapTransientRetries.Add(1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                LatticeTenantLabel.ForTree(treeName));

            Logger.LogWarning(ex,
                "Bootstrap drain for tree '{TreeName}' from source '{SourceClusterId}' encountered a transient fault; retrying within the configured bounded budget",
                treeName, sourceClusterId);

            return true;
        }

        return (maxAttempts, initial, max, ClassifyAndCount);
    }

    /// <summary>
    /// Computes the bounded-exponential backoff for the supplied
    /// 1-based attempt number. Mirrors
    /// <see cref="BoundedExponentialRetryPolicy"/>'s schedule so an
    /// operator who configures the policy via
    /// <see cref="LatticeReplicationOptions.BootstrapTransientRetry"/>
    /// observes the documented doubling cadence regardless of which
    /// loop drives the retry.
    /// </summary>
    private static TimeSpan ComputeBackoff(int attempt, TimeSpan initial, TimeSpan max)
    {
        var shift = attempt - 1;
        if (shift >= 31)
        {
            return max;
        }
        var multiplier = 1L << shift;
        var ticks = initial.Ticks * multiplier;
        if (ticks < 0 || ticks > max.Ticks)
        {
            return max;
        }
        return TimeSpan.FromTicks(ticks);
    }

    /// <summary>
    /// Performs a single attempt of the snapshot export + apply
    /// drain. Re-opens the full snapshot stream (no upper bound),
    /// applies every entry, and transitions to
    /// <see cref="LatticeBootstrapState.IncrementalHandoff"/> on a
    /// clean completion. A throw from this method either re-enters
    /// the retry policy (transient) or bubbles to
    /// <see cref="ProcessNextPhaseAsync"/>'s catch-block (non-transient
    /// / budget-exhausted).
    /// </summary>
    private async Task DrainSnapshotOnceAsync(CancellationToken cancellationToken)
    {
        var treeName = TreeName;
        var sourceClusterId = state.State.SourceClusterId;

        // Issue #4526: no reader may observe the import part-way. Arm the read
        // fence on every shard before anything is applied, and wait - without
        // failing - while a migration or resize would move the tree off the
        // shards it covers.
        if (!await ArmReadFenceAsync().ConfigureAwait(true))
        {
            return;
        }

        // Issue #4549: a tree that has never been written is registered by the
        // drain's first write, and registering it stamps its lineage, which
        // resets the tree's applied identities and with them the bootstrap drop
        // floor. Register it now, before the tree frontier's epoch is captured and
        // the floor installed, so neither is reset mid-drain.
        await EnsureTreeRegisteredAsync(treeName).ConfigureAwait(true);

        // Resolve the per-tree merge mode once up-front. The resolver
        // is O(1) (a cached dictionary read in the default
        // ConfiguredLatticeMergeModeResolver implementation) and the
        // mode is invariant for the lifetime of the drain - re-resolving
        // per entry would be both pointless and a hot-path allocation
        // risk. A `null` return from the resolver means "this tree is
        // not enumerated in ReplicatedTrees"; in that case we default
        // to LwwRegister, preserving the historical hardcode for trees
        // that bootstrap intra-cluster only without an explicit replication
        // declaration.
        var mergeMode = _mergeModeResolver.Resolve(treeName) ?? LatticeMergeMode.LwwRegister;
        var preCapture = await CaptureReceiverEntriesAsync(
                treeName,
                sourceClusterId,
                mergeMode,
                cancellationToken)
            .ConfigureAwait(true);

        // Pass sourceClusterId through to the snapshot provider so that
        // cross-cluster adapters (RemoteSnapshotProvider) can address
        // the correct sender peer. The default intra-cluster provider's
        // default interface implementation ignores the argument and
        // delegates to the two-arg overload, so this is a no-op for
        // hosts that do not register a cross-cluster adapter.
        //
        // Every attempt - the first, a transient retry, and a resume
        // after a crash - exports with NO upper bound. The export's
        // asOfHlc is a strict upper bound, not a resume point, and the
        // stream arrives in leaf-chain order rather than HLC order, so
        // LastAppliedHlc (the highest HLC seen so far) says nothing about
        // which entries are still outstanding. Passing it here dropped
        // every unapplied entry stamped above it, and nothing afterwards
        // is guaranteed to redeliver them: the handoff pin seals the
        // source-origin coordinate at the snapshot's causal-stable cut,
        // which can sit above them, so the incremental stream dedupes them
        // as already covered. Re-applying the overlap is a no-op under
        // per-key LWW.
        // Capture the tree frontier's epoch before the export is requested
        // (#4586 part 2b): the handoff pins the export only if no replacement
        // of the tree's contents happened in between.
        var frontierEpoch = (await _grainFactory.GetGrain<IReplicationTreeFrontierGrain>(treeName)
            .GetAsync(cancellationToken)
            .ConfigureAwait(true)).Epoch;

        // The barriers that already hold a sibling's arrival for an operation
        // this tree has not arrived at (#4684). Read before the export is
        // requested, so the export opens after every one of those arrivals.
        var crossTreeCandidates = await CaptureCrossTreeImportCandidatesAsync(treeName, sourceClusterId)
            .ConfigureAwait(true);

        var snapshot = await _snapshotProvider
            .ExportAsync(treeName, sourceClusterId, HybridLogicalClock.Zero, cancellationToken)
            .ConfigureAwait(true);

        // Update the durable handoff metadata to whatever the latest
        // export reports. On crash recovery this overwrites the prior
        // export's metadata - safe because the receiver will have
        // applied every entry up through the new export's AsOfHlc by
        // the time it reaches IncrementalHandoff, and recent
        // exact-identity dedupe and per-key LWW merge make any overlap safe.
        state.State.SnapshotAsOfHlc = snapshot.AsOfHlc;
        state.State.CausalStableFrontier = snapshot.CausalStableFrontier;
        state.State.SnapshotExportEpoch = snapshot.ExportEpoch;
        state.State.FrontierEpoch = frontierEpoch;
        state.State.ExportedFrontier = null;
        // Whether the receiver held no source-origin row before this import began
        // (issue #4537). Captured only while no entry of the import has been
        // applied: a resumed or re-driven drain of a partial import would see its
        // own rows, so it keeps the value its first attempt recorded.
        if (!state.State.ImportApplied)
        {
            state.State.HeldNoSourceRowsAtImportStart = preCapture.HeldNoSourceRows;
        }

        // From here the tree holds the export's lineage, so a pushed batch the
        // source read under any other lineage is refused (issue #4673). Recorded
        // at drain open, durable with the write below, so a stale push that
        // escapes it must straddle the whole drain, where the end-of-drain scan
        // finds the row it left. Kept when the reconcile below skips.
        RecordDrainedLineage(sourceClusterId, snapshot.OpenGeneration);

        // Recorded before the first entry is applied: from here on the tree may
        // hold a partial import, so a failure keeps the read fence up (#4526).
        state.State.ImportApplied = true;
        state.State.EntriesApplied = 0;
        var pivotedToApplying = false;
        if (state.State.Phase != LatticeBootstrapState.ApplyingSnapshot)
        {
            state.State.Phase = LatticeBootstrapState.ApplyingSnapshot;
            pivotedToApplying = true;
        }
        await state.WriteStateAsync().ConfigureAwait(true);

        // Issue #4549. Installed before the drain, and only once the import is
        // recorded: from here on a failure keeps the tree fenced and re-drives
        // the bootstrap until a drain completes, so the floor never outlives an
        // abandoned import.
        var floorInstalled = await InstallBootstrapFloorAsync(treeName, sourceClusterId, snapshot, cancellationToken).ConfigureAwait(true);

        // Lazy-initialise the duration anchor on resume after a silo
        // failover: TryInitiateBootstrapAsync set it on kickoff, but a
        // crashed activation that reactivates here would otherwise
        // produce a null timer and skip the terminal duration record.
        _drainStartTimestamp ??= Stopwatch.GetTimestamp();

        if (pivotedToApplying)
        {
            Logger.LogInformation(
                "Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': RequestingSnapshot -> ApplyingSnapshot (LastAppliedHlc={LastAppliedHlc})",
                treeName, sourceClusterId, state.State.LastAppliedHlc);
        }

        int sinceLastPersist = 0;
        var carriedKeys = new HashSet<string>(StringComparer.Ordinal);

        // Open the bootstrap-drain ambient scope ONCE for the entire
        // drain rather than per entry. The scope is invariant across
        // every <see cref="IReplicationApplier.ApplyAsync"/> call in
        // this loop, so a per-entry <c>BeginScope()</c> would generate
        // one scope value per snapshot row - millions of redundant
        // operations on a large snapshot. Hoisting also makes the
        // scope's lifetime exactly match the drain's lifetime: the
        // <c>using</c> deterministically restores the prior ambient
        // value before the post-drain
        // <see cref="PinAndCompleteAsync"/> tick re-enters
        // <see cref="ProcessNextPhaseAsync"/>. The
        // <see cref="LatticeReplicationOptions.BootstrapTransientRetry"/>
        // outer retry loop reopens the scope on every retry attempt
        // (the catch unwinds the <c>using</c> normally), so the flag
        // is also correctly restored on a fault path.
        //
        // The applier-side bypass is documented at length on
        // <see cref="LatticeBootstrapApplyContext"/>; the short version
        // is: the snapshot exporter walks shards/leaves in arbitrary
        // order, so applying the steady-state per-origin HWM gate to
        // bootstrap entries can drop a still-pending saga key with a
        // strictly-earlier source HLC and break per-saga all-or-nothing
        // visibility on the bootstrapped peer. The post-drain
        // <see cref="Grains.IReplicationHighWaterMarkGrain.MergeBootstrapFrontierAsync"/>
        // in <see cref="PinAndCompleteAsync"/> atomically installs the
        // per-origin HWM at the snapshot's AsOfHlc, so steady-state
        // dedup is preserved across the bootstrap-to-incremental
        // handoff. Receiver-side idempotency during the drain is
        // upheld by leaf-level LWW and the per-leaf / per-tree saga
        // dedup primitives.
        using var bootstrapScope = LatticeBootstrapApplyContext.BeginScope();

        await CapturePoisonedSagasBeforeDrainAsync(treeName, sourceClusterId).ConfigureAwait(true);
        HashSet<Guid>? shippedPrepared = null;

        // The sagas the export carries, as prepared rows or decision rows: a
        // re-seed clears every other pending bucket from the source (#4533).
        // Collected on every drain, because a re-seed request can be recorded
        // while the drain runs.
        var carriedSagas = new HashSet<Guid>();
        var decidedSagas = new Dictionary<Guid, bool>();
        var crossTreeBarriers = new HashSet<string>(StringComparer.Ordinal);
        var namedCrossTreeOperations = new HashSet<string>(StringComparer.Ordinal);

        await foreach (var entry in snapshot.Entries.ConfigureAwait(true))
        {
            if (!string.IsNullOrEmpty(entry.CrossTreeOperationId))
            {
                namedCrossTreeOperations.Add(entry.CrossTreeOperationId);
            }

            if (entry.TransactionId != Guid.Empty)
            {
                if (entry.SettledDecision is { } settled)
                {
                    decidedSagas[entry.TransactionId] = settled;
                }
                else if (entry.IsPrepared || entry.Value is null)
                {
                    // A prepared row, or a value-less row naming a saga the
                    // source knows but cannot settle.
                    carriedSagas.Add(entry.TransactionId);
                }
            }

            if (entry.IsDecision)
            {
                await ApplySettledDecisionAsync(_grainFactory, treeName, entry).ConfigureAwait(true);

                // A cross-tree sub-saga's decision replaces its terminal here, so
                // it arrives at the receiver's barrier as the terminal would
                // (#4683). Recorded only after the decision, so a sibling the
                // barrier finalizes never sees this tree undecided.
                if (await NotifyImportedCrossTreeArrivalAsync(treeName, sourceClusterId, entry, cancellationToken)
                        .ConfigureAwait(true) is { } barrier)
                {
                    crossTreeBarriers.Add(barrier);
                }

                continue;
            }

            carriedKeys.Add(entry.Key);

            if (entry.IsPrepared && entry.TransactionId != Guid.Empty)
            {
                (shippedPrepared ??= new HashSet<Guid>()).Add(entry.TransactionId);
            }

            if (ToSnapshotWalRecord(entry, treeName, sourceClusterId, mergeMode) is not { } record)
            {
                continue;
            }

            var applied = await _replicationApplier.ApplyAsync(record, cancellationToken).ConfigureAwait(true);
            if (applied.Deferred)
            {
                // Issue #4604. Deferred means "not applied by this delivery and
                // must be re-shipped", and the drain is the delivery: nothing
                // else re-sends a snapshot row, and completing the drain would
                // pin the frontier past it. Stop here - before the entry is
                // counted or its clock folded into the handoff seal - and fail
                // the attempt, so the full snapshot is re-exported and
                // re-drained once whatever deferred it (a coordinated restore's
                // receive fence, a restored copy's fence, an in-flight
                // duplicate) has let go. Re-applying the overlap is a no-op.
                Logger.LogWarning(
                    "Bootstrap drain of tree '{TreeName}' from source '{SourceClusterId}': the applier deferred the snapshot entry for key '{Key}', so this attempt stops and the snapshot is re-drained rather than completing past it",
                    treeName, sourceClusterId, entry.Key);
                throw new LatticeBootstrapEntryDeferredException(treeName, entry.Key);
            }

            state.State.EntriesApplied++;

            // Bootstrap progress instruments: increment once per
            // successfully-applied entry so operators can watch
            // entries/second and bytes/second in real time without
            // waiting for the terminal duration histogram.
            LatticeReplicationMetrics.BootstrapEntriesReceived.Add(1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                LatticeTenantLabel.ForTree(treeName));
            var byteCount = entry.Value?.Length ?? 0;
            LatticeReplicationMetrics.BootstrapBytesReceived.Add(byteCount,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                LatticeTenantLabel.ForTree(treeName));

            // Track the highest source HLC observed; PinAndCompleteAsync
            // folds it into the source-origin seal. Persisting it in
            // batches is safe because a resume re-exports the full stream
            // and never narrows the export to this cursor.
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

        await ReconcileReapedSourceDeletesAsync(
                preCapture,
                carriedKeys,
                snapshot.OpenGeneration,
                snapshot.CloseGeneration,
                mergeMode,
                cancellationToken)
            .ConfigureAwait(true);

        await ReconcileForeignDeletesAsync(
                treeName,
                sourceClusterId,
                snapshot,
                carriedKeys,
                mergeMode,
                floorInstalled,
                cancellationToken)
            .ConfigureAwait(true);

        // The export's applied frontier describes the imported contents only if
        // the source generation held still under its lineage (#4586 part 2b).
        // Persisted by the drain-end write; the handoff pins it.
        state.State.ExportedFrontier = BootstrapFrontierInstall.Decide(
            snapshot.OpenGeneration,
            snapshot.CloseGeneration,
            snapshot.SourceFrontier);

        // Settle the poisoned sagas this re-seed was asked for (#4591). A saga
        // the export shipped as prepared rows was still in flight at the source:
        // its staged buckets (the receiver's own pre-poison ones and the export's)
        // are kept, and its terminal - withheld while the poison held - arrives
        // once the poison retires and commits it whole. Any other poisoned saga
        // is decided (the export shipped its outcome as committed rows and its
        // decision row) or gone from the source, so the buckets the receiver
        // staged before the poison can never be drained and are discarded. Done
        // before the phase moves on, so a crash re-runs the whole drain.
        await DiscardSettledPoisonedSagasAsync(treeName, sourceClusterId, shippedPrepared).ConfigureAwait(true);

        // A re-seed the export postdates: the sender has withheld every saga
        // record since before the export, so every pending bucket from it is
        // either carried by the export or stale. Still behind the read fence.
        if (state.State.ReseedAfterEpochs.TryGetValue(sourceClusterId, out var reseedAfter) && snapshot.ExportEpoch > reseedAfter)
        {
            var cleared = await StalePendingClearer
                .ClearAsync(_grainFactory, treeName, sourceClusterId, carriedSagas, decidedSagas, cancellationToken)
                .ConfigureAwait(true);
            if (cleared > 0)
            {
                Logger.LogWarning(
                    "Re-seed of tree '{TreeName}' from '{SourceClusterId}' settled {Count} leftover pending saga(s): each decided "
                    + "one by its decision, and each the source purged discarded, its committed values carried by the export.",
                    treeName, sourceClusterId, cleared);
            }

            // Consumed; persisted with the phase transition below.
            state.State.ReseedAfterEpochs.Remove(sourceClusterId);
        }

        // The import settled cross-tree sub-sagas whose sibling trees may still
        // be pre-saga here: the tree stays read-fenced until every barrier it
        // arrived at has decided (#4683). Persisted with the phase below.
        await RecordUnnamedCrossTreeArrivalsAsync(
                treeName, crossTreeCandidates, namedCrossTreeOperations, crossTreeBarriers, cancellationToken)
            .ConfigureAwait(true);
        state.State.PendingCrossTreeBarriers = await UndecidedBarriersAsync(crossTreeBarriers).ConfigureAwait(true);

        // Every snapshot entry is applied: the import is whole, so lift the read
        // fence before leaving the drain (issue #4526) - unless a cross-tree
        // barrier above holds it. Lifted before the phase is persisted: a crash
        // in between resumes the drain, which re-arms the fence and re-applies
        // the (idempotent) import.
        if (state.State.PendingCrossTreeBarriers.Count == 0)
        {
            await LiftReadFenceAsync().ConfigureAwait(true);
        }

        state.State.Phase = LatticeBootstrapState.IncrementalHandoff;
        await state.WriteStateAsync().ConfigureAwait(true);

        Logger.LogInformation(
            "Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': ApplyingSnapshot -> IncrementalHandoff (LastAppliedHlc={LastAppliedHlc})",
            treeName, sourceClusterId, state.State.LastAppliedHlc);
    }

    private async Task<BootstrapReceiverPreCapture> CaptureReceiverEntriesAsync(
        string treeName,
        string sourceClusterId,
        LatticeMergeMode mergeMode,
        CancellationToken cancellationToken)
    {
        var capture = new BootstrapReceiverPreCapture();
        var registry = _grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(true);
        var shardMap = await registry.GetShardMapAsync(treeName).ConfigureAwait(true)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);
        var everything = new VersionVector();

        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = _grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(true);
            while (leafId is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leaf = _grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                var delta = await leaf.GetDeltaSinceAsync(everything).ConfigureAwait(true);
                foreach (var row in delta.Entries.Values)
                {
                    if (string.Equals(row.OriginClusterId, sourceClusterId, StringComparison.Ordinal))
                    {
                        capture.SourceRowCount++;
                    }
                }

                var liveEntries = await leaf.GetLiveRawEntriesAsync().ConfigureAwait(true);
                foreach (var entry in liveEntries)
                {
                    if (entry.ExpiresAtTicks != 0
                        || !string.Equals(entry.OriginClusterId, sourceClusterId, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    capture.SourceEntries[entry.Key] = new BootstrapCapturedEntry(
                        entry.Timestamp,
                        entry.MergeMode ?? mergeMode);
                }

                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(true);
            }
        }

        return capture;
    }

    private async Task ReconcileReapedSourceDeletesAsync(
        BootstrapReceiverPreCapture preCapture,
        HashSet<string> carriedKeys,
        SnapshotSourceGeneration? openGeneration,
        SnapshotSourceGeneration? closeGeneration,
        LatticeMergeMode mergeMode,
        CancellationToken cancellationToken)
    {
        var treeName = TreeName;
        var sourceClusterId = state.State.SourceClusterId;
        state.State.AlignedLineageBySource.TryGetValue(sourceClusterId, out var alignedLineage);
        var decision = BootstrapDeleteReconcile.Decide(
            isScopedExport: false,
            openGeneration,
            closeGeneration,
            alignedLineage == Guid.Empty ? null : alignedLineage,
            state.State.HeldNoSourceRowsAtImportStart,
            preCapture.SourceEntries.Keys.Any(key => !carriedKeys.Contains(key)),
            await AnySourceRowAbsentFromExportAsync(treeName, sourceClusterId, carriedKeys, mergeMode, cancellationToken)
                .ConfigureAwait(true),
            mergeMode);

        RecordBootstrapReconcile(treeName, sourceClusterId, decision.Outcome);

        if (decision.OweRetry)
        {
            state.State.ReconcileOwedBySource[sourceClusterId] = true;
        }
        else
        {
            state.State.ReconcileOwedBySource.Remove(sourceClusterId);
            state.State.OwedRetryAttemptsBySource.Remove(sourceClusterId);
            state.State.OwedRetryNotBeforeTicksBySource.Remove(sourceClusterId);
        }

        if (decision.RecordAlignedLineage && openGeneration?.Lineage is { } lineage)
        {
            state.State.AlignedLineageBySource[sourceClusterId] = lineage;
            RecordDrainedLineage(sourceClusterId, openGeneration);
        }

        // The aligned-lineage and owed slots are persisted by the drain-end write
        // that follows. A crash before it resumes the drain, which re-exports,
        // re-captures, and decides again from the persisted import-start state.
        if (!decision.ShouldReconcile)
        {
            return;
        }

        foreach (var (key, captured) in preCapture.SourceEntries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (carriedKeys.Contains(key))
            {
                continue;
            }

            var record = new WalRecord
            {
                TreeId = treeName,
                Op = MutationKind.Delete,
                Key = key,
                Value = null,
                Timestamp = captured.Timestamp,
                IsTombstone = true,
                OriginClusterId = sourceClusterId,
                Mode = captured.MergeMode,
            };
            var applied = await _replicationApplier.ApplyAsync(record, cancellationToken).ConfigureAwait(true);
            if (applied.Deferred)
            {
                // Same rule as a deferred snapshot row (issue #4604): nothing else
                // re-sends this delete, so fail the attempt and re-drain.
                throw new LatticeBootstrapEntryDeferredException(treeName, key);
            }
        }
    }

    /// <summary>
    /// Installs the bootstrap drop floor from the frontier the export carried
    /// when it opened (issue #4549), or clears any earlier floor when the export
    /// carries none read under its opening lineage. Returns whether a floor is
    /// in force for this drain.
    /// <para>
    /// The floor starts provisional, so a delivery below it is deferred until the
    /// import closes stable. The install bumps the tree's floor epoch; the epoch
    /// is raised in the tree registry and every shard root is armed with it, which
    /// returns once each has finished the writes it admitted under an older epoch
    /// and refuses any later one. So no write admitted before the install lands
    /// after the reconcile scan. The source's own origin is never floored: its
    /// writes are re-shipped in offset order with their deletes.
    /// </para>
    /// </summary>
    private async Task<bool> InstallBootstrapFloorAsync(
        string treeName,
        string sourceClusterId,
        SnapshotStream snapshot,
        CancellationToken cancellationToken)
    {
        var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
        if (!BootstrapForeignDeleteReconcile.FrontierMatchesOpen(snapshot.OpenFrontier, snapshot.OpenGeneration))
        {
            await hwm.ClearBootstrapFloorAsync(cancellationToken).ConfigureAwait(true);
            return false;
        }

        var frontier = snapshot.OpenFrontier!;
        var lowWatermarks = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        foreach (var (origin, lowWatermark) in frontier.LowWatermarks)
        {
            if (!string.Equals(origin, sourceClusterId, StringComparison.Ordinal))
            {
                lowWatermarks[origin] = lowWatermark;
            }
        }

        var epoch = await hwm.SetBootstrapFloorAsync(lowWatermarks, frontier.Held, cancellationToken).ConfigureAwait(true);

        var registry = _grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(true);
        await registry.RaiseReplicationFloorEpochAsync(physicalTreeId, epoch).ConfigureAwait(true);
        var shardMap = await registry.GetShardMapAsync(treeName).ConfigureAwait(true)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);
        var arms = new List<Task>();
        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            arms.Add(_grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}")
                .ArmReplicationFloorEpochAsync(epoch));
        }

        await Task.WhenAll(arms).ConfigureAwait(true);
        return true;
    }

    /// <summary>
    /// Deletes the receiver's rows of origins other than the source whose keys
    /// the export lacks and whose writes the source had applied before the
    /// export opened (issue #4549), then makes the floor final. Pending saga
    /// prepares are covered too: the stale saga's bucket on that leaf is
    /// discarded, so its later commit installs nothing. Scanned after the
    /// shards were armed, so every write admitted before the floor was installed
    /// is seen, and every later one below the floor was deferred. A floor
    /// installed for an export that turned out unstable is cleared instead, so
    /// the deferred deliveries apply when re-shipped, and the reconcile owes a
    /// retry that installs a fresh one.
    /// </summary>
    private async Task ReconcileForeignDeletesAsync(
        string treeName,
        string sourceClusterId,
        SnapshotStream snapshot,
        HashSet<string> carriedKeys,
        LatticeMergeMode mergeMode,
        bool floorInstalled,
        CancellationToken cancellationToken)
    {
        if (!floorInstalled)
        {
            return;
        }

        var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
        if (!BootstrapForeignDeleteReconcile.IsEligible(
                snapshot.OpenFrontier, snapshot.OpenGeneration, snapshot.CloseGeneration, mergeMode))
        {
            await hwm.ClearBootstrapFloorAsync(cancellationToken).ConfigureAwait(true);
            state.State.ReconcileOwedBySource[sourceClusterId] = true;
            Logger.LogWarning(
                "Bootstrap of tree '{TreeName}' from '{SourceClusterId}': the source tree changed during the export, so the "
                + "bootstrap drop floor is cleared and the reconcile is owed a retry",
                treeName, sourceClusterId);
            return;
        }

        var frontier = snapshot.OpenFrontier!;
        var localClusterId = _optionsMonitor.Get(treeName).ClusterId;
        var registry = _grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(true);
        var shardMap = await registry.GetShardMapAsync(treeName).ConfigureAwait(true)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);
        var allSlots = new int[shardMap.VirtualShardCount];
        for (var i = 0; i < allSlots.Length; i++)
        {
            allSlots[i] = i;
        }

        var doomed = new HashSet<(string Key, HybridLogicalClock Timestamp)>();
        var staleSagas = new HashSet<(GrainId Leaf, Guid TransactionId)>();
        var orphanAboveWatermark = false;
        bool Doomed(string key, string? rowOrigin, HybridLogicalClock timestamp, long expiresAtTicks, LatticeMergeMode mode)
        {
            if (expiresAtTicks != 0 || mode != LatticeMergeMode.LwwRegister || carriedKeys.Contains(key))
            {
                return false;
            }

            // A local write is stored without an origin; the source keys it by this cluster's id.
            var origin = string.IsNullOrEmpty(rowOrigin) ? localClusterId : rowOrigin;
            if (string.Equals(origin, sourceClusterId, StringComparison.Ordinal))
            {
                return false;
            }

            switch (BootstrapForeignDeleteReconcile.Classify(frontier, origin, timestamp))
            {
                case ForeignOrphanVerdict.Delete:
                    return true;
                case ForeignOrphanVerdict.Owed:
                    orphanAboveWatermark = true;
                    return false;
                default:
                    return false;
            }
        }

        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = _grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(true);
            while (leafId is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leaf = _grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                foreach (var entry in await leaf.GetLiveRawEntriesAsync().ConfigureAwait(true))
                {
                    if (Doomed(entry.Key, entry.OriginClusterId, entry.Timestamp, entry.ExpiresAtTicks, entry.MergeMode ?? mergeMode))
                    {
                        doomed.Add((entry.Key, entry.Timestamp));
                    }
                }

                foreach (var pending in await leaf.GetPendingMutationsForSlotsAsync(allSlots, shardMap.VirtualShardCount).ConfigureAwait(true))
                {
                    if (pending.TransactionId != Guid.Empty
                        && !pending.IsTombstone
                        && Doomed(pending.Key, pending.OriginClusterId, pending.Timestamp, pending.ExpiresAtTicks, pending.Mode))
                    {
                        staleSagas.Add((leafId.Value, pending.TransactionId));
                    }
                }

                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(true);
            }
        }

        foreach (var (key, timestamp) in doomed)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Stamped with the source's id, which vouches for the delete: the
            // applier never applies an entry stamped with this cluster's own id.
            var record = new WalRecord
            {
                TreeId = treeName,
                Op = MutationKind.Delete,
                Key = key,
                Value = null,
                Timestamp = timestamp,
                IsTombstone = true,
                OriginClusterId = sourceClusterId,
                Mode = LatticeMergeMode.LwwRegister,
            };
            var applied = await _replicationApplier.ApplyAsync(record, cancellationToken).ConfigureAwait(true);
            if (applied.Deferred)
            {
                throw new LatticeBootstrapEntryDeferredException(treeName, key);
            }
        }

        // A pending prepare the export lacks belongs to a saga the source had
        // already decided: a saga still open there is exported as prepared rows.
        // The export carries its outcome, so the bucket is stale. It is dropped,
        // durably, rather than shadowed by a tombstone: an unmarked replicated
        // prepare is superseded only by a row stamped strictly above it, so a
        // tombstone at its stamp would not hide it, and one above could beat a
        // legitimate later write.
        foreach (var (leafId, transactionId) in staleSagas)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _grainFactory.GetGrain<IBPlusLeafGrain>(leafId)
                .DiscardPendingTransactionAsync(transactionId)
                .ConfigureAwait(true);
        }

        if (doomed.Count > 0 || staleSagas.Count > 0)
        {
            Logger.LogWarning(
                "Bootstrap of tree '{TreeName}' from '{SourceClusterId}' deleted {Count} row(s) and discarded {Sagas} stale saga "
                + "bucket(s) of other origins that the source had applied and then deleted before the export",
                treeName, sourceClusterId, doomed.Count, staleSagas.Count);
        }

        // An orphan at or above its origin's watermark may be a write the source
        // applied during the export and then deleted, or one still on its way
        // there, so this export proves nothing about it. The watermark rises as
        // delivery proceeds, so the reconcile is owed a retry that settles it.
        if (orphanAboveWatermark)
        {
            state.State.ReconcileOwedBySource[sourceClusterId] = true;
        }

        await hwm.FinalizeBootstrapFloorAsync(cancellationToken).ConfigureAwait(true);
    }

    /// <summary>
    /// Whether, now the drain is over, the receiver holds a live, non-expiring
    /// source-origin row whose key the export did not carry (issue #4549). Such a
    /// row reached the receiver during the drain - after a source restore it may
    /// be from an older lineage - so the receiver may not align with the export's
    /// lineage over it.
    /// </summary>
    private async Task<bool> AnySourceRowAbsentFromExportAsync(
        string treeName,
        string sourceClusterId,
        HashSet<string> carriedKeys,
        LatticeMergeMode mergeMode,
        CancellationToken cancellationToken)
    {
        var registry = _grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(true);
        var shardMap = await registry.GetShardMapAsync(treeName).ConfigureAwait(true)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);
        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = _grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(true);
            while (leafId is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leaf = _grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                foreach (var entry in await leaf.GetLiveRawEntriesAsync().ConfigureAwait(true))
                {
                    if (entry.ExpiresAtTicks == 0
                        && (entry.MergeMode ?? mergeMode) == LatticeMergeMode.LwwRegister
                        && string.Equals(entry.OriginClusterId, sourceClusterId, StringComparison.Ordinal)
                        && !carriedKeys.Contains(entry.Key))
                    {
                        return true;
                    }
                }

                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(true);
            }
        }

        return false;
    }

    private async Task EnsureTreeRegisteredAsync(string treeName)
    {
        var registry = _grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(true);
        if (!await registry.ExistsAsync(physicalTreeId).ConfigureAwait(true))
        {
            await registry.RegisterAsync(physicalTreeId).ConfigureAwait(true);
        }
    }

    private async Task CapturePoisonedSagasBeforeDrainAsync(string treeName, string sourceClusterId)
    {
        if (string.IsNullOrEmpty(sourceClusterId))
        {
            return;
        }

        if (!string.Equals(state.State.PoisonSettleOriginClusterId, sourceClusterId, StringComparison.Ordinal))
        {
            state.State.PoisonSettleOriginClusterId = "";
            state.State.PoisonSettleTransactionIds = new List<Guid>();
        }

        if (state.State.PoisonSettleTransactionIds.Count == 0)
        {
            var poisoned = await _grainFactory.GetGrain<IReceiverSagaPoisonGrain>(treeName)
                .GetPoisonedAsync(sourceClusterId)
                .ConfigureAwait(true);
            if (poisoned.Count == 0)
            {
                return;
            }

            state.State.PoisonSettleOriginClusterId = sourceClusterId;
            state.State.PoisonSettleTransactionIds = poisoned.Distinct().ToList();
            await state.WriteStateAsync().ConfigureAwait(true);
        }
    }

    private async Task DiscardSettledPoisonedSagasAsync(
        string treeName,
        string sourceClusterId,
        HashSet<Guid>? shippedPrepared)
    {
        if (state.State.PoisonSettleTransactionIds.Count == 0
            || !string.Equals(state.State.PoisonSettleOriginClusterId, sourceClusterId, StringComparison.Ordinal))
        {
            return;
        }

        var settled = shippedPrepared is null
            ? state.State.PoisonSettleTransactionIds
            : state.State.PoisonSettleTransactionIds.Where(t => !shippedPrepared.Contains(t)).ToList();
        await DiscardPendingTransactionsFromLeavesAsync(treeName, settled).ConfigureAwait(true);
    }

    private async Task DiscardPendingTransactionsFromLeavesAsync(
        string treeName,
        IReadOnlyCollection<Guid> transactionIds)
    {
        if (transactionIds.Count == 0)
        {
            return;
        }

        var registry = _grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(true);
        var shardMap = await registry.GetShardMapAsync(treeName).ConfigureAwait(true)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);

        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            var shard = _grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(true);
            while (leafId is not null)
            {
                var leaf = _grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                foreach (var transactionId in transactionIds)
                {
                    await leaf.DiscardPendingTransactionAsync(transactionId).ConfigureAwait(true);
                }

                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(true);
            }
        }
    }

    private async Task RetireSettledPoisonedSagasAsync(string treeName, string sourceClusterId)
    {
        if (state.State.PoisonSettleTransactionIds.Count == 0
            || !string.Equals(state.State.PoisonSettleOriginClusterId, sourceClusterId, StringComparison.Ordinal))
        {
            return;
        }

        await _grainFactory.GetGrain<IReceiverSagaPoisonGrain>(treeName)
            .RetireAsync(sourceClusterId, state.State.PoisonSettleTransactionIds)
            .ConfigureAwait(true);
        state.State.PoisonSettleOriginClusterId = "";
        state.State.PoisonSettleTransactionIds = new List<Guid>();
    }

    /// <summary>
    /// Pins the snapshot's as-of HLC and causal-stable frontier on
    /// the per-tree <see cref="IReplicationHighWaterMarkGrain"/> and
    /// completes the bootstrap. The pin is the snapshot/incremental handoff seam.
    /// It installs the dependency vector but no drop floor (#4463): incremental
    /// writes the snapshot already holds are absorbed by recent exact-identity
    /// dedupe and per-key LWW idempotency, and writes it does not hold apply.
    /// </summary>
    private async Task PinAndCompleteAsync()
    {
        // The import stays read-fenced until every cross-tree barrier it
        // arrived at has decided (#4683); the phase timer re-checks.
        if (!await ReleaseCrossTreeHoldAsync().ConfigureAwait(true))
        {
            return;
        }

        var treeName = TreeName;
        var sourceClusterId = state.State.SourceClusterId;
        var asOfHlc = state.State.SnapshotAsOfHlc;
        var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);

        // Seal the source cluster's own consumption coordinate into the
        // pinned frontier at the snapshot's causal-stable cut. The cut is
        // the maximum coordinate across the CausalStableFrontier: every
        // entry the snapshot materialises - including the source-origin
        // baseline entries appended to the local WAL during apply - is at
        // or below it. A CausalStableFrontier, however, carries a
        // coordinate for an origin only when that origin authored data;
        // when the source authored nothing of its own its frontier has no
        // self entry, leaving HWM[source]=0 after the pin. The fall-off
        // detector then reads every retained source-origin baseline as a
        // trim gap and re-triggers bootstrap on every probe, looping
        // forever (worse under a durable WAL, which never discards the
        // baselines). The snapshot's AsOfHlc cannot be used as the seal:
        // the export echoes back the upper bound it was opened with, and the
        // drain always opens it unbounded, so it is zero.
        // Pinning the vector for source at the cut restores the
        // invariant that HWM[source] covers every locally-retained
        // source-origin entry. The seal is NOT a drop threshold (#4463):
        // writes the source makes after the export on a leaf whose clock is
        // at or below the cut are not in the snapshot and must still apply.
        var frontier = state.State.CausalStableFrontier;
        var cut = HybridLogicalClock.Zero;
        foreach (var clock in frontier.Entries.Values)
        {
            if (clock.CompareTo(cut) > 0)
            {
                cut = clock;
            }
        }

        // Fold the applied-entry coordinate into the cut. Every snapshot
        // entry is appended to the local WAL stamped OriginClusterId=source
        // (see ApplyEntriesAsync), so LastAppliedHlc is the maximum
        // source-attributed HLC the bootstrap materialised - an authoritative,
        // receiver-local lower bound for the source-origin seal. The
        // causal-stable frontier alone is the consumer-ack meet
        // (min over consumer VCs) and can omit or zero the source coordinate
        // whenever a consumer ack lags: a cold bootstrap, or a stuck receiver
        // whose own lagging VC feeds back into the producer's meet. Sealing
        // HWM[source] below the entries just applied makes the fall-off
        // detector read the retained source-origin baselines as a perpetual
        // trim gap and re-bootstrap forever (worse under a durable WAL, which
        // never discards the baselines). Folding LastAppliedHlc into the cut
        // closes that gap without trusting the producer to have populated the
        // frontier's source component.
        if (state.State.LastAppliedHlc.CompareTo(cut) > 0)
        {
            cut = state.State.LastAppliedHlc;
        }

        // Fold the receiver's own oldest-retained source-origin WAL
        // coordinate into the cut. The fall-off detector
        // (LatticeFallOffLogDetector) declares a fall-off whenever
        // HWM[source] is strictly below the oldest entry the source authored
        // that the *local* WAL still retains - and
        // ILatticeWalIntrospection.GetOldestAvailableHlcByOriginAsync is a
        // purely local, per-origin reading of that WAL. Applied remote
        // entries (including source-origin tombstones that delete a key and
        // so never re-materialise as a live snapshot entry) are appended to
        // the local WAL with their authoring origin preserved. Such a
        // tombstone can sit strictly above LastAppliedHlc (the max *live*
        // snapshot entry the bootstrap re-materialised), so sealing only at
        // the applied cursor still leaves HWM[source] below the retained
        // tombstone and the detector re-bootstraps on every probe forever
        // (observed live against the MultiSiteManufacturing sample: localHwm
        // frozen one entry below senderOldest while the loop never settled).
        // Sealing at the very value the detector probes guarantees, by
        // construction, that HWM[source] is at or above the local oldest
        // source entry after the pin, so the false-positive fall-off cannot
        // recur. This is safe because every locally-retained source entry has
        // already been applied (a trimmed prefix is by definition durably
        // consumed), so the seal never advances past unapplied data.
        if (!string.IsNullOrEmpty(sourceClusterId))
        {
            var localOldestByOrigin = await _walIntrospection
                .GetOldestAvailableHlcByOriginAsync(treeName, CancellationToken.None)
                .ConfigureAwait(true);
            if (localOldestByOrigin.TryGetValue(sourceClusterId, out var localOldestSource)
                && localOldestSource.CompareTo(cut) > 0)
            {
                cut = localOldestSource;
            }
        }

        if (!string.IsNullOrEmpty(sourceClusterId)
            && cut.CompareTo(frontier.GetClock(sourceClusterId)) > 0)
        {
            frontier = frontier.Clone();
            frontier.Entries[sourceClusterId] = cut;
        }

        // Idempotent: MergeBootstrapFrontierAsync raises the per-origin
        // high-water-mark vector to the pointwise maximum of what it already
        // holds and the supplied frontier, and installs no drop floor (it does
        // not consult asOfHlc), so a crash between this call and the
        // WriteStateAsync below replays safely on reactivation. It must not
        // REPLACE the vector (#4464): this receiver may already have applied
        // an origin's writes above the source's frontier, and moving the
        // vector backwards would strand an entry parked on a dependency those
        // writes met.
        await hwm
            .MergeBootstrapFrontierAsync(asOfHlc, frontier, CancellationToken.None)
            .ConfigureAwait(true);

        // Install the export on the tree frontier (#4586 part 2b): the tree now
        // reflects the source's contents, which ends a re-seed the tree was
        // awaiting after a replacement of its contents. Each origin starts from
        // the export's watermark for it - zero when the export carried none or
        // its source generation moved - and rises on the watermarks its own
        // sender ships from here on. Refused
        // when the contents were replaced during the drain; the replacement's
        // own forced gap brings a fresh bootstrap.
        var pinned = await _grainFactory.GetGrain<IReplicationTreeFrontierGrain>(treeName)
            .PinAsync(
                state.State.FrontierEpoch,
                state.State.ExportedFrontier?.LowWatermarks ?? EmptyFrontierWatermarks,
                state.State.ExportedFrontier?.Held ?? EmptyFrontierHeld,
                CancellationToken.None)
            .ConfigureAwait(true);
        if (!pinned)
        {
            Logger.LogInformation(
                "Bootstrap of tree {Tree} from {Source} completed, but the tree frontier did not install it: its contents were replaced during the drain or it is in degraded mode.",
                treeName,
                sourceClusterId);
        }

        // Re-arm the causal-apply buffer: the merge can satisfy a parked
        // entry's dependencies, and a later apply of the dependency itself no
        // longer advances the vector, so nothing else would drain it (#4464).
        await _grainFactory.GetGrain<ICausalApplyBufferGrain>(treeName)
            .DrainAsync()
            .ConfigureAwait(true);

        await RetireSettledPoisonedSagasAsync(treeName, sourceClusterId).ConfigureAwait(true);

        state.State.Phase = LatticeBootstrapState.LiveIncremental;
        state.State.InProgress = false;
        // Record the completed full re-seed for the sender's ack echo (#4534).
        // Not while a re-seed request the drain did not consume is outstanding
        // (recorded after the drain finished): the echo would release the
        // sender's saga records over stale pending buckets (#4533). The sender's
        // next request starts the bootstrap that consumes it.
        if (state.State.SnapshotExportEpoch > 0
            && !string.IsNullOrEmpty(state.State.SourceClusterId)
            && !state.State.ReseedAfterEpochs.ContainsKey(state.State.SourceClusterId))
        {
            var source = state.State.SourceClusterId;
            if (!state.State.CompletedExportEpochs.TryGetValue(source, out var previous)
                || previous < state.State.SnapshotExportEpoch)
            {
                state.State.CompletedExportEpochs[source] = state.State.SnapshotExportEpoch;
            }
        }
        await state.WriteStateAsync().ConfigureAwait(true);

        // Terminal duration recording: outcome=live. Reset the anchor
        // so a subsequent re-bootstrap on the same activation does not
        // double-count.
        RecordBootstrapDuration(treeName, state.State.SourceClusterId, LatticeReplicationMetrics.BootstrapOutcomeLive);

        Logger.LogInformation(
            "Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': IncrementalHandoff -> LiveIncremental (LastAppliedHlc={LastAppliedHlc})",
            treeName, state.State.SourceClusterId, state.State.LastAppliedHlc);

        await CompleteCoordinatorAsync().ConfigureAwait(true);
    }

    /// <summary>
    /// Records the <see cref="LatticeReplicationMetrics.BootstrapDuration"/>
    /// histogram (in milliseconds) with the supplied outcome tag and
    /// resets the per-activation drain-start anchor. No-op when the
    /// anchor is <see langword="null"/> (e.g. a Failed transition
    /// without a prior kickoff anchor or a duplicate terminal call).
    /// </summary>
    private void RecordBootstrapDuration(string treeName, string sourceClusterId, string outcome)
    {
        if (_drainStartTimestamp is not long start)
        {
            return;
        }

        var elapsedMs = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
        LatticeReplicationMetrics.BootstrapDuration.Record(
            elapsedMs,
            new System.Diagnostics.TagList
            {
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, outcome),
                LatticeTenantLabel.ForTree(treeName),
            });
        _drainStartTimestamp = null;
    }

    private static void RecordBootstrapReconcile(
        string treeName,
        string sourceClusterId,
        BootstrapReconcileOutcome outcome)
    {
        switch (outcome)
        {
            case BootstrapReconcileOutcome.Reconciled:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled),
                    LatticeTenantLabel.ForTree(treeName));
                break;
            case BootstrapReconcileOutcome.SkippedScoped:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedScoped),
                    LatticeTenantLabel.ForTree(treeName));
                break;
            case BootstrapReconcileOutcome.SkippedUnstable:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedUnstable),
                    LatticeTenantLabel.ForTree(treeName));
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeOwedRetry),
                    LatticeTenantLabel.ForTree(treeName));
                break;
            case BootstrapReconcileOutcome.SkippedDeleted:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedDeleted),
                    LatticeTenantLabel.ForTree(treeName));
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeOwedRetry),
                    LatticeTenantLabel.ForTree(treeName));
                break;
            case BootstrapReconcileOutcome.SkippedUnknown:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedUnknown),
                    LatticeTenantLabel.ForTree(treeName));
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeOwedRetry),
                    LatticeTenantLabel.ForTree(treeName));
                break;
            case BootstrapReconcileOutcome.SkippedLineageMismatch:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedLineageMismatch),
                    LatticeTenantLabel.ForTree(treeName));
                break;
            case BootstrapReconcileOutcome.SkippedNeverAligned:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedNeverAligned),
                    LatticeTenantLabel.ForTree(treeName));
                break;
            case BootstrapReconcileOutcome.Aligned:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeAligned),
                    LatticeTenantLabel.ForTree(treeName));
                break;
            case BootstrapReconcileOutcome.SkippedNotLww:
                LatticeReplicationMetrics.BootstrapReconcile.Add(1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedNotLww),
                    LatticeTenantLabel.ForTree(treeName));
                break;
        }
    }

    /// <summary>
    /// Records a decision row from the export (#4482): the snapshot settled
    /// saga <see cref="SnapshotEntry.TransactionId"/> with
    /// <see cref="SnapshotEntry.SettledDecision"/>, so the receiver's
    /// transaction registry records the same outcome. A saga record the
    /// source's write-ahead log retained from before the cut and re-ships
    /// after the bootstrap is then settled against that outcome on the
    /// receiver instead of being staged in a pending bucket no terminal will
    /// drain. Idempotent: a repeat records the same outcome.
    /// <para>
    /// The row is deliberately not forgotten. Re-shipping a long retained
    /// tail can outlast the receiver's decision retention, and a prepare
    /// arriving after the row was purged would strand again. The receiver
    /// cannot yet observe the incremental stream passing the export's cut,
    /// which is what would make retiring the row safe, so it retains one
    /// row per saga the source stored at the export (#4524).
    /// </para>
    /// </summary>
    internal static async Task ApplySettledDecisionAsync(IGrainFactory grainFactory, string treeName, SnapshotEntry entry)
    {
        if (entry.SettledDecision is not { } committed || entry.TransactionId == Guid.Empty)
        {
            return;
        }

        var registry = Orleans.Lattice.BPlusTree.Grains.TxRegistryRouting.GetRegistry(grainFactory, treeName, entry.TransactionId);
        await Orleans.Lattice.BPlusTree.Grains.TxRegistryWriteRetry.MarkDecisionAsync(
            registry,
            entry.TransactionId,
            committed).ConfigureAwait(true);
    }

    /// <summary>
    /// Converts one exported <see cref="SnapshotEntry"/> into the
    /// <see cref="WalRecord"/> the drain applies through the replication
    /// applier, or <see langword="null"/> for an entry the drain skips. A
    /// prepared row routes into the receiver's per-tx pending bucket; a
    /// committed row applies as a plain Set, or as a Delete when it carries
    /// <see cref="SnapshotEntry.IsTombstone"/>: a bootstrap can land on a
    /// receiver copy that already holds the key (a peer that fell off the log
    /// re-bootstraps in place), so a delete the source committed must travel
    /// as a tombstone rather than as an absence (#4481).
    /// </summary>
    internal static WalRecord? ToSnapshotWalRecord(
        SnapshotEntry entry,
        string treeName,
        string sourceClusterId,
        LatticeMergeMode mergeMode)
    {
        // Discriminate prepared-saga rows from committed-projection
        // rows. A prepared row routes through the per-tx pending
        // bucket on the receiver via the IsPrepared/TransactionId
        // slots on WalRecord; the matching terminal record arrives
        // through the post-snapshot incremental WAL stream and
        // flips visibility atomically per saga. A committed
        // projection row routes through the canonical Set/Delete
        // apply path. The single WalRecord shape covers both
        // because the steady-state replication path uses the
        // identical discriminators.
        var isPrepared = entry.IsPrepared;
        var isTombstone = entry.IsTombstone;

        if (!isPrepared && !isTombstone && entry.Value is null)
        {
            // A committed row with no value and no tombstone flag carries
            // nothing to apply; defend against custom providers that
            // might surface one. A committed tombstone is applied as a
            // Delete below.
            return null;
        }

        if (isPrepared && entry.TransactionId == Guid.Empty)
        {
            // A prepared row without a transaction id has no
            // routing key for the receiver-side per-tx pending
            // bucket. The default provider never emits one; treat
            // a custom provider's malformed entry as a no-op
            // rather than throwing - a throw here would loop the
            // entire drain on the same bad entry every retry.
            return null;
        }

        // Route the snapshot entry through the canonical replication
        // applier seam so every decorator stacked on
        // <see cref="IReplicationApplier"/> (dead-letter tracking,
        // causal-apply buffer, host-supplied observers) sees
        // bootstrap-arrived entries identically to live-incremental
        // entries. The legacy drain bypassed the applier and wrote
        // straight to <see cref="IReplicationApplyGrain"/>,
        // so any decorator that fired only on the applier path
        // missed every bootstrap entry; the applier itself preserves
        // the source HLC and origin id verbatim, so re-routing
        // through it is correctness-preserving for the underlying
        // tree.
        var op = (isPrepared, isTombstone) switch
        {
            (_, true) => MutationKind.Delete,
            _ => MutationKind.Set,
        };
        return new WalRecord
        {
            TreeId = treeName,
            Op = op,
            Key = entry.Key,
            Value = isTombstone ? null : entry.Value,
            Timestamp = entry.Timestamp,
            IsTombstone = isTombstone,
            ExpiresAtTicks = entry.ExpiresAtTicks,
            OriginClusterId = sourceClusterId,
            Mode = mergeMode,
            VectorClock = null,
            IsPrepared = isPrepared,
            TransactionId = entry.TransactionId,
            AtomicBatchSize = entry.AtomicBatchSize,
            AtomicBatchIndex = entry.AtomicBatchIndex,
            // Carry the typed CRDT delta so a bootstrap-restored prepared
            // CRDT entry folds its per-replica delta into the receiver's
            // current visible state on the saga's terminal commit (the
            // union) instead of installing the prepared LWW value. The
            // tree's resolved mergeMode already routes the prepared apply
            // through the fold; a plain LWW prepare carries Delta=null and
            // stays on the unchanged path.
            Delta = entry.Delta,
        };
    }
}

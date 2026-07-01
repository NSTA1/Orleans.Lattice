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
/// snapshot stream at
/// <see cref="BootstrapCoordinatorState.LastAppliedHlc"/> rather
/// than from <see cref="HybridLogicalClock.Zero"/>.
/// </para>
/// </summary>
internal sealed class LatticeBootstrapCoordinatorGrain(
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
    IPersistentState<BootstrapCoordinatorState> state)
    : CoordinatorGrain<LatticeBootstrapCoordinatorGrain>(context, reminderRegistry, logger),
      ILatticeBootstrapCoordinatorGrain
{
    /// <summary>
    /// Number of snapshot entries applied between
    /// <c>WriteStateAsync</c> calls
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
    private readonly IReplicationApplier _replicationApplier =
        replicationApplier ?? throw new ArgumentNullException(nameof(replicationApplier));
    private readonly ILatticeMergeModeResolver _mergeModeResolver =
        mergeModeResolver ?? throw new ArgumentNullException(nameof(mergeModeResolver));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ILatticeWalIntrospection _walIntrospection =
        walIntrospection ?? throw new ArgumentNullException(nameof(walIntrospection));

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
        return Task.FromResult(new BootstrapCoordinatorStatus(state.State.Phase, source));
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
                // Terminal duration recording: outcome=failed. Emit
                // before the structured log so a log-tail consumer who
                // joins on (treeName, sourceClusterId) sees the metric
                // and the log in the canonical order.
                RecordBootstrapDuration(TreeName, state.State.SourceClusterId, LatticeReplicationMetrics.BootstrapOutcomeFailed);

                Logger.LogInformation(
                    "Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': {PreviousPhase} -> Failed (LastAppliedHlc={LastAppliedHlc})",
                    TreeName, state.State.SourceClusterId, prevPhase, state.State.LastAppliedHlc);

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
    /// <para>
    /// Wraps the export + apply loop in a bounded transient-retry
    /// policy
    /// (<see cref="LatticeReplicationOptions.BootstrapTransientRetry"/>).
    /// A classified-transient fault (e.g. a gRPC
    /// <c>StatusCode.Unavailable</c> from
    /// <c>RemoteSnapshotProvider</c>) consumes one retry slot and
    /// re-opens the snapshot from the persisted cursor; the per-origin
    /// HWM dedupe makes the overlap a no-op, so replay is bounded by
    /// <see cref="CursorPersistEntryInterval"/> × consumed retries.
    /// Non-transient faults pivot to <see cref="LatticeBootstrapState.Failed"/>
    /// on the first failure via the catch block in
    /// <see cref="ProcessNextPhaseAsync"/>. Budget exhaustion re-throws
    /// the final classified-transient exception verbatim so the
    /// same catch block records the failure outcome.
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
            catch (Exception ex) when (attempt < maxAttempts && classifier(ex))
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
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId));

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
    /// drain. Re-opens the snapshot stream from the current
    /// <see cref="BootstrapCoordinatorState.LastAppliedHlc"/> cursor,
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

        // Pass sourceClusterId through to the snapshot provider so that
        // cross-cluster adapters (RemoteSnapshotProvider) can address
        // the correct sender peer. The default intra-cluster provider's
        // default interface implementation ignores the argument and
        // delegates to the two-arg overload, so this is a no-op for
        // hosts that do not register a cross-cluster adapter.
        var snapshot = await _snapshotProvider
            .ExportAsync(treeName, sourceClusterId, state.State.LastAppliedHlc, cancellationToken)
            .ConfigureAwait(true);

        // Update the durable handoff metadata to whatever the latest
        // export reports. On crash recovery this overwrites the prior
        // export's metadata - safe because the receiver will have
        // applied every entry up through the new export's AsOfHlc by
        // the time it reaches IncrementalHandoff, and the per-origin
        // HWM dedupe makes any overlap a no-op.
        state.State.SnapshotAsOfHlc = snapshot.AsOfHlc;
        state.State.CausalStableFrontier = snapshot.CausalStableFrontier;
        var pivotedToApplying = false;
        if (state.State.Phase != LatticeBootstrapState.ApplyingSnapshot)
        {
            state.State.Phase = LatticeBootstrapState.ApplyingSnapshot;
            pivotedToApplying = true;
        }
        await state.WriteStateAsync().ConfigureAwait(true);

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
        // <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
        // in <see cref="PinAndCompleteAsync"/> atomically installs the
        // per-origin HWM at the snapshot's AsOfHlc, so steady-state
        // dedup is preserved across the bootstrap-to-incremental
        // handoff. Receiver-side idempotency during the drain is
        // upheld by leaf-level LWW and the per-leaf / per-tree saga
        // dedup primitives.
        using var bootstrapScope = LatticeBootstrapApplyContext.BeginScope();

        await foreach (var entry in snapshot.Entries.ConfigureAwait(true))
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

            if (!isPrepared && entry.Value is null)
            {
                // Tombstones are not emitted by the default provider
                // on the committed-projection path (it skips dead
                // keys), but defend against custom providers that
                // might surface them.
                continue;
            }

            if (isPrepared && entry.TransactionId == Guid.Empty)
            {
                // A prepared row without a transaction id has no
                // routing key for the receiver-side per-tx pending
                // bucket. The default provider never emits one; treat
                // a custom provider's malformed entry as a no-op
                // rather than throwing - a throw here would loop the
                // entire drain on the same bad entry every retry.
                continue;
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
                (true, true) => MutationKind.Delete,
                _ => MutationKind.Set,
            };
            var record = new WalRecord
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
            await _replicationApplier.ApplyAsync(record, cancellationToken).ConfigureAwait(true);

            // Bootstrap progress instruments: increment once per
            // successfully-applied entry so operators can watch
            // entries/second and bytes/second in real time without
            // waiting for the terminal duration histogram.
            LatticeReplicationMetrics.BootstrapEntriesReceived.Add(1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId));
            var byteCount = entry.Value?.Length ?? 0;
            LatticeReplicationMetrics.BootstrapBytesReceived.Add(byteCount,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId));

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

        Logger.LogInformation(
            "Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': ApplyingSnapshot -> IncrementalHandoff (LastAppliedHlc={LastAppliedHlc})",
            treeName, sourceClusterId, state.State.LastAppliedHlc);
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
        // the export echoes it back from the resume lower-bound (the
        // receiver's LastAppliedHlc), so it is zero for a cold bootstrap.
        // Pinning HWM[source] at the cut (monotonic max) restores the
        // invariant that HWM[source] covers every locally-retained
        // source-origin entry and makes incremental entries at or below the
        // cut proper dedupe no-ops.
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

        // Idempotent: PinSnapshotAsync is a monotonic max + frontier
        // merge, so a crash between this call and the WriteStateAsync
        // below replays safely on reactivation - the second pin with
        // identical (asOfHlc, frontier) is a no-op.
        await hwm
            .PinSnapshotAsync(asOfHlc, frontier, CancellationToken.None)
            .ConfigureAwait(true);

        state.State.Phase = LatticeBootstrapState.LiveIncremental;
        state.State.InProgress = false;
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
        LatticeReplicationMetrics.BootstrapDuration.Record(elapsedMs,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, outcome));
        _drainStartTimestamp = null;
    }
}

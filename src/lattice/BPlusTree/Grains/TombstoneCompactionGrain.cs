using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;
using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Singleton-per-tree grain that owns a single reminder for tombstone compaction.
/// When the reminder fires, a grain timer is started that processes one shard per
/// tick - avoiding a long-running grain call that could hit Orleans timeouts for
/// large trees. Failed shards are retried once before being skipped.
/// <para>
/// Compaction progress is persisted so that a silo restart mid-compaction can
/// resume where it left off. A one-minute keepalive reminder is registered at the
/// start of compaction and unregistered on completion; if the silo restarts, the
/// keepalive fires and resumes the in-flight pass.
/// </para>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class TombstoneCompactionGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<TombstoneCompactionGrain> logger,
    [PersistentState("tombstone-compaction", LatticeOptions.StorageProviderName)]
    IPersistentState<TombstoneCompactionState> state) : ITombstoneCompactionGrain, IRemindable, IGrainBase
{
    private const string ReminderName = "tombstone-compaction";
    private const string KeepaliveReminderName = "compaction-keepalive";
    private const int MaxRetriesPerShard = 1;

    /// <summary>Trigger label written to pass-level telemetry.</summary>
    internal const string TriggerReminder = "reminder";
    internal const string TriggerRatio = "ratio";
    internal const string TriggerSize = "size";
    internal const string TriggerOperator = "operator";

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _compactionTimer;
    private readonly PublishEventsGate _eventsGate = new();

    /// <summary>
    /// Trigger label of the in-flight pass; written by
    /// <see cref="StartCompactionAsync"/> / <see cref="RunCompactionPassAsync"/>
    /// and consumed by <see cref="CompleteCompactionAsync"/> /
    /// <see cref="ProcessNextShardAsync"/> to tag pass-level telemetry.
    /// Defaults to <see cref="TriggerReminder"/>.
    /// </summary>
    private string _currentTriggerKind = TriggerReminder;

    /// <summary>Wall-clock start of the in-flight pass, set on transition into <c>InProgress</c>.</summary>
    private long _passStartTimestamp;

    /// <summary>
    /// Optional shard-scope filter for the in-flight pass. When non-null,
    /// <see cref="ProcessNextShardAsync"/> compacts only the listed
    /// physical shard indices instead of the full topology. Used by
    /// <see cref="RequestCompactionAsync"/> to scope an out-of-cycle pass.
    /// </summary>
    private int[]? _scopedShardIndices;

    /// <summary>
    /// Snapshot of the resolved <see cref="LatticeOptions.CompactionShardTickInterval"/>
    /// for the in-flight pass. Set when the pass transitions into
    /// <c>InProgress</c> (via <see cref="BeginCompactionStateAsync"/> or
    /// <see cref="BeginScopedCompactionStateAsync"/>) and read once when
    /// the grain timer is registered. Mid-pass option changes do not
    /// retroactively reshape the in-flight pass; the next pass picks up
    /// the new value. Falls back to
    /// <see cref="LatticeOptions.DefaultCompactionShardTickInterval"/> if
    /// the snapshot has not been set (defence-in-depth for callers that
    /// register the timer without going through the begin helpers - none
    /// exist in this repository today, but the fallback prevents a
    /// zero-period timer if someone adds one later).
    /// </summary>
    private TimeSpan _currentTickInterval = LatticeOptions.DefaultCompactionShardTickInterval;

    /// <summary>
    /// Snapshot of the resolved <see cref="LatticeOptions.CompactionLeafBatchSize"/>
    /// for the in-flight pass. Set when the pass transitions into
    /// <c>InProgress</c> alongside <see cref="_currentTickInterval"/>;
    /// caps the number of leaves visited per shard timer tick. Mid-pass
    /// option changes do not retroactively reshape the in-flight pass.
    /// Falls back to <see cref="LatticeOptions.DefaultCompactionLeafBatchSize"/>
    /// if the snapshot has not been set.
    /// </summary>
    private int _currentLeafBatchSize = LatticeOptions.DefaultCompactionLeafBatchSize;

    /// <summary>
    /// Test-only seam exposing the snapshot of
    /// <see cref="LatticeOptions.CompactionLeafBatchSize"/> captured
    /// when the in-flight pass began.
    /// </summary>
    internal int CurrentLeafBatchSizeForTests => _currentLeafBatchSize;

    /// <summary>
    /// Everything a leaf batch may mutate about its position within the current
    /// shard, as one value. The chain walk resumes from a <b>key</b>, the
    /// dirty-leaves fast path from an <b>index</b> into its persisted snapshot,
    /// and the snapshot itself is pulled and cleared by the same batch - but
    /// every caller that guards a state write has to save and restore all of it
    /// as a unit, or a failing persist leaves the activation's position ahead of
    /// disk (issue 1973). The superseded leaf-id cursor is carried too, so
    /// discarding it is itself revertible.
    /// </summary>
    private readonly record struct ShardLeafCursor(
        string? Key,
        int DirtyIndex,
        string? LegacyLeafId,
        string[]? DirtyLeaves,
        HybridLogicalClock DirtyAdvance);

    /// <summary>Captures the current in-shard position.</summary>
    private ShardLeafCursor CaptureShardCursor()
        => new(state.State.NextLeafKeyInShard,
               state.State.CurrentShardDirtyIndex,
               state.State.NextLeafIdInShard,
               state.State.CurrentShardDirtyLeaves,
               state.State.CurrentShardDirtyAdvance);

    /// <summary>Restores a previously captured in-shard position.</summary>
    private void RestoreShardCursor(ShardLeafCursor cursor)
    {
        state.State.NextLeafKeyInShard = cursor.Key;
        state.State.CurrentShardDirtyIndex = cursor.DirtyIndex;
        state.State.NextLeafIdInShard = cursor.LegacyLeafId;
        state.State.CurrentShardDirtyLeaves = cursor.DirtyLeaves;
        state.State.CurrentShardDirtyAdvance = cursor.DirtyAdvance;
    }

    /// <summary>
    /// Resets the in-shard resume position, so the next batch starts at the
    /// beginning of whatever shard it enters. Also clears the superseded
    /// leaf-id cursor, so state written by an older build cannot survive into a
    /// pass that no longer reads it.
    /// </summary>
    private void ClearShardCursor()
    {
        state.State.NextLeafKeyInShard = null;
        state.State.CurrentShardDirtyIndex = 0;
        state.State.NextLeafIdInShard = null;
    }

    /// <summary>
    /// Test-only seam exposing the snapshot of
    /// <see cref="LatticeOptions.CompactionShardTickInterval"/> captured
    /// when the in-flight pass began. Returns the
    /// <see cref="LatticeOptions.DefaultCompactionShardTickInterval"/>
    /// fallback if no pass has been begun on this activation.
    /// </summary>
    internal TimeSpan CurrentTickIntervalForTests => _currentTickInterval;

    private bool IsCompactionDisabled => Options.TombstoneGracePeriod == Timeout.InfiniteTimeSpan;

    private IHostApplicationLifetime? _lifetime;
    private bool _lifetimeResolved;

    /// <summary>
    /// Resolves the optional <see cref="IHostApplicationLifetime"/> from the
    /// activation's service provider. Cached after first lookup; returns
    /// <see langword="null"/> on non-hosted test activations. Mirrors the
    /// lazy-resolve pattern the atomic-write saga coordinator established.
    /// </summary>
    private IHostApplicationLifetime? ResolveLifetime()
    {
        if (_lifetimeResolved) return _lifetime;
        _lifetimeResolved = true;
        _lifetime = context.ActivationServices?.GetService<IHostApplicationLifetime>();
        return _lifetime;
    }

    /// <summary>
    /// Fast-fails an operator-driven compaction pass with
    /// <see cref="LatticeShuttingDownException"/> when the host has begun
    /// shutting down, before the pass issues any leaf compaction writes. A
    /// no-op on a healthy host or a non-hosted test activation. The reminder-
    /// and timer-driven passes are not guarded here: they have no external
    /// caller to surface the typed exception to and are resumed from their
    /// persisted cursor by the keepalive reminder on the next activation.
    /// </summary>
    private void ThrowIfShuttingDown()
    {
        if (ResolveLifetime() is { } lifetime && lifetime.ApplicationStopping.IsCancellationRequested)
            throw new LatticeShuttingDownException(
                $"Tombstone compaction of tree '{TreeId}' refused: the silo is shutting down (ApplicationStopping is signalled); "
                + "the write was not dispatched to the write-ahead-log writer.");
    }


    public async Task EnsureReminderAsync()
    {
        if (IsCompactionDisabled) return;

        var period = ClampPeriod(Options.TombstoneGracePeriod);
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: ReminderName,
            dueTime: period,
            period: period);
    }

    public async Task RunCompactionPassAsync()
    {
        if (IsCompactionDisabled) return;
        ThrowIfShuttingDown();

        _currentTriggerKind = TriggerOperator;
        _passStartTimestamp = Stopwatch.GetTimestamp();
        // Snapshot the leaf batch size for this operator-driven pass so
        // the per-shard loop below honours the same cap as the
        // reminder-driven pass.
        _currentLeafBatchSize = (await optionsResolver.ResolveAsync(TreeId)).CompactionLeafBatchSize;
        try
        {
            var (physicalTreeId, physicalShards) = await ResolveShardTopologyAsync();
            foreach (var shardIndex in physicalShards)
            {
                // Operator-driven full pass: drive CompactShardBatchAsync
                // until it reports the shard is done. The in-shard cursor
                // lives on state.State.NextLeafKeyInShard (chain walk) /
                // CurrentShardDirtyIndex (dirty-set fast path) and is reset
                // between shards so each shard starts from its leftmost
                // leaf. This path does not persist between batches; the
                // operator is willing to block on the full pass.
                ClearShardCursor();
                bool shardDone;
                do
                {
                    shardDone = await CompactShardBatchAsync(physicalTreeId, shardIndex, Options.TombstoneGracePeriod);
                }
                while (!shardDone);
            }
            // Clear the transient in-memory cursor on completion so the
            // next reminder-driven pass starts fresh.
            ClearShardCursor();
        }
        finally
        {
            RecordPassDuration(_currentTriggerKind);
        }
    }

    public async Task<bool> RequestCompactionAsync(int shardIndex, string triggerKind)
    {
        var honoured = await TryBeginRequestedCompactionAsync(shardIndex, triggerKind);
        if (!honoured) return false;

        // Bookkeeping accepted - now start the per-tick timer outside
        // the bookkeeping helper so unit tests can drive the
        // state-machine transition without the Orleans timer
        // infrastructure (which is unavailable in `Substitute.For<IGrainContext>()`).
        _compactionTimer = this.RegisterGrainTimer(
            OnCompactionTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: _currentTickInterval));
        return true;
    }

    /// <summary>
    /// Validates the cooldown / topology guards, persists the trigger
    /// timestamp, and transitions the grain into a single-shard
    /// in-progress pass. Returns <c>true</c> when the request was
    /// honoured (the caller still needs to start the per-tick timer);
    /// <c>false</c> when the request was dropped. Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task<bool> TryBeginRequestedCompactionAsync(int shardIndex, string triggerKind)
    {
        ArgumentNullException.ThrowIfNull(triggerKind);
        if (IsCompactionDisabled) return false;
        if (_compactionTimer is not null || state.State.InProgress) return false;
        if (triggerKind != TriggerRatio && triggerKind != TriggerSize && triggerKind != TriggerOperator)
        {
            throw new ArgumentException($"Unknown triggerKind '{triggerKind}'.", nameof(triggerKind));
        }

        // Cooldown gating - operator requests bypass.
        if (triggerKind != TriggerOperator)
        {
            var cooldown = Options.CompactionTriggerCooldown;
            if (cooldown > TimeSpan.Zero
                && state.State.LastTriggerAt is { } map
                && map.TryGetValue(shardIndex, out var lastAt)
                && DateTimeOffset.UtcNow - lastAt < cooldown)
            {
                return false;
            }
        }

        var (physicalTreeId, physicalShards) = await ResolveShardTopologyAsync();
        var found = false;
        for (var i = 0; i < physicalShards.Count; i++)
        {
            if (physicalShards[i] == shardIndex) { found = true; break; }
        }
        if (!found) return false;

        state.State.LastTriggerAt ??= [];
        state.State.LastTriggerAt[shardIndex] = DateTimeOffset.UtcNow;
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            // Best-effort: a failed bookkeeping write should not block
            // the requested compaction. Cooldown enforcement degrades
            // to "no record exists" on the next request, which means
            // back-to-back triggers may pass during the persist outage
            // - acceptable for an event-storm guard.
            logger.LogWarning(ex, "Failed to persist compaction trigger timestamp for tree {TreeId} shard {ShardIndex}", TreeId, shardIndex);
        }

        _currentTriggerKind = triggerKind;
        _scopedShardIndices = [shardIndex];
        try
        {
            await BeginScopedCompactionStateAsync(physicalTreeId, [shardIndex]);
        }
        catch
        {
            _scopedShardIndices = null;
            throw;
        }
        return true;
    }

    /// <summary>
    /// Persists the in-progress marker for a shard-scoped pass and
    /// registers the keepalive reminder, but does not start the grain
    /// timer. Exposed as <c>internal</c> so unit tests can drive the
    /// state-machine transition without the Orleans timer
    /// infrastructure.
    /// </summary>
    internal async Task BeginScopedCompactionStateAsync(string physicalTreeId, int[] shardIndices)
    {
        _passStartTimestamp = Stopwatch.GetTimestamp();
        // Snapshot the resolved tick interval at pass start; mid-pass
        // option changes do not retroactively reshape the in-flight pass.
        var resolvedScoped = await optionsResolver.ResolveAsync(TreeId);
        _currentTickInterval = resolvedScoped.CompactionShardTickInterval;
        _currentLeafBatchSize = resolvedScoped.CompactionLeafBatchSize;
        var prevInProgress = state.State.InProgress;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhysicalTreeId = state.State.PhysicalTreeId;
        var prevPhysicalShardIndices = state.State.PhysicalShardIndices;
        var prevShardCursorScoped = CaptureShardCursor();

        state.State.InProgress = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.PhysicalTreeId = physicalTreeId;
        state.State.PhysicalShardIndices = shardIndices;
        // A scoped pass operates on a distinct shard list; any cursor
        // left over from a prior pass would apply to the wrong shard.
        ClearShardCursor();
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.PhysicalTreeId = prevPhysicalTreeId;
            state.State.PhysicalShardIndices = prevPhysicalShardIndices;
            RestoreShardCursor(prevShardCursorScoped);
            throw;
        }

        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));
    }

    private void RecordPassDuration(string triggerKind)
    {
        if (_passStartTimestamp == 0) return;
        var elapsedMs = (Stopwatch.GetTimestamp() - _passStartTimestamp) * 1000.0 / Stopwatch.Frequency;
        _passStartTimestamp = 0;
        var triggerTag = triggerKind switch
        {
            TriggerReminder => LatticeMetrics.TriggerReminderTag,
            TriggerRatio => LatticeMetrics.TriggerRatioTag,
            TriggerSize => LatticeMetrics.TriggerSizeTag,
            TriggerOperator => LatticeMetrics.TriggerOperatorTag,
            _ => new KeyValuePair<string, object?>(LatticeMetrics.TagTrigger, triggerKind),
        };
        LatticeMetrics.CompactionPassDuration.Record(elapsedMs,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            triggerTag,
            LatticeTenantLabel.ForTree(TreeId));
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (IsCompactionDisabled) return;

        if (reminderName == ReminderName)
        {
            // Defensively re-register if the configured period drifts
            // from whatever Orleans is firing. Compare against the
            // TombstoneGracePeriod option each tick; if the effective period
            // differs, RegisterOrUpdateReminder replaces the schedule.
            var desired = ClampPeriod(Options.TombstoneGracePeriod);
            var actualPeriod = status.Period;
            if (actualPeriod != desired)
            {
                await reminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: context.GrainId,
                    reminderName: ReminderName,
                    dueTime: desired,
                    period: desired);
            }

            // Periodic compaction trigger - start a new pass if idle.
            if (_compactionTimer is not null) return;
            await StartCompactionAsync(startFromShard: 0);
        }
        else if (reminderName == KeepaliveReminderName)
        {
            // Keepalive fired - either resume a persisted in-flight pass or
            // clean up if compaction already finished.
            if (state.State.InProgress && _compactionTimer is null)
            {
                await StartCompactionAsync(startFromShard: state.State.NextShardIndex);
            }
            else if (!state.State.InProgress)
            {
                await UnregisterKeepaliveAsync();
            }
        }
    }

    /// <summary>
    /// Begins a compaction pass: persists in-progress state, registers the
    /// keepalive reminder, and starts the grain timer. Exposed as
    /// <c>internal</c> for unit testing (tests call
    /// <see cref="BeginCompactionStateAsync"/> + <see cref="ProcessNextShardAsync"/>
    /// directly to avoid the Orleans timer infrastructure).
    /// </summary>
    internal async Task StartCompactionAsync(int startFromShard)
    {
        // Reminder-driven entry point - tag pass-level telemetry as `reminder`.
        _currentTriggerKind = TriggerReminder;
        _passStartTimestamp = Stopwatch.GetTimestamp();
        await BeginCompactionStateAsync(startFromShard);

        // Fire immediately, then tick every CompactionShardTickInterval per shard.
        _compactionTimer = this.RegisterGrainTimer(
            OnCompactionTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: _currentTickInterval));
    }

    /// <summary>
    /// Persists the in-progress marker and registers the keepalive reminder
    /// without starting the grain timer. Used by <see cref="StartCompactionAsync"/>
    /// and directly by unit tests.
    /// </summary>
    internal async Task BeginCompactionStateAsync(int startFromShard)
    {
        // Resolve the current shard topology (alias + physical shard list) and
        // persist it so the pass is resumable across silo restarts and immune
        // to mid-pass shard-map mutations (audit bug #1).
        var (physicalTreeId, physicalShards) = await ResolveShardTopologyAsync();

        // Snapshot the resolved tick interval at pass start; mid-pass
        // option changes do not retroactively reshape the in-flight pass.
        var resolvedBegin = await optionsResolver.ResolveAsync(TreeId);
        _currentTickInterval = resolvedBegin.CompactionShardTickInterval;
        _currentLeafBatchSize = resolvedBegin.CompactionLeafBatchSize;

        // Snapshot before mutating so a transient WriteStateAsync failure
        // does not leave the in-memory InProgress / shard cursor ahead of
        // disk. The keepalive ReceiveReminder branch reads
        // state.State.InProgress directly and would short-circuit against
        // a dirty value otherwise.
        var prevInProgress = state.State.InProgress;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhysicalTreeId = state.State.PhysicalTreeId;
        var prevPhysicalShardIndices = state.State.PhysicalShardIndices;
        var prevShardCursorBegin = CaptureShardCursor();

        state.State.InProgress = true;
        state.State.NextShardIndex = startFromShard;
        state.State.ShardRetries = 0;
        state.State.PhysicalTreeId = physicalTreeId;
        state.State.PhysicalShardIndices = [.. physicalShards];
        // Preserve the in-shard cursor only when this is a resume into
        // the same shard the cursor refers to (the keepalive-fired
        // resume path). A pass that begins at a different shard - or
        // that begins at shard 0 with no in-flight state - must start
        // each shard's leaf walk from the leftmost leaf.
        if (startFromShard != prevNextShardIndex)
        {
            ClearShardCursor();
        }
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.PhysicalTreeId = prevPhysicalTreeId;
            state.State.PhysicalShardIndices = prevPhysicalShardIndices;
            RestoreShardCursor(prevShardCursorBegin);
            throw;
        }

        // Register a 1-minute keepalive so the grain is reactivated after a
        // silo restart. The minimum Orleans reminder period is 1 minute.
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));
    }

    private async Task OnCompactionTimerTick(CancellationToken ct)
    {
        await ProcessNextShardAsync();
    }

    /// <summary>
    /// Processes the next shard in the compaction pass. If all shards are done,
    /// completes the pass. Called by the grain timer tick; exposed as
    /// <c>internal</c> for unit testing without the Orleans timer infrastructure.
    /// </summary>
    internal async Task ProcessNextShardAsync()
    {
        var physicalShards = state.State.PhysicalShardIndices;
        if (physicalShards is null || physicalShards.Length == 0)
        {
            // State pre-dates the fix - refresh from the registry.
            var (physTreeId, shards) = await ResolveShardTopologyAsync();
            state.State.PhysicalTreeId = physTreeId;
            state.State.PhysicalShardIndices = [.. shards];
            physicalShards = state.State.PhysicalShardIndices;
        }

        if (state.State.NextShardIndex >= physicalShards.Length)
        {
            await CompleteCompactionAsync();
            return;
        }

        var physicalTreeId = state.State.PhysicalTreeId ?? TreeId;
        var shardIndex = physicalShards[state.State.NextShardIndex];

        // Distinguish "compaction failed -> apply retry/skip policy" from
        // "compaction succeeded but persist failed -> revert and rethrow".
        // Lifting the success-path persist out of the outer try ensures a
        // rethrown InvalidOperationException from the snapshot/restore
        // doesn't accidentally trip the compaction-failure handler and
        // bump ShardRetries.
        bool compactionSucceeded = false;
        bool batchCompletedShard = false;
        // Snapshot the cursor *before* the batch call so we can revert
        // any in-memory mutation made inside CompactShardBatchAsync if a
        // subsequent state write fails.
        var prevCursorBeforeBatch = CaptureShardCursor();
        try
        {
            batchCompletedShard = await CompactShardBatchAsync(physicalTreeId, shardIndex, Options.TombstoneGracePeriod);
            compactionSucceeded = true;
        }
        catch (Exception ex)
        {
            // Restore the in-memory cursor; the batch method may have
            // mutated the in-shard cursor before it threw, and
            // we have not yet persisted that mutation. Reverting keeps
            // memory in sync with disk so the shard retry resumes from
            // the same cursor as before the failed attempt.
            RestoreShardCursor(prevCursorBeforeBatch);
            logger.LogWarning(ex, "Tombstone compaction failed for shard {ShardIndex} of tree {TreeId}", shardIndex, TreeId);
            if (state.State.ShardRetries < MaxRetriesPerShard)
            {
                LatticeMetrics.CompactionShardRetries.Add(1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                    LatticeTenantLabel.ForTree(TreeId));
                var prevShardRetries = state.State.ShardRetries;
                state.State.ShardRetries++;
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    state.State.ShardRetries = prevShardRetries;
                    throw;
                }
            }
            else
            {
                LatticeMetrics.CompactionShardSkipped.Add(1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                    LatticeTenantLabel.ForTree(TreeId));
                // Exhausted retries for this shard - skip to next and
                // clear the in-shard cursor so the next shard starts
                // from its leftmost leaf.
                var prevNextShardIndex = state.State.NextShardIndex;
                var prevShardRetries = state.State.ShardRetries;
                state.State.NextShardIndex++;
                state.State.ShardRetries = 0;
                ClearShardCursor();
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    state.State.NextShardIndex = prevNextShardIndex;
                    state.State.ShardRetries = prevShardRetries;
                    RestoreShardCursor(prevCursorBeforeBatch);
                    throw;
                }
            }
        }

        if (compactionSucceeded)
        {
            if (batchCompletedShard)
            {
                // Snapshot the cursor before advancing so a transient
                // WriteStateAsync failure does not leave NextShardIndex /
                // ShardRetries / cursor ahead of disk, which would cause the next
                // tick to skip a shard or believe a retry budget has been spent.
                var prevNextShardIndex = state.State.NextShardIndex;
                var prevShardRetries = state.State.ShardRetries;
                state.State.NextShardIndex++;
                state.State.ShardRetries = 0;
                ClearShardCursor();
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    state.State.NextShardIndex = prevNextShardIndex;
                    state.State.ShardRetries = prevShardRetries;
                    // Revert to the position as it was BEFORE the batch, not as
                    // the batch left it: the batch cleared the cursor on
                    // reaching the end of the shard, and reverting to that
                    // cleared value would leave this activation believing the
                    // shard was finished while storage still owes the walk.
                    RestoreShardCursor(prevCursorBeforeBatch);
                    throw;
                }
            }
            else
            {
                // Mid-shard batch boundary: persist the in-shard cursor so
                // the next timer tick resumes from the same position. The
                // batch method has already mutated the cursor in
                // memory; on persist failure restore the prior cursor so
                // memory stays in sync with disk.
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    RestoreShardCursor(prevCursorBeforeBatch);
                    throw;
                }
            }
        }
    }

    internal async Task CompleteCompactionAsync()
    {
        _compactionTimer?.Dispose();
        _compactionTimer = null;

        // Snapshot before mutating so a transient WriteStateAsync failure
        // does not leave InProgress / cursor / topology cleared in-memory
        // ahead of disk. The keepalive ReceiveReminder branch reads
        // state.State.InProgress directly: with a dirty (false) in-memory
        // value it would unregister the keepalive against a still-in-progress
        // disk record, forcing the next regular reminder tick to restart the
        // pass from shard 0 instead of resuming where it left off.
        var prevInProgress = state.State.InProgress;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhysicalTreeId = state.State.PhysicalTreeId;
        var prevPhysicalShardIndices = state.State.PhysicalShardIndices;
        var prevShardCursorComplete = CaptureShardCursor();

        state.State.InProgress = false;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.PhysicalTreeId = null;
        state.State.PhysicalShardIndices = [];
        ClearShardCursor();
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.PhysicalTreeId = prevPhysicalTreeId;
            state.State.PhysicalShardIndices = prevPhysicalShardIndices;
            RestoreShardCursor(prevShardCursorComplete);
            throw;
        }

        await UnregisterKeepaliveAsync();

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            LatticeMetrics.KindCompaction,
            LatticeTenantLabel.ForTree(TreeId));

        RecordPassDuration(_currentTriggerKind);
        _scopedShardIndices = null;
        // Reset trigger label so a stale value doesn't carry over to a
        // subsequent reminder-driven pass that forgot to set it.
        _currentTriggerKind = TriggerReminder;

        await PublishCompactionCompletedAsync();

        // This grain does no work between passes - free the activation.
        // The next reminder tick will reactivate it.
        this.DeactivateOnIdle();
    }

    private async Task PublishCompactionCompletedAsync()
    {
        var opts = Options;
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(LatticeTreeEventKind.CompactionCompleted, TreeId);
        await LatticeEventPublisher.PublishAsync(context.ActivationServices, opts, evt, logger);
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            // Best effort - the keepalive will be cleaned up on the next tick
            // if it fires while InProgress is false.
            logger.LogWarning(ex, "Failed to unregister keepalive reminder for tree {TreeId}", TreeId);
        }
    }

    public async Task UnregisterReminderAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, ReminderName);
            if (reminder is not null)
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
        }
        catch (Exception ex) { logger.LogWarning(ex, "Failed to unregister compaction reminder for tree {TreeId}", TreeId); }

        await UnregisterKeepaliveAsync();

        _compactionTimer?.Dispose();
        _compactionTimer = null;

        this.DeactivateOnIdle();
    }

    private async Task<bool> CompactShardBatchAsync(string physicalTreeId, int shardIndex, TimeSpan gracePeriod)
    {
        // Compaction is a maintenance pass: every WAL envelope emitted
        // by the leaf-level reap loop must be classified
        // `MutationCategory.Maintenance` so producer-side replication
        // filtering can short-circuit it. The scope flows via
        // `RequestContext` into the leaf grain call, where the inner
        // `LatticeMaintenanceContext.BeginScope()` in
        // `BPlusLeafGrain.CompactTombstonesAsync` is then a structural
        // no-op (the bit is already set). Belt-and-braces: the outer
        // scope here covers any future leaf path that emits without its
        // own inner scope.
        using var maintenanceScope = LatticeMaintenanceContext.BeginScope();

        // Trigger-label scope: only opened when at least one policy knob
        // is non-default, so the v3.4.0 reminder-only deployment leaves
        // the per-leaf instruments un-tagged and existing dashboards
        // that filter on `trigger=""` keep matching exactly.
        var opts = Options;
        IDisposable? triggerScope = null;
        if (opts.MinTombstoneRatioForCompaction > 0.0 || opts.MaxLeafEntriesBeforeForcedCompaction > 0)
        {
            triggerScope = LatticeCompactionTriggerContext.BeginScope(_currentTriggerKind);
        }
        try
        {
            var shardKey = $"{physicalTreeId}/{shardIndex}";
            var shardRoot = grainFactory.GetGrain<IShardRootGrain>(shardKey);

            // Path selection. On the first batch entering this shard
            // (CurrentShardDirtyLeaves==null AND no in-shard cursor),
            // pull the shard-root dirty-leaves snapshot. A non-empty
            // snapshot locks the shard into the fast path; an empty
            // snapshot falls back to the legacy chain walk so a tree
            // with no accumulated signal yet (fresh activation, upgraded
            // silo) still progresses.
            //
            // Discard a leaf-id cursor left by an older build and restart the
            // shard. Per-leaf compaction is idempotent, so the cost is a
            // re-walk; trusting the id would risk a resumed walk reporting the
            // shard done against a leaf that activated empty (issue 1973).
            if (!string.IsNullOrEmpty(state.State.NextLeafIdInShard))
            {
                ClearShardCursor();
            }

            var resumeFrom = state.State.NextLeafKeyInShard;
            string[]? dirtyLeaves = state.State.CurrentShardDirtyLeaves;
            if (dirtyLeaves is null && string.IsNullOrEmpty(resumeFrom))
            {
                var snapshot = await shardRoot.GetDirtyLeavesSinceLastCompactionAsync();
                LatticeMetrics.CompactionShardDirtyLeaves.Record(snapshot.DirtyLeaves.Count,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, physicalTreeId),
                    LatticeTenantLabel.ForTree(physicalTreeId));
                if (snapshot.DirtyLeaves.Count > 0)
                {
                    dirtyLeaves = new string[snapshot.DirtyLeaves.Count];
                    for (int i = 0; i < snapshot.DirtyLeaves.Count; i++)
                        dirtyLeaves[i] = snapshot.DirtyLeaves[i].ToString();
                    state.State.CurrentShardDirtyLeaves = dirtyLeaves;
                    state.State.CurrentShardDirtyAdvance = snapshot.ObservedAdvance;
                }
            }

            var pathTag = dirtyLeaves is not null ? LatticeMetrics.PathDirtySetTag : LatticeMetrics.PathWalkTag;
            using var pathScope = LatticeCompactionPathContext.BeginScope(
                dirtyLeaves is not null ? LatticeMetrics.PathDirtySet : LatticeMetrics.PathWalk);

            var batchSize = _currentLeafBatchSize > 0 ? _currentLeafBatchSize : LatticeOptions.DefaultCompactionLeafBatchSize;

            // Both paths spend the same shared budget, so the leaf cap and the
            // wall-clock net are one implementation rather than a hand-rolled
            // counter per path (issue 1973).
            var budget = LeafWalkBudget.ForBackgroundDrain(batchSize, Options);

            if (dirtyLeaves is not null)
            {
                // Dirty-leaves fast path. This walk is over a persisted, finite
                // list the shard root nominated, not over sibling pointers, so
                // its resume position is an index into that list rather than a
                // key: there is no chain to re-descend, and the list itself -
                // not a chain of virtual grains - is what bounds the walk.
                var dirtyIndex = state.State.CurrentShardDirtyIndex;
                if (dirtyIndex < 0 || dirtyIndex > dirtyLeaves.Length) dirtyIndex = 0;

                while (dirtyIndex < dirtyLeaves.Length)
                {
                    var dirtyLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(GrainId.Parse(dirtyLeaves[dirtyIndex]));
                    try
                    {
                        await dirtyLeaf.CompactTombstonesAsync(gracePeriod);
                    }
                    catch
                    {
                        RecordSkippedLeaf(physicalTreeId, pathTag, triggerScope is not null);
                        throw;
                    }

                    dirtyIndex++;
                    budget.RecordLeafVisited();

                    // resultsCollected is 1 because the unit of progress here is
                    // a leaf compacted and the index advanced, not a row
                    // returned. Only stop where the next batch can resume: an
                    // exhausted list falls through to the completion branch
                    // rather than yielding a cursor pointing past its end.
                    if (dirtyIndex < dirtyLeaves.Length && budget.ShouldYield()) break;
                }

                if (dirtyIndex >= dirtyLeaves.Length)
                {
                    // Dirty-set path completion: drain the shard-root dirty
                    // set up to the watermark we observed at snapshot time.
                    // Best-effort - a transient failure here just leaves the
                    // entries in place for the next pass to re-walk.
                    var advance = state.State.CurrentShardDirtyAdvance;
                    ClearShardCursor();
                    state.State.CurrentShardDirtyLeaves = null;
                    state.State.CurrentShardDirtyAdvance = default;
                    try
                    {
                        await shardRoot.ClearDirtyLeavesUpToAsync(advance);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex,
                            "Failed to clear shard-root dirty-leaves watermark for {ShardKey}",
                            shardKey);
                    }
                    return true;
                }

                // Batch boundary on the fast path. The persistence of the
                // cursor itself is deferred to the caller
                // (ProcessNextShardAsync) so the batch + cursor write is one
                // atomic state transition rather than two.
                state.State.NextLeafKeyInShard = null;
                state.State.CurrentShardDirtyIndex = dirtyIndex;
                return false;
            }

            // Legacy chain walk, bounded by the same budget and resumed by KEY.
            // A key is re-descended onto whichever leaf now owns it, so a leaf
            // that no longer resolves cannot end the walk early and leave the
            // rest of the shard silently uncompacted - the failure mode the old
            // leaf-id cursor had to be defended against case by case
            // (issues 1970, 1973).
            var walk = await BoundedLeafWalk.StartAsync(grainFactory, shardRoot, resumeFrom, budget);
            while (walk.HasLeaf)
            {
                var leaf = walk.CurrentLeaf;
                try
                {
                    await leaf.CompactTombstonesAsync(gracePeriod);
                }
                catch
                {
                    RecordSkippedLeaf(physicalTreeId, pathTag, triggerScope is not null);
                    throw;
                }

                if (!await walk.MoveNextAsync()) break;
            }

            // Cursor semantics:
            //   walk.Completed -> end of shard reached, return done=true.
            //   otherwise      -> batch boundary, persist the resume key.
            // The persistence of the cursor itself is deferred to the
            // caller (ProcessNextShardAsync) so the batch + cursor write
            // is one atomic state transition rather than two.
            state.State.CurrentShardDirtyIndex = 0;
            state.State.NextLeafKeyInShard = walk.Completed ? null : walk.ResumeFromInclusive;
            return walk.Completed;
        }
        finally
        {
            triggerScope?.Dispose();
        }
    }

    /// <summary>
    /// Tags the per-leaf visited counter with <c>outcome=skipped</c> for a leaf
    /// the coordinator gave up on, so operators can distinguish it from a leaf
    /// that legitimately had nothing to reap (<c>outcome=noop</c>) or actively
    /// reaped (<c>outcome=reaped</c>). The caller re-throws afterwards, so the
    /// surrounding shard-level retry/skip logic in <c>ProcessNextShardAsync</c>
    /// still drives the shard.retries / shard.skipped counters.
    /// </summary>
    private void RecordSkippedLeaf(string physicalTreeId, KeyValuePair<string, object?> pathTag, bool tagTrigger)
    {
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, physicalTreeId);
        var tenantTag = LatticeTenantLabel.ForTree(physicalTreeId);
        if (tagTrigger)
        {
            var trig = _currentTriggerKind switch
            {
                TriggerReminder => LatticeMetrics.TriggerReminderTag,
                TriggerRatio => LatticeMetrics.TriggerRatioTag,
                TriggerSize => LatticeMetrics.TriggerSizeTag,
                TriggerOperator => LatticeMetrics.TriggerOperatorTag,
                _ => new KeyValuePair<string, object?>(LatticeMetrics.TagTrigger, _currentTriggerKind),
            };
            LatticeMetrics.CompactionLeavesVisited.Add(
                1,
                new System.Diagnostics.TagList
                {
                    treeTag,
                    LatticeMetrics.OutcomeSkipped,
                    trig,
                    pathTag,
                    tenantTag,
                });
        }
        else
        {
            LatticeMetrics.CompactionLeavesVisited.Add(
                1,
                new System.Diagnostics.TagList
                {
                    treeTag,
                    LatticeMetrics.OutcomeSkipped,
                    pathTag,
                    tenantTag,
                });
        }
    }

    /// <summary>
    /// Resolves the physical tree id and the list of distinct physical shard
    /// indices for the current tree from the registry. Falls back to the
    /// logical tree id and a default identity <see cref="ShardMap"/> when
    /// the registry has no record (e.g. an unregistered tree in a test
    /// harness).
    /// </summary>
    private async Task<(string physicalTreeId, IReadOnlyList<int> physicalShards)> ResolveShardTopologyAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resolved = await registry.ResolveAsync(TreeId);
        var physicalTreeId = string.IsNullOrEmpty(resolved) ? TreeId : resolved;
        var resolvedOpts = await optionsResolver.ResolveAsync(TreeId);
        var map = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolvedOpts.ShardCount);
        return (physicalTreeId, map.GetPhysicalShardIndices());
    }

    private static TimeSpan ClampPeriod(TimeSpan gracePeriod) =>
        gracePeriod < TimeSpan.FromMinutes(1) ? TimeSpan.FromMinutes(1) : gracePeriod;
}

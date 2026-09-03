using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Merges all entries from a source tree into the target tree using LWW semantics,
/// preserving original timestamps. Works shard-by-shard: each source shard's leaf
/// chain is drained into memory (including tombstones) and then merged into the
/// target tree's shards.
/// <para>
/// Follows the same reminder + keepalive + grain-timer pattern used by
/// <see cref="TreeSnapshotGrain"/>.
/// Progress is persisted per-shard so that a silo restart mid-merge can
/// resume without data loss.
/// </para>
/// Key format: <c>{targetTreeId}</c>.
/// </summary>
internal sealed class TreeMergeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeMergeGrain> logger,
    [PersistentState("tree-merge", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeMergeState> state) : ITreeMergeGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "merge-keepalive";
    /// <summary>
    /// Maximum number of attempts (first try + retries combined) before a
    /// source shard is poisoned and the merge advances to the next shard.
    /// <para>
    /// The retry counter is incremented BEFORE each attempt so that
    /// a non-throwing crash mid-merge still counts against the budget. With
    /// <c>MaxAttemptsPerShard = 2</c>, a shard gets its first try plus one
    /// retry on reactivation after a silo crash.
    /// </para>
    /// </summary>
    private const int MaxRetriesPerShard = 2;

    private string TargetTreeId => context.GrainId.Key.ToString()!;
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _mergeTimer;

    public async Task MergeAsync(string sourceTreeId)
    {
        ArgumentNullException.ThrowIfNull(sourceTreeId);

        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TargetTreeId, LatticeOperation.Admin);

        if (string.Equals(TargetTreeId, sourceTreeId, StringComparison.Ordinal))
            throw new ArgumentException("Source tree ID must differ from the target tree ID.", nameof(sourceTreeId));

        if (sourceTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            throw new ArgumentException($"Source tree ID must not start with the reserved prefix '{LatticeConstants.SystemTreePrefix}'.", nameof(sourceTreeId));

        // Defence in depth behind the LatticeGrain.MergeAsync source guard and
        // access-gate check. The coordinator drains the source through the
        // internal shard/leaf tiers, which sit *below* the access-gate seam, so
        // nothing here re-authorizes the read; a source in the dogfooded sys-
        // system-data namespace or the structural t/ tenant namespace is
        // therefore refused outright unless the merge was initiated by
        // first-party machinery under a system-origin scope.
        if (!LatticeAccessGateContext.IsSystemOrigin
            && (sourceTreeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
                || LatticeTenantTrees.IsTenantScoped(sourceTreeId)))
            throw new ArgumentException(
                $"Source tree ID '{sourceTreeId}' is reserved: a merge source may not name a tree in the " +
                $"'{LatticeConstants.SystemDataTreePrefix}' system-data namespace or the " +
                $"'{LatticeTenantTrees.SegmentPrefix}' tenant namespace.",
                nameof(sourceTreeId));

        if (state.State.InProgress)
        {
            // Idempotent if same source.
            if (state.State.SourceTreeId == sourceTreeId)
                return;

            throw new InvalidOperationException(
                $"A merge is already in progress for tree '{TargetTreeId}' from source '{state.State.SourceTreeId}'.");
        }

        // Validate source tree exists.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        if (!await registry.ExistsAsync(sourceTreeId))
            throw new InvalidOperationException(
                $"Source tree '{sourceTreeId}' does not exist.");

        var sourceResolvedOpts = await optionsResolver.ResolveAsync(sourceTreeId);

        await InitiateMergeStateAsync(sourceTreeId, sourceResolvedOpts.ShardCount);
        await StartMergeAsync();
    }

    /// <summary>
    /// Persists merge intent. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateMergeStateAsync(string sourceTreeId, int sourceShardCount)
    {
        // Resolve both aliases and the source's current physical shard list
        // from the registry so that mid-merge map mutations (e.g. adaptive
        // splits on either side) can't mis-route subsequent ticks (audit bug #5).
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var sourceResolved = await registry.ResolveAsync(sourceTreeId);
        var targetResolved = await registry.ResolveAsync(TargetTreeId);
        var sourcePhysicalTreeId = string.IsNullOrEmpty(sourceResolved) ? sourceTreeId : sourceResolved;
        var targetPhysicalTreeId = string.IsNullOrEmpty(targetResolved) ? TargetTreeId : targetResolved;

        var sourceResolvedOpts = await optionsResolver.ResolveAsync(sourceTreeId);
        var sourceMap = await registry.GetShardMapAsync(sourceTreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, sourceResolvedOpts.ShardCount);
        var sourcePhysicalShards = sourceMap.GetPhysicalShardIndices();

        // Snapshot every field the mutation set touches so a failing
        // WriteStateAsync can leave the activation observably equal to
        // what disk (and peers) see. Without this, the in-memory
        // InProgress / SourceTreeId / etc. would survive the throw and
        // the MergeAsync idempotency guard would short-circuit retries
        // on dirty values - a transient storage failure becoming a
        // permanent "merge never started" state until activation recycles.
        var prevInProgress = state.State.InProgress;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevSourceTreeId = state.State.SourceTreeId;
        var prevSourceShardCount = state.State.SourceShardCount;
        var prevSourcePhysicalTreeId = state.State.SourcePhysicalTreeId;
        var prevTargetPhysicalTreeId = state.State.TargetPhysicalTreeId;
        var prevSourcePhysicalShards = state.State.SourcePhysicalShards;
        var prevComplete = state.State.Complete;
        var prevDrainCursor = state.State.DrainCursorKey;

        state.State.InProgress = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.DrainCursorKey = null;
        state.State.SourceTreeId = sourceTreeId;
        state.State.SourceShardCount = sourceShardCount;
        state.State.SourcePhysicalTreeId = sourcePhysicalTreeId;
        state.State.TargetPhysicalTreeId = targetPhysicalTreeId;
        state.State.SourcePhysicalShards = [.. sourcePhysicalShards];
        state.State.Complete = false;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.SourceTreeId = prevSourceTreeId;
            state.State.SourceShardCount = prevSourceShardCount;
            state.State.SourcePhysicalTreeId = prevSourcePhysicalTreeId;
            state.State.TargetPhysicalTreeId = prevTargetPhysicalTreeId;
            state.State.SourcePhysicalShards = prevSourcePhysicalShards;
            state.State.Complete = prevComplete;
            state.State.DrainCursorKey = prevDrainCursor;
            throw;
        }
    }

    public async Task RunMergePassAsync()
    {
        if (!state.State.InProgress) return;

        await EnsureTopologyResolvedAsync();

        while (state.State.NextShardIndex < state.State.SourcePhysicalShards.Length)
        {
            await ProcessCurrentShardAsync();
        }

        await CompleteMergeAsync();
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == KeepaliveReminderName)
        {
            // Defensively re-register the keepalive if the current period drifts
            // from the configured value. The keepalive period
            // is a fixed 1-minute constant today, so this check is primarily
            // a safety net for future constant bumps and for reminders that
            // survive an Orleans upgrade.
            var desired = TimeSpan.FromMinutes(1);
            if (status.Period != desired && state.State.InProgress)
            {
                await reminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: context.GrainId,
                    reminderName: KeepaliveReminderName,
                    dueTime: desired,
                    period: desired);
            }

            if (state.State.InProgress && _mergeTimer is null)
            {
                await StartMergeTimerAsync();
            }
            else if (!state.State.InProgress)
            {
                await UnregisterKeepaliveAsync();
                this.DeactivateOnIdle();
            }
        }
    }

    private async Task StartMergeAsync()
    {
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        await StartMergeTimerAsync();
    }

    private Task StartMergeTimerAsync()
    {
        _mergeTimer = this.RegisterGrainTimer(
            OnMergeTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(2)));
        return Task.CompletedTask;
    }

    private async Task OnMergeTimerTick(CancellationToken ct)
    {
        await ProcessNextShardAsync();
    }

    /// <summary>
    /// Processes the next source shard. If all shards are done,
    /// completes the merge. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task ProcessNextShardAsync()
    {
        await EnsureTopologyResolvedAsync();

        if (state.State.NextShardIndex >= state.State.SourcePhysicalShards.Length)
        {
            await CompleteMergeAsync();
            return;
        }

        await ProcessCurrentShardAsync();
    }

    /// <summary>
    /// Processes the current source shard, with crash-resume retry semantics:
    /// <list type="number">
    ///   <item>Check the poison cap. If <see cref="TreeMergeState.ShardRetries"/>
    ///   has reached <see cref="MaxRetriesPerShard"/>, skip the shard, reset the
    ///   retry counter, and advance the cursor. The shard is considered poisoned
    ///   and is logged at Warning level.</item>
    ///   <item>Increment <c>ShardRetries</c> and persist BEFORE attempting the
    ///   merge. This ensures that a non-throwing failure - a process crash or
    ///   silo restart mid-merge - still counts against the retry budget on
    ///   reactivation, preventing an infinite retry loop against a shard that
    ///   deterministically kills the silo.</item>
    ///   <item>Run the merge. On success, reset <c>ShardRetries</c> to 0 and
    ///   advance the cursor. On exception, leave the incremented retry counter
    ///   in place and surface the error; the next tick re-enters this method
    ///   and either retries or skips per step 1.</item>
    /// </list>
    /// </summary>
    private async Task ProcessCurrentShardAsync()
    {
        var shardIndex = state.State.SourcePhysicalShards[state.State.NextShardIndex];

        if (state.State.ShardRetries >= MaxRetriesPerShard)
        {
            logger.LogWarning(
                "Poisoning source shard {ShardIndex} of tree {SourceTreeId} into {TargetTreeId} after {Retries} attempts; skipping.",
                shardIndex, state.State.SourceTreeId, TargetTreeId, state.State.ShardRetries);
            // Snapshot the two fields the poison-skip mutates so a failing
            // persist doesn't leak an in-memory advance ahead of disk.
            // The next reminder tick would otherwise re-poison the next
            // shard while disk still pointed at this one.
            var prevNextShardIndex = state.State.NextShardIndex;
            var prevShardRetries = state.State.ShardRetries;
            var prevDrainCursor = state.State.DrainCursorKey;
            state.State.NextShardIndex++;
            state.State.ShardRetries = 0;
            // Each shard owns its own sweep, so the cursor never carries across
            // a shard advance - a stale key would re-descend into the wrong
            // shard's keyspace.
            state.State.DrainCursorKey = null;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.NextShardIndex = prevNextShardIndex;
                state.State.ShardRetries = prevShardRetries;
                state.State.DrainCursorKey = prevDrainCursor;
                throw;
            }
            return;
        }

        // Increment the retry counter BEFORE attempting the merge so a
        // non-throwing crash (silo restart, host kill) still burns budget
        // on reactivation. Without this, ShardRetries stays at 0 across
        // restarts and a deterministic-crash shard would loop forever.
        //
        // Snapshot ShardRetries so a failing persist of the pre-merge
        // increment doesn't leak the bumped counter into in-memory state
        // while disk holds the old value - the documented safety invariant
        // requires in-memory and disk to agree on the burn-budget for the
        // poison cap to function deterministically across reactivation.
        var prevShardRetriesPreMerge = state.State.ShardRetries;
        state.State.ShardRetries++;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.ShardRetries = prevShardRetriesPreMerge;
            throw;
        }

        try
        {
            var cursorBefore = state.State.DrainCursorKey;
            var (sweepComplete, resumeFrom) = await MergeShardAsync(shardIndex);

            if (!sweepComplete)
            {
                // A bounded pass that yielded. Persist the resume position and
                // stay on this shard; the next tick continues from the key.
                //
                // The retry budget is reset only when the cursor actually moved.
                // A partial pass that advanced the cursor is forward progress,
                // not a failed attempt, and burning budget for it would poison a
                // large but perfectly healthy shard after two passes. A pass
                // that somehow yielded without moving the cursor keeps the
                // increment, so a genuinely stuck shard still reaches the cap.
                var madeProgress = !string.Equals(resumeFrom, cursorBefore, StringComparison.Ordinal);
                var prevRetriesPartial = state.State.ShardRetries;
                state.State.DrainCursorKey = resumeFrom;
                if (madeProgress) state.State.ShardRetries = 0;
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    state.State.DrainCursorKey = cursorBefore;
                    state.State.ShardRetries = prevRetriesPartial;
                    throw;
                }
                return;
            }

            // Snapshot the fields the success-advance mutates. A
            // failing persist here would otherwise advance the in-memory
            // shard cursor past a shard whose drain disk doesn't yet
            // know was complete - a reactivation would then re-drain
            // it (safe, LWW-idempotent on payloads, but burns budget).
            var prevNextShardIndexSuccess = state.State.NextShardIndex;
            var prevShardRetriesSuccess = state.State.ShardRetries;
            var prevDrainCursorSuccess = state.State.DrainCursorKey;
            state.State.NextShardIndex++;
            state.State.ShardRetries = 0;
            state.State.DrainCursorKey = null;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.NextShardIndex = prevNextShardIndexSuccess;
                state.State.ShardRetries = prevShardRetriesSuccess;
                state.State.DrainCursorKey = prevDrainCursorSuccess;
                throw;
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Merge failed for source shard {ShardIndex} of tree {SourceTreeId} into {TargetTreeId} (attempt {Attempt}/{Max})",
                shardIndex, state.State.SourceTreeId, TargetTreeId, state.State.ShardRetries, MaxRetriesPerShard);

            // ShardRetries was already incremented above. The next tick
            // will re-enter ProcessCurrentShardAsync which either retries
            // this shard or poisons it per the top-of-method check.
            throw;
        }
    }

    /// <summary>
    /// Ensures that the aliased physical tree ids and the source physical shard
    /// list have been resolved for the current pass. Back-fills state persisted
    /// by pre-fix versions or by tests that only set <see cref="TreeMergeState.SourceShardCount"/>.
    /// </summary>
    private async Task EnsureTopologyResolvedAsync()
    {
        var needsResolve =
            state.State.SourcePhysicalShards is null ||
            state.State.SourcePhysicalShards.Length == 0 ||
            string.IsNullOrEmpty(state.State.SourcePhysicalTreeId) ||
            string.IsNullOrEmpty(state.State.TargetPhysicalTreeId);

        if (!needsResolve) return;

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var sourceTreeId = state.State.SourceTreeId
            ?? throw new InvalidOperationException("Cannot resolve topology without a source tree id.");
        if (string.IsNullOrEmpty(state.State.SourcePhysicalTreeId))
        {
            var resolved = await registry.ResolveAsync(sourceTreeId);
            state.State.SourcePhysicalTreeId = string.IsNullOrEmpty(resolved) ? sourceTreeId : resolved;
        }
        if (string.IsNullOrEmpty(state.State.TargetPhysicalTreeId))
        {
            var resolved = await registry.ResolveAsync(TargetTreeId);
            state.State.TargetPhysicalTreeId = string.IsNullOrEmpty(resolved) ? TargetTreeId : resolved;
        }

        var sourceResolvedOpts2 = await optionsResolver.ResolveAsync(sourceTreeId);
        var sourceMap = await registry.GetShardMapAsync(sourceTreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, sourceResolvedOpts2.ShardCount);
        state.State.SourcePhysicalShards = [.. sourceMap.GetPhysicalShardIndices()];
    }

    /// <summary>
    /// Drains entries (including tombstones) from the source shard's leaf chain,
    /// streaming each leaf's delta to the target shards before loading the next
    /// leaf. Streaming (rather than buffering the entire shard) bounds peak memory
    /// for shards holding millions of keys (audit bug #4).
    /// <para>
    /// <b>Work-bounded and resumable</b> (issue 1973). One call visits at most
    /// <see cref="LatticeOptions.BackgroundDrainLeavesPerPass"/> leaves and then
    /// returns the key the next pass must re-descend onto, so merging a
    /// thousand-leaf shard is steady background work rather than one unbounded
    /// turn that leaves this coordinator unable to report progress or complete.
    /// </para>
    /// <para>
    /// <b>What a pass boundary makes observable.</b> Nothing on the target that
    /// a leaf boundary did not already. This drain has never been atomic: it
    /// merges each leaf's delta into the target shards as it goes, so a reader
    /// of the target has always been able to observe some source keys merged
    /// and others not. Every entry is forwarded under its original HLC, so both
    /// a re-drained leaf and an interleaved concurrent write resolve to the same
    /// LWW fixed point. What a bound adds is that the partial state is now
    /// durable across a crash instead of restarting the shard, which strictly
    /// reduces the work an interruption costs.
    /// </para>
    /// <para>
    /// <b>What it does not change.</b> The merge neither freezes the source nor
    /// shadow-forwards its writes - the source tree is unmodified by contract -
    /// so a write landing on a region the drain has already passed is not
    /// carried across. That is pre-existing and inherent to the documented
    /// "eventually convergent (LWW)" guarantee rather than to the work bound:
    /// the drain already advanced a shard at a time across timer ticks, so a
    /// write behind the drain position was already missed at a shard boundary.
    /// Bounding subdivides that same boundary; it does not introduce a window
    /// the operation did not already have. A merge that must not miss
    /// concurrent writes needs a source-side barrier, which this operation does
    /// not offer.
    /// </para>
    /// </summary>
    /// <returns>
    /// Whether the source shard's whole leaf chain has been swept, and the key
    /// the next pass resumes from when it has not.
    /// </returns>
    private async Task<(bool SweepComplete, string? ResumeFromInclusive)> MergeShardAsync(int sourceShardIndex)
    {
        var sourceTreeId = state.State.SourceTreeId!;
        var sourcePhysicalTreeId = state.State.SourcePhysicalTreeId ?? sourceTreeId;
        var targetPhysicalTreeId = state.State.TargetPhysicalTreeId ?? TargetTreeId;
        var sourceShardKey = $"{sourcePhysicalTreeId}/{sourceShardIndex}";
        var sourceShard = grainFactory.GetGrain<IShardRootGrain>(sourceShardKey);

        // Resolve the target tree's shard map (falling back to the default
        // identity map when the tree has no custom map persisted).
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var targetResolvedOpts = await optionsResolver.ResolveAsync(TargetTreeId);
        var targetShardMap = await registry.GetShardMapAsync(TargetTreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, targetResolvedOpts.ShardCount);

        // Walk the source leaf chain, flushing each leaf's delta through the
        // target shard map before loading the next leaf.
        var emptyVector = new VersionVector();
        var walk = await BoundedLeafWalk.StartAsync(
            grainFactory,
            sourceShard,
            state.State.DrainCursorKey,
            LeafWalkBudget.ForBackgroundDrain(targetResolvedOpts));

        while (walk.HasLeaf)
        {
            var leaf = walk.CurrentLeaf;
            var delta = await leaf.GetDeltaSinceAsync(emptyVector);

            if (delta.Entries.Count > 0)
            {
                // Group this leaf's entries by target physical shard.
                var targetBuckets = new Dictionary<int, Dictionary<string, LwwValue<byte[]>>>();
                foreach (var (key, lww) in delta.Entries)
                {
                    var targetIdx = targetShardMap.Resolve(key);
                    if (!targetBuckets.TryGetValue(targetIdx, out var bucket))
                    {
                        bucket = [];
                        targetBuckets[targetIdx] = bucket;
                    }
                    bucket[key] = lww;
                }

                // Merge this leaf's buckets into each target shard in parallel.
                var tasks = new List<Task>(targetBuckets.Count);
                foreach (var (targetIdx, bucket) in targetBuckets)
                {
                    var targetShardKey = $"{targetPhysicalTreeId}/{targetIdx}";
                    var targetShard = grainFactory.GetGrain<IShardRootGrain>(targetShardKey);
                    tasks.Add(targetShard.MergeManyAsync(bucket));
                }
                await Task.WhenAll(tasks);
            }

            if (!await walk.MoveNextAsync()) break;
        }

        return (walk.Completed, walk.ResumeFromInclusive);
    }

    internal async Task CompleteMergeAsync()
    {
        _mergeTimer?.Dispose();
        _mergeTimer = null;

        // Snapshot every field the completion-flip mutates. Without this,
        // a failing WriteStateAsync would leave InProgress=false and
        // Complete=true in memory while disk still says the merge is
        // running. IsCompleteAsync would then lie to callers (returning
        // true on the dirty in-memory value); the keepalive reminder
        // would still tick and re-enter RunMergePassAsync which now
        // short-circuits at its !InProgress guard - the merge halts on
        // this activation while disk-loaded reactivations would resume.
        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevDrainCursorComplete = state.State.DrainCursorKey;

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.DrainCursorKey = null;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.DrainCursorKey = prevDrainCursorComplete;
            throw;
        }

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TargetTreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "merge"),
            LatticeTenantLabel.ForTree(TargetTreeId));

        await UnregisterKeepaliveAsync();
        this.DeactivateOnIdle();
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
            logger.LogWarning(ex, "Failed to unregister merge keepalive reminder for tree {TreeId}", TargetTreeId);
        }
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() =>
        Task.FromResult(!state.State.InProgress);
}

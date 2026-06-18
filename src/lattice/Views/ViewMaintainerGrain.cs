using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="IViewMaintainerGrain"/>. One cluster-wide activation per
/// view (keyed by view name) tails every source WAL partition from the durable
/// checkpoint, projects each user mutation, coalesces repeated view-key writes
/// (last-writer-wins on the source HLC), applies the survivors to the
/// <c>view-{name}</c> tree, advances and persists the checkpoint, and reports the
/// applied cursor to the WAL garbage collector.
/// <para>
/// <b>HLC-LWW idempotent apply (Phase 1).</b> There is no public "set with an
/// explicit source HLC" path on <see cref="ILattice"/>, so the maintainer does
/// not compare source HLC against the view-local HLC. Instead it realises
/// last-writer-wins in two layers: (a) within a drain pass it coalesces by view
/// key keeping the highest source <see cref="ViewWrite.Timestamp"/> (the LWW
/// decision point), and (b) it applies contiguous WAL offset ranges in offset
/// order per partition - and a source key lives on exactly one partition, so
/// per-partition offset order is HLC order for that key. A crash mid-pass simply
/// re-applies the same in-order suffix from the last persisted checkpoint, which
/// is idempotent.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    ILogger<ViewMaintainerGrain> logger,
    IViewCatalog catalog,
    ICommitLogReader commitLogReader,
    IWalCursorRegistry cursorRegistry,
    LatticeOptionsResolver optionsResolver,
    IOptionsMonitor<LatticeViewOptions> viewOptions,
    IOptionsMonitor<LatticeOptions> latticeOptions,
    [PersistentState("view-checkpoint", LatticeOptions.StorageProviderName)]
    IPersistentState<ViewCheckpointState> state)
    : IGrainBase, IRemindable, IViewMaintainerGrain
{
    private const string KeepaliveReminderName = "view-maintainer-keepalive";

    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(20);

    private static readonly Histogram<long> ApplyLag = LatticeMetrics.ViewApplyLag;
    private static readonly Histogram<long> BacklogDepth = LatticeMetrics.ViewBacklogDepth;
    private static readonly Counter<long> Applied = LatticeMetrics.ViewApplied;
    private static readonly Counter<long> KeyCollisions = LatticeMetrics.ViewKeyCollisions;
    private static readonly Counter<long> ViewAtomicStagingBackstop = LatticeMetrics.ViewAtomicStagingBackstop;
    private static readonly Counter<long> LagBudgetEviction = LatticeMetrics.ViewLagBudgetEviction;

    private IGrainTimer? _timer;
    private string? _consumerId;

    // Set in EnsureActiveAsync when this view is ShipView and the source WAL is not
    // locally readable here (a thin consumer cluster). A suppressed maintainer does
    // not drain, pin the WAL, or rebuild: the view tree is received via replication.
    private bool _shipViewSuppressed;

    // UTC ticks of the last lag-budget force-eviction this activation (0 = none).
    // Gates re-eviction so a view kept chronically over budget by sustained writes
    // is rebuilt at most once per LagEvictionCooldown rather than on every drain.
    private long _lastLagEvictionTicks;

    // Set once EnsureActiveAsync has run on this activation. A keepalive reminder
    // can wake a freshly reactivated grain before any EnsureActiveAsync call; until
    // activation has established the ShipView-suppression and projection-version
    // state, the reminder routes through EnsureActiveAsync rather than draining with
    // default (unsuppressed, unchecked) state.
    private bool _activated;

    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    private string ViewName => context.GrainId.Key.ToString()!;

    // Cached per-activation: ViewName is fixed for the lifetime of the grain, so
    // the derived cursor-consumer id is interpolated once rather than on every
    // drain / rebuild pass.
    private string ConsumerId => _consumerId ??= $"view:{ViewName}";

    // The live view tree is generation-addressed: the active generation is durable
    // maintainer state advanced only by a shadow-swap. Resolved fresh each call
    // because a rebuild can flip the active generation within an activation.
    private string ViewTreeId => GenerationTreeId(state.State.ActiveGeneration);

    private LatticeViewOptions Options => viewOptions.Get(ViewName);

    private KeyValuePair<string, object?> ViewTag => new(LatticeMetrics.TagView, ViewName);

    /// <inheritdoc />
    public async Task EnsureActiveAsync(CancellationToken cancellationToken = default)
    {
        // Authorise this turn's view-tree writes (rebuild + initial drain). The flag
        // flows on RequestContext to every nested view-tree call, so a direct user
        // write - which never opens this scope - is rejected by the ILattice guard.
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            logger.LogWarning("View '{ViewName}' has no registration; maintainer cannot start.", ViewName);
            return;
        }

        // ShipView producer designation (ASSUMPTION - Decision A): a cluster is the
        // producer for a ShipView view iff the view's source tree WAL is locally
        // readable here. A thin consumer that registered the view but has no local
        // source WAL suppresses its maintainer entirely - no reminder, no timer, no
        // drain, no cursor pin - and receives the view tree through replication.
        // DeriveLocally (the default, and every existing deployment) always has the
        // source locally and is never suppressed.
        if (Options.ReplicationMode == LatticeViewReplicationMode.ShipView
            && !await IsSourceLocallyReadableAsync(registration, cancellationToken))
        {
            _shipViewSuppressed = true;
            _activated = true;
            logger.LogInformation(
                "View '{ViewName}' is ShipView with no locally-readable source WAL; suppressing the maintainer on this consumer cluster (the view tree is received via replication).",
                ViewName);
            return;
        }

        _shipViewSuppressed = false;

        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        // A projection-version change means the view's logic is no longer the one
        // that built the persisted state; rebuild from current source state.
        if (!string.IsNullOrEmpty(state.State.ProjectionVersion)
            && !string.Equals(state.State.ProjectionVersion, registration.ProjectionVersion, StringComparison.Ordinal))
        {
            logger.LogInformation(
                "View '{ViewName}' projection version changed ({Old} -> {New}); rebuilding.",
                ViewName, state.State.ProjectionVersion, registration.ProjectionVersion);
            await RebuildAsync(cancellationToken);
        }
        else if (string.IsNullOrEmpty(state.State.ProjectionVersion))
        {
            state.State.ProjectionVersion = registration.ProjectionVersion;
            await state.WriteStateAsync();
        }

        StartTimer();
        _activated = true;
        await DrainAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<int> DrainAsync(CancellationToken cancellationToken = default)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return 0;
        }

        // ShipView consumer (Decision A): the maintainer is suppressed, so a drain
        // is a no-op. The view tree is maintained by replication, not this grain.
        if (_shipViewSuppressed)
        {
            return 0;
        }

        // Reclaim a swapped-out generation tree once its post-swap reader grace has
        // elapsed; runs on the regular drain cadence so reclamation is crash-safe
        // (durable) and never blocks the swap itself.
        await TryReclaimPendingGenerationAsync(cancellationToken);

        // Lag-budget eviction (the GC contract): a view that has fallen further
        // behind than its configured MaxLagBudget - chronically slow, or a crashed
        // maintainer reactivated on a keepalive tick - unpins the source WAL and
        // re-onboards via rebuild so it can no longer pin WAL retention. Disabled
        // (zero overhead) when the budget is 0 (the default).
        if (await TryEvictForLagBudgetAsync(registration, cancellationToken))
        {
            return 0;
        }

        if (registration.IsAggregation)
        {
            return await DrainAggregationAsync(registration, cancellationToken);
        }

        var options = Options;
        var batchSize = options.BatchSize > 0 ? options.BatchSize : LatticeViewOptions.DefaultBatchSize;
        var sourceTreeId = registration.SourceTreeId;
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);

        // Fall-off-log guard: if the oldest still-readable offset has advanced
        // past our next-to-read position, the entries we need were trimmed and we
        // must rebuild from current source state rather than tail-replay.
        for (var partition = 0; partition < partitions; partition++)
        {
            var checkpoint = state.State.AppliedOffsets.GetValueOrDefault(partition, -1);
            var tail = await commitLogReader.GetTailOffsetAsync(sourceTreeId, partition, cancellationToken);
            if (tail > checkpoint + 1)
            {
                logger.LogWarning(
                    "View '{ViewName}' fell off the WAL on partition {Partition} (tail {Tail} > checkpoint {Checkpoint}); rebuilding.",
                    ViewName, partition, tail, checkpoint);
                await RebuildAsync(cancellationToken);
                return 0;
            }
        }

        var collected = new List<ViewWrite>();
        var advancedOffsets = new Dictionary<int, long>();
        var highest = state.State.HighestAppliedTimestamp;
        var completedTransactions = new List<Guid>();
        long backlogRead = 0;

        for (var partition = 0; partition < partitions; partition++)
        {
            var checkpoint = state.State.AppliedOffsets.GetValueOrDefault(partition, -1);
            var lastOffset = checkpoint;
            var readThisPartition = 0;

            await foreach (var (offset, mutation) in commitLogReader
                .ReadAsync(sourceTreeId, partition, checkpoint, cancellationToken)
                )
            {
                lastOffset = offset;
                readThisPartition++;
                backlogRead++;

                switch (Classify(mutation, out var terminalCommit, out var terminalAbort))
                {
                    case StagingDisposition.Apply:
                        RecordOrdinaryOverStagedKey(mutation);
                        foreach (var write in registration.Projection!.Project(mutation))
                        {
                            collected.Add(write);
                        }

                        break;

                    case StagingDisposition.Stage:
                        HandleStagingEntry(mutation, partition, offset, terminalCommit, terminalAbort, completedTransactions);
                        break;

                    case StagingDisposition.Skip:
                    default:
                        break;
                }

                // Advance the consumed-HLC high-water mark for every entry read
                // past, applicable or not. It pins WAL GC to what has been
                // consumed and is the position the read-your-writes barrier waits
                // on, so a skipped maintenance / transaction-terminal entry at the
                // source head must not leave it stuck below a non-applicable head.
                if (mutation.Timestamp > highest)
                {
                    highest = mutation.Timestamp;
                }

                if (readThisPartition >= batchSize)
                {
                    break;
                }
            }

            advancedOffsets[partition] = lastOffset;
        }

        // Bounded-buffer / retention backstop: if staging would grow without
        // bound or an un-terminated batch can no longer be held under the WAL
        // retention ceiling, abandon incremental staging and rebuild from
        // current committed source state (which excludes the uncommitted
        // prepares). The rebuild owns the checkpoint and cursor for this pass.
        if (StagingBackstopTripped(options, await GetSourceWalRetentionAsync(sourceTreeId)))
        {
            await RebuildAsync(cancellationToken);
            return 0;
        }

        // A re-keyed unconstrained range delete cannot be lowered to exact view
        // writes; the projection emits a RangeReconcile asking us to re-derive the
        // affected range from source. The conservative, always-correct realisation
        // is a full rebuild (it reads current source state and re-advances the
        // checkpoint to head), so do that and let the rebuild own this pass.
        if (collected.Exists(static w => w.Kind == ViewWriteKind.RangeReconcile))
        {
            logger.LogInformation(
                "View '{ViewName}' observed an unconstrained range delete on a re-keyed projection; rebuilding to reconcile the affected range.",
                ViewName);
            await RebuildAsync(cancellationToken);
            return 0;
        }

        var viewTree = grainFactory.GetGrain<ILattice>(ViewTreeId);
        DetectAndReportCollisions(collected);
        var appliedCount = await ApplySurvivorsAsync(viewTree, collected, cancellationToken);

        // Flush every atomic batch that completed this pass through the view
        // tree's atomic primitive, after the ordinary survivors so a committed
        // batch wins a same-pass non-atomic write to the same key. Each batch is
        // projected through the SAME filter / re-key projection as ordinary
        // writes - staging only defers WHEN they are applied, not HOW.
        appliedCount += await FlushCompletedFilterBatchesAsync(viewTree, registration, completedTransactions, cancellationToken);

        // Hold the persisted resume offset back below the lowest still-staged
        // entry so a restart re-reads and re-stages an incomplete batch.
        ApplyCheckpointHoldBack(advancedOffsets, partitions);

        var offsetsAdvanced = false;
        foreach (var (partition, offset) in advancedOffsets)
        {
            if (state.State.AppliedOffsets.GetValueOrDefault(partition, -1) != offset)
            {
                state.State.AppliedOffsets[partition] = offset;
                offsetsAdvanced = true;
            }
        }

        state.State.HighestAppliedTimestamp = highest;
        state.State.ProjectionVersion = registration.ProjectionVersion;

        if (offsetsAdvanced || appliedCount > 0)
        {
            await state.WriteStateAsync();
        }

        var blockedAtHlc = ComputeBlockedAtHlc();
        if (highest > HybridLogicalClock.Zero || blockedAtHlc is not null)
        {
            await cursorRegistry.ReportCursorAsync(sourceTreeId, ConsumerId, highest, blockedAtHlc, cancellationToken)
                ;
        }

        ApplyLag.Record(await ComputeLagAsync(sourceTreeId, partitions, cancellationToken), ViewTag);
        BacklogDepth.Record(backlogRead, ViewTag);
        if (appliedCount > 0)
        {
            Applied.Add(appliedCount, ViewTag);
        }

        // Run any reconcile a cross-tree degrade scheduled this pass, after the
        // checkpoint is persisted so the rebuild does not clear the staging buffer
        // under the flush loop.
        await RunPendingCrossTreeReconcileAsync(cancellationToken);

        return appliedCount;
    }

    /// <inheritdoc />
    public async Task<long> GetLagAsync(CancellationToken cancellationToken = default)
    {
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return 0;
        }

        var partitions = await optionsResolver.GetWalPartitionsAsync(registration.SourceTreeId);
        return await ComputeLagAsync(registration.SourceTreeId, partitions, cancellationToken);
    }

    /// <inheritdoc />
    public async Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        if (target <= HybridLogicalClock.Zero)
        {
            // Nothing committed at or before zero to wait for.
            return;
        }

        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (state.State.HighestAppliedTimestamp >= target)
            {
                return;
            }

            await DrainAsync(cancellationToken);

            if (state.State.HighestAppliedTimestamp >= target)
            {
                return;
            }

            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"View '{ViewName}' did not apply source HLC {target} within {timeout}.");
            }

            var remaining = deadline - DateTime.UtcNow;
            var delay = remaining < PollInterval ? remaining : PollInterval;
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    /// <inheritdoc />
    public async Task<HybridLogicalClock> CaptureSourceHeadHlcAsync(CancellationToken cancellationToken = default)
    {
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return HybridLogicalClock.Zero;
        }

        var sourceTreeId = registration.SourceTreeId;
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);
        var head = HybridLogicalClock.Zero;

        for (var partition = 0; partition < partitions; partition++)
        {
            var headOffset = await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken);
            if (headOffset <= 0)
            {
                continue;
            }

            // Read only the tail entry (offset headOffset - 1) by starting the
            // cursored read two below the head; its HLC is this partition's head.
            await foreach (var (_, mutation) in commitLogReader
                .ReadAsync(sourceTreeId, partition, headOffset - 2, cancellationToken))
            {
                if (mutation.Timestamp > head)
                {
                    head = mutation.Timestamp;
                }
            }
        }

        return head;
    }

    /// <inheritdoc />
    public async Task RebuildAsync(CancellationToken cancellationToken = default)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return;
        }

        // ShipView (ASSUMPTION - Decision B): pin the stable generation-0
        // view-{name} tree id and rebuild in place so the replicated tree id is
        // stable and matches the operator's replicated-trees entry. Transient
        // divergence on a producer rebuild is acceptable per the best-effort
        // contract and heals on consumers via replication anti-entropy.
        if (Options.ReplicationMode == LatticeViewReplicationMode.ShipView)
        {
            await InPlaceRebuildAsync(registration, cancellationToken);
            return;
        }

        // DeriveLocally: build a complete new generation in the background, then
        // atomically swap it in (see ViewMaintainerGrain.ShadowSwap). Readers never
        // observe a half-built view.
        var built = await BuildShadowAsync(registration, cancellationToken);
        await SwapToShadowAsync(registration, built.Offsets, built.Highest, cancellationToken);
    }

    /// <summary>
    /// Returns whether the view's source tree WAL is locally readable on this
    /// cluster - the ShipView producer-designation probe (Decision A). True when any
    /// source partition has a head offset greater than zero. A view whose source has
    /// never been written here (a thin consumer cluster, or - as a documented
    /// edge case - a producer that has not yet received its first source write) reads
    /// as not locally readable.
    /// </summary>
    private async Task<bool> IsSourceLocallyReadableAsync(ViewRegistration registration, CancellationToken cancellationToken)
    {
        var sourceTreeId = registration.SourceTreeId;
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);
        for (var partition = 0; partition < partitions; partition++)
        {
            if (await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken) > 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Force-evicts the view when its lag exceeds the configured
    /// <see cref="LatticeViewOptions.MaxLagBudget"/>: unpins the source WAL (so a
    /// chronically-slow or dead view stops holding WAL garbage collection) and
    /// re-onboards the view via <see cref="RebuildAsync"/> from current committed
    /// source state, which re-pins the cursor at the rebuilt head. Returns whether
    /// an eviction happened. A budget of zero (the default) disables eviction and
    /// short-circuits before any extra WAL reads. After an eviction the maintainer
    /// observes a <see cref="LatticeViewOptions.LagEvictionCooldown"/> before it
    /// will force-evict again, so a view kept chronically over budget by sustained
    /// writes drains normally between evictions rather than thrashing on a rebuild
    /// every drain. Crash-safe and idempotent: the rebuild owns the checkpoint and
    /// cursor, so a crash mid-eviction simply re-evicts on the next drain.
    /// </summary>
    private async Task<bool> TryEvictForLagBudgetAsync(ViewRegistration registration, CancellationToken cancellationToken)
    {
        var budget = Options.MaxLagBudget;
        if (budget <= 0)
        {
            return false;
        }

        var sourceTreeId = registration.SourceTreeId;
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);
        var lag = await ComputeLagAsync(sourceTreeId, partitions, cancellationToken);
        if (lag <= budget)
        {
            return false;
        }

        // Post-eviction cooldown (hysteresis): once evicted, do not rebuild again
        // until the cooldown elapses. Under sustained over-budget writes the view
        // keeps draining normally in between rather than thrashing on a rebuild
        // every drain.
        var cooldown = Options.LagEvictionCooldown;
        if (cooldown <= TimeSpan.Zero)
        {
            cooldown = LatticeViewOptions.DefaultLagEvictionCooldown;
        }

        var nowTicks = DateTime.UtcNow.Ticks;
        if (_lastLagEvictionTicks != 0 && nowTicks - _lastLagEvictionTicks < cooldown.Ticks)
        {
            return false;
        }

        logger.LogWarning(
            "View '{ViewName}' lag {Lag} exceeded MaxLagBudget {Budget}; force-evicting (unpinning the source WAL and rebuilding from current source state).",
            ViewName, lag, budget);
        LagBudgetEviction.Add(1, ViewTag);
        _lastLagEvictionTicks = nowTicks;

        // Unpin the source WAL before rebuilding so the GC is released even if the
        // rebuild is slow; the rebuild re-pins at the rebuilt head.
        await cursorRegistry.UnregisterAsync(sourceTreeId, ConsumerId, cancellationToken);
        await RebuildAsync(cancellationToken);
        return true;
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        if (reminderName != KeepaliveReminderName)
        {
            return;
        }

        try
        {
            // A reminder can wake a cold-reactivated grain that never ran
            // EnsureActiveAsync this activation; route through it once so ShipView
            // suppression and projection-version re-evaluation are established
            // before any drain, instead of draining with default state.
            //
            // A ShipView producer that activated over a still-empty source was
            // suppressed (the source was not yet locally readable). Re-route a
            // suppressed maintainer through EnsureActiveAsync on every keepalive so
            // it re-probes source readability and un-suppresses (starts draining and
            // pinning) once the source has since become locally readable - otherwise
            // a fresh producer would stay suppressed until restart.
            if (!_activated || _shipViewSuppressed)
            {
                await EnsureActiveAsync(CancellationToken.None);
            }
            else
            {
                if (_timer is null)
                {
                    StartTimer();
                }

                await DrainAsync(CancellationToken.None);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "View '{ViewName}' drain on keepalive tick failed; will retry.", ViewName);
        }
    }

    private async Task<long> ComputeLagAsync(string sourceTreeId, int partitions, CancellationToken cancellationToken)
    {
        long lag = 0;
        for (var partition = 0; partition < partitions; partition++)
        {
            var head = await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken);
            var checkpoint = state.State.AppliedOffsets.GetValueOrDefault(partition, -1);
            var partitionLag = head - (checkpoint + 1);
            if (partitionLag > 0)
            {
                lag += partitionLag;
            }
        }

        return lag;
    }

    private async Task<int> ApplySurvivorsAsync(ILattice viewTree, List<ViewWrite> collected, CancellationToken cancellationToken)
    {
        if (!collected.Exists(static w => w.Kind == ViewWriteKind.RangeDelete))
        {
            // Fast path: only point writes. Coalesce by view key (LWW on the
            // source HLC) and apply each survivor.
            var applied = 0;
            foreach (var write in ViewWriteCoalescer.Coalesce(collected))
            {
                await ApplyAsync(viewTree, write, cancellationToken);
                applied++;
            }

            return applied;
        }

        // Range path: a range delete cannot be globally coalesced by view key
        // against point writes, and its outcome interleaves with point writes by
        // source HLC. Apply every collected write in ascending source-HLC order
        // (stable), so a point write with a higher HLC than a range delete
        // survives it and a lower one is removed by it - the convergent
        // last-writer-wins outcome regardless of which source partition each
        // write arrived on.
        var appliedOrdered = 0;
        collected.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));
        foreach (var write in collected)
        {
            await ApplyAsync(viewTree, write, cancellationToken);
            appliedOrdered++;
        }

        return appliedOrdered;
    }

    private void DetectAndReportCollisions(IEnumerable<ViewWrite> collected)
    {
        var collisions = ViewKeyCollisionDetector.Detect(collected);
        if (collisions.Count == 0)
        {
            return;
        }

        KeyCollisions.Add(collisions.Count, ViewTag);
        logger.LogWarning(
            "View '{ViewName}' detected {Count} re-key collision(s) in a drain batch (e.g. view key '{Example}' produced by multiple distinct source keys); the key re-map is not injective. Resolving by source-HLC last-writer-wins.",
            ViewName, collisions.Count, collisions[0]);
    }

    private static async Task ApplyAsync(ILattice viewTree, ViewWrite write, CancellationToken cancellationToken)
    {
        switch (write.Kind)
        {
            case ViewWriteKind.Upsert:
                if (write.ExpiresAtTicks > 0)
                {
                    var remaining = write.ExpiresAtTicks - DateTime.UtcNow.Ticks;
                    if (remaining <= 0)
                    {
                        // Already expired by the time it would be applied: removing
                        // the key is the correct convergent outcome.
                        await viewTree.DeleteAsync(write.Key, cancellationToken);
                        return;
                    }

                    await viewTree.SetAsync(write.Key, write.Value!, TimeSpan.FromTicks(remaining), cancellationToken)
                        ;
                    return;
                }

                await viewTree.SetAsync(write.Key, write.Value!, cancellationToken);
                return;

            case ViewWriteKind.Delete:
                await viewTree.DeleteAsync(write.Key, cancellationToken);
                return;

            case ViewWriteKind.RangeDelete:
                // Key-preserving range retraction: the view key equals the source
                // key, so removing the view's slice of [Key, EndKey) is exact.
                await viewTree.DeleteRangeAsync(write.Key, write.EndKey!, cancellationToken);
                return;

            default:
                // ViewWriteKind.CrdtDelta is reserved for a later phase, and
                // ViewWriteKind.RangeReconcile is resolved to a rebuild before
                // apply; neither is ever applied here.
                return;
        }
    }

    private void StartTimer()
    {
        var period = Options.CoalesceWindow;
        if (period <= TimeSpan.Zero)
        {
            period = LatticeViewOptions.DefaultCoalesceWindow;
        }

        _timer = this.RegisterGrainTimer(
            OnTimerTickAsync,
            new GrainTimerCreationOptions(dueTime: period, period: period));
    }

    private async Task OnTimerTickAsync(CancellationToken cancellationToken)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        try
        {
            await DrainAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "View '{ViewName}' background drain pass failed; will retry.", ViewName);
        }
    }
}

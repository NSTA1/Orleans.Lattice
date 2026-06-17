using System.Diagnostics.Metrics;
using System.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Views;

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

    private IGrainTimer? _timer;
    private string? _consumerId;

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
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            logger.LogWarning("View '{ViewName}' has no registration; maintainer cannot start.", ViewName);
            return;
        }

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
        await DrainAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<int> DrainAsync(CancellationToken cancellationToken = default)
    {
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return 0;
        }

        // Reclaim a swapped-out generation tree once its post-swap reader grace has
        // elapsed; runs on the regular drain cadence so reclamation is crash-safe
        // (durable) and never blocks the swap itself.
        await TryReclaimPendingGenerationAsync(cancellationToken);

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
        ApplyCheckpointHoldBack(advancedOffsets);

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
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return;
        }

        // Build a complete new generation in the background, then atomically swap
        // it in (see ViewMaintainerGrain.ShadowSwap). Readers never observe a
        // half-built view.
        var built = await BuildShadowAsync(registration, cancellationToken);
        await SwapToShadowAsync(registration, built.Offsets, built.Highest, cancellationToken);
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName)
        {
            return;
        }

        if (_timer is null)
        {
            StartTimer();
        }

        try
        {
            await DrainAsync(CancellationToken.None);
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
        foreach (var write in collected
            .OrderBy(static w => w.Timestamp.WallClockTicks)
            .ThenBy(static w => w.Timestamp.Counter))
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

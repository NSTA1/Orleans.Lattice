using System.Diagnostics.Metrics;
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
internal sealed class ViewMaintainerGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    ILogger<ViewMaintainerGrain> logger,
    IViewCatalog catalog,
    ICommitLogReader commitLogReader,
    IWalCursorRegistry cursorRegistry,
    LatticeOptionsResolver optionsResolver,
    IOptionsMonitor<LatticeViewOptions> viewOptions,
    [PersistentState("view-checkpoint", LatticeOptions.StorageProviderName)]
    IPersistentState<ViewCheckpointState> state)
    : IGrainBase, IRemindable, IViewMaintainerGrain
{
    private const string KeepaliveReminderName = "view-maintainer-keepalive";

    private static readonly Histogram<long> ApplyLag = LatticeMetrics.ViewApplyLag;
    private static readonly Histogram<long> BacklogDepth = LatticeMetrics.ViewBacklogDepth;
    private static readonly Counter<long> Applied = LatticeMetrics.ViewApplied;

    private IGrainTimer? _timer;
    private string? _consumerId;
    private string? _viewTreeId;

    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    private string ViewName => context.GrainId.Key.ToString()!;

    // Cached per-activation: ViewName is fixed for the lifetime of the grain, so
    // the derived cursor-consumer id and view-tree id are interpolated once
    // rather than on every drain / rebuild pass.
    private string ConsumerId => _consumerId ??= $"view:{ViewName}";

    private string ViewTreeId => _viewTreeId ??= $"view-{ViewName}";

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
            && !string.Equals(state.State.ProjectionVersion, registration.Projection.ProjectionVersion, StringComparison.Ordinal))
        {
            logger.LogInformation(
                "View '{ViewName}' projection version changed ({Old} -> {New}); rebuilding.",
                ViewName, state.State.ProjectionVersion, registration.Projection.ProjectionVersion);
            await RebuildAsync(cancellationToken);
        }
        else if (string.IsNullOrEmpty(state.State.ProjectionVersion))
        {
            state.State.ProjectionVersion = registration.Projection.ProjectionVersion;
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

                if (IsApplicable(mutation))
                {
                    foreach (var write in registration.Projection.Project(mutation))
                    {
                        collected.Add(write);
                    }

                    if (mutation.Timestamp > highest)
                    {
                        highest = mutation.Timestamp;
                    }
                }

                if (readThisPartition >= batchSize)
                {
                    break;
                }
            }

            advancedOffsets[partition] = lastOffset;
        }

        var survivors = ViewWriteCoalescer.Coalesce(collected);
        var viewTree = grainFactory.GetGrain<ILattice>(ViewTreeId);
        var appliedCount = 0;
        foreach (var write in survivors)
        {
            await ApplyAsync(viewTree, write, cancellationToken);
            appliedCount++;
        }

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
        state.State.ProjectionVersion = registration.Projection.ProjectionVersion;

        if (offsetsAdvanced || appliedCount > 0)
        {
            await state.WriteStateAsync();
        }

        if (highest > HybridLogicalClock.Zero)
        {
            await cursorRegistry.ReportCursorAsync(sourceTreeId, ConsumerId, highest, cancellationToken)
                ;
        }

        ApplyLag.Record(await ComputeLagAsync(sourceTreeId, partitions, cancellationToken), ViewTag);
        BacklogDepth.Record(backlogRead, ViewTag);
        if (appliedCount > 0)
        {
            Applied.Add(appliedCount, ViewTag);
        }

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
    public async Task RebuildAsync(CancellationToken cancellationToken = default)
    {
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return;
        }

        var sourceTreeId = registration.SourceTreeId;
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);

        // Capture the source head per partition BEFORE scanning so any source
        // mutation committed during the rebuild is picked up by the resumed tail
        // (and re-applied idempotently if it was also seen in the scan).
        var capturedOffsets = new Dictionary<int, long>();
        for (var partition = 0; partition < partitions; partition++)
        {
            var head = await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken);
            capturedOffsets[partition] = head - 1;
        }

        var viewTree = grainFactory.GetGrain<ILattice>(ViewTreeId);
        var sourceTree = grainFactory.GetGrain<ILattice>(sourceTreeId);

        // In-place rebuild: clear the current view, then re-project current source
        // state. Phase 1 has no shadow tree / atomic swap, so there is a brief
        // window where the view is partially populated. A later phase replaces
        // this with a shadow-swap rebuild.
        var existingKeys = new List<string>();
        await foreach (var key in viewTree.KeysAsync(cancellationToken: cancellationToken))
        {
            existingKeys.Add(key);
        }

        foreach (var key in existingKeys)
        {
            await viewTree.DeleteAsync(key, cancellationToken);
        }

        var highest = HybridLogicalClock.Zero;
        await foreach (var key in sourceTree.KeysAsync(cancellationToken: cancellationToken))
        {
            var versioned = await sourceTree.GetWithVersionAsync(key, cancellationToken);
            if (versioned.Value is null)
            {
                continue;
            }

            // Synthesize a Set mutation so the rebuild reuses the exact projection
            // logic the tail path uses. ExpiresAtTicks is not recoverable from the
            // value-with-version read, so rebuilt entries lose any TTL (Phase 1
            // limitation; documented).
            var synthetic = new LatticeMutation
            {
                TreeId = sourceTreeId,
                Kind = MutationKind.Set,
                Key = key,
                Value = versioned.Value,
                Timestamp = versioned.Version,
                Category = MutationCategory.User,
            };

            foreach (var write in registration.Projection.Project(synthetic))
            {
                await ApplyAsync(viewTree, write, cancellationToken);
            }

            if (versioned.Version > highest)
            {
                highest = versioned.Version;
            }
        }

        state.State.AppliedOffsets = capturedOffsets;
        state.State.HighestAppliedTimestamp = highest;
        state.State.ProjectionVersion = registration.Projection.ProjectionVersion;
        await state.WriteStateAsync();

        if (highest > HybridLogicalClock.Zero)
        {
            await cursorRegistry.ReportCursorAsync(sourceTreeId, ConsumerId, highest, cancellationToken)
                ;
        }
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

    private static bool IsApplicable(in LatticeMutation mutation)
    {
        // Skip background maintenance entries (compaction tombstones et al), the
        // prepared (uncommitted) half of an atomic write, and transaction
        // terminals. The atomic-write staging path is a later phase; for Phase 1
        // we simply never expose uncommitted or transactional state to the view.
        if (mutation.Category == MutationCategory.Maintenance)
        {
            return false;
        }

        if (mutation.IsPrepared)
        {
            return false;
        }

        return mutation.Kind is not (MutationKind.TxCommit or MutationKind.TxAbort);
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

            default:
                // ViewWriteKind.CrdtDelta is reserved for a later phase and is
                // never emitted by a Phase 1 projection.
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

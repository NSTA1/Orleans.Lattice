using System.Diagnostics.Metrics;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Aggregation (grouped-reduce) drain path of the view maintainer. Mirrors the
/// filter / re-project <see cref="ViewMaintainerGrain.DrainAsync"/> structure -
/// tail every source WAL partition from the durable checkpoint, project each
/// user mutation, advance and persist the per-partition offsets, report the
/// applied cursor - but folds <see cref="AggregationContribution"/>s into the
/// view's per-group accumulators (count / sum / min / max / set-union) through an
/// <see cref="AggregationApplier"/> instead of applying LWW upserts/deletes.
/// <para>
/// Contributions are applied in ascending source-HLC order so each source key's
/// read-before-write retraction folds in commit order. An unconstrained range
/// delete the projection cannot lower (a
/// <see cref="AggregationContributionKind.RangeReconcile"/>) escalates to a
/// rebuild, exactly as the filter path does.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    private static readonly Counter<long> AggregationApplied = LatticeMetrics.ViewAggregationApplied;

    private AggregationApplier CreateAggregationApplier(ILattice viewTree)
        => CreateAggregationApplierOver(new LatticeViewStore(viewTree));

    private AggregationApplier CreateAggregationApplierOver(IAggregationViewStore store)
    {
        var options = Options;
        var fanout = options.AggregationFanout > 0 ? options.AggregationFanout : LatticeViewOptions.DefaultAggregationFanout;
        return new AggregationApplier(
            store,
            catalog.TryGet(ViewName)!.AggregationProjection!.Aggregation,
            fanout,
            Math.Max(0, options.AggregationMaxGroupEntries),
            state.State.RebuildGeneration.ToString());
    }

    private async Task<int> DrainAggregationAsync(ViewRegistration registration, CancellationToken cancellationToken)
    {
        var options = Options;
        var batchSize = options.BatchSize > 0 ? options.BatchSize : LatticeViewOptions.DefaultBatchSize;
        var sourceTreeId = registration.SourceTreeId;
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);

        // Fall-off-log guard: trimmed entries force a rebuild from current source.
        for (var partition = 0; partition < partitions; partition++)
        {
            var checkpoint = state.State.AppliedOffsets.GetValueOrDefault(partition, -1);
            var tail = await commitLogReader.GetTailOffsetAsync(sourceTreeId, partition, cancellationToken);
            if (tail > checkpoint + 1)
            {
                logger.LogWarning(
                    "Aggregation view '{ViewName}' fell off the WAL on partition {Partition} (tail {Tail} > checkpoint {Checkpoint}); rebuilding.",
                    ViewName, partition, tail, checkpoint);
                await RebuildAsync(cancellationToken);
                return 0;
            }
        }

        var contributions = new List<AggregationContribution>();
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
                .ReadAsync(sourceTreeId, partition, checkpoint, cancellationToken))
            {
                lastOffset = offset;
                readThisPartition++;
                backlogRead++;

                switch (Classify(mutation, out var terminalCommit, out var terminalAbort))
                {
                    case StagingDisposition.Apply:
                        foreach (var contribution in registration.AggregationProjection!.Project(mutation))
                        {
                            contributions.Add(contribution);
                        }

                        break;

                    case StagingDisposition.Stage:
                        HandleStagingEntry(mutation, partition, offset, terminalCommit, terminalAbort, completedTransactions);
                        break;

                    case StagingDisposition.Skip:
                    default:
                        break;
                }

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

        // Bounded-buffer / retention backstop, identical to the filter path.
        if (StagingBackstopTripped(options, await GetSourceWalRetentionAsync(sourceTreeId)))
        {
            await RebuildAsync(cancellationToken);
            return 0;
        }

        // An unconstrained range delete cannot be lowered to exact retractions; a
        // full rebuild reconciles the affected range and re-advances the checkpoint.
        if (contributions.Exists(static c => c.Kind == AggregationContributionKind.RangeReconcile))
        {
            logger.LogInformation(
                "Aggregation view '{ViewName}' observed an unconstrained range delete; rebuilding to reconcile the affected range.",
                ViewName);
            await RebuildAsync(cancellationToken);
            return 0;
        }

        var viewTree = grainFactory.GetGrain<ILattice>(ViewTreeId);
        var applier = CreateAggregationApplier(viewTree);

        // Apply in ascending source-HLC order so each source key's read-before-write
        // retraction folds in source-commit order (a source key lives on one
        // partition, so this is exact for that key).
        var applied = 0;
        foreach (var contribution in contributions
            .OrderBy(static c => c.Timestamp.WallClockTicks)
            .ThenBy(static c => c.Timestamp.Counter))
        {
            await applier.ApplyAsync(contribution, cancellationToken);
            applied++;
        }

        // Fold every atomic batch that completed this pass into the group
        // accumulators through the SAME aggregation applier (its per-group
        // atomic membership+accumulator flip), after the ordinary contributions.
        applied += await FlushCompletedAggregationBatchesAsync(applier, viewTree, registration, completedTransactions, cancellationToken);

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

        if (offsetsAdvanced || applied > 0)
        {
            await state.WriteStateAsync();
        }

        var blockedAtHlc = ComputeBlockedAtHlc();
        if (highest > HybridLogicalClock.Zero || blockedAtHlc is not null)
        {
            await cursorRegistry.ReportCursorAsync(sourceTreeId, ConsumerId, highest, blockedAtHlc, cancellationToken);
        }

        ApplyLag.Record(await ComputeLagAsync(sourceTreeId, partitions, cancellationToken), ViewTag);
        BacklogDepth.Record(backlogRead, ViewTag);
        if (applied > 0)
        {
            AggregationApplied.Add(applied, ViewTag);
        }

        // Run any reconcile a cross-tree degrade scheduled this pass, after the
        // checkpoint is persisted so the rebuild does not clear the staging buffer
        // under the flush loop.
        await RunPendingCrossTreeReconcileAsync(cancellationToken);

        return applied;
    }

    /// <summary>
    /// Folds every aggregation atomic batch in <paramref name="completed"/> that
    /// is still staged into the group accumulators: projects each prepared entry
    /// through the aggregation projection and applies the contributions in
    /// ascending source-HLC order through the existing per-group atomic
    /// membership+accumulator flip (which is itself crash-idempotent), then
    /// evicts the batch from the staging buffer.
    /// <para>
    /// A cross-tree atomic batch is not folded into the live tree immediately:
    /// its contributions are replayed through a <see cref="BufferingAggregationViewStore"/>
    /// layered over the live tree so the net row writes its read-before-write
    /// flip would produce are <i>captured</i> without mutating the tree, then the
    /// captured slice is contributed to the view-side coordinator for a joint
    /// flip across every participant view tree (degrading to a per-tree-slice flip
    /// of the captured slice on a readiness timeout). Until the joint decision is
    /// observed the batch stays staged and is retried on a later drain.
    /// </para>
    /// </summary>
    private async Task<int> FlushCompletedAggregationBatchesAsync(
        AggregationApplier applier,
        ILattice viewTree,
        ViewRegistration registration,
        List<Guid> completed,
        CancellationToken cancellationToken)
    {
        var applied = 0;
        foreach (var txId in completed)
        {
            if (!_staging.TryGetValue(txId, out var tx))
            {
                continue;
            }

            var contributions = new List<AggregationContribution>();
            foreach (var prepared in tx.PreparesByIndex.Values)
            {
                foreach (var contribution in registration.AggregationProjection!.Project(prepared))
                {
                    contributions.Add(contribution);
                }
            }

            if (tx.CrossTreeOperationId is not null)
            {
                // Capture this aggregation view's slice (the net row writes its
                // atomic flip would produce) without touching the live tree, then
                // rendezvous it for the joint cross-tree flip.
                var buffering = new BufferingAggregationViewStore(new LatticeViewStore(viewTree));
                var capturing = CreateAggregationApplierOver(buffering);
                foreach (var contribution in contributions
                    .OrderBy(static c => c.Timestamp.WallClockTicks)
                    .ThenBy(static c => c.Timestamp.Counter))
                {
                    await capturing.ApplyAsync(contribution, cancellationToken);
                }

                var (upserts, deletes) = buffering.Capture();
                var resolved = await HandleCrossTreeBatchAsync(viewTree, txId, tx, upserts, deletes, cancellationToken);
                if (resolved)
                {
                    applied += upserts.Count + deletes.Count;
                    _staging.Remove(txId);
                    MarkResolved(txId);
                }

                continue;
            }

            foreach (var contribution in contributions
                .OrderBy(static c => c.Timestamp.WallClockTicks)
                .ThenBy(static c => c.Timestamp.Counter))
            {
                await applier.ApplyAsync(contribution, cancellationToken);
                applied++;
            }

            _staging.Remove(txId);
            MarkResolved(txId);
        }

        return applied;
    }
}

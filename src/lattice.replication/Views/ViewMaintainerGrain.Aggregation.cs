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
    {
        var options = Options;
        var fanout = options.AggregationFanout > 0 ? options.AggregationFanout : LatticeViewOptions.DefaultAggregationFanout;
        return new AggregationApplier(
            new LatticeViewStore(viewTree),
            catalog.TryGet(ViewName)!.AggregationProjection!.Aggregation,
            fanout,
            Math.Max(0, options.AggregationMaxGroupEntries));
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

                if (IsApplicable(mutation))
                {
                    foreach (var contribution in registration.AggregationProjection!.Project(mutation))
                    {
                        contributions.Add(contribution);
                    }
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

        if (highest > HybridLogicalClock.Zero)
        {
            await cursorRegistry.ReportCursorAsync(sourceTreeId, ConsumerId, highest, cancellationToken);
        }

        ApplyLag.Record(await ComputeLagAsync(sourceTreeId, partitions, cancellationToken), ViewTag);
        BacklogDepth.Record(backlogRead, ViewTag);
        if (applied > 0)
        {
            AggregationApplied.Add(applied, ViewTag);
        }

        return applied;
    }
}

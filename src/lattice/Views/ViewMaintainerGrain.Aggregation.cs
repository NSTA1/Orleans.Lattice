using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Views;

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
        var projection = catalog.TryGet(ViewName)!.AggregationProjection!;
        return new AggregationApplier(
            store,
            projection.Aggregation,
            fanout,
            Math.Max(0, options.AggregationMaxGroupEntries),
            state.State.RebuildGeneration.ToString(),
            projection as ILatticeFoldProjection,
            ViewName,
            ViewTenantTag);
    }

    private async Task<int> DrainAggregationAsync(ViewRegistration registration, CancellationToken cancellationToken)
    {
        var options = Options;
        var batchSize = options.BatchSize > 0 ? options.BatchSize : LatticeViewOptions.DefaultBatchSize;
        var sourceTreeId = registration.SourceTreeId;
        var walTreeId = state.State.BoundPhysicalTreeId;
        batchSize = ApplyBackpressureBatchScaling(walTreeId, batchSize, options);
        var partitions = await optionsResolver.GetWalPartitionsAsync(walTreeId);

        // Hold every entry of this log the view has not durably consumed against
        // the WAL GC on every silo, before reading it (issue #4584).
        await EnsureReadRegisteredAsync(walTreeId);
        RestoreRepairedTransactions(walTreeId);

        // Tail the source WAL through the shared subscriber (identical mechanics
        // to the filter path); the handler folds each applicable entry into the
        // contribution buffer and stages atomic-batch members. PinWal is false:
        // the maintainer reports its own cursor after the fold below.
        var contributions = new List<AggregationContribution>();
        var completedTransactions = new List<Guid>();
        SeedStillStagedCompletedTransactions(completedTransactions);
        // Classify while folding: the drain loop already visits every projected
        // contribution, so record whether any is a RangeReconcile here instead of
        // re-walking the whole buffer with a separate .Exists pass after the drain.
        var hasRangeReconcile = false;
        var ordinarySpans = new List<OrdinaryWriteSpan>();
        var numericSourceKeys = registration.AggregationProjection!.Aggregation is AggregationKind.Count or AggregationKind.Sum
            ? new HashSet<string>(StringComparer.Ordinal)
            : null;
        var handler = new ViewDrainHandler(
            this,
            mutation =>
            {
                numericSourceKeys?.Add(mutation.Key);
                var spanStart = contributions.Count;
                foreach (var contribution in registration.AggregationProjection!.Project(ProjectionInput(registration, mutation)))
                {
                    contributions.Add(contribution);
                    if (contribution.Kind == AggregationContributionKind.RangeReconcile)
                    {
                        hasRangeReconcile = true;
                    }
                }

                RecordOrdinaryWriteSpan(ordinarySpans, mutation, spanStart, contributions.Count);
            },
            completedTransactions);

        var readFrom = DrainReadCheckpoints(walTreeId, state.State.AppliedOffsets);
        var drainContext = new WalSubscriptionContext(walTreeId, ConsumerId, partitions, readFrom)
        {
            HighestApplied = state.State.HighestAppliedTimestamp,
            BatchSize = batchSize,
            MaintenancePolicy = WalMaintenancePolicy.Skip,
            PinWal = false,
        };

        var drain = await subscriber.DrainAsync(drainContext, handler, cancellationToken);
        if (drain.FellOffLog)
        {
            logger.LogWarning(
                "Aggregation view '{ViewName}' fell off the WAL on source '{SourceTree}'; rebuilding.",
                ViewName, sourceTreeId);
            await RebuildAsync(cancellationToken);
            return 0;
        }

        var advancedOffsets = new Dictionary<int, long>(drain.AdvancedOffsets);
        var highest = drain.HighestTimestamp;
        var backlogRead = drain.EntriesRead;

        // Bounded-buffer / retention backstop, identical to the filter path.
        if (!HasCommittedJointOwnership && (StagingBackstopTripped(options, await GetSourceWalRetentionAsync(walTreeId))
            || AbandonedBatchCommitted()
            || BackstopDegradePending()
            || await OrphanedCommittedBatchAsync(walTreeId, partitions, readFrom, advancedOffsets, cancellationToken)))
        {
            await RebuildAsync(cancellationToken);
            return 0;
        }

        // An unconstrained range delete cannot be lowered to exact retractions; a
        // full rebuild reconciles the affected range and re-advances the checkpoint.
        if (hasRangeReconcile)
        {
            logger.LogInformation(
                "Aggregation view '{ViewName}' observed an unconstrained range delete; rebuilding to reconcile the affected range.",
                ViewName);
            await RebuildAsync(cancellationToken);
            return 0;
        }

        // A contribution of an ordinary write the source already superseded by
        // last-writer-wins (an older write logged after a newer one, issue #4885)
        // would retract the newer membership and re-add the stale one.
        if (!registration.Accumulative)
        {
            await DropWritesSupersededAtSourceAsync(
                contributions, ordinarySpans, static c => c.SourceKey, walTreeId, partitions, cancellationToken);
        }

        var viewTree = grainFactory.GetGrain<ILattice>(ViewTreeId);
        var applier = CreateAggregationApplier(viewTree);
        if (!registration.Accumulative)
        {
            await ConfirmCompletedBatchesAgainstSourceAsync(
                completedTransactions, walTreeId, partitions, ConsumedThrough(readFrom, advancedOffsets), cancellationToken);
            GateBatchesOnUnappliedReplacements(completedTransactions, ConsumedThrough(readFrom, advancedOffsets));
        }
        SetJointReadHorizon(readFrom, advancedOffsets, completedTransactions);
        var jointApplied = await FlushCompletedAggregationBatchesAsync(
            viewTree, registration, completedTransactions, cancellationToken, crossTreeOnly: true);
        if (DeferPublicationForJointRepair(walTreeId, readFrom, advancedOffsets))
        {
            return jointApplied;
        }

        // Apply in ascending source-HLC order so each source key's read-before-write
        // retraction folds in source-commit order (a source key lives on one
        // partition, so this is exact for that key).
        var applied = jointApplied;
        contributions.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));
        foreach (var contribution in contributions)
        {
            await applier.ApplyAsync(contribution, cancellationToken);
            applied++;
        }

        if (numericSourceKeys is not null)
        {
            // Source admission can suppress an interrupted event, and its
            // successor can project nothing. Repair still precedes checkpoint retirement.
            foreach (var sourceKey in numericSourceKeys)
            {
                await applier.CompletePendingNumericSourceAsync(sourceKey, cancellationToken);
            }
        }

        // Fold every atomic batch that completed this pass into the group
        // accumulators through the SAME aggregation applier (its per-group
        // atomic membership+accumulator flip), after the ordinary contributions.
        // Joint batches rendezvous before ordinary public groups change, so a
        // stale registered membership cannot roll back a published regrouping.
        applied += await FlushCompletedAggregationBatchesAsync(
            viewTree, registration, completedTransactions, cancellationToken, crossTreeOnly: false);

        // Hold the persisted resume offset back below the lowest still-staged
        // entry so a restart re-reads and re-stages an incomplete batch.
        RecordDrainReadOffsets(walTreeId, readFrom, advancedOffsets);
        var replayFenceChanged = UpdateReplayFence(walTreeId, readFrom, advancedOffsets);
        CoverReadPosition(advancedOffsets, readFrom);
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

        var reconcileScheduled = PersistPendingCrossTreeReconcile();
        var repairRetired = RetireJointRepairHolds(advancedOffsets);
        if (offsetsAdvanced || applied > 0 || replayFenceChanged || reconcileScheduled || repairRetired)
        {
            LowerReadPositions(walTreeId, state.State.AppliedOffsets);
            await PersistJointStateAsync();
            PublishReadPositions(walTreeId, state.State.AppliedOffsets);
            await AcknowledgeRetiredJointRepairsAsync();
        }

        var blockedAtHlc = ComputeBlockedAtHlc();
        if (highest > HybridLogicalClock.Zero || blockedAtHlc is not null)
        {
            await cursorRegistry.ReportCursorAsync(walTreeId, ConsumerId, highest, blockedAtHlc, cancellationToken);
            await ReportDrainCheckpointsAsync(walTreeId, highest, cancellationToken);
        }

        ApplyLag.Record(await ComputeLagAsync(walTreeId, partitions, cancellationToken), ViewTag, ViewTenantTag);
        BacklogDepth.Record(backlogRead, ViewTag, ViewTenantTag);
        if (applied > 0)
        {
            AggregationApplied.Add(applied, ViewTag, ViewTenantTag);
        }

        // Run any reconcile a cross-tree degrade scheduled this pass, after the
        // checkpoint is persisted so the rebuild does not clear the staging buffer
        // under the flush loop.
        await RunPendingCrossTreeReconcileAsync(cancellationToken);

        return applied;
    }

    /// <summary>
    /// Folds every aggregation atomic batch in <paramref name="completed"/> that
    /// is still staged into the group accumulators. Each batch's contributions are
    /// projected, applied in ascending source-HLC order through a
    /// <see cref="BufferingAggregationViewStore"/> layered over the live tree so
    /// the net row writes (accumulators + fanout rows) its read-before-write flip
    /// would produce are <i>captured</i> without mutating the tree, then the
    /// captured slice is flipped <b>atomically</b> - a single-tree atomic write for
    /// a single-tree batch (so the whole batch becomes visible together rather than
    /// member-by-member), or contributed to the view-side coordinator for a joint
    /// cross-tree flip across every participant view tree (degrading to a
    /// per-tree-slice flip of the captured slice on a readiness timeout). Until a
    /// cross-tree joint decision is observed the batch stays staged and is retried
    /// on a later drain; resolved batches are evicted from the staging buffer.
    /// </summary>
    private async Task<int> FlushCompletedAggregationBatchesAsync(
        ILattice viewTree,
        ViewRegistration registration,
        List<Guid> completed,
        CancellationToken cancellationToken,
        bool crossTreeOnly)
    {
        var applied = 0;
        foreach (var txId in completed)
        {
            if (!_staging.TryGetValue(txId, out var tx)
                || (tx.CrossTreeOperationId is not null) != crossTreeOnly)
            {
                continue;
            }

            var closure = registration.Accumulative ? null : LocalPublicationClosure(txId, completed);
            if (closure is { Count: > 1 })
            {
                applied += await PublishLocalAggregationClosureAsync(viewTree, registration, closure, cancellationToken);
                continue;
            }

            var contributions = new List<AggregationContribution>();
            foreach (var prepared in tx.PreparesByIndex.Values)
            {
                // Supersession skip (see FlushCompletedFilterBatchesAsync): a
                // higher-HLC ordinary write to this source key, seen while the
                // batch was staged, is the source last-writer; dropping the
                // prepared entry keeps the group accumulator convergent with the
                // source instead of folding in a superseded atomic contribution.
                if (!registration.Accumulative && (IsSupersededByOrdinary(prepared) || IsSourceSuperseded(tx, prepared)))
                {
                    continue;
                }

                foreach (var contribution in registration.AggregationProjection!.Project(ProjectionInput(registration, prepared)))
                {
                    contributions.Add(contribution);
                }
            }

            // Fold in ascending source-HLC order so each source key's
            // read-before-write retraction applies in commit order.
            contributions.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));

            // Capture the net row writes this batch's atomic flip would produce
            // (accumulators + fanout rows) without mutating the live tree, so the
            // whole batch flips atomically - all group accumulator changes visible
            // together - rather than member-by-member. Mirrors the cross-tree path.
            var buffering = new BufferingAggregationViewStore(new LatticeViewStore(viewTree));
            var capturing = CreateAggregationApplierOver(buffering);
            foreach (var contribution in contributions)
            {
                await capturing.ApplyAsync(contribution, cancellationToken);
            }

            var (upserts, deletes) = buffering.Capture();

            if (tx.CrossTreeOperationId is not null)
            {
                // Rendezvous this view's captured slice for the joint cross-tree
                // flip across every participant view tree.
                var resolved = await HandleCrossTreeBatchAsync(viewTree, txId, tx, upserts, deletes, cancellationToken);
                if (resolved)
                {
                    applied += upserts.Count + deletes.Count;
                    RecordFlushedPreparesOverStagedKeys(tx);
                    ReleaseStagedKeys(tx);
                    _staging.Remove(txId);
                    MarkResolved(txId);
                }

                continue;
            }

            // Single-tree atomic batch: flip the captured net slice atomically into
            // the view tree - upserts and retraction deletes ride one mixed atomic
            // op keyed by the deterministic view-saga id (so a replay re-attaches)
            // so the whole batch becomes visible together.
            await FlipLocalSliceAsync(viewTree, txId, upserts, deletes, cancellationToken);
            applied += upserts.Count + deletes.Count;
            RecordFlushedPreparesOverStagedKeys(tx);
            ReleaseStagedKeys(tx);
            _staging.Remove(txId);
            MarkResolved(txId);
        }

        return applied;
    }
}

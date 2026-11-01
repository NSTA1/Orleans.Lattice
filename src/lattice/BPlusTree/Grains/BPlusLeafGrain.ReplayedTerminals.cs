namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    private ValueTask ApplyReplayMutationAsync(LatticeMutation mutation)
    {
        if (mutation.Kind == MutationKind.TxCommit)
            return new ValueTask(ApplyReplayTxCommitAsync(mutation.TransactionId, mutation.Timestamp));

        ((ILeafProjection)this).Apply(mutation);
        return ValueTask.CompletedTask;
    }

    private async Task ApplyReplayTxCommitAsync(
        Guid transactionId, HybridLogicalClock terminalTimestamp, int remainingForwards = 64)
    {
        HashSet<string>? forwarded = null;
        while (HasDeclaredSpan
            && _pendingTx is not null
            && _pendingTx.TryGetValue(transactionId, out var bucket))
        {
            Dictionary<GrainId, List<PendingMutationSnapshot>>? targets = null;
            foreach (var (key, value) in bucket)
            {
                if (forwarded?.Contains(key) == true || DeclaresKey(key))
                    continue;
                if (!TryResolveSpanForwardTarget(key, out var target, out var reason))
                    throw new InvalidOperationException(
                        $"Cannot route a prepared replay commit outside this leaf's span ({reason}).");

                targets ??= [];
                if (!targets.TryGetValue(target, out var mutations))
                    targets[target] = mutations = [];
                mutations.Add(CaptureReplayPrepare(transactionId, key, value));
            }

            if (targets is null)
                break;
            if (remainingForwards == 0)
                throw new InvalidOperationException("The prepared replay commit exceeded its leaf-forwarding bound.");

            // Keep the source bucket and its checkpoint clamp until every
            // destination has durably accepted its typed prepares and commit.
            foreach (var (target, mutations) in targets)
            {
                await grainFactory.GetGrain<IBPlusLeafGrain>(target)
                    .ApplyPreparedReplayCommitAsync(transactionId, mutations, terminalTimestamp, remainingForwards - 1);
                forwarded ??= new HashSet<string>(StringComparer.Ordinal);
                foreach (var mutation in mutations)
                    forwarded.Add(mutation.Key);
            }
            // Another split may have narrowed the donor during delivery.
        }

        ApplyReplayTxCommitLocally(transactionId, forwarded);
        AdvanceProjectionClock(terminalTimestamp);
    }

    private PendingMutationSnapshot CaptureReplayPrepare(
        Guid transactionId, string key, Primitives.LwwValue<byte[]> value)
    {
        var delta = _pendingTxDeltas is not null
            && _pendingTxDeltas.TryGetValue(transactionId, out var deltas)
            && deltas.TryGetValue(key, out var item) ? item : default;
        var batch = _pendingTxBatches is not null
            && _pendingTxBatches.TryGetValue(transactionId, out var batches)
            && batches.TryGetValue(key, out var membership) ? membership : default;
        return new PendingMutationSnapshot
        {
            TransactionId = transactionId,
            Key = key,
            Value = value.IsTombstone ? null : value.Value,
            Timestamp = value.Timestamp,
            IsTombstone = value.IsTombstone,
            ExpiresAtTicks = value.ExpiresAtTicks,
            OriginClusterId = value.OriginClusterId,
            VectorClock = value.VectorClock,
            Delta = delta.Delta,
            Mode = delta.Mode,
            AtomicBatchSize = batch.Size,
            AtomicBatchIndex = batch.Index,
            StampIsOriginal = IsPrepareStampOriginal(transactionId, key),
        };
    }

    /// <inheritdoc />
    public async Task ApplyPreparedReplayCommitAsync(
        Guid transactionId,
        List<PendingMutationSnapshot> preparedMutations,
        HybridLogicalClock terminalTimestamp,
        int remainingForwards = 64)
    {
        await AwaitReplayBarrierAsync();
        EnsureInternalOrigin(LatticeOperation.Write);
        ArgumentNullException.ThrowIfNull(preparedMutations);
        ArgumentOutOfRangeException.ThrowIfNegative(remainingForwards);
        if (transactionId == Guid.Empty)
            throw new ArgumentException("A replay commit requires a transaction id.", nameof(transactionId));
        if (preparedMutations.Any(mutation => mutation.TransactionId != transactionId))
            throw new ArgumentException("Every prepare must belong to the replayed transaction.", nameof(preparedMutations));
        await EnsureTerminalWitnessHydratedAsync();

        var records = new List<WalRecord>(preparedMutations.Count + 1);
        foreach (var mutation in preparedMutations)
        {
            if (IsTerminalWitnessed(transactionId, mutation.Key)
                || (mutation.StampIsOriginal && mutation.Mode == LatticeMergeMode.LwwRegister
                    && IsRowAtOrAboveOriginalStamp(mutation.Key, mutation.Timestamp)))
                continue;
            records.Add(new WalRecord
            {
                TreeId = state.State.TreeId ?? string.Empty,
                ShardIndex = state.State.ShardIndex ?? 0,
                Op = mutation.IsTombstone ? MutationKind.Delete : MutationKind.Set,
                Key = mutation.Key,
                Value = mutation.Value,
                Timestamp = mutation.Timestamp,
                IsTombstone = mutation.IsTombstone,
                ExpiresAtTicks = mutation.ExpiresAtTicks,
                OriginClusterId = mutation.OriginClusterId,
                VectorClock = mutation.VectorClock,
                TransactionId = transactionId,
                IsPrepared = true,
                Delta = mutation.Delta,
                Mode = mutation.Mode,
                AtomicBatchSize = mutation.AtomicBatchSize,
                AtomicBatchIndex = mutation.AtomicBatchIndex,
                PrepareStampOriginal = mutation.StampIsOriginal,
                IsMigrated = mutation.StampIsOriginal,
            });
        }
        if (records.Count == 0)
            return;

        records.Add(new WalRecord
        {
            TreeId = state.State.TreeId ?? string.Empty,
            ShardIndex = state.State.ShardIndex ?? 0,
            Op = MutationKind.TxCommit,
            Key = (state.State.ShardIndex ?? 0).ToString(System.Globalization.CultureInfo.InvariantCulture),
            TransactionId = transactionId,
            Timestamp = terminalTimestamp,
        });
        var writer = ResolveCommitLogWriter();
        IReadOnlyList<long>? replayedOffsets = null;
        if (writer is not null)
            replayedOffsets = await writer.AppendManyAsync(records);

        // The WAL append may have overlapped another split. Bucket the exact
        // typed prepares, then re-route before draining, just as on the donor.
        foreach (var record in records)
        {
            if (!record.IsPrepared || IsTerminalWitnessed(transactionId, record.Key))
                continue;
            ((ILeafProjection)this).Apply(WalRecordConverter.FromWalRecord(record));
        }
        await ApplyReplayTxCommitAsync(transactionId, terminalTimestamp, remainingForwards);
        var visibleStamp = HybridLogicalClock.Zero;
        foreach (var record in records)
            if (record.IsPrepared && DeclaresKey(record.Key)
                && Cache.TryGetRow(record.Key, out var row) && row.Timestamp > visibleStamp)
                visibleStamp = row.Timestamp;
        PublishVersionAdvance(visibleStamp);
        ClearSagaShadow(transactionId);
        // The re-appended records are applied (issue #5572).
        if (replayedOffsets is not null)
        {
            MarkLiveAppendApplied(records, replayedOffsets);
        }
        await PublishDigestUpwardInlineAsync();
    }
}

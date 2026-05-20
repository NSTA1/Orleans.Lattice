using System.Globalization;
using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Activation-time reconciliation for the per-batch partition +
/// manifest schema (the activation-time recovery stage of the
/// two-phase WAL commit). The two-phase commit
/// protocol can leave the persisted state inconsistent across crash
/// boundaries: a batch's phase 0 (candidate-row in the manifest
/// partition) and phase 1 (entry rows in its own batch partition)
/// commit independently in parallel, then the per-shard
/// <see cref="PhaseTwoWorker"/> commits phase 2 (delete the
/// candidate-row, add the manifest row, upsert TAIL) atomically. A
/// silo crash between phase 0/1 and phase 2 leaves an <i>orphan</i>:
/// a batch partition with phase-1 entry rows plus a phase-0
/// candidate-row in the manifest partition, but no phase-2 manifest
/// row.
/// <para>
/// Reconciliation discovers orphans with a <b>single anchored range
/// query</b> against the shard's manifest partition
/// (<c>RowKey ge 'C' and RowKey lt 'D'</c>) - no cross-partition
/// scan over the shard's live batch partitions. The candidate-row
/// carries the batch's <c>endOffsetInclusive</c> in its
/// <c>Offset</c> column, so the reconciler does not need to read
/// the orphan's entry rows to learn how far the batch extends. This
/// turns activation-time recovery from O(live batches in the shard)
/// per restart into O(in-flight batches at the moment of the crash),
/// which is bounded by <c>WalMaxPendingBatches</c> and typically
/// 0 in steady state.
/// </para>
/// <list type="bullet">
///   <item><description>
///     <b>Rolls forward</b> orphans whose <c>startOffset</c>
///     contiguously extends the current TAIL (no gap below them);
///     these are the writes that lost only the phase-2 commit and
///     are otherwise durable. Their missing manifest rows are added
///     in strict offset order, their candidate-rows are deleted, and
///     TAIL is advanced.
///   </description></item>
///   <item><description>
///     <b>Rolls back</b> orphans below or above a gap; the batch
///     partition is fully deleted and the candidate-row is deleted
///     because the producer's WAL grain restarts at <c>TAIL + 1</c>
///     and would otherwise observe an unreferenced batch sitting
///     above the offset it expects to be the next monotonic append
///     slot.
///   </description></item>
/// </list>
/// <para>
/// Reconciliation is idempotent: a second call with no intervening
/// writes is a no-op, because the second pass observes no remaining
/// candidate-rows (phase-2 deleted them, or rollback deleted them).
/// It is also safe to interleave with concurrent appends - the
/// activation hook runs in <c>OnActivateAsync</c> before the grain
/// accepts any traffic, so the producer is quiescent for the
/// duration.
/// </para>
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    /// <summary>
    /// Activation-time reconciliation hook called by the WAL grain
    /// immediately before <see cref="GetHighestOffsetAsync"/> on
    /// activation. See the class doc for the algorithm; see the WAL
    /// design notes for the per-batch / manifest commit-and-recover
    /// rationale.
    /// </summary>
    public async Task ReconcileAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);

        // Step 1: discover orphans with a single anchored range query
        // against the manifest partition's candidate-row band
        // (RowKey ge 'C' and RowKey lt 'D'). Each remaining C-row is
        // by definition a phase-0/phase-1 batch whose phase-2 never
        // landed; the row's Offset column carries endOffsetInclusive
        // so we do not need to read the orphan's entry rows.
        var orphans = await ReadOutstandingCandidatesAsync(
            table, manifestPartitionKey, treeId, shardIndex, cancellationToken).ConfigureAwait(false);

        if (orphans.Count == 0)
        {
            return;
        }

        // Step 2: read TAIL. -1 means "no manifest yet"; a fresh shard
        // post-activation starts from 0 so contiguity below means
        // startOffset == 0.
        var currentTail = await ReadTailAsync(table, manifestPartitionKey, cancellationToken).ConfigureAwait(false);

        // Step 3: plan rollforward vs rollback. The rollforward set
        // is the prefix of orphans whose start offsets are contiguous
        // with the current tail; everything after the first
        // contiguity break is rolled back.
        var plan = PlanReconciliation(currentTail, orphans);

        // Step 4: execute the plan. Rollforward first so the manifest
        // reflects every salvageable batch before the rollback step
        // deletes the unsalvageable ones; this also lets the
        // grain-side OnActivateAsync observe the new TAIL even if a
        // subsequent rollback fails on a transient I/O error.
        if (plan.RollForward.Count > 0)
        {
            await CommitRollForwardAsync(
                table, manifestPartitionKey, plan.RollForward, cancellationToken).ConfigureAwait(false);
        }

        foreach (var rollback in plan.RollBack)
        {
            await RollBackOrphanAsync(table, manifestPartitionKey, rollback, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A batch partition that has phase-0 / phase-1 data but no
    /// matching phase-2 manifest row. The reconciler discovers
    /// orphans by enumerating outstanding candidate-rows in the
    /// shard's manifest partition; <see cref="EndOffsetInclusive"/>
    /// is read from the C-row's <c>Offset</c> column and
    /// <see cref="BatchPartitionKey"/> is re-derived from
    /// <c>(treeId, shardIndex, startOffset)</c>.
    /// </summary>
    internal readonly record struct OrphanBatch(
        long StartOffset,
        long EndOffsetInclusive,
        string BatchPartitionKey);

    /// <summary>
    /// The decision the reconciliation algorithm renders for a shard.
    /// <see cref="RollForward"/> is the prefix of orphans whose start
    /// offsets are contiguous with <see cref="ResultingTail"/>;
    /// <see cref="RollBack"/> is everything after the first
    /// contiguity break.
    /// </summary>
    internal readonly record struct ReconciliationPlan(
        long ResultingTail,
        IReadOnlyList<OrphanBatch> RollForward,
        IReadOnlyList<OrphanBatch> RollBack);

    /// <summary>
    /// Pure-logic planner: given the current TAIL and the orphan
    /// batches discovered for the shard (sorted ascending by start
    /// offset, with their entry-derived end offsets already
    /// recovered), decides which orphans to roll forward and which
    /// to roll back. Strictly contiguous orphans (no gap below them)
    /// roll forward; everything after the first contiguity break
    /// rolls back. Exposed internally so unit tests can pin the
    /// algorithm without a live Azure Tables endpoint.
    /// </summary>
    internal static ReconciliationPlan PlanReconciliation(long currentTail, IReadOnlyList<OrphanBatch> orphansAscending)
    {
        ArgumentNullException.ThrowIfNull(orphansAscending);
        if (orphansAscending.Count == 0)
        {
            return new ReconciliationPlan(currentTail, Array.Empty<OrphanBatch>(), Array.Empty<OrphanBatch>());
        }

        // Pre-size both lists at the upper bound (every orphan ends up
        // on one side or the other). Activation-only, but avoids the
        // capacity-doubling churn on the dominant rollforward case where
        // every orphan is contiguous.
        var rollForward = new List<OrphanBatch>(orphansAscending.Count);
        var rollBack = new List<OrphanBatch>(orphansAscending.Count);
        var resultingTail = currentTail;
        var contiguityBroken = false;

        for (var i = 0; i < orphansAscending.Count; i++)
        {
            var orphan = orphansAscending[i];
            if (orphan.EndOffsetInclusive < orphan.StartOffset)
            {
                // Defensive: an orphan with end < start is malformed;
                // route it to rollback so the planner never advances
                // TAIL past nonsense.
                rollBack.Add(orphan);
                contiguityBroken = true;
                continue;
            }

            if (contiguityBroken)
            {
                rollBack.Add(orphan);
                continue;
            }

            // Contiguity holds iff the orphan begins exactly at
            // resultingTail + 1 (or at offset 0 when there is no
            // committed tail yet).
            var expectedStart = resultingTail + 1L;
            if (orphan.StartOffset == expectedStart)
            {
                rollForward.Add(orphan);
                resultingTail = orphan.EndOffsetInclusive;
            }
            else
            {
                contiguityBroken = true;
                rollBack.Add(orphan);
            }
        }

        return new ReconciliationPlan(resultingTail, rollForward, rollBack);
    }

    /// <summary>
    /// Returns the persisted TAIL offset for the supplied manifest
    /// partition, or <c>-1L</c> if the TAIL row is absent.
    /// </summary>
    private static async Task<long> ReadTailAsync(
        TableClient table,
        string manifestPartitionKey,
        CancellationToken cancellationToken)
    {
        try
        {
            var response = await table.GetEntityAsync<AzureTableWalEntity>(
                manifestPartitionKey,
                TailRowKey,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return response.Value.Offset;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return -1L;
        }
    }

    /// <summary>
    /// Enumerates outstanding candidate-rows (phase-0 stamps) in the
    /// shard's manifest partition. Each remaining C-row is by
    /// definition a phase-0/phase-1 batch whose phase-2 commit never
    /// landed; phase 2 atomically deletes the C-row alongside the
    /// M-row insert and TAIL upsert, so a C-row's continued
    /// existence post-restart is exactly the orphan signal. The
    /// C-row's <c>Offset</c> column carries
    /// <c>endOffsetInclusive</c> so the reconciler does not need to
    /// read the orphan's entry rows. Returns the list in ascending
    /// start-offset order because the row keys
    /// (<c>C{startOffset:D19}</c>) sort lexicographically and Azure
    /// Tables returns query results in ascending row-key order.
    /// </summary>
    private static async Task<List<OrphanBatch>> ReadOutstandingCandidatesAsync(
        TableClient table,
        string manifestPartitionKey,
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        var filter =
            $"PartitionKey eq '{Escape(manifestPartitionKey)}' and RowKey ge '{CandidateRowKeyPrefix}' and RowKey lt '{CandidateRowKeyExclusiveUpperBound}'";

        var orphans = new List<OrphanBatch>();
        await foreach (var row in table
            .QueryAsync<AzureTableWalEntity>(filter, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            // RowKey shape: C{startOffset:D19}. Slice past the
            // single-byte 'C' prefix and parse the 19-digit suffix.
            var startOffset = long.Parse(
                row.RowKey.AsSpan(CandidateRowKeyPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture);
            var endOffsetInclusive = row.Offset;

            // Defensive: a C-row whose Offset (endOffsetInclusive) is
            // below its derived startOffset is malformed - either a
            // partial write the SDK retried mid-flight, or a manual
            // edit. Skip rather than fault; PlanReconciliation also
            // defends against this shape so a leak here would route
            // to rollback anyway.
            if (endOffsetInclusive < startOffset)
            {
                continue;
            }

            var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, startOffset);
            orphans.Add(new OrphanBatch(startOffset, endOffsetInclusive, batchPartitionKey));
        }
        return orphans;
    }

    /// <summary>
    /// Commits the rollforward portion of the plan: deletes each
    /// orphan's phase-0 candidate-row and adds its phase-2 manifest
    /// row in ascending start-offset order, plus a single TAIL
    /// upsert at the end. The work is chunked into transactions of
    /// up to <c>MaxRolledForwardBatchesPerChunk</c> orphans so the
    /// 100-action per-transaction cap is respected
    /// (<c>2 * 49 + 1 = 99</c> actions per chunk); the last chunk
    /// includes the TAIL upsert so the visible tail moves atomically
    /// with the final M-row.
    /// </summary>
    private static async Task CommitRollForwardAsync(
        TableClient table,
        string manifestPartitionKey,
        IReadOnlyList<OrphanBatch> rollForward,
        CancellationToken cancellationToken)
    {
        // Each orphan contributes 2 actions (C-delete + M-add); the
        // last chunk also carries the shared TAIL upsert. The cap is
        // therefore (100 - 1) / 2 = 49 orphans per transaction,
        // matching the PhaseTwoWorker's coalescing cap.
        const int chunkSize = 49;
        var resultingTail = rollForward[^1].EndOffsetInclusive;

        for (var i = 0; i < rollForward.Count; i += chunkSize)
        {
            var end = Math.Min(i + chunkSize, rollForward.Count);
            var actions = new List<TableTransactionAction>(((end - i) * 2) + 1);
            for (var j = i; j < end; j++)
            {
                actions.Add(new TableTransactionAction(
                    TableTransactionActionType.Delete,
                    new AzureTableWalEntity
                    {
                        PartitionKey = manifestPartitionKey,
                        RowKey = BuildCandidateRowKey(rollForward[j].StartOffset),
                        Offset = rollForward[j].EndOffsetInclusive,
                        Payload = null,
                    },
                    ETag.All));
                actions.Add(new TableTransactionAction(
                    TableTransactionActionType.Add,
                    new AzureTableWalEntity
                    {
                        PartitionKey = manifestPartitionKey,
                        RowKey = BuildManifestRowKey(rollForward[j].StartOffset),
                        Offset = rollForward[j].EndOffsetInclusive,
                        Payload = null,
                    }));
            }

            // Only the last chunk advances TAIL; intermediate chunks
            // commit C-delete + M-add pairs only. The strict
            // ascending order means an intermediate-chunk crash
            // leaves a consistent (partial) tail at the highest
            // M-row of the preceding chunk on the next reconciliation
            // pass.
            if (end == rollForward.Count)
            {
                actions.Add(new TableTransactionAction(
                    TableTransactionActionType.UpsertReplace,
                    new AzureTableWalEntity
                    {
                        PartitionKey = manifestPartitionKey,
                        RowKey = TailRowKey,
                        Offset = resultingTail,
                        Payload = null,
                    }));
            }

            await table.SubmitTransactionAsync(actions, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Rolls back a single orphan by deleting every entry row in its
    /// batch partition and its phase-0 candidate-row in the manifest
    /// partition. The entry-row deletion uses the same chunked
    /// transactional delete helper as <see cref="TrimAsync"/> so a
    /// crash mid-rollback leaves a partial deletion that the next
    /// reconciliation pass completes idempotently. The candidate-row
    /// is deleted last so a crash between the two steps leaves the
    /// C-row in place, and the next reconciliation pass re-discovers
    /// the orphan and retries rollback against an already-empty
    /// batch partition (which is a no-op).
    /// </summary>
    private static async Task RollBackOrphanAsync(
        TableClient table,
        string manifestPartitionKey,
        OrphanBatch orphan,
        CancellationToken cancellationToken)
    {
        var entryFilter =
            $"PartitionKey eq '{Escape(orphan.BatchPartitionKey)}' and RowKey ge '{EntryRowKeyPrefix}' and RowKey lt 'F'";
        await DeletePartitionInChunksAsync(table, entryFilter, cancellationToken).ConfigureAwait(false);

        // Delete the candidate-row unconditionally. ETag.All matches
        // any version; a 404 (already deleted by a concurrent
        // reconciliation pass) is swallowed below to keep rollback
        // idempotent.
        try
        {
            await table.DeleteEntityAsync(
                manifestPartitionKey,
                BuildCandidateRowKey(orphan.StartOffset),
                ETag.All,
                cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // Already gone - idempotent.
        }
    }
}


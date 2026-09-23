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
/// candidate-rows (phase-2 deleted them, or rollback deleted them),
/// and a candidate that turns out to carry a manifest row is treated
/// as committed rather than re-added. It runs in two places - the
/// grain's <c>OnActivateAsync</c>, before the grain accepts traffic,
/// and the grain's post-failure resync, which first waits for every
/// other in-flight flush on the shard to settle. Neither can rule out
/// a phase-2 transaction the provider abandoned on a deadline landing
/// late, so the commit is conditional on the TAIL it read and on the
/// manifest rows it adds not existing; losing that race fails the
/// transaction and the pass is re-planned, never lowering TAIL
/// (#3348).
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
        var activity = GetShardActivity(manifestPartitionKey);

        await activity.ReconcileGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Quiesce this instance's own writes first. On the grain's
            // post-failure resync, earlier flushes can still be in phase
            // 1 and accepted phase-2 commits can still be queued in the
            // worker; the scan below would read either as an orphan and
            // roll it back under a commit that is about to reference it,
            // or race that commit for the same M-row (#3348).
            await WaitForShardWritesToSettleAsync(activity, manifestPartitionKey, cancellationToken).ConfigureAwait(false);

            // A committer this instance cannot see (a phase-2
            // transaction abandoned on its deadline but landing late, or
            // another process) can still add a manifest row or move TAIL
            // between this pass's reads and its commit. The commit is
            // conditional on both, so it fails with 409 / 412 instead of
            // regressing TAIL; re-planning from fresh reads then treats
            // whatever landed as committed.
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await ReconcileOnceAsync(table, manifestPartitionKey, treeId, shardIndex, cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }
                catch (RequestFailedException ex)
                    when (attempt < MaxReconcileAttempts && IsConcurrentManifestConflict(ex))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
        }
        finally
        {
            activity.ReconcileGate.Release();
        }
    }

    /// <summary>
    /// Waits until no append against the shard is in motion on this
    /// provider instance and the shard's phase-2 worker, if any, has
    /// settled every commit it accepted. Appends are drained first
    /// because an append in phase 1 enqueues its commit only when phase
    /// 1 lands.
    /// </summary>
    private async Task WaitForShardWritesToSettleAsync(
        WalShardActivity activity,
        string manifestPartitionKey,
        CancellationToken cancellationToken)
    {
        await activity.Writes.WhenIdleAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        if (_phaseTwoWorkers.TryGetValue(manifestPartitionKey, out var worker))
        {
            await worker.OutstandingCommits.WhenIdleAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Maximum number of plan-and-commit passes
    /// <see cref="ReconcileAsync"/> makes when its commit loses a race
    /// with a concurrent manifest writer. Each retry re-reads TAIL and
    /// the manifest, so it observes whatever the competing writer
    /// committed and treats it as already done.
    /// </summary>
    internal const int MaxReconcileAttempts = 4;

    /// <summary>
    /// True when <paramref name="ex"/> is the signature of a lost race
    /// against a concurrent manifest writer: 409 (a manifest row or
    /// TAIL this pass tried to add already exists) or 412 (TAIL moved
    /// since this pass read it).
    /// </summary>
    internal static bool IsConcurrentManifestConflict(RequestFailedException ex) =>
        ex.Status is 409 or 412;

    private async Task ReconcileOnceAsync(
        TableClient table,
        string manifestPartitionKey,
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        // Step 1: discover orphans. The C-row scan is always run
        // first so a silo upgraded from the legacy mode (where every
        // in-flight batch stamped a C-row) finds and reconciles every
        // pre-upgrade orphan. When
        // EliminateCandidateRowOnHotPath is on, the C-row scan
        // returns nothing for batches appended in the new mode, and
        // the supplementary discovery scan below enumerates batch
        // partitions above TAIL to find them. The two scans are
        // unioned and sorted by start offset before planning so the
        // planner sees a single ascending sequence.
        var orphans = await ReadOutstandingCandidatesAsync(
            table, manifestPartitionKey, treeId, shardIndex, cancellationToken).ConfigureAwait(false);

        // Read TAIL early so the D-mode discovery scan can anchor its
        // PartitionKey lower bound; -1 means "no manifest yet". The
        // ETag makes the eventual TAIL write conditional, so this pass
        // can never lower a TAIL a concurrent committer advanced.
        var (currentTail, tailETag) = await ReadTailAsync(table, manifestPartitionKey, cancellationToken).ConfigureAwait(false);

        if (_options.EliminateCandidateRowOnHotPath)
        {
            var partitionScanOrphans = await ReadOutstandingBatchPartitionsAboveTailAsync(
                table, treeId, shardIndex, currentTail, cancellationToken).ConfigureAwait(false);
            if (partitionScanOrphans.Count > 0)
            {
                // Merge while filtering duplicates by StartOffset: a
                // pre-upgrade orphan can have *both* a C-row and a
                // batch partition, in which case the C-row form wins
                // (its HasCandidateRow = true triggers the C-delete in
                // CommitRollForwardAsync / RollBackOrphanAsync).
                var existingStarts = new HashSet<long>(orphans.Count);
                for (var i = 0; i < orphans.Count; i++)
                {
                    existingStarts.Add(orphans[i].StartOffset);
                }
                for (var i = 0; i < partitionScanOrphans.Count; i++)
                {
                    if (existingStarts.Add(partitionScanOrphans[i].StartOffset))
                    {
                        orphans.Add(partitionScanOrphans[i]);
                    }
                }
                orphans.Sort(static (a, b) => a.StartOffset.CompareTo(b.StartOffset));
            }
        }

        // Step 2: fold in manifest rows above TAIL. A batch "orphaned"
        // above TAIL can in fact be committed - its M-row landed but
        // TAIL was later lowered by a racing writer (#3348). Re-adding
        // that M-row would fail the whole transaction with 409 on every
        // pass, wedging the shard's activation forever; leaving it
        // unseen (C-mode, where phase 2 deleted the C-row) lets the
        // next append reuse its offsets. The committed row is instead
        // authoritative and TAIL is rolled forward over it.
        await MergeCommittedAboveTailAsync(
            table, manifestPartitionKey, treeId, shardIndex, currentTail, orphans, cancellationToken).ConfigureAwait(false);

        if (orphans.Count == 0)
        {
            return;
        }

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
                table, manifestPartitionKey, plan.RollForward, currentTail, tailETag, plan.ResultingTail, cancellationToken)
                .ConfigureAwait(false);
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
    /// <para>
    /// <see cref="AlreadyCommitted"/> marks a candidate that turned out
    /// to carry a manifest row after all (discovered above TAIL, but
    /// committed); its <see cref="EndOffsetInclusive"/> is then the
    /// manifest row's <c>Offset</c>. Such a batch is never rolled back
    /// and never has its manifest row re-added.
    /// </para>
    /// </summary>
    internal readonly record struct OrphanBatch(
        long StartOffset,
        long EndOffsetInclusive,
        string BatchPartitionKey,
        bool HasCandidateRow,
        bool AlreadyCommitted = false);

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
            if (orphan.AlreadyCommitted)
            {
                // A committed manifest row is authoritative: it is never
                // rolled back (that would strand an M-row over deleted
                // entries) and it re-anchors contiguity at its end, the
                // same way the PhaseTwoWorker's monotonic TAIL does.
                // CommitRollForwardAsync skips its M-row add and only
                // tidies any leftover candidate-row.
                rollForward.Add(orphan);
                resultingTail = Math.Max(resultingTail, orphan.EndOffsetInclusive);
                contiguityBroken = false;
                continue;
            }

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
    /// partition together with its ETag, or <c>(-1L, null)</c> if the
    /// TAIL row is absent.
    /// </summary>
    private static async Task<(long Offset, ETag? ETag)> ReadTailAsync(
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
            return (response.Value.Offset, response.Value.ETag);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return (-1L, null);
        }
    }

    /// <summary>
    /// Folds committed manifest rows into <paramref name="orphansAscending"/>:
    /// an orphan whose start offset has an <c>M</c> row becomes an
    /// <see cref="OrphanBatch.AlreadyCommitted"/> copy whose end offset
    /// is the manifest row's <c>Offset</c>, and an <c>M</c> row above
    /// <paramref name="currentTail"/> with no matching orphan is added
    /// as one. One anchored range query from the lower of the lowest
    /// orphan start and <c>TAIL + 1</c>; in steady state no manifest
    /// row sits above TAIL, so the query returns nothing.
    /// </summary>
    private static async Task MergeCommittedAboveTailAsync(
        TableClient table,
        string manifestPartitionKey,
        string treeId,
        int shardIndex,
        long currentTail,
        List<OrphanBatch> orphansAscending,
        CancellationToken cancellationToken)
    {
        var lowerStart = currentTail + 1;
        if (orphansAscending.Count > 0 && orphansAscending[0].StartOffset < lowerStart)
        {
            lowerStart = orphansAscending[0].StartOffset;
        }

        var filter =
            $"PartitionKey eq '{Escape(manifestPartitionKey)}' and RowKey ge '{BuildManifestRowKey(lowerStart)}' and RowKey lt '{TailRowKey}'";

        Dictionary<long, long>? committedEnds = null;
        await foreach (var row in table
            .QueryAsync<AzureTableWalEntity>(filter, select: new[] { "RowKey", "Offset" }, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            var startOffset = long.Parse(
                row.RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture);
            committedEnds ??= new Dictionary<long, long>();
            committedEnds[startOffset] = row.Offset;
        }

        if (committedEnds is null)
        {
            return;
        }

        for (var i = 0; i < orphansAscending.Count; i++)
        {
            var orphan = orphansAscending[i];
            if (committedEnds.Remove(orphan.StartOffset, out var committedEnd))
            {
                orphansAscending[i] = orphan with
                {
                    EndOffsetInclusive = committedEnd,
                    AlreadyCommitted = true,
                };
            }
        }

        var added = false;
        foreach (var (startOffset, endOffsetInclusive) in committedEnds)
        {
            if (startOffset > currentTail)
            {
                orphansAscending.Add(new OrphanBatch(
                    startOffset,
                    endOffsetInclusive,
                    BuildBatchPartitionKey(treeId, shardIndex, startOffset),
                    HasCandidateRow: false,
                    AlreadyCommitted: true));
                added = true;
            }
        }

        if (added)
        {
            orphansAscending.Sort(static (a, b) => a.StartOffset.CompareTo(b.StartOffset));
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
            orphans.Add(new OrphanBatch(startOffset, endOffsetInclusive, batchPartitionKey, HasCandidateRow: true));
        }
        return orphans;
    }

    /// <summary>
    /// D-mode discovery scan: enumerates batch partitions whose
    /// <c>startOffset</c> is strictly greater than the persisted
    /// <c>TAIL</c>. Used when
    /// <see cref="AzureTableWalStorageOptions.EliminateCandidateRowOnHotPath"/>
    /// is on - the hot path no longer writes a C-row, so the C-row
    /// scan returns nothing for batches appended in that mode and the
    /// reconciler discovers orphans by their batch partition keys
    /// instead. <c>startOffset</c> is parsed from the partition-key
    /// suffix; <c>endOffsetInclusive</c> is recovered by reading the
    /// max entry row-key in the orphan's batch partition.
    /// <para>
    /// Soundness: phase-2 advances <c>TAIL</c> atomically with the
    /// manifest row, so any batch partition with
    /// <c>startOffset &gt; TAIL</c> is either a phase-1 commit whose
    /// phase-2 never landed, or a batch whose manifest row landed
    /// without advancing <c>TAIL</c>; the caller distinguishes the two
    /// with a manifest-row lookup before planning. The grain only
    /// reconciles with no flush of its own in motion.
    /// <c>TrimAsync</c> only deletes rows
    /// strictly below <c>TAIL</c>, so committed-but-untrimmed
    /// batches never appear in this scan.
    /// </para>
    /// </summary>
    private async Task<List<OrphanBatch>> ReadOutstandingBatchPartitionsAboveTailAsync(
        TableClient table,
        string treeId,
        int shardIndex,
        long currentTail,
        CancellationToken cancellationToken)
    {
        // Build the partition-key range that covers every possible
        // batch partition for this shard whose startOffset > TAIL.
        // Batch partition keys have shape
        //   {BatchPartitionPrefix}|{encoded}|{shardIndex}|S{startOffset:D19}
        // Lexicographic order on the D19-padded suffix matches
        // numeric order on startOffset.
        var lowerStart = currentTail + 1L;
        var shardPrefix = BuildBatchPartitionKey(treeId, shardIndex, 0L);
        // shardPrefix ends with "S{0:D19}"; strip the offset suffix to
        // get the shard-scoped prefix `_b_|{encoded}|{shardIndex}|`.
        var sIndex = shardPrefix.LastIndexOf('S');
        var shardPartitionPrefix = shardPrefix.Substring(0, sIndex);
        var lowerInclusive = shardPartitionPrefix + "S" + lowerStart.ToString("D19", CultureInfo.InvariantCulture);
        // 'T' sorts immediately after 'S'; this is the canonical
        // exclusive upper bound for every "S..." partition under the
        // shard prefix.
        var upperExclusive = shardPartitionPrefix + "T";

        // First pass: project (PartitionKey, RowKey) for every entry
        // row in every matching batch partition. Steady state returns
        // zero rows. The reconciler groups by partition key and takes
        // max(RowKey) to recover endOffsetInclusive without a
        // per-partition follow-up query.
        var filter =
            $"PartitionKey ge '{Escape(lowerInclusive)}' and PartitionKey lt '{Escape(upperExclusive)}' and RowKey ge '{EntryRowKeyPrefix}' and RowKey lt 'F'";
        var perPartitionMaxRowKey = new Dictionary<string, string>(StringComparer.Ordinal);
        await foreach (var row in table
            .QueryAsync<AzureTableWalEntity>(filter, select: new[] { "PartitionKey", "RowKey" }, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            if (!perPartitionMaxRowKey.TryGetValue(row.PartitionKey, out var existing)
                || string.CompareOrdinal(row.RowKey, existing) > 0)
            {
                perPartitionMaxRowKey[row.PartitionKey] = row.RowKey;
            }
        }

        var orphans = new List<OrphanBatch>(perPartitionMaxRowKey.Count);
        foreach (var kv in perPartitionMaxRowKey)
        {
            // PartitionKey shape: `{shardPartitionPrefix}S{startOffset:D19}`.
            // Slice past the prefix and the single 'S' marker.
            var pk = kv.Key;
            var sPos = pk.LastIndexOf('S');
            if (sPos < 0 || sPos + 1 + 19 > pk.Length)
            {
                continue;
            }
            var startOffset = long.Parse(
                pk.AsSpan(sPos + 1, 19),
                NumberStyles.None,
                CultureInfo.InvariantCulture);

            // RowKey shape: `{EntryRowKeyPrefix}{offset:D19}`. Slice
            // past the prefix to recover endOffsetInclusive.
            var rowKey = kv.Value;
            if (rowKey.Length < EntryRowKeyPrefix.Length + 19)
            {
                continue;
            }
            var endOffsetInclusive = long.Parse(
                rowKey.AsSpan(EntryRowKeyPrefix.Length, 19),
                NumberStyles.None,
                CultureInfo.InvariantCulture);

            orphans.Add(new OrphanBatch(startOffset, endOffsetInclusive, pk, HasCandidateRow: false));
        }
        return orphans;
    }

    /// <summary>
    /// Commits the rollforward portion of the plan: deletes each
    /// orphan's phase-0 candidate-row and adds its phase-2 manifest
    /// row in ascending start-offset order, plus a single TAIL write
    /// at the end. The work is chunked into transactions of up to 49
    /// orphans so the 100-action per-transaction cap is respected
    /// (<c>2 * 49 + 1 = 99</c> actions per chunk); the last chunk
    /// includes the TAIL write so the visible tail moves atomically
    /// with the final M-row.
    /// <para>
    /// An <see cref="OrphanBatch.AlreadyCommitted"/> orphan contributes
    /// no M-row add (its row exists), only the delete of any leftover
    /// candidate-row. The TAIL write is conditional: an
    /// <c>UpdateReplace</c> against the ETag read at the start of the
    /// pass, or an <c>Add</c> when no TAIL existed, and it is omitted
    /// when <paramref name="resultingTail"/> does not advance
    /// <paramref name="currentTail"/>. A TAIL a concurrent committer
    /// advanced in between therefore fails the transaction with 412 /
    /// 409 - which <see cref="ReconcileAsync"/> retries from fresh
    /// reads - instead of being silently lowered, which would expose
    /// committed offsets for reuse (#3348).
    /// </para>
    /// </summary>
    private static async Task CommitRollForwardAsync(
        TableClient table,
        string manifestPartitionKey,
        IReadOnlyList<OrphanBatch> rollForward,
        long currentTail,
        ETag? tailETag,
        long resultingTail,
        CancellationToken cancellationToken)
    {
        // Each orphan contributes at most 2 actions (M-add unless
        // already committed; C-delete only when the orphan was
        // discovered via its C-row, i.e. HasCandidateRow == true); the
        // last chunk also carries the shared TAIL write. Worst-case
        // action count per transaction stays at 2 * 49 + 1 = 99 under
        // the 100-action cap.
        const int chunkSize = 49;

        for (var i = 0; i < rollForward.Count; i += chunkSize)
        {
            var end = Math.Min(i + chunkSize, rollForward.Count);
            var actions = new List<TableTransactionAction>(((end - i) * 2) + 1);
            for (var j = i; j < end; j++)
            {
                if (rollForward[j].HasCandidateRow)
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
                }
                if (!rollForward[j].AlreadyCommitted)
                {
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
            }

            // Only the last chunk advances TAIL; intermediate chunks
            // commit C-delete + M-add pairs only. An intermediate-chunk
            // crash leaves M-rows above TAIL, which the next pass marks
            // AlreadyCommitted and rolls TAIL forward over.
            if (end == rollForward.Count && resultingTail > currentTail)
            {
                var tail = new AzureTableWalEntity
                {
                    PartitionKey = manifestPartitionKey,
                    RowKey = TailRowKey,
                    Offset = resultingTail,
                    Payload = null,
                };
                actions.Add(tailETag is { } etag
                    ? new TableTransactionAction(TableTransactionActionType.UpdateReplace, tail, etag)
                    : new TableTransactionAction(TableTransactionActionType.Add, tail));
            }

            if (actions.Count > 0)
            {
                await table.SubmitTransactionAsync(actions, cancellationToken).ConfigureAwait(false);
            }
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

        if (!orphan.HasCandidateRow)
        {
            // D-mode orphan: no C-row was ever written, so there is
            // nothing to delete in the manifest partition. The batch
            // partition entry rows have already been wiped above.
            return;
        }

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


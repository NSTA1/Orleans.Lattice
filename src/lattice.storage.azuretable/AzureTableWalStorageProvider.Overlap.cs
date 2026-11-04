using System.Globalization;
using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Overlap rejection for <see cref="AppendBatchAsync"/> and
/// <see cref="AppendEncodedBatchAsync"/>.
/// <para>
/// Each batch lands in its own partition keyed by its start offset, so
/// storage alone never stops a batch from overlapping one written at a
/// different start offset; the <see cref="IWalStorageProvider"/>
/// contract requires that overlap to be rejected. The shard's
/// <see cref="WalShardOverlapGuard"/> admits an append starting above
/// every written batch with no I/O. Anything else is checked against the
/// batches in motion on this instance and then with one query for an
/// entry row in <c>[start, end]</c> belonging to a batch partition with
/// a different start offset. A batch holds at most
/// <see cref="MaxEntriesPerBatch"/> entries, so only partitions starting
/// in <c>[start - MaxEntriesPerBatch + 1, end]</c> can hold one, which
/// bounds the query's partition-key range. Same-start re-appends are left
/// to the phase-1 idempotent-replay proof.
/// </para>
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    /// <summary>
    /// Claims <c>[startOffset, endOffsetInclusive]</c> on
    /// <paramref name="guard"/> after proving it overlaps no batch
    /// written or in motion at a different start offset. Throws
    /// <see cref="InvalidOperationException"/> on overlap. The claim is
    /// held only when the method returns normally, and returns the commit
    /// fence generation it was made under (issue #5810).
    /// </summary>
    private async Task<long> ClaimOverlapFreeAsync(
        TableClient table,
        WalShardActivity activity,
        string manifestPartitionKey,
        string treeId,
        int shardIndex,
        long startOffset,
        long endOffsetInclusive,
        CancellationToken cancellationToken)
    {
        var guard = activity.Overlap;
        if (!activity.Fence.TryGetGeneration(out var generation) || !guard.IsBounded)
        {
            // The bound and the fence are filled from the same read of TAIL, so the
            // guard's bound covers the TAIL value the fence's ETag was observed with.
            var (bound, tail, tailETag) = await ReadWrittenUpperBoundAsync(
                table, manifestPartitionKey, treeId, shardIndex, cancellationToken).ConfigureAwait(false);
            generation = activity.Fence.Fill(guard, tailETag, tail, bound);
        }

        switch (guard.Claim(startOffset, endOffsetInclusive, out var inMotionStart))
        {
            case WalShardOverlapGuard.ClaimOutcome.AboveWritten:
                return generation;
            case WalShardOverlapGuard.ClaimOutcome.Conflict:
                throw CreateOverlapException(treeId, shardIndex, startOffset, endOffsetInclusive, inMotionStart);
        }

        long writtenStart;
        try
        {
            writtenStart = await FindOverlappingWrittenBatchAsync(
                table, treeId, shardIndex, startOffset, endOffsetInclusive, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            guard.Release(startOffset, endOffsetInclusive);
            throw;
        }

        if (writtenStart >= 0L)
        {
            guard.Release(startOffset, endOffsetInclusive);
            throw CreateOverlapException(treeId, shardIndex, startOffset, endOffsetInclusive, writtenStart);
        }

        return generation;
    }

    /// <summary>
    /// Reads an upper bound on the end offset of every batch written for
    /// the shard: the persisted <c>TAIL</c>, raised by any batch
    /// partition above it that phase 2 has not committed. Also returns
    /// the <c>TAIL</c> value and ETag it was read with.
    /// </summary>
    private async Task<(long Bound, long Tail, ETag? TailETag)> ReadWrittenUpperBoundAsync(
        TableClient table,
        string manifestPartitionKey,
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        var (tail, tailETag) = await ReadTailAsync(table, manifestPartitionKey, cancellationToken).ConfigureAwait(false);
        var bound = tail;
        var aboveTail = await ReadOutstandingBatchPartitionsAboveTailAsync(
            table, treeId, shardIndex, tail, cancellationToken).ConfigureAwait(false);
        foreach (var batch in aboveTail)
        {
            if (batch.EndOffsetInclusive > bound)
            {
                bound = batch.EndOffsetInclusive;
            }
        }

        return (bound, tail, tailETag);
    }

    /// <summary>
    /// Returns the start offset of a written batch, other than one
    /// starting at <paramref name="startOffset"/>, that holds an entry in
    /// <c>[startOffset, endOffsetInclusive]</c>, or <c>-1</c> when there
    /// is none.
    /// </summary>
    private static async Task<long> FindOverlappingWrittenBatchAsync(
        TableClient table,
        string treeId,
        int shardIndex,
        long startOffset,
        long endOffsetInclusive,
        CancellationToken cancellationToken)
    {
        var lowestStart = Math.Max(0L, startOffset - MaxEntriesPerBatch + 1L);
        var filter =
            $"PartitionKey ge '{Escape(BuildBatchPartitionKey(treeId, shardIndex, lowestStart))}'"
            + $" and PartitionKey le '{Escape(BuildBatchPartitionKey(treeId, shardIndex, endOffsetInclusive))}'"
            + $" and PartitionKey ne '{Escape(BuildBatchPartitionKey(treeId, shardIndex, startOffset))}'"
            + $" and RowKey ge '{BuildEntryRowKey(startOffset)}'"
            + $" and RowKey le '{BuildEntryRowKey(endOffsetInclusive)}'";

        await foreach (var row in table
            .QueryAsync<AzureTableWalEntity>(filter, maxPerPage: 1, select: new[] { "PartitionKey" }, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            var marker = row.PartitionKey.LastIndexOf('S');
            return long.Parse(row.PartitionKey.AsSpan(marker + 1), NumberStyles.None, CultureInfo.InvariantCulture);
        }

        return -1L;
    }

    private static InvalidOperationException CreateOverlapException(
        string treeId,
        int shardIndex,
        long startOffset,
        long endOffsetInclusive,
        long otherStartOffset) =>
        new($"Append batch for '{treeId}/{shardIndex}' covering offsets {startOffset}..{endOffsetInclusive} overlaps the batch "
            + $"starting at offset {otherStartOffset}; a WAL offset must not be written twice.");
}

using System.Collections.Concurrent;
using System.Globalization;
using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// The trim sweep: <see cref="TrimAsync"/> deletes entry rows by one key-range query
/// over the shard's batch partitions rather than one query per manifest row, so it
/// also reaches a batch partition no manifest row names. Such a partition is left by a
/// batch whose phase 2 never committed - a stale writer's refused batch (issue #5810),
/// or a failed one whose writer stopped before reconciling it - and, below <c>TAIL</c>,
/// nothing else ever deletes it.
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    /// <summary>
    /// The manifest partitions this instance has swept from their first batch
    /// partition. A steady trim sweeps only the span above the previous trim
    /// watermark; the first trim of a shard on this instance, and every trim that
    /// reaches the persisted <c>TAIL</c> (a tree discard is one), sweeps the whole
    /// committed prefix, which is what reaches a leak that landed below an earlier
    /// watermark.
    /// </summary>
    private readonly ConcurrentDictionary<string, byte> _fullySweptShards = new(StringComparer.Ordinal);

    /// <summary>
    /// Deletes every entry row at or below <paramref name="sweepMark"/> in the shard's
    /// batch partitions starting in <c>[lowestStart, sweepMark]</c>, whether or not a
    /// manifest row names the batch, and returns the key of the first row the query
    /// returned that is not one, or <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <paramref name="sweepMark"/> never exceeds the persisted <c>TAIL</c>. A batch
    /// partition starting at or below it is therefore committed, or can never be: the
    /// phase-2 worker commits in start order, with its manifest row and <c>TAIL</c> in
    /// one transaction, so a live batch of the shard's writer starts above <c>TAIL</c>,
    /// and reconciliation rolls forward only a batch starting at <c>TAIL + 1</c>. The
    /// trim watermark covering every deleted row is durable before this runs.
    /// </para>
    /// <para>
    /// The query is streamed page by page and each partition's rows are deleted in
    /// transactions of at most <see cref="MaxTransactionActions"/> as its group closes,
    /// so the key set is never held whole. Each row's keys are checked against the
    /// shard and the bounds before it is deleted; a row that fails the check is left in
    /// place and reported.
    /// </para>
    /// </remarks>
    private static async Task<string?> SweepEntryRowsAsync(
        TableClient table,
        string treeId,
        int shardIndex,
        long lowestStart,
        long sweepMark,
        CancellationToken cancellationToken)
    {
        var lowerPartition = BuildBatchPartitionKey(treeId, shardIndex, lowestStart);
        var upperPartition = BuildBatchPartitionKey(treeId, shardIndex, sweepMark);
        var shardPrefix = lowerPartition.Substring(0, lowerPartition.Length - 19);
        var filter =
            $"PartitionKey ge '{Escape(lowerPartition)}' and PartitionKey le '{Escape(upperPartition)}'"
            + $" and RowKey ge '{BuildEntryRowKey(0L)}' and RowKey le '{BuildEntryRowKey(sweepMark)}'";

        string? unexpected = null;
        string? group = null;
        var groupValid = false;
        var pending = new List<TableTransactionAction>(MaxTransactionActions);
        await foreach (var page in table
            .QueryAsync<AzureTableWalEntity>(filter, maxPerPage: 1000, select: new[] { "PartitionKey", "RowKey" }, cancellationToken: cancellationToken)
            .AsPages()
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var row in page.Values)
            {
                if (!string.Equals(row.PartitionKey, group, StringComparison.Ordinal))
                {
                    await SubmitDeletesAsync(table, pending, cancellationToken).ConfigureAwait(false);
                    group = row.PartitionKey;
                    groupValid = IsSweepablePartition(group, shardPrefix, treeId, shardIndex, lowestStart, sweepMark);
                }

                if (!groupValid || !IsSweepableEntryRow(row.RowKey, sweepMark))
                {
                    unexpected ??= $"{row.PartitionKey}/{row.RowKey}";
                    continue;
                }

                pending.Add(new TableTransactionAction(TableTransactionActionType.Delete, row, ETag.All));
                if (pending.Count == MaxTransactionActions)
                {
                    await SubmitDeletesAsync(table, pending, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        await SubmitDeletesAsync(table, pending, cancellationToken).ConfigureAwait(false);
        return unexpected;
    }

    private static async Task SubmitDeletesAsync(
        TableClient table,
        List<TableTransactionAction> pending,
        CancellationToken cancellationToken)
    {
        if (pending.Count == 0)
        {
            return;
        }

        await table.SubmitTransactionAsync(pending, cancellationToken).ConfigureAwait(false);
        pending.Clear();
    }

    /// <summary>
    /// True when <paramref name="partitionKey"/> is exactly the key of one of the
    /// shard's batch partitions starting in <c>[lowestStart, sweepMark]</c>.
    /// </summary>
    private static bool IsSweepablePartition(
        string partitionKey,
        string shardPrefix,
        string treeId,
        int shardIndex,
        long lowestStart,
        long sweepMark) =>
        partitionKey.Length == shardPrefix.Length + 19
        && partitionKey.StartsWith(shardPrefix, StringComparison.Ordinal)
        && long.TryParse(partitionKey.AsSpan(shardPrefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out var start)
        && start >= lowestStart
        && start <= sweepMark
        && string.Equals(partitionKey, BuildBatchPartitionKey(treeId, shardIndex, start), StringComparison.Ordinal);

    /// <summary>True when <paramref name="rowKey"/> is an entry row key at or below <paramref name="sweepMark"/>.</summary>
    private static bool IsSweepableEntryRow(string rowKey, long sweepMark) =>
        rowKey.Length == EntryRowKeyPrefix.Length + 19
        && rowKey.StartsWith(EntryRowKeyPrefix, StringComparison.Ordinal)
        && long.TryParse(rowKey.AsSpan(EntryRowKeyPrefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out var offset)
        && offset <= sweepMark;
}

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
    /// When the next trim of each manifest partition on this instance sweeps its batch
    /// partitions from the first, as a <see cref="System.Diagnostics.Stopwatch"/>
    /// timestamp. A steady trim sweeps only the span above the previous trim watermark;
    /// the first trim once this time has come sweeps the whole committed prefix, which
    /// is what reaches a leak that landed below an earlier watermark, and moves it one
    /// <see cref="AzureTableWalStorageOptions.TrimFullSweepInterval"/> on.
    /// </summary>
    private readonly ConcurrentDictionary<string, long> _nextFullSweeps = new(StringComparer.Ordinal);

    /// <summary>
    /// Test seam: the fraction of <see cref="AzureTableWalStorageOptions.TrimFullSweepInterval"/>
    /// by which a WAL partition first seen by this instance is treated as already
    /// overdue, drawn from <c>[0, 1)</c>. <c>1</c> makes its first trim a full sweep;
    /// <c>0</c> defers that sweep a whole interval.
    /// </summary>
    internal Func<double> FullSweepJitterForTesting { get; set; } = Random.Shared.NextDouble;

    /// <summary>
    /// True when the next trim of <paramref name="manifestPartitionKey"/> should sweep
    /// from the first batch partition.
    /// </summary>
    /// <remarks>
    /// A WAL partition first seen by this instance is scheduled for its first full
    /// sweep a random fraction of the interval ahead, so the full sweeps of every
    /// partition a silo serves spread over one interval rather than all landing on the
    /// first trims after a restart. A leak a stopped process left behind is reclaimed at
    /// most one interval later; it is never read in the meantime.
    /// </remarks>
    private bool IsFullSweepDue(string manifestPartitionKey)
    {
        var interval = _options.TrimFullSweepInterval;
        if (interval <= TimeSpan.Zero)
        {
            return true;
        }

        var now = System.Diagnostics.Stopwatch.GetTimestamp();
        var due = _nextFullSweeps.GetOrAdd(
            manifestPartitionKey,
            static (_, state) => After(state.Now, state.Interval, 1d - Math.Clamp(state.Jitter(), 0d, 1d)),
            (Now: now, Interval: interval, Jitter: FullSweepJitterForTesting));
        return now >= due;
    }

    /// <summary>Schedules the next full sweep of the partition one interval after <paramref name="sweepStartedAt"/>.</summary>
    private void NoteFullSweep(string manifestPartitionKey, long sweepStartedAt) =>
        _nextFullSweeps[manifestPartitionKey] = After(sweepStartedAt, _options.TrimFullSweepInterval, 1d);

    private static long After(long timestamp, TimeSpan interval, double fraction)
    {
        var ticks = fraction * interval.TotalSeconds * System.Diagnostics.Stopwatch.Frequency;
        return ticks >= long.MaxValue - timestamp ? long.MaxValue : timestamp + (long)ticks;
    }

    /// <summary>
    /// Deletes every entry row at or below <paramref name="sweepMark"/> in the shard's
    /// batch partitions starting in <c>[lowestStart, sweepMark]</c>, whether or not a
    /// manifest row names the batch, and returns the key of the first row the query
    /// returned that is not one, or <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deleting a batch partition that no manifest row names is safe for two reasons,
    /// one for each side of the persisted <c>TAIL</c>. <paramref name="sweepMark"/> never
    /// exceeds that <c>TAIL</c>, so no batch above it is touched: neither the batches
    /// this instance has accepted but not committed, which the reported highest offset
    /// folds in, nor an interrupted one that reconciliation may still roll forward.
    /// Below it, a batch can still be live - the phase-2 worker commits whatever has
    /// arrived, so a batch whose phase 1 is slow can commit after a later one has moved
    /// <c>TAIL</c> past it - and what keeps such a batch out of reach is
    /// <see cref="IWalStorageProvider.TrimAsync"/>'s contract: a trim runs only through
    /// an offset every consumer has acknowledged, which lies below every append still
    /// in flight, and a discard or a WAL-move trim runs only once the partition takes
    /// no more writes. The trim watermark covering every deleted row is durable before
    /// this runs.
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

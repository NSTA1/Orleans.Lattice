using System.Globalization;
using System.Runtime.CompilerServices;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// The windowed batch-row reader shared by <see cref="ReadAsync"/> and
/// <see cref="ReadFilteredAsync"/> (issue #5877).
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    /// <summary>
    /// Streams the entry rows of a read window in offset order, fetching up to
    /// <see cref="AzureTableWalStorageOptions.ReadBatchFetchConcurrency"/> batches
    /// concurrently.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Rows are consumed strictly in manifest order, which is offset order because
    /// batches never overlap. The entry cap and the upper bound are applied only as
    /// rows are consumed, so a batch fetched ahead contributes nothing past the point
    /// the serial read would have stopped.
    /// </para>
    /// <para>
    /// Another batch is started only while the batches already in flight cannot
    /// fill the window by their manifest extents. An extent may exceed the rows
    /// present (a window starting mid-batch, a trimmed prefix, a partial phase-1
    /// batch), never the reverse, so no batch is queried that the serial read would
    /// not have queried. When the extents over-state the rows, the window is filled
    /// by later rounds.
    /// </para>
    /// <para>
    /// A fault or cancellation in one batch surfaces when that batch is reached, and
    /// nothing from a later batch is yielded. Batches still in flight are cancelled
    /// and observed when the enumeration ends, whether by completion, fault or
    /// disposal.
    /// </para>
    /// </remarks>
    /// <param name="table">The WAL table.</param>
    /// <param name="treeId">The WAL's tree id.</param>
    /// <param name="shardIndex">The WAL partition index.</param>
    /// <param name="manifestFilter">The manifest-row filter, ascending by start offset.</param>
    /// <param name="firstWantedOffset">The first offset the window wants, already clamped to the trim watermark.</param>
    /// <param name="toOffsetInclusive">The window's last offset, or <see cref="long.MaxValue"/> for an open window.</param>
    /// <param name="upperRowKeyClause">The batch query's row-key upper bound clause.</param>
    /// <param name="maxEntries">The most rows to yield.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    private async IAsyncEnumerable<AzureTableWalEntity> ReadWindowRowsAsync(
        TableClient table,
        string treeId,
        int shardIndex,
        string manifestFilter,
        long firstWantedOffset,
        long toOffsetInclusive,
        string upperRowKeyClause,
        int maxEntries,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var concurrency = Math.Max(1, _options.ReadBatchFetchConcurrency);
        using var fetchCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var inFlight = new Queue<(Task<List<AzureTableWalEntity>> Rows, long Extent)>();
        var manifest = table
            .QueryAsync<AzureTableWalEntity>(manifestFilter, cancellationToken: cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        var manifestDone = false;
        var consumed = 0;
        var plannedExtent = 0L;

        try
        {
            while (true)
            {
                while (!manifestDone
                    && consumed < maxEntries
                    && (inFlight.Count == 0
                        || (inFlight.Count < concurrency && consumed + plannedExtent < maxEntries)))
                {
                    if (!await manifest.MoveNextAsync().ConfigureAwait(false))
                    {
                        manifestDone = true;
                        break;
                    }

                    var manifestRow = manifest.Current;
                    var startOffset = long.Parse(
                        manifestRow.RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                        NumberStyles.None,
                        CultureInfo.InvariantCulture);

                    // Manifest rows ascend by start offset, so the first batch that
                    // starts past the window ends the scan.
                    if (startOffset > toOffsetInclusive)
                    {
                        manifestDone = true;
                        break;
                    }

                    // The manifest row's Offset column is the batch's last offset.
                    var lower = Math.Max(firstWantedOffset, startOffset);
                    var upper = Math.Min(manifestRow.Offset, toOffsetInclusive);
                    var extent = upper >= lower ? upper - lower + 1 : 0;

                    inFlight.Enqueue((
                        FetchBatchRowsAsync(
                            table,
                            BuildBatchPartitionKey(treeId, shardIndex, startOffset),
                            BuildEntryRowKey(lower),
                            upperRowKeyClause,
                            maxEntries - consumed,
                            fetchCancellation.Token),
                        extent));
                    plannedExtent += extent;
                }

                if (inFlight.Count == 0)
                {
                    yield break;
                }

                // Where the serial read would issue its next batch query with the
                // caller's token, so a cancelled read stops at the same boundary
                // even when that batch was already fetched ahead.
                cancellationToken.ThrowIfCancellationRequested();
                var finished = await Task.WhenAny(inFlight.Select(x => (Task)x.Rows)).ConfigureAwait(false); var picked = inFlight.First(x => ReferenceEquals(x.Rows, finished)); inFlight = new Queue<(Task<List<AzureTableWalEntity>> Rows, long Extent)>(inFlight.Where(x => !ReferenceEquals(x.Rows, finished))); var (next, nextExtent) = picked; // RED MUTANT M1 (#5897): consume in completion order
                plannedExtent -= nextExtent;
                var rows = await next.ConfigureAwait(false);
                foreach (var entity in rows)
                {
                    if (consumed >= maxEntries || entity.Offset > toOffsetInclusive)
                    {
                        yield break;
                    }

                    consumed++;
                    yield return entity;
                }

                if (consumed >= maxEntries)
                {
                    yield break;
                }
            }
        }
        finally
        {
            // Runs on completion, on a fault and when the caller disposes the
            // enumerator early. The look-ahead fetches run on their own linked
            // token rather than the caller's, because ending the read must stop
            // them even when the caller never cancels: an early break or a fault
            // in an earlier batch would otherwise leave queries running against
            // the table after the read has returned. Each is then awaited, so no
            // fetch outlives the read and none faults unobserved.
            fetchCancellation.Cancel();
            while (inFlight.Count > 0)
            {
                try
                {
                    await inFlight.Dequeue().Rows.ConfigureAwait(false);
                }
                catch
                {
                    // Abandoned look-ahead: its result, fault or cancellation is
                    // not part of this read, but it is observed so it cannot
                    // surface as an unobserved task exception.
                }
            }

            await manifest.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads at most <paramref name="cap"/> entry rows of one batch partition from
    /// <paramref name="lowerRowKey"/>, in row-key order.
    /// </summary>
    private static async Task<List<AzureTableWalEntity>> FetchBatchRowsAsync(
        TableClient table,
        string batchPartitionKey,
        string lowerRowKey,
        string upperRowKeyClause,
        int cap,
        CancellationToken cancellationToken)
    {
        var filter =
            $"PartitionKey eq '{Escape(batchPartitionKey)}' and RowKey ge '{lowerRowKey}' and {upperRowKeyClause}";
        var rows = new List<AzureTableWalEntity>();
        await foreach (var entity in table
            .QueryAsync<AzureTableWalEntity>(filter, maxPerPage: Math.Min(cap, 1000), cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            rows.Add(entity);
            if (rows.Count >= cap)
            {
                break;
            }
        }

        return rows;
    }
}

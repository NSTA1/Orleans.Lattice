using System.Collections.Concurrent;
using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// The durable trim watermark (issue #4621): a per-shard row in the manifest
/// partition recording the highest offset any trim has trimmed through, raised
/// before the trim deletes anything.
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    /// <summary>
    /// Row-key of the per-shard trim watermark row in the manifest partition. It
    /// sorts after <see cref="TailRowKey"/>, so no manifest range query
    /// (<c>RowKey ge 'M' and RowKey lt 'TAIL'</c>) ever returns it.
    /// </summary>
    internal const string TrimWatermarkRowKey = "TRIMMARK";

    /// <summary>
    /// The highest trim watermark this instance has written or read, per manifest
    /// partition. Reads clamp to it, so an entry a crash left behind at or below
    /// a watermark this silo knows of is never returned. A trim on another silo
    /// reaches the cache on the next <see cref="GetTrimWatermarkAsync"/>; until
    /// then such a leftover, which is a contiguous suffix of the trimmed range
    /// because a trim deletes in ascending order, reads exactly as it did before
    /// the trim.
    /// </summary>
    private readonly ConcurrentDictionary<string, long> _trimWatermarks = new(StringComparer.Ordinal);

    /// <summary>
    /// Test seam invoked after the watermark row is durable and before any entry
    /// is deleted, so a test can fail the trim at exactly that point.
    /// </summary>
    internal Func<Task>? AfterTrimWatermarkRaisedForTesting { get; set; }

    /// <inheritdoc />
    /// <remarks>
    /// The larger of the recorded watermark row and the offset below the lowest
    /// stored entry. The readable log has no holes on this provider - phase 2
    /// commits in strict offset order and reconciliation rolls a gapped orphan
    /// back - so every offset below the lowest stored entry was trimmed. That
    /// keeps the answer exact for a shard trimmed before the row existed.
    /// </remarks>
    public async Task<long?> GetTrimWatermarkAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);
        var recorded = await ReadTrimWatermarkAsync(table, manifestPartitionKey, cancellationToken).ConfigureAwait(false);
        var lowest = await GetLowestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
        var inferred = lowest >= 0
            ? lowest - 1
            : await GetHighestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
        var watermark = Math.Max(recorded, inferred);
        NoteTrimWatermark(manifestPartitionKey, watermark);
        return watermark;
    }

    /// <summary>
    /// Durably raises the shard's trim watermark row to at least
    /// <paramref name="throughOffsetInclusive"/>, never lowering it.
    /// </summary>
    private async Task RaiseTrimWatermarkAsync(
        TableClient table,
        string manifestPartitionKey,
        long throughOffsetInclusive,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AzureTableWalEntity? existing = null;
            try
            {
                existing = (await table.GetEntityAsync<AzureTableWalEntity>(
                    manifestPartitionKey,
                    TrimWatermarkRowKey,
                    cancellationToken: cancellationToken).ConfigureAwait(false)).Value;
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
            }

            try
            {
                if (existing is null)
                {
                    await table.AddEntityAsync(
                        new AzureTableWalEntity
                        {
                            PartitionKey = manifestPartitionKey,
                            RowKey = TrimWatermarkRowKey,
                            Offset = throughOffsetInclusive,
                        },
                        cancellationToken).ConfigureAwait(false);
                }
                else if (existing.Offset < throughOffsetInclusive)
                {
                    existing.Offset = throughOffsetInclusive;
                    await table.UpdateEntityAsync(existing, existing.ETag, TableUpdateMode.Replace, cancellationToken)
                        .ConfigureAwait(false);
                }

                NoteTrimWatermark(manifestPartitionKey, Math.Max(existing?.Offset ?? -1L, throughOffsetInclusive));
                return;
            }
            catch (RequestFailedException ex) when (ex.Status is 409 or 412)
            {
                // Another trim raised it concurrently: re-read and merge.
            }
        }
    }

    private static async Task<long> ReadTrimWatermarkAsync(
        TableClient table,
        string manifestPartitionKey,
        CancellationToken cancellationToken)
    {
        try
        {
            return (await table.GetEntityAsync<AzureTableWalEntity>(
                manifestPartitionKey,
                TrimWatermarkRowKey,
                cancellationToken: cancellationToken).ConfigureAwait(false)).Value.Offset;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return -1L;
        }
    }

    private void NoteTrimWatermark(string manifestPartitionKey, long watermark)
        => _trimWatermarks.AddOrUpdate(manifestPartitionKey, watermark, (_, current) => Math.Max(current, watermark));

    /// <summary>
    /// The first offset a read may return: <paramref name="firstWantedOffset"/>,
    /// raised past the trim watermark this instance knows of.
    /// </summary>
    private long ClampToTrimWatermark(string manifestPartitionKey, long firstWantedOffset)
        => _trimWatermarks.TryGetValue(manifestPartitionKey, out var watermark) && watermark >= firstWantedOffset
            ? watermark + 1
            : firstWantedOffset;
}

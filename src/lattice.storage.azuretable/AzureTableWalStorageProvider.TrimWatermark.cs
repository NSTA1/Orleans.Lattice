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
    /// <para>
    /// A shard with no stored entry was trimmed through its persisted <c>TAIL</c>,
    /// never through the highest offset: that one folds in batches this instance
    /// accepted whose phase 2 has not landed, which were never stored, let alone
    /// trimmed. Counting them, and caching the result, hid them from every later
    /// read on this instance (issue #6036). <c>TAIL</c> is read before the lowest
    /// offset, so a batch committing between the two is seen by the second read
    /// rather than counted as trimmed by the first.
    /// </para>
    /// </remarks>
    public async Task<long?> GetTrimWatermarkAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var (manifestPartitionKey, watermark) = await ReadStoredTrimWatermarkAsync(treeId, shardIndex, cancellationToken)
            .ConfigureAwait(false);
        NoteTrimWatermark(manifestPartitionKey, watermark);
        return watermark;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Derives the watermark exactly as <see cref="GetTrimWatermarkAsync"/> does,
    /// then replaces this instance's cached value with it instead of keeping the
    /// larger one, so a cached value above the stored trim point stops clamping
    /// reads (issue #6036). The stored answer never falls below a watermark a
    /// trim made durable, because the row is raised before the trim deletes
    /// anything.
    /// </remarks>
    public async Task<long?> RefreshTrimWatermarkAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var (manifestPartitionKey, watermark) = await ReadStoredTrimWatermarkAsync(treeId, shardIndex, cancellationToken)
            .ConfigureAwait(false);
        _trimWatermarks[manifestPartitionKey] = watermark;
        return watermark;
    }

    private async Task<(string ManifestPartitionKey, long Watermark)> ReadStoredTrimWatermarkAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);
        var recorded = await ReadTrimWatermarkAsync(table, manifestPartitionKey, cancellationToken).ConfigureAwait(false);
        var (persistedTail, _) = await ReadHighestOffsetsAsync(table, treeId, shardIndex, cancellationToken).ConfigureAwait(false);
        var lowest = await GetLowestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
        var inferred = lowest >= 0 ? lowest - 1 : persistedTail;
        return (manifestPartitionKey, Math.Max(recorded, inferred));
    }

    /// <summary>Test seam: raises this instance's cached watermark for a shard as a poisoned probe would.</summary>
    internal void NoteTrimWatermarkForTesting(string treeId, int shardIndex, long watermark) =>
        NoteTrimWatermark(BuildManifestPartitionKey(treeId, shardIndex), watermark);

    /// <summary>
    /// Durably raises the shard's trim watermark row to at least
    /// <paramref name="throughOffsetInclusive"/>, never lowering it, and returns the
    /// value the row held before, or -1 when it was absent.
    /// </summary>
    private async Task<long> RaiseTrimWatermarkAsync(
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
                return existing?.Offset ?? -1L;
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
            ? (watermark == long.MaxValue ? long.MaxValue : watermark + 1)
            : firstWantedOffset;
}

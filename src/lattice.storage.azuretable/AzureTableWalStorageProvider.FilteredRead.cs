using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;
using Azure.Data.Tables;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// The filtered replay read (issue #3565) for
/// <see cref="AzureTableWalStorageProvider"/>.
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    /// <inheritdoc />
    /// <remarks>
    /// <para>
    /// Walks the same manifest and batch partitions as <see cref="ReadAsync"/>,
    /// with the batch query additionally bounded above by the window's last row
    /// key. Each examined row is classified from its routing prefix before it is
    /// decoded: a compressed row is inflated into a pooled buffer rather than a
    /// fresh array, and a row the filter excludes is neither decoded nor
    /// retained. The table service still returns every row of the window - the
    /// key lives inside the payload, so the service cannot select on it - which
    /// makes the saving the per-row decode and inflate, not the transfer.
    /// </para>
    /// <para>
    /// A trailing excluded row is remembered by reference, not by content,
    /// while the scan runs, so only the one row that ends the window is ever
    /// re-read for its routing-only projection.
    /// </para>
    /// </remarks>
    public async IAsyncEnumerable<WalEntry> ReadFilteredAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int maxEntries,
        WalKeyFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per read.");
        }

        if (fromOffsetExclusive == long.MaxValue || toOffsetInclusive <= fromOffsetExclusive)
        {
            yield break;
        }

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var firstWantedOffset = Math.Max(0L, fromOffsetExclusive + 1L);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);
        var manifestFilter =
            $"PartitionKey eq '{Escape(manifestPartitionKey)}' and RowKey ge '{ManifestRowKeyPrefix}' and RowKey lt '{TailRowKey}' and Offset ge {firstWantedOffset.ToString(CultureInfo.InvariantCulture)}";
        var upperRowKey = BuildEntryRowKey(toOffsetInclusive);
        var routing = filter.IsUnbounded ? null : _routing;

        var examined = 0;
        AzureTableWalEntity? trailingExcluded = null;
        await foreach (var manifestRow in table
            .QueryAsync<AzureTableWalEntity>(manifestFilter, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            if (examined >= maxEntries)
            {
                break;
            }

            var startOffset = long.Parse(
                manifestRow.RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture);

            // Manifest rows ascend by start offset, so the first batch that
            // starts past the window ends the scan.
            if (startOffset > toOffsetInclusive)
            {
                break;
            }

            var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, startOffset);
            var batchLowerInclusiveRowKey = BuildEntryRowKey(Math.Max(firstWantedOffset, startOffset));
            var batchFilter =
                $"PartitionKey eq '{Escape(batchPartitionKey)}' and RowKey ge '{batchLowerInclusiveRowKey}' and RowKey le '{upperRowKey}'";

            await foreach (var entity in table
                .QueryAsync<AzureTableWalEntity>(batchFilter, maxPerPage: Math.Min(maxEntries - examined, 1000), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (examined >= maxEntries || entity.Offset > toOffsetInclusive)
                {
                    break;
                }

                examined++;
                if (TryDecodeUnlessExcluded(entity, filter, routing, out var mutation))
                {
                    trailingExcluded = null;
                    yield return new WalEntry { Offset = entity.Offset, Mutation = mutation };
                }
                else
                {
                    trailingExcluded = entity;
                }
            }
        }

        if (trailingExcluded is not null)
        {
            yield return new WalEntry
            {
                Offset = trailingExcluded.Offset,
                Mutation = DecodeRoutingOnly(trailingExcluded, routing),
            };
        }
    }

    /// <summary>
    /// Decodes <paramref name="entity"/> unless <paramref name="filter"/>
    /// excludes it, in which case nothing is decoded and <see langword="false"/>
    /// is returned. With a routing reader the verdict comes from the payload's
    /// routing prefix; without one, or when the prefix cannot be read, from the
    /// full decode.
    /// </summary>
    private bool TryDecodeUnlessExcluded(
        AzureTableWalEntity entity,
        in WalKeyFilter filter,
        WalRecordRoutingReader? routing,
        out LatticeMutation mutation)
    {
        mutation = default;
        var encoded = DecompressPayloadPooled(entity.Payload, (byte)entity.Compression, out var rented);
        try
        {
            if (encoded.IsEmpty)
            {
                // The defensive empty-payload shape ReadAsync decodes as a
                // default mutation: no key, so never excluded.
                return true;
            }

            if (routing is not null && routing.TryClassify(encoded, in filter, out var excluded) && excluded)
            {
                return false;
            }

            var record = _serializer.Deserialize(encoded);
            mutation = WalRecordConverter.FromWalRecord(in record);
            if (filter.Excludes(mutation.Kind, mutation.Key))
            {
                mutation = default;
                return false;
            }

            return true;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// The routing-only projection of an excluded row: kind and key, every
    /// other field default.
    /// </summary>
    private LatticeMutation DecodeRoutingOnly(AzureTableWalEntity entity, WalRecordRoutingReader? routing)
    {
        var encoded = DecompressPayloadPooled(entity.Payload, (byte)entity.Compression, out var rented);
        try
        {
            if (routing is not null && routing.TryReadRoutingOnly(encoded, out var routingOnly))
            {
                return WalRecordConverter.FromWalRecord(in routingOnly);
            }

            var record = _serializer.Deserialize(encoded);
            return WalFilteredRead.RoutingOnly(WalRecordConverter.FromWalRecord(in record));
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }
}

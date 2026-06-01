using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Azure Table entity carrying one persisted <see cref="WalEntry"/>.
/// One entity per appended entry; the per-partition head-pointer
/// sentinel uses the same shape with a fixed <see cref="ITableEntity.RowKey"/>.
/// <para>
/// Internal because the WAL contract is the public surface - the
/// table-row schema is an implementation detail of the Azure provider.
/// Kept simple (two domain properties + the four mandatory
/// <see cref="ITableEntity"/> fields) so a future schema migration
/// can be diffed against this single class.
/// </para>
/// </summary>
internal sealed class AzureTableWalEntity : ITableEntity
{
    /// <inheritdoc />
    public string PartitionKey { get; set; } = string.Empty;

    /// <inheritdoc />
    public string RowKey { get; set; } = string.Empty;

    /// <inheritdoc />
    public DateTimeOffset? Timestamp { get; set; }

    /// <inheritdoc />
    public ETag ETag { get; set; }

    /// <summary>
    /// The dense, monotonically-increasing per-shard offset assigned to
    /// the entry at append time. Duplicated from the row key so the
    /// provider can return it without parsing the row-key prefix.
    /// </summary>
    public long Offset { get; set; }

    /// <summary>
    /// Orleans-binary-serialised <see cref="LatticeMutation"/> payload.
    /// Null on the per-partition head sentinel (see
    /// <see cref="AzureTableWalStorageProvider.HeadRowKey"/>).
    /// </summary>
    public byte[]? Payload { get; set; }

    /// <summary>
    /// Summed encoded payload byte length of every entry in the batch
    /// this row records. Carried only on manifest M-rows
    /// (<see cref="AzureTableWalStorageProvider.ManifestRowKeyPrefix"/>);
    /// zero on entry rows, the TAIL pointer, and candidate rows. The
    /// byte-accurate storage-usage aggregator sums this column across a
    /// shard's live M-rows to report the shard's retained WAL footprint
    /// in O(manifest-rows) without reading any entry payload. A legacy
    /// M-row written before this column existed decodes to <c>0</c>,
    /// which under-reports that batch's bytes until it is trimmed - an
    /// acceptable, monotonically-self-healing approximation for the
    /// advisory uses of the figure.
    /// </summary>
    public long PayloadBytes { get; set; }
}

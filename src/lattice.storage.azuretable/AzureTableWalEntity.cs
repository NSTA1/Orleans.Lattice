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

    /// <summary>
    /// The <see cref="LatticeCompression"/> algorithm tag applied to
    /// <see cref="Payload"/>, stored as an <see cref="int"/> because
    /// Azure Table Storage has no single-byte EDM property type (the
    /// underlying value is always a <see cref="LatticeCompression"/>
    /// byte cast to <see cref="int"/>). <c>0</c>
    /// (<see cref="LatticeCompression.None"/>) means the payload is the
    /// verbatim Orleans-binary-serialised <see cref="WalRecord"/>; a
    /// non-zero tag means <see cref="Payload"/> is <c>[4-byte
    /// little-endian uncompressed length][compressed bytes]</c> and the
    /// provider decompresses it on read via the registered
    /// <see cref="ILatticeCompressor"/> whose
    /// <see cref="ILatticeCompressor.Algorithm"/> matches the tag.
    /// <para>
    /// Backwards-compatible by construction: a row written before this
    /// column existed decodes the absent property to <c>0</c>, so legacy
    /// rows read back as uncompressed with no migration. Carried on
    /// entry rows only; the head sentinel, TAIL pointer, candidate rows,
    /// and manifest M-rows leave it at the default <c>0</c>.
    /// </para>
    /// </summary>
    public int Compression { get; set; }

    /// <summary>
    /// Per-batch idempotency sentinel: a collision-resistant hash over the
    /// whole phase-1 batch's canonical content (start offset, entry count,
    /// and every entry's offset + compression tag + payload bytes). Carried
    /// only on the <i>first</i> entry row of a batch (the row whose
    /// <see cref="Offset"/> equals the batch's start offset); left
    /// <see langword="null"/> on every other entry row, the head sentinel,
    /// the TAIL pointer, candidate rows, and manifest M-rows.
    /// <para>
    /// Because a phase-1 batch is committed in a single Azure Table
    /// transaction (atomic all-or-nothing within the partition), the first
    /// row's presence proves the whole batch is durable, and this hash
    /// proves <i>which</i> batch's bytes are durable. The
    /// <c>409 EntityAlreadyExists</c> idempotent-replay guard
    /// (<see cref="AzureTableWalStorageProvider.IsIdempotentPhaseOneReplayAsync"/>)
    /// reads back this single row and compares the resident hash to the hash
    /// of the batch the call tried to write - an O(1) proof that replaces the
    /// former O(batch-size) per-entry read-back while still detecting any
    /// divergent payload anywhere in the batch. A row written before this
    /// column existed decodes the absent property to <see langword="null"/>,
    /// which the guard treats as a legacy row and falls back to the
    /// per-entry read-back for, so the schema is forward- and
    /// backward-compatible with no migration.
    /// </para>
    /// </summary>
    public byte[]? BatchHash { get; set; }

    /// <summary>
    /// Number of entry rows in the phase-1 batch this row begins. Carried
    /// only on the first entry row of a batch (alongside
    /// <see cref="BatchHash"/>); zero on every other row. Compared by the
    /// idempotent-replay guard so a resident batch of a different length can
    /// never be mistaken for a byte-identical replay even in the
    /// astronomically unlikely event of a <see cref="BatchHash"/> collision.
    /// Defaults to <c>0</c> for legacy rows written before the column
    /// existed.
    /// </summary>
    public int BatchEntryCount { get; set; }
}

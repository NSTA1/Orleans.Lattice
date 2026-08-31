namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persisted state row for <see cref="Grains.ILeafSnapshotStorageGrain"/>.
/// Holds a point-in-time copy of a single leaf grain's entry cache
/// together with the WAL offset the snapshot is consistent with.
/// <para>
/// The blob is the safety net for the leaf-state collapse: a
/// leaf whose persisted <c>ProjectionCheckpointOffset</c> would
/// otherwise fall off WAL retention captures its current projection
/// into this row, and the reactivation path prefers the snapshot
/// over a from-scratch WAL replay whenever the snapshot offset is
/// strictly newer than the persisted checkpoint.
/// </para>
/// <para>
/// At most one snapshot exists per leaf; a successful capture
/// overwrites the previous blob via a single Orleans
/// <c>WriteStateAsync</c> call on the storage grain. No historical
/// retention is intended; the WAL remains the long-term audit trail.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafSnapshotBlob)]
internal sealed class LeafSnapshotBlob
{
    /// <summary>
    /// WAL offset (under "applied through offset N inclusive"
    /// semantics) at which the projection in <see cref="Rows"/> is
    /// consistent. A reactivation that prefers this snapshot must
    /// resume WAL replay strictly after this offset
    /// (<c>SnapshotOffset + 1</c>). Defaults to the "nothing
    /// captured" sentinel <c>-1</c>.
    /// </summary>
    [Id(0)] public long SnapshotOffset { get; set; } = -1;

    /// <summary>
    /// Legacy canonical byte-row contents of the leaf's entry cache at
    /// <see cref="SnapshotOffset"/>. May be empty when the leaf had
    /// no live keys at capture time (a freshly created leaf whose
    /// projection is empty still has a meaningful checkpoint
    /// offset). Never <see langword="null"/>.
    /// <para>
    /// This slot is the <em>legacy</em> row carrier. A blob captured by a
    /// build that encodes rows into <see cref="EncodedRows"/> leaves this
    /// empty, and every reader should go through
    /// <see cref="EnumerateRows"/> / <see cref="GetRowCount"/> rather than
    /// reading it directly, so it sees the rows whichever encoding they
    /// arrived in. The slot itself is retained forever: blobs persisted
    /// before the binary encoding existed carry their rows here and must
    /// stay readable indefinitely.
    /// </para>
    /// </summary>
    [Id(1)] public IReadOnlyList<LeafSnapshotRow> Rows { get; set; } = Array.Empty<LeafSnapshotRow>();

    /// <summary>
    /// Wall-clock <see cref="DateTime.Ticks"/> at the moment the
    /// snapshot was captured. Diagnostic only - the reactivation
    /// preference logic compares <see cref="SnapshotOffset"/>
    /// against the leaf's persisted checkpoint, not this stamp.
    /// </summary>
    [Id(2)] public long CapturedAtTicks { get; set; }

    /// <summary>
    /// Precomputed byte-accurate footprint of <see cref="Rows"/> using the
    /// same UTF-8-key + stored-value-length formula the leaf surface uses
    /// for <see cref="LeafStats.StateBytes"/>. Populated once at capture
    /// time by <see cref="Grains.ILeafSnapshotStorageGrain.SaveAsync"/> so
    /// <see cref="Grains.ILeafSnapshotStorageGrain.GetSnapshotByteSizeAsync"/>
    /// is a constant-time field read. Wire-compatible: legacy persisted
    /// blobs without this field decode to <c>0</c>, and the storage grain
    /// lazily back-fills the slot on the first byte-size read so the figure
    /// converges to the correct value without forcing a re-capture.
    /// </summary>
    [Id(3)] public long SnapshotBytes { get; set; }

    /// <summary>
    /// Per-partition WAL offset the projection in <see cref="Rows"/> is
    /// consistent through, under the same "applied through offset N
    /// inclusive" semantics as the scalar <see cref="SnapshotOffset"/>.
    /// Slot <c>p</c> holds the checkpoint offset partition <c>p</c> was
    /// captured at; slot <c>0</c> mirrors <see cref="SnapshotOffset"/>.
    /// A partition that had never checkpointed at capture time holds the
    /// <c>-1</c> "nothing applied" sentinel.
    /// <para>
    /// This exists because under the default <c>WalPartitions = 8</c> the
    /// scalar <see cref="SnapshotOffset"/> only describes partition 0; the
    /// coverage-gated WAL-GC trim floor (see
    /// <c>BPlusLeafGrain.ResolveDurablePinForPartition</c>) needs the
    /// per-partition covered offset to authorise trimming each partition's
    /// checkpointed prefix, and the rehydrate path needs it to advance each
    /// partition's persisted checkpoint independently. Wire-compatible:
    /// legacy blobs captured before this field decode to <see langword="null"/>,
    /// which the readers treat as "only partition 0 is covered, at
    /// <see cref="SnapshotOffset"/>".
    /// </para>
    /// </summary>
    [Id(4)] public long[]? SnapshotOffsetsByPartition { get; set; }

    /// <summary>
    /// Compact binary encoding of the leaf's entry cache produced by
    /// <see cref="LeafSnapshotCodec.Encode"/>, or <see langword="null"/> for a
    /// legacy blob whose rows live in <see cref="Rows"/>.
    /// <para>
    /// The legacy shape persists <see cref="Rows"/> as an object graph, which
    /// under the default JSON grain-storage serializer costs a property-name
    /// envelope and a base64 string for every single row. The frame collapses
    /// that to one length-prefixed buffer of raw value bytes, and lets the
    /// decode path run without materialising a key string, a base64 buffer, or
    /// a scratch array per row.
    /// </para>
    /// <para>
    /// Adoption is by dual-read with lazy rewrite, never by migration.
    /// <see cref="LeafSnapshotCodec.HasFrameMagic"/> sniffs the encoding on
    /// load, so a legacy blob is decoded from <see cref="Rows"/> exactly as it
    /// always was, and is only re-encoded into this slot when the leaf next
    /// captures naturally. Nothing rewrites a blob eagerly and nothing
    /// discards one: the coverage-gated WAL GC trims a checkpointed prefix
    /// because a snapshot covers it, so a snapshot that became unreadable over
    /// a trimmed prefix would be data loss rather than a slow start.
    /// </para>
    /// <para>
    /// Wire-compatible: legacy persisted blobs decode this slot to
    /// <see langword="null"/>, and a blob carrying a frame leaves
    /// <see cref="Rows"/> empty, so exactly one of the two ever holds rows.
    /// </para>
    /// </summary>
    [Id(5)] public byte[]? EncodedRows { get; set; }

    /// <summary>
    /// <see langword="true"/> when this blob claims to carry its rows as a
    /// <see cref="LeafSnapshotCodec"/> binary frame. Claiming is not the same
    /// as being valid - see <see cref="ValidateRowPayload"/>.
    /// <para>
    /// Declared as a method rather than a property on purpose: the blob is
    /// persisted through grain-storage serializers that serialise public
    /// properties reflectively, and a computed property would be written into
    /// every persisted row.
    /// </para>
    /// </summary>
    internal bool HasBinaryRowPayload()
        => EncodedRows is { Length: > 0 } frame && LeafSnapshotCodec.HasFrameMagic(frame);

    /// <summary>
    /// Verifies that this blob's row payload can be read in full, in whichever
    /// encoding it arrived in: a binary frame must pass the codec's header,
    /// checksum, and structural checks, and a legacy row list must not carry a
    /// <see langword="null"/> key. A non-empty <see cref="EncodedRows"/> that
    /// is not a frame at all is rejected outright rather than silently falling
    /// back to <see cref="Rows"/>, since that shape can only mean corruption.
    /// <para>
    /// A <see langword="false"/> result must be treated by the caller as "no
    /// snapshot" - not as a snapshot with fewer rows, and never as coverage.
    /// Reporting coverage for a prefix this blob cannot actually reproduce is
    /// what would let the WAL GC trim the last durable copy of it.
    /// </para>
    /// </summary>
    internal bool ValidateRowPayload()
    {
        if (EncodedRows is { Length: > 0 } frame)
        {
            return LeafSnapshotCodec.Validate(frame);
        }

        var rows = Rows;
        if (rows is null)
        {
            return true;
        }

        for (var i = 0; i < rows.Count; i++)
        {
            if (rows[i].Key is null)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Number of rows this blob carries, in whichever encoding it uses. Reads
    /// the frame header rather than decoding any row.
    /// </summary>
    internal int GetRowCount()
    {
        if (EncodedRows is { Length: > 0 } frame)
        {
            return LeafSnapshotCodec.TryGetRowCount(frame, out var count) ? count : 0;
        }

        return Rows?.Count ?? 0;
    }

    /// <summary>
    /// Returns an allocation-free, encoding-agnostic view over this blob's
    /// rows. Callers that have not already validated the payload should call
    /// <see cref="ValidateRowPayload"/> first; enumerating an unvalidated
    /// malformed frame throws rather than yielding a silently short row set.
    /// </summary>
    internal LeafSnapshotRowSequence EnumerateRows()
        => EncodedRows is { Length: > 0 } frame
            ? LeafSnapshotRowSequence.FromFrame(frame)
            : LeafSnapshotRowSequence.FromLegacyRows(Rows);
}

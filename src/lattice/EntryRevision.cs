using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// One revision of a single key in its change-history timeline, as returned by
/// <see cref="ILattice.ScanEntryHistoryAsync"/>. A revision mirrors the
/// stored <see cref="HistoryRow"/> shape (the durable history-view substrate) so a
/// reader can render the timeline without consulting the source tree, and is also
/// the mapping target for a retained write-ahead-log mutation on the best-effort
/// fallback path.
/// <para>
/// Values are size-bounded previews so a whole value never crosses the wire
/// unnecessarily: <see cref="ValuePreview"/> / <see cref="Delta"/> are clipped to a
/// fixed preview budget and <see cref="ValueTruncated"/> records whether the clip
/// happened, while <see cref="ValueLength"/> always reports the full source length.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.EntryRevision)]
[Immutable]
public readonly record struct EntryRevision
{
    /// <summary>The revision's hybrid-logical-clock timestamp - the timeline order key.</summary>
    [Id(0)] public HybridLogicalClock Hlc { get; init; }

    /// <summary>What the underlying source mutation was.</summary>
    [Id(1)] public HistoryRowKind Kind { get; init; }

    /// <summary>
    /// The source key this revision belongs to. For a
    /// <see cref="HistoryRowKind.RangeTombstone"/> marker this is the inclusive
    /// start of the swept range.
    /// </summary>
    [Id(2)] public string SourceKey { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored the source mutation, or
    /// <see langword="null"/> for a local write.
    /// </summary>
    [Id(3)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// A size-bounded preview of the LWW value bytes for a
    /// <see cref="HistoryRowKind.Set"/> revision when the active
    /// <see cref="HistoryRetentionMode"/> retained them; <see langword="null"/>
    /// when shaped to <see cref="HistoryRetentionMode.MetadataOnly"/>, and on
    /// delete, CRDT-delta, and range-tombstone revisions. When
    /// <see cref="ValueTruncated"/> is <see langword="true"/> this is shorter than
    /// the full value; <see cref="ValueLength"/> always reports the full length.
    /// </summary>
    [Id(4)] public byte[]? ValuePreview { get; init; }

    /// <summary>The full byte length of the source LWW value, or <c>0</c> when the revision carried no value.</summary>
    [Id(5)] public int ValueLength { get; init; }

    /// <summary>Whether <see cref="ValuePreview"/> (or <see cref="Delta"/>) was clipped to the preview budget.</summary>
    [Id(6)] public bool ValueTruncated { get; init; }

    /// <summary>
    /// A content hash (xxHash64) of the source LWW value, or <c>0</c> when the
    /// revision carried no value. Populated for a <see cref="HistoryRowKind.Set"/>
    /// revision even under <see cref="HistoryRetentionMode.MetadataOnly"/>, where it
    /// is the only value fingerprint retained.
    /// </summary>
    [Id(7)] public long ValueHash { get; init; }

    /// <summary>
    /// A size-bounded preview of the CRDT author delta for a
    /// <see cref="HistoryRowKind.CrdtDelta"/> revision; <see langword="null"/> for
    /// LWW, delete, and range-tombstone revisions. Clipped to the preview budget
    /// (with <see cref="ValueTruncated"/> set) so an inspection read never ships an
    /// unbounded delta.
    /// </summary>
    [Id(8)] public byte[]? Delta { get; init; }

    /// <summary>
    /// The declared convergence rule of the source mutation, so a
    /// <see cref="HistoryRowKind.CrdtDelta"/> revision's delta can be matched to the
    /// right decoder.
    /// </summary>
    [Id(9)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// The <see cref="HistoryRetentionMode"/> the maintainer applied when it wrote
    /// the stored revision, recording why a given revision does or does not carry
    /// value bytes. Always <see cref="HistoryRetentionMode.FullValue"/> for a
    /// revision read from the write-ahead-log fallback window (where the live value
    /// bytes are still present).
    /// </summary>
    [Id(10)] public HistoryRetentionMode RetentionShape { get; init; }

    /// <summary>
    /// The exclusive upper bound of the swept range for a
    /// <see cref="HistoryRowKind.RangeTombstone"/> marker; <see langword="null"/>
    /// for point revisions.
    /// </summary>
    [Id(11)] public string? EndKey { get; init; }

    /// <summary>
    /// The sparse vector-clock frontier captured at commit time, available only on
    /// the write-ahead-log fallback path; always <see langword="null"/> on the
    /// history-view path, because the durable history substrate intentionally does
    /// not persist the frontier per revision.
    /// </summary>
    [Id(12)] public VersionVector? VectorClock { get; init; }
}

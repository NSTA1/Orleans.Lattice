using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// One durable revision in a per-key history view. A history view re-keys every
/// source mutation into an append-only row stored at view key
/// <c>{sourceKey}/{encodedHlc}</c>; the serialized <see cref="HistoryRow"/> is the
/// value of that row. Distinct source HLCs map to distinct view keys, so nothing
/// folds and the full timeline is retained durably - independently of source
/// write-ahead-log garbage collection.
/// <para>
/// The projection emits the maximal row (full value for LWW, author delta for
/// CRDT); the view maintainer then <em>shapes</em> it per the source tree's live
/// <see cref="HistoryRetentionMode"/> at drain time, which is why a metadata-only
/// row carries <see cref="ValueHash"/> and <see cref="ValueLength"/> but a
/// <see langword="null"/> <see cref="Value"/>. <see cref="RetentionShape"/>
/// records the mode that shaped this particular row, so a window/mode change is
/// legible at the row boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.HistoryRow)]
[Immutable]
public readonly record struct HistoryRow
{
    /// <summary>The source entry's hybrid logical clock - the revision's order key.</summary>
    [Id(0)] public HybridLogicalClock Timestamp { get; init; }

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
    /// The LWW value bytes for a <see cref="HistoryRowKind.Set"/> revision when
    /// the active <see cref="HistoryRetentionMode"/> retained them
    /// (<see cref="HistoryRetentionMode.FullValue"/>, or a recent
    /// <see cref="HistoryRetentionMode.Hybrid"/> revision); <see langword="null"/>
    /// when shaped to <see cref="HistoryRetentionMode.MetadataOnly"/>, and on
    /// delete, CRDT, and range-tombstone rows.
    /// </summary>
    [Id(4)] public byte[]? Value { get; init; }

    /// <summary>
    /// The CRDT author delta for a <see cref="HistoryRowKind.CrdtDelta"/>
    /// revision; <see langword="null"/> for LWW, delete, and range-tombstone rows.
    /// </summary>
    [Id(5)] public byte[]? Delta { get; init; }

    /// <summary>
    /// A content hash (xxHash64) of the source LWW value, or <c>0</c> when the
    /// revision carried no value. Always populated for a
    /// <see cref="HistoryRowKind.Set"/> revision (even under
    /// <see cref="HistoryRetentionMode.MetadataOnly"/>, where it is the only value
    /// fingerprint retained).
    /// </summary>
    [Id(6)] public long ValueHash { get; init; }

    /// <summary>
    /// The byte length of the source LWW value, or <c>0</c> when the revision
    /// carried no value. Retained under every mode so a metadata-only row still
    /// reports the value size.
    /// </summary>
    [Id(7)] public int ValueLength { get; init; }

    /// <summary>
    /// The declared convergence rule of the source mutation, mirrored so the
    /// element-level provenance decoder can pick the matching delta deserializer
    /// for a <see cref="HistoryRowKind.CrdtDelta"/> revision.
    /// </summary>
    [Id(8)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// The <see cref="HistoryRetentionMode"/> the maintainer applied when it wrote
    /// this row. Records the shape boundary so a reader can tell why a given
    /// revision does or does not carry value bytes after a live mode change.
    /// </summary>
    [Id(9)] public HistoryRetentionMode RetentionShape { get; init; }

    /// <summary>
    /// The exclusive upper bound of the swept range for a
    /// <see cref="HistoryRowKind.RangeTombstone"/> marker; <see langword="null"/>
    /// for point revisions.
    /// </summary>
    [Id(10)] public string? EndKey { get; init; }

    /// <summary>
    /// Compares two rows by value: every scalar field plus the <see cref="Value"/>
    /// and <see cref="Delta"/> bytes compared by content. The compiler-generated
    /// record-struct equality compares those arrays with
    /// <see cref="EqualityComparer{T}.Default"/>, which for a <see cref="byte"/>
    /// array is reference equality, so two structurally identical rows - and a row
    /// that round-trips through serialization versus its pre-serialization self -
    /// would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The row to compare against.</param>
    public bool Equals(HistoryRow other) =>
        Timestamp.Equals(other.Timestamp)
        && Kind == other.Kind
        && string.Equals(SourceKey, other.SourceKey, StringComparison.Ordinal)
        && string.Equals(OriginClusterId, other.OriginClusterId, StringComparison.Ordinal)
        && ValueHash == other.ValueHash
        && ValueLength == other.ValueLength
        && Mode == other.Mode
        && RetentionShape == other.RetentionShape
        && string.Equals(EndKey, other.EndKey, StringComparison.Ordinal)
        && BytesEqual(Value, other.Value)
        && BytesEqual(Delta, other.Delta);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Timestamp);
        hash.Add(Kind);
        hash.Add(SourceKey, StringComparer.Ordinal);
        hash.Add(OriginClusterId, StringComparer.Ordinal);
        hash.Add(ValueHash);
        hash.Add(ValueLength);
        hash.Add(Mode);
        hash.Add(RetentionShape);
        hash.Add(EndKey, StringComparer.Ordinal);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        if (Delta is { } delta)
        {
            hash.AddBytes(delta);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}

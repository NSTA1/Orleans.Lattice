namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only record of a single revision in a key's change-history timeline,
/// returned by <see cref="ILatticeStateQuery.GetEntryHistoryAsync"/>. Mirrors a
/// stored history-view revision (or a retained write-ahead-log mutation on the
/// best-effort fallback path): the order key, the kind of mutation, its origin
/// and category, a size-bounded value-or-metadata view per the tree's retention
/// mode, and - for a CRDT revision whose bytes were retained in full - the
/// decoded element-level member changes.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.EntryRevisionRecord)]
[Immutable]
public sealed record EntryRevisionRecord
{
    /// <summary>The revision's hybrid-logical-clock timestamp - the timeline order key.</summary>
    [Id(0)] public HybridLogicalClock Hlc { get; init; }

    /// <summary>What the underlying source mutation was (set, delete, CRDT delta, or range tombstone).</summary>
    [Id(1)] public HistoryRowKind Kind { get; init; }

    /// <summary>
    /// Whether the revision was a user-driven write or a library-internal
    /// maintenance write. The durable history substrate does not stamp the
    /// category per revision, so a history revision is always reported as
    /// <see cref="MutationCategory.User"/>; the field is present so the surface
    /// mirrors <see cref="StateChangeNotification.Category"/> and can carry a
    /// retained category once the substrate persists one.
    /// </summary>
    [Id(2)] public MutationCategory Category { get; init; }

    /// <summary>
    /// The source key this revision belongs to. For a
    /// <see cref="HistoryRowKind.RangeTombstone"/> marker this is the inclusive
    /// start of the swept range.
    /// </summary>
    [Id(3)] public required string SourceKey { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored the source mutation, or
    /// <see langword="null"/> for a local write.
    /// </summary>
    [Id(4)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// A size-bounded preview of the LWW value bytes for a
    /// <see cref="HistoryRowKind.Set"/> revision when the active retention mode
    /// retained them; <see langword="null"/> under
    /// <see cref="HistoryRetentionMode.MetadataOnly"/> and on delete, CRDT-delta,
    /// and range-tombstone revisions. When <see cref="Truncated"/> is
    /// <see langword="true"/> this is shorter than the full value;
    /// <see cref="ValueLength"/> always reports the full length.
    /// </summary>
    [Id(5)] public byte[]? ValuePreview { get; init; }

    /// <summary>The full byte length of the source LWW value, or <c>0</c> when the revision carried no value.</summary>
    [Id(6)] public int ValueLength { get; init; }

    /// <summary>Whether <see cref="ValuePreview"/> (or <see cref="Delta"/>) was clipped to the preview budget.</summary>
    [Id(7)] public bool Truncated { get; init; }

    /// <summary>
    /// A content hash (xxHash64) of the source LWW value, or <c>0</c> when the
    /// revision carried no value. Populated for a <see cref="HistoryRowKind.Set"/>
    /// revision even under <see cref="HistoryRetentionMode.MetadataOnly"/>, where
    /// it is the only value fingerprint retained.
    /// </summary>
    [Id(8)] public long ValueHash { get; init; }

    /// <summary>
    /// A size-bounded preview of the CRDT author delta for a
    /// <see cref="HistoryRowKind.CrdtDelta"/> revision; <see langword="null"/> for
    /// LWW, delete, and range-tombstone revisions.
    /// </summary>
    [Id(9)] public byte[]? Delta { get; init; }

    /// <summary>
    /// The declared convergence rule of the source mutation
    /// (<see cref="LatticeMergeMode.LwwRegister"/> for an opaque value), so a
    /// CRDT revision's delta can be matched to the right decoder.
    /// </summary>
    [Id(10)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// The decoded element-level member-change events for a CRDT revision whose
    /// bytes were retained in full at preview time; empty for an LWW revision, a
    /// non-CRDT tree, a metadata-only or truncated CRDT revision, or when no
    /// decoder is registered for the shape.
    /// </summary>
    [Id(11)] public IReadOnlyList<CrdtMemberChange> MemberChanges { get; init; } = Array.Empty<CrdtMemberChange>();

    /// <summary>
    /// The per-revision retention descriptor: the mode that shaped this row and
    /// whether its value bytes were retained, so a consumer can detect a
    /// retention transition between adjacent revisions of the same key.
    /// </summary>
    [Id(12)] public RevisionRetention Retention { get; init; }

    /// <summary>
    /// The exclusive upper bound of the swept range for a
    /// <see cref="HistoryRowKind.RangeTombstone"/> marker; <see langword="null"/>
    /// for point revisions.
    /// </summary>
    [Id(13)] public string? EndKey { get; init; }

    /// <summary>
    /// Compares two revisions by value, with <see cref="ValuePreview"/>,
    /// <see cref="Delta"/>, and the <see cref="MemberChanges"/> sequence compared
    /// by content. The compiler-generated record equality compares the
    /// <see cref="byte"/> arrays with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality) and the <see cref="MemberChanges"/> list by reference,
    /// so two structurally identical revisions - and, in particular, a revision and
    /// its post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The revision to compare against.</param>
    public bool Equals(EntryRevisionRecord? other) =>
        other is not null
        && Hlc.Equals(other.Hlc)
        && Kind == other.Kind
        && Category == other.Category
        && string.Equals(SourceKey, other.SourceKey, StringComparison.Ordinal)
        && string.Equals(OriginClusterId, other.OriginClusterId, StringComparison.Ordinal)
        && ValueLength == other.ValueLength
        && Truncated == other.Truncated
        && ValueHash == other.ValueHash
        && Mode == other.Mode
        && Retention.Equals(other.Retention)
        && string.Equals(EndKey, other.EndKey, StringComparison.Ordinal)
        && BytesEqual(ValuePreview, other.ValuePreview)
        && BytesEqual(Delta, other.Delta)
        && MemberChangesEqual(MemberChanges, other.MemberChanges);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Hlc);
        hash.Add(Kind);
        hash.Add(Category);
        hash.Add(SourceKey, StringComparer.Ordinal);
        hash.Add(OriginClusterId, StringComparer.Ordinal);
        hash.Add(ValueLength);
        hash.Add(Truncated);
        hash.Add(ValueHash);
        hash.Add(Mode);
        hash.Add(Retention);
        hash.Add(EndKey, StringComparer.Ordinal);
        if (ValuePreview is { } preview)
        {
            hash.AddBytes(preview);
        }

        if (Delta is { } delta)
        {
            hash.AddBytes(delta);
        }

        if (MemberChanges is { } changes)
        {
            hash.Add(changes.Count);
            foreach (var change in changes)
            {
                hash.Add(change);
            }
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));

    private static bool MemberChangesEqual(
        IReadOnlyList<CrdtMemberChange>? left,
        IReadOnlyList<CrdtMemberChange>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        for (var i = 0; i < left.Count; i++)
        {
            if (!left[i].Equals(right[i]))
            {
                return false;
            }
        }

        return true;
    }
}

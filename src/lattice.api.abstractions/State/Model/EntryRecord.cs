using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only record of a single entry returned by the entry / key-range
/// inspection endpoint. Values are size-bounded previews so whole values do
/// not cross the wire unnecessarily.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.EntryRecord)]
[Immutable]
public sealed record EntryRecord
{
    /// <summary>The entry key.</summary>
    [Id(0)] public required string Key { get; init; }

    /// <summary>
    /// A size-bounded preview of the value bytes. When
    /// <see cref="Truncated"/> is <see langword="true"/> this is shorter than
    /// the full value; <see cref="ValueLength"/> always reports the full length.
    /// </summary>
    [Id(1)] public byte[] ValuePreview { get; init; } = Array.Empty<byte>();

    /// <summary>Full length, in bytes, of the stored value.</summary>
    [Id(2)] public int ValueLength { get; init; }

    /// <summary>Whether <see cref="ValuePreview"/> was truncated to the preview budget.</summary>
    [Id(3)] public bool Truncated { get; init; }

    /// <summary>The entry's hybrid-logical-clock timestamp.</summary>
    [Id(4)] public HybridLogicalClock Hlc { get; init; }

    /// <summary>Whether the entry is a tombstone (deleted) marker.</summary>
    [Id(5)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the entry expires, or <c>0</c> when it does
    /// not expire.
    /// </summary>
    [Id(6)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// CRDT shape tag when the value is a typed CRDT, or <see langword="null"/>
    /// for an opaque byte value.
    /// </summary>
    [Id(7)] public string? CrdtShape { get; init; }

    /// <summary>
    /// The decoded element-level members of the value's <em>current</em> folded
    /// CRDT state, when the entry is a typed CRDT (<see cref="CrdtShape"/> is
    /// non-<see langword="null"/>) and its shape has a registered decoder. This
    /// is a point-in-time snapshot of the materialised value - the live members
    /// presently in the set / map / register / sequence, a counter's net total, or
    /// a flag's boolean state - with removed elements excluded. It is not a
    /// per-revision change timeline. Empty for an opaque last-writer-wins value, an
    /// empty CRDT, or when no decoder is available for the shape.
    /// </summary>
    [Id(8)] public IReadOnlyList<CrdtMemberValue> CurrentMembers { get; init; } = Array.Empty<CrdtMemberValue>();

    /// <summary>
    /// The per-key convergence discriminator recorded for this entry, or
    /// <see langword="null"/> when the entry is a plain last-writer-wins value.
    /// Resolved from the leaf's own per-key mode map, so it is reported even on a
    /// local, non-replicated, or mixed-mode tree where the per-tree merge-mode
    /// resolver reports nothing. Additive; pre-existing callers may ignore it.
    /// </summary>
    [Id(9)] public LatticeMergeMode? MergeMode { get; init; }

    /// <summary>
    /// <see langword="true"/> when <see cref="ValuePreview"/> is the raw stored
    /// bytes rather than a decoded logical CRDT projection - i.e. a genuinely
    /// opaque value (plain last-writer-wins), or a typed CRDT whose logical value
    /// could not be decoded here (no registered shape/decoder, or a deployment
    /// without the CRDT shape registry). When <see langword="false"/> the entry's
    /// logical value decoded successfully and is described by
    /// <see cref="CrdtShape"/> / <see cref="CurrentMembers"/>. Internal CRDT
    /// serialization is never presented as if it were the value without this flag
    /// set. Additive; pre-existing callers may ignore it.
    /// </summary>
    [Id(10)] public bool Raw { get; init; }

    /// <summary>
    /// Compares two records by value, with <see cref="ValuePreview"/> compared by
    /// content and <see cref="CurrentMembers"/> compared element by element. The
    /// compiler-generated record equality compares the <see cref="byte"/> array and
    /// the member list with <see cref="EqualityComparer{T}.Default"/> (reference
    /// equality), so two structurally identical records - and, in particular, a
    /// record and its post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The record to compare against.</param>
    public bool Equals(EntryRecord? other) =>
        other is not null
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && ValueLength == other.ValueLength
        && Truncated == other.Truncated
        && Hlc.Equals(other.Hlc)
        && IsTombstone == other.IsTombstone
        && ExpiresAtTicks == other.ExpiresAtTicks
        && string.Equals(CrdtShape, other.CrdtShape, StringComparison.Ordinal)
        && MergeMode == other.MergeMode
        && Raw == other.Raw
        && BytesEqual(ValuePreview, other.ValuePreview)
        && MembersEqual(CurrentMembers, other.CurrentMembers);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Key, StringComparer.Ordinal);
        if (ValuePreview is { } preview)
        {
            hash.AddBytes(preview);
        }

        hash.Add(ValueLength);
        hash.Add(Truncated);
        hash.Add(Hlc);
        hash.Add(IsTombstone);
        hash.Add(ExpiresAtTicks);
        hash.Add(CrdtShape, StringComparer.Ordinal);
        if (CurrentMembers is { } members)
        {
            foreach (var member in members)
            {
                hash.Add(member);
            }
        }

        hash.Add(MergeMode);
        hash.Add(Raw);
        return hash.ToHashCode();
    }

    private static bool MembersEqual(
        IReadOnlyList<CrdtMemberValue>? left,
        IReadOnlyList<CrdtMemberValue>? right)
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

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}

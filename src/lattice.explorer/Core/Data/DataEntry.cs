using Orleans.Lattice.Api.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// The explorer's view of a single entry, projected from the state-API
/// <see cref="EntryRecord"/>. Carries the (possibly truncated) value bytes plus
/// the metadata the Data tab surfaces.
/// </summary>
public sealed record DataEntry
{
    /// <summary>The entry key.</summary>
    public required string Key { get; init; }

    /// <summary>The fetched value bytes, truncated to a preview when <see cref="Truncated"/> is set.</summary>
    public byte[] Value { get; init; } = Array.Empty<byte>();

    /// <summary>The full length, in bytes, of the stored value.</summary>
    public int ValueLength { get; init; }

    /// <summary>Whether <see cref="Value"/> is a truncated preview of the full value.</summary>
    public bool Truncated { get; init; }

    /// <summary>The entry's hybrid-logical-clock timestamp.</summary>
    public HybridLogicalClock Hlc { get; init; }

    /// <summary>Whether the entry is a tombstone (deleted) marker.</summary>
    public bool IsTombstone { get; init; }

    /// <summary>Absolute UTC tick at which the entry expires, or <c>0</c> when it does not expire.</summary>
    public long ExpiresAtTicks { get; init; }

    /// <summary>The CRDT shape tag when the value is a typed CRDT, or <see langword="null"/>.</summary>
    public string? CrdtShape { get; init; }

    /// <summary>
    /// The decoded element-level members of the value's current folded CRDT
    /// state when the entry is a typed CRDT, or an empty list for an opaque
    /// last-writer-wins value, an empty CRDT, or a deployment without a decoder
    /// for the shape. A point-in-time snapshot of the materialised value, not a
    /// per-revision change timeline.
    /// </summary>
    public IReadOnlyList<DataCrdtMember> CurrentMembers { get; init; } = Array.Empty<DataCrdtMember>();

    /// <summary>Projects a state-API <see cref="EntryRecord"/> into a <see cref="DataEntry"/>.</summary>
    public static DataEntry From(EntryRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);

        return new DataEntry
        {
            Key = record.Key,
            Value = record.ValuePreview,
            ValueLength = record.ValueLength,
            Truncated = record.Truncated,
            Hlc = record.Hlc,
            IsTombstone = record.IsTombstone,
            ExpiresAtTicks = record.ExpiresAtTicks,
            CrdtShape = record.CrdtShape,
            CurrentMembers = MapMembers(record.CurrentMembers),
        };
    }

    private static IReadOnlyList<DataCrdtMember> MapMembers(IReadOnlyList<CrdtMemberValue> members)
    {
        if (members.Count == 0)
        {
            return Array.Empty<DataCrdtMember>();
        }

        var mapped = new DataCrdtMember[members.Count];
        for (var i = 0; i < members.Count; i++)
        {
            mapped[i] = DataCrdtMember.From(members[i]);
        }

        return mapped;
    }

    /// <summary>
    /// Compares two entries by value, with <see cref="Value"/> compared by content
    /// and <see cref="CurrentMembers"/> compared element by element. The
    /// compiler-generated record equality compares the <see cref="byte"/> array and
    /// the member list with <see cref="EqualityComparer{T}.Default"/> (reference
    /// equality), so two structurally identical entries - and, in particular, an
    /// entry and its post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The entry to compare against.</param>
    public bool Equals(DataEntry? other) =>
        other is not null
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && ValueLength == other.ValueLength
        && Truncated == other.Truncated
        && Hlc.Equals(other.Hlc)
        && IsTombstone == other.IsTombstone
        && ExpiresAtTicks == other.ExpiresAtTicks
        && string.Equals(CrdtShape, other.CrdtShape, StringComparison.Ordinal)
        && BytesEqual(Value, other.Value)
        && MembersEqual(CurrentMembers, other.CurrentMembers);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Key, StringComparer.Ordinal);
        if (Value is { } value)
        {
            hash.AddBytes(value);
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

        return hash.ToHashCode();
    }

    private static bool MembersEqual(
        IReadOnlyList<DataCrdtMember>? left,
        IReadOnlyList<DataCrdtMember>? right)
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

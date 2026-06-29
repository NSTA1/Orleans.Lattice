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
}

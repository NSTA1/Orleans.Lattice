using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A single element-level provenance event decoded from a CRDT's stored state
/// and/or its author deltas by an <see cref="ICrdtProvenanceDecoder"/>: which
/// element changed, whether it was added or removed, the replica that authored
/// the change, and the causal ordinal (dot counter) that change carried.
/// <para>
/// <strong>Causal, not wall-clock, order.</strong> <see cref="Ordinal"/> is the
/// replica-local dot counter - a causal coordinate, monotonic per
/// <see cref="ReplicaId"/> but not comparable as a timestamp across replicas
/// and carrying no wall-clock meaning. Two events from different replicas with
/// the same <see cref="Ordinal"/> are concurrent, not simultaneous. A real
/// wall-clock reading is present in <see cref="WallClock"/> only when the
/// decoder was given the owning mutation's hybrid-logical-clock stamp (the
/// delta-sequence path); the folded-state fallback has no mutation to draw it
/// from and leaves <see cref="WallClock"/> as <see langword="null"/>, exposing
/// causal order only.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrdtMemberChange)]
[Immutable]
public readonly record struct CrdtMemberChange
{
    /// <summary>
    /// The element bytes the change applies to. Element identity is by content
    /// (byte equality); an empty array is a valid element. Never
    /// <see langword="null"/> on decoder-produced events.
    /// </summary>
    [Id(0)] public byte[] Element { get; init; }

    /// <summary>Whether the element was added or removed by this change.</summary>
    [Id(1)] public CrdtMemberChangeKind Kind { get; init; }

    /// <summary>The id of the replica that authored the change.</summary>
    [Id(2)] public string ReplicaId { get; init; }

    /// <summary>
    /// The causal ordinal (dot counter) the change carried. Monotonic per
    /// <see cref="ReplicaId"/>; a causal coordinate, not a wall-clock time. See
    /// the type remarks for the causal-versus-wall-clock distinction.
    /// </summary>
    [Id(3)] public long Ordinal { get; init; }

    /// <summary>
    /// The wall-clock hybrid-logical-clock stamp of the owning mutation, when
    /// the decoder was supplied one (the delta-sequence path);
    /// <see langword="null"/> when only causal order is available (the
    /// folded-state fallback).
    /// </summary>
    [Id(4)] public HybridLogicalClock? WallClock { get; init; }

    /// <summary>
    /// Compares two changes by value, with <see cref="Element"/> compared by
    /// content. The compiler-generated record-struct equality compares
    /// <see cref="Element"/> with <see cref="EqualityComparer{T}.Default"/>, which
    /// for a <see cref="byte"/> array is reference equality - so two changes built
    /// from independently allocated but byte-identical <see cref="Element"/> arrays
    /// (including a change and its post-serialization self) would otherwise never
    /// compare equal, contradicting this type's documented by-content element
    /// identity.
    /// </summary>
    /// <param name="other">The change to compare against.</param>
    public bool Equals(CrdtMemberChange other) =>
        BytesEqual(Element, other.Element)
        && Kind == other.Kind
        && string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal)
        && Ordinal == other.Ordinal
        && Nullable.Equals(WallClock, other.WallClock);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Element is { } element)
        {
            hash.AddBytes(element);
        }

        hash.Add(Kind);
        hash.Add(ReplicaId, StringComparer.Ordinal);
        hash.Add(Ordinal);
        hash.Add(WallClock);
        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}

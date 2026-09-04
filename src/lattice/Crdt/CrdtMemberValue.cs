namespace Orleans.Lattice;

/// <summary>
/// A single present member of a CRDT's <em>current</em> folded state, decoded by
/// <see cref="ICrdtProvenanceDecoder.DecodeCurrentValue(object)"/>. This is the
/// value-level projection used to render a typed CRDT entry as its materialised
/// contents (e.g. the live elements of an OR-Set, the net total of a PN-counter,
/// the current value(s) of a register).
/// <para>
/// <strong>Live members only - no add/remove kind.</strong> Unlike
/// <see cref="CrdtMemberChange"/> - a provenance <em>event</em> that may be an
/// add or a remove and whose folded-state reconstruction surfaces every surviving
/// causal dot (so a removed-then-re-added OR-Set element, or even a fully-removed
/// element whose add dots linger under the tombstone, still appears) - a
/// <see cref="CrdtMemberValue"/> only ever represents a value that is part of the
/// current state. Removed elements are, by definition, absent. There is therefore
/// no kind discriminator: every projected member is present.
/// </para>
/// <para>
/// <see cref="ReplicaId"/> and <see cref="Ordinal"/> carry useful provenance for
/// shapes that have it (the authoring replica and causal dot counter of the
/// surviving member). For aggregate shapes that have no per-element provenance -
/// a PN-counter's net total, a flag's boolean - <see cref="ReplicaId"/> is empty
/// and <see cref="Ordinal"/> carries a shape-specific scalar (the counter's value)
/// or zero.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrdtMemberValue)]
[Immutable]
public readonly record struct CrdtMemberValue
{
    /// <summary>
    /// The member's bytes: the live element of a set, the value of a register or
    /// sequence node, a key surrogate for a map, or a textual rendering of an
    /// aggregate value (a counter total, a flag state). Never
    /// <see langword="null"/> on decoder-produced members; an empty array is a
    /// valid member.
    /// </summary>
    [Id(0)] public byte[] Element { get; init; }

    /// <summary>
    /// The id of the replica whose surviving dot contributes this member, or an
    /// empty string for an aggregate value with no single authoring replica (a
    /// counter total, a flag state).
    /// </summary>
    [Id(1)] public string ReplicaId { get; init; }

    /// <summary>
    /// The causal ordinal (per-replica dot counter) of the surviving member, or a
    /// shape-specific scalar where no causal dot applies (a PN-counter's net
    /// value), or zero (a flag state). A causal coordinate, not a wall-clock time.
    /// </summary>
    [Id(2)] public long Ordinal { get; init; }

    /// <summary>
    /// Compares two members by value, with <see cref="Element"/> compared by
    /// content. The compiler-generated record-struct equality compares
    /// <see cref="Element"/> with <see cref="EqualityComparer{T}.Default"/> -
    /// reference equality for a <see cref="byte"/> array - so two members built
    /// from independently allocated but byte-identical <see cref="Element"/> arrays
    /// (including a member and its post-serialization self) would otherwise never
    /// compare equal.
    /// </summary>
    /// <param name="other">The member to compare against.</param>
    public bool Equals(CrdtMemberValue other) =>
        BytesEqual(Element, other.Element)
        && string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal)
        && Ordinal == other.Ordinal;

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Element is { } element)
        {
            hash.AddBytes(element);
        }

        hash.Add(ReplicaId, StringComparer.Ordinal);
        hash.Add(Ordinal);
        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}

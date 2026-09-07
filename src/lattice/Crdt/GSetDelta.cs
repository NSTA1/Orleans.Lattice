namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a grow-only (G) set mutation. Carries the elements
/// added since the receiver's cursor. A grow-only set has no removes, so a
/// delta is purely additive.
/// <para>
/// Apply semantics on the receiver: union <see cref="Adds"/> into the local
/// element set. The result is independent of arrival order, duplicate
/// delivery, and partial overlap with the local state - set union is
/// commutative, associative, and idempotent.
/// </para>
/// <para>
/// Emitters always populate <see cref="Adds"/> (use an empty array for a
/// no-op); use <see cref="Empty"/> to author a no-op delta without allocating
/// a fresh empty array. The <see langword="default"/> instance has a
/// <c>null</c> collection and is intended only as the zero-value of the struct
/// - consumers should either treat <c>null</c> as empty or assert non-null at
/// the apply boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.GSetDelta)]
[Immutable]
public readonly record struct GSetDelta
{
    /// <summary>
    /// The element byte arrays added since the receiver's cursor. An empty
    /// list indicates a no-op delta. Never <c>null</c> on emitter-produced
    /// deltas.
    /// </summary>
    [Id(0)] public IReadOnlyList<byte[]> Adds { get; init; }

    /// <summary>
    /// A reusable no-op delta with an empty (but non-null) <see cref="Adds"/>
    /// collection. Backed by <see cref="Array.Empty{T}"/> so repeated access
    /// does not allocate.
    /// </summary>
    public static GSetDelta Empty { get; } = new()
    {
        Adds = Array.Empty<byte[]>(),
    };

    /// <summary>
    /// Compares two deltas by value, with <see cref="Adds"/> compared
    /// element-by-element and each element byte array compared by content. The
    /// compiler-generated record-struct equality compares the <see cref="Adds"/>
    /// collection reference with <see cref="EqualityComparer{T}.Default"/>, so
    /// two structurally identical deltas built from independently allocated
    /// collections - and, in particular, a delta and its post-serialization
    /// self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The delta to compare against.</param>
    public bool Equals(GSetDelta other) => AddsEqual(Adds, other.Adds);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Adds is { } adds)
        {
            hash.Add(adds.Count);
            foreach (var element in adds)
            {
                if (element is { } bytes)
                {
                    hash.AddBytes(bytes);
                }
                else
                {
                    hash.Add(0);
                }
            }
        }

        return hash.ToHashCode();
    }

    private static bool AddsEqual(IReadOnlyList<byte[]>? left, IReadOnlyList<byte[]>? right)
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
            if (!BytesEqual(left[i], right[i]))
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

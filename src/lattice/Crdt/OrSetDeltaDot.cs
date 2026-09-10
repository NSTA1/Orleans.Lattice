namespace Orleans.Lattice;

/// <summary>
/// A single entry in an observed-remove (OR) set delta: a unique
/// (replica id, counter) "dot" attached to an element. The dot context
/// allows concurrent adds and removes of the same element across
/// clusters to converge - a remove cancels exactly the dots it observed,
/// so a concurrent add on another replica with a different dot survives
/// the merge.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrSetDeltaDot)]
[Immutable]
public readonly record struct OrSetDeltaDot
{
    /// <summary>The element bytes the dot is attached to. Never <c>null</c> on emitter-produced dots.</summary>
    [Id(0)] public byte[] Element { get; init; }

    /// <summary>
    /// The id of the replica that authored the dot. Combined with
    /// <see cref="Counter"/> this forms a globally-unique identifier for
    /// a single add operation.
    /// </summary>
    [Id(1)] public string ReplicaId { get; init; }

    /// <summary>
    /// The replica-local monotonic counter at the moment the dot was
    /// authored. Strictly greater than any prior counter from the same
    /// replica.
    /// </summary>
    [Id(2)] public long Counter { get; init; }

    /// <summary>
    /// Compares two dots by value, with <see cref="Element"/> compared by
    /// content. The compiler-generated record-struct equality compares the
    /// <see cref="Element"/> byte array with <see cref="EqualityComparer{T}.Default"/> -
    /// reference equality for a <see cref="byte"/> array - so two dots built
    /// from independently allocated but byte-identical elements (including a
    /// dot and its post-serialization self) would otherwise never compare
    /// equal, silently breaking any dedup or membership check framed as
    /// record equality over these dots.
    /// </summary>
    /// <param name="other">The dot to compare against.</param>
    public bool Equals(OrSetDeltaDot other) =>
        BytesEqual(Element, other.Element)
        && string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal)
        && Counter == other.Counter;

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Element is { } value)
        {
            hash.AddBytes(value);
        }

        hash.Add(ReplicaId, StringComparer.Ordinal);
        hash.Add(Counter);
        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}

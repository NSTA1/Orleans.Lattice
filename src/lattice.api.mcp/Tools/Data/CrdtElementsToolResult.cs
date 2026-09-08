namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of an OR-Set, MV-Register, or Sequence read tool: the
/// current member / value bytes, each base64-encoded in the tool's JSON
/// structured content. An OR-Set read is unordered; an MV-Register read carries
/// one value normally and more than one only while concurrent writes are
/// unresolved; a Sequence read preserves collaborative insertion order. An absent
/// or unreadable key yields an empty list.
/// </summary>
public sealed record CrdtElementsToolResult
{
    /// <summary>Logical tree the CRDT lives on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The CRDT key.</summary>
    public required string Key { get; init; }

    /// <summary>The current element / value bytes (base64-encoded in JSON structured content).</summary>
    public required IReadOnlyList<byte[]> Elements { get; init; }

    /// <summary>
    /// Compares two results by value, with <see cref="Elements"/> compared
    /// element-by-element and each value byte array compared by content. The
    /// compiler-generated record equality compares the <see cref="Elements"/>
    /// list with <see cref="EqualityComparer{T}.Default"/> (reference equality),
    /// so two structurally identical results - and, in particular, a result and
    /// its post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The result to compare against.</param>
    public bool Equals(CrdtElementsToolResult? other) =>
        other is not null
        && string.Equals(TreeId, other.TreeId, StringComparison.Ordinal)
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && ElementsEqual(Elements, other.Elements);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(TreeId, StringComparer.Ordinal);
        hash.Add(Key, StringComparer.Ordinal);
        if (Elements is { } elements)
        {
            hash.Add(elements.Count);
            foreach (var element in elements)
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

    private static bool ElementsEqual(IReadOnlyList<byte[]>? left, IReadOnlyList<byte[]>? right)
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

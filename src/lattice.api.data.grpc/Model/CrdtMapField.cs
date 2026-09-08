namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// One field of an OR-Map read: the field name and its current concurrent values
/// (one normally, more than one only while concurrent writes to that field are
/// unresolved).
/// </summary>
/// <remarks>
/// Not <c>[Immutable]</c>: it nests mutable value buffers materialised from the
/// decoded map, so it must remain copy-eligible across the gRPC boundary.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtMapField)]
public sealed record CrdtMapField
{
    /// <summary>The map field name.</summary>
    [Id(0)] public required string Field { get; init; }

    /// <summary>The field's current concurrent value bytes.</summary>
    [Id(1)] public List<byte[]> Values { get; init; } = [];

    /// <summary>
    /// Compares two fields by value, with <see cref="Values"/> compared
    /// element-by-element and each value byte array compared by content. The
    /// compiler-generated record equality compares the <see cref="Values"/>
    /// list with <see cref="EqualityComparer{T}.Default"/> (reference equality),
    /// so two structurally identical fields - and, in particular, a field and
    /// its post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The field to compare against.</param>
    public bool Equals(CrdtMapField? other) =>
        other is not null
        && string.Equals(Field, other.Field, StringComparison.Ordinal)
        && ValuesEqual(Values, other.Values);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Field, StringComparer.Ordinal);
        if (Values is { } values)
        {
            hash.Add(values.Count);
            foreach (var value in values)
            {
                if (value is { } bytes)
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

    private static bool ValuesEqual(List<byte[]>? left, List<byte[]>? right)
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

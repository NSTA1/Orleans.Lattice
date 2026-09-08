namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the OR-Map read tool: each live field mapped to its
/// current concurrent value bytes (one normally, more than one only while a
/// field's concurrent writes are unresolved), each value base64-encoded in the
/// tool's JSON structured content. Tombstoned and absent fields are omitted; an
/// absent or unreadable key yields an empty map.
/// </summary>
public sealed record CrdtMapToolResult
{
    /// <summary>Logical tree the map lives on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The map key.</summary>
    public required string Key { get; init; }

    /// <summary>Each live field mapped to its current concurrent value bytes.</summary>
    public required IReadOnlyDictionary<string, IReadOnlyList<byte[]>> Fields { get; init; }

    /// <summary>
    /// Compares two results by value, with <see cref="Fields"/> compared as an
    /// order-independent map whose per-field value lists are compared
    /// element-by-element and each value byte array compared by content. The
    /// compiler-generated record equality compares the <see cref="Fields"/>
    /// dictionary with <see cref="EqualityComparer{T}.Default"/> (reference
    /// equality), so two structurally identical results - and, in particular, a
    /// result and its post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The result to compare against.</param>
    public bool Equals(CrdtMapToolResult? other) =>
        other is not null
        && string.Equals(TreeId, other.TreeId, StringComparison.Ordinal)
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && FieldsEqual(Fields, other.Fields);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(TreeId, StringComparer.Ordinal);
        hash.Add(Key, StringComparer.Ordinal);
        if (Fields is { } fields)
        {
            var fieldsHash = 0;
            foreach (var (field, values) in fields)
            {
                var entryHash = new HashCode();
                entryHash.Add(field, StringComparer.Ordinal);
                if (values is { } list)
                {
                    entryHash.Add(list.Count);
                    foreach (var value in list)
                    {
                        if (value is { } bytes)
                        {
                            entryHash.AddBytes(bytes);
                        }
                        else
                        {
                            entryHash.Add(0);
                        }
                    }
                }

                fieldsHash ^= entryHash.ToHashCode();
            }

            hash.Add(fieldsHash);
        }

        return hash.ToHashCode();
    }

    private static bool FieldsEqual(
        IReadOnlyDictionary<string, IReadOnlyList<byte[]>>? left,
        IReadOnlyDictionary<string, IReadOnlyList<byte[]>>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        foreach (var (field, leftValues) in left)
        {
            if (!right.TryGetValue(field, out var rightValues) || !ValuesEqual(leftValues, rightValues))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ValuesEqual(IReadOnlyList<byte[]>? left, IReadOnlyList<byte[]>? right)
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

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A plain key / value entry used as both a returned entry of a bounded range
/// read and an upsert leg of a write batch on the MCP data tools. The value is
/// the full opaque byte payload, carried as a base64 string in the tool's JSON
/// structured content. Deliberately free of Orleans serialization attributes:
/// the MCP SDK serializes it with <c>System.Text.Json</c>, not the Orleans
/// wire format.
/// </summary>
public sealed record DataEntryDto
{
    /// <summary>The entry key.</summary>
    public required string Key { get; init; }

    /// <summary>The full value bytes (base64-encoded in JSON structured content).</summary>
    public byte[] Value { get; init; } = Array.Empty<byte>();

    /// <summary>
    /// Compares two entries by value, with <see cref="Value"/> compared by
    /// content. The compiler-generated record equality compares the
    /// <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical entries - and, in
    /// particular, an entry and its post-serialization self - would otherwise
    /// never compare equal.
    /// </summary>
    /// <param name="other">The entry to compare against.</param>
    public bool Equals(DataEntryDto? other) =>
        other is not null
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && BytesEqual(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Key, StringComparer.Ordinal);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}

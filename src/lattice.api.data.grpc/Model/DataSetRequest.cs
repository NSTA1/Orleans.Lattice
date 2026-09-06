namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the point-set RPC. A serializable mirror of the
/// <c>(treeId, key, value)</c> argument triple routed onto
/// <c>ILatticeDataApi.SetAsync</c>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataSetRequest)]
public sealed record DataSetRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The entry key to write.</summary>
    [Id(1)] public required string Key { get; init; }

    /// <summary>The value bytes to store.</summary>
    [Id(2)] public byte[] Value { get; init; } = Array.Empty<byte>();

    /// <summary>
    /// Compares two requests by value, with <see cref="Value"/> compared by
    /// content. The compiler-generated record equality compares the
    /// <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical requests - and, in
    /// particular, a request and its post-serialization self - would otherwise
    /// never compare equal.
    /// </summary>
    /// <param name="other">The request to compare against.</param>
    public bool Equals(DataSetRequest? other) =>
        other is not null
        && string.Equals(TreeId, other.TreeId, StringComparison.Ordinal)
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && BytesEqual(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(TreeId, StringComparer.Ordinal);
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

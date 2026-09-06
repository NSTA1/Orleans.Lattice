namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for creating a provider-backed runtime materialised view. The
/// payload is opaque to the transport and is never echoed in a response.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminCreateViewRequest)]
[Immutable]
public sealed record TreeAdminCreateViewRequest
{
    /// <summary>The logical materialised-view name.</summary>
    [Id(0)] public required string ViewName { get; init; }

    /// <summary>The directly writable source tree id.</summary>
    [Id(1)] public required string SourceTreeId { get; init; }

    /// <summary>The host-registered runtime projection provider key.</summary>
    [Id(2)] public required string ProviderKey { get; init; }

    /// <summary>The bounded opaque provider payload.</summary>
    [Id(3)] public byte[] Payload { get; init; } = [];

    /// <summary>
    /// Compares two requests by value, with <see cref="Payload"/> compared by
    /// content. The compiler-generated record equality compares the
    /// <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical requests - and, in
    /// particular, a request and its post-serialization self - would otherwise
    /// never compare equal.
    /// </summary>
    /// <param name="other">The request to compare against.</param>
    public bool Equals(TreeAdminCreateViewRequest? other) =>
        other is not null
        && string.Equals(ViewName, other.ViewName, StringComparison.Ordinal)
        && string.Equals(SourceTreeId, other.SourceTreeId, StringComparison.Ordinal)
        && string.Equals(ProviderKey, other.ProviderKey, StringComparison.Ordinal)
        && BytesEqual(Payload, other.Payload);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(ViewName, StringComparer.Ordinal);
        hash.Add(SourceTreeId, StringComparer.Ordinal);
        hash.Add(ProviderKey, StringComparer.Ordinal);
        if (Payload is { } payload)
        {
            hash.AddBytes(payload);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the unified typed-CRDT write RPC. A single discriminated
/// message carries every CRDT mutation: <see cref="Op"/> selects the verb and the
/// server reads only the fields that verb needs (an unused field is left at its
/// default). This keeps the wire contract to one write RPC instead of one per
/// primitive while the facade stays explicitly typed.
/// </summary>
/// <remarks>
/// Not <c>[Immutable]</c>: it nests a mutable <see cref="Element"/> buffer that is
/// handed to the grain-bound write path, so it must remain copy-eligible across
/// the gRPC marshalling boundary.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtWriteRequest)]
public sealed record CrdtWriteRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The key the CRDT is stored under.</summary>
    [Id(1)] public required string Key { get; init; }

    /// <summary>The typed mutation to apply.</summary>
    [Id(2)] public required CrdtWriteOp Op { get; init; }

    /// <summary>The writer id, for verbs that attribute the mutation to a replica; empty otherwise.</summary>
    [Id(3)] public string ReplicaId { get; init; } = string.Empty;

    /// <summary>The signed amount for counter verbs; ignored otherwise.</summary>
    [Id(4)] public long Amount { get; init; }

    /// <summary>The opaque element / value bytes for set, register, sequence, and map verbs; empty otherwise.</summary>
    [Id(5)] public byte[] Element { get; init; } = Array.Empty<byte>();

    /// <summary>The map field for OR-Map verbs; empty otherwise.</summary>
    [Id(6)] public string Field { get; init; } = string.Empty;

    /// <summary>The zero-based index for sequence verbs; ignored otherwise.</summary>
    [Id(7)] public int Index { get; init; }

    /// <summary>
    /// Compares two requests by value, with <see cref="Element"/> compared by
    /// content. The compiler-generated record equality compares the
    /// <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical requests - and, in
    /// particular, a request and its post-serialization self - would otherwise
    /// never compare equal.
    /// </summary>
    /// <param name="other">The request to compare against.</param>
    public bool Equals(CrdtWriteRequest? other) =>
        other is not null
        && string.Equals(TreeId, other.TreeId, StringComparison.Ordinal)
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && Op == other.Op
        && string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal)
        && Amount == other.Amount
        && string.Equals(Field, other.Field, StringComparison.Ordinal)
        && Index == other.Index
        && BytesEqual(Element, other.Element);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(TreeId, StringComparer.Ordinal);
        hash.Add(Key, StringComparer.Ordinal);
        hash.Add(Op);
        hash.Add(ReplicaId, StringComparer.Ordinal);
        hash.Add(Amount);
        hash.Add(Field, StringComparer.Ordinal);
        hash.Add(Index);
        if (Element is { } element)
        {
            hash.AddBytes(element);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}

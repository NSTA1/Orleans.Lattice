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
}

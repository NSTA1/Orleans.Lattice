namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the non-atomic bulk-write RPC. Carries the target
/// <see cref="TreeId"/> and the <see cref="Upserts"/> to write, routed onto
/// <c>ILatticeDataApi.SetManyAsync</c>. Unlike the atomic batch there is no
/// idempotency key and no delete leg: the write is non-atomic and upsert-only.
/// </summary>
/// <remarks>
/// Deliberately <b>not</b> marked <c>[Immutable]</c>: it nests mutable
/// <see cref="DataEntry"/> value buffers that are unioned into the grain-bound
/// batch, so it must remain copy-eligible across the gRPC marshalling boundary.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataSetManyRequest)]
public sealed record DataSetManyRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The key / value pairs to write non-atomically. An empty list is a no-op.</summary>
    [Id(1)] public List<DataEntry> Upserts { get; init; } = [];
}

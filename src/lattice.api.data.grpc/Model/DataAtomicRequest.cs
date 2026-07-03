namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the single-tree atomic-batch RPC. Carries the target
/// <see cref="TreeId"/>, the <see cref="Batch"/> of upserts and deletes to
/// commit all-or-nothing, and the <see cref="OperationId"/> idempotency key
/// routed onto <c>ILatticeDataApi.SetManyAtomicAsync</c>.
/// </summary>
/// <remarks>
/// Deliberately <b>not</b> marked <c>[Immutable]</c>: it nests the mutable
/// <see cref="DataAtomicBatch"/> whose buffers are unioned into the grain-bound
/// atomic batch, so it must remain copy-eligible across the gRPC marshalling
/// boundary.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataAtomicRequest)]
public sealed record DataAtomicRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The upserts and deletes to commit atomically.</summary>
    [Id(1)] public DataAtomicBatch Batch { get; init; } = new();

    /// <summary>Stable idempotency key. Must be non-empty and must not contain <c>'/'</c>.</summary>
    [Id(2)] public required string OperationId { get; init; }
}

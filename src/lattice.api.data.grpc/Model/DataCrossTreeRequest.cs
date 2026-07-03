namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the cross-tree atomic-batch RPC. Carries the per-tree
/// <see cref="Batches"/> to commit all-or-nothing across every participating
/// tree and the required <see cref="OperationId"/> idempotency key routed onto
/// <c>ILatticeDataApi.SetManyAtomicCrossTreeAsync</c>.
/// </summary>
/// <remarks>
/// Deliberately <b>not</b> marked <c>[Immutable]</c>: it nests mutable
/// <see cref="DataTreeBatch"/> slices whose buffers are unioned into the
/// coordinator grain's batch, so it must remain copy-eligible across the gRPC
/// marshalling boundary.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataCrossTreeRequest)]
public sealed record DataCrossTreeRequest
{
    /// <summary>The per-tree slices to commit atomically. Tree ids must be distinct and non-empty.</summary>
    [Id(0)] public List<DataTreeBatch> Batches { get; init; } = [];

    /// <summary>Required cross-tree idempotency key. Must not contain <c>'/'</c>.</summary>
    [Id(1)] public required string OperationId { get; init; }
}

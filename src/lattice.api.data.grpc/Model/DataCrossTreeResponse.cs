namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire response for the cross-tree atomic-batch RPC. Reports the terminal
/// <see cref="Outcome"/>: <see cref="CrossTreeAtomicWriteOutcome.Committed"/>
/// when every tree's batch committed, or
/// <see cref="CrossTreeAtomicWriteOutcome.PreconditionFailed"/> when a guard
/// aborted the batch with nothing committed. A denied leg is not an outcome
/// value - it surfaces as a gRPC <c>PermissionDenied</c> status.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataCrossTreeResponse)]
[Immutable]
public sealed record DataCrossTreeResponse
{
    /// <summary>The terminal outcome of the cross-tree atomic write.</summary>
    [Id(0)] public CrossTreeAtomicWriteOutcome Outcome { get; init; }
}

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Wire request for the dead-letter-count RPC. A serializable mirror of the
/// <c>treeId</c> argument of
/// <see cref="ILatticeStateQuery.GetDeadLetterCountAsync"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcStateTypeAliases.DeadLetterCountRequest)]
[Immutable]
public sealed record DeadLetterCountRequest
{
    /// <summary>The governed tree whose dead-letter queue is counted.</summary>
    [Id(0)] public required string TreeId { get; init; }
}

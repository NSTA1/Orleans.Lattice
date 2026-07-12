namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Wire response for the dead-letter-count RPC. Carries the number of
/// strict-mode dead-letter entries retained for the requested tree (<c>0</c>
/// when the tree has none, schema enforcement is not registered, or the caller
/// may not read the tree).
/// </summary>
[GenerateSerializer]
[Alias(GrpcStateTypeAliases.DeadLetterCountResponse)]
[Immutable]
public sealed record DeadLetterCountResponse
{
    /// <summary>The governed tree that was counted.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The number of dead-letter entries retained for the tree.</summary>
    [Id(1)] public int Count { get; init; }
}

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Wire response for the tree-structure RPC. A serializable mirror of
/// <see cref="TreeStructureResult"/> (which is a transport-free plain record
/// on the facade) so the result can cross the gRPC boundary over the
/// Orleans-serialized, code-first contract.
/// </summary>
[GenerateSerializer]
[Alias(GrpcStateTypeAliases.StructureResponse)]
[Immutable]
public sealed record StructureResponse
{
    /// <summary>Lookup outcome mapped from the facade result.</summary>
    [Id(0)] public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    [Id(1)] public required string TreeId { get; init; }

    /// <summary>The root nodes of the response, in deterministic key-range order.</summary>
    [Id(2)] public IReadOnlyList<NodeStateSummary> Roots { get; init; } = Array.Empty<NodeStateSummary>();

    /// <summary>Whether the node-count budget was exhausted and some subtrees were truncated.</summary>
    [Id(3)] public bool Truncated { get; init; }
}

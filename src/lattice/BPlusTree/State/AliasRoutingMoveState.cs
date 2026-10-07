using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>Durable intent for a registry-owned bare alias routing move.</summary>
[GenerateSerializer]
[Alias(TypeAliases.AliasRoutingMoveState)]
[Immutable]
internal sealed record AliasRoutingMoveState
{
    /// <summary>The logical name being moved.</summary>
    [Id(0)] public required string TreeId { get; init; }
    /// <summary>The physical copy being left.</summary>
    [Id(1)] public required string Source { get; init; }
    /// <summary>The physical copy being entered.</summary>
    [Id(2)] public required string Destination { get; init; }
    /// <summary>The source copy's addressing map.</summary>
    [Id(3)] public required ShardMap SourceMap { get; init; }
    /// <summary>The destination copy's addressing map.</summary>
    [Id(4)] public required ShardMap DestinationMap { get; init; }
    /// <summary>The complete row before publication, or null for a new logical name.</summary>
    [Id(5)] public TreeRegistryEntry? Before { get; init; }
    /// <summary>The row to publish atomically with the destination map.</summary>
    [Id(6)] public required TreeRegistryEntry After { get; init; }
    /// <summary>The unique ownership token shared by the publication and source fences.</summary>
    [Id(7)] public required string OperationId { get; init; }
}

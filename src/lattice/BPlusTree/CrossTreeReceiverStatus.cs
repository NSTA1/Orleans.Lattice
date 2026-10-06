namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A read of one <see cref="ILatticeCrossTreeReceiverGrain"/> (issue #4684):
/// whether it has opened and decided, its identity, its frozen wait set, and
/// the trees that have arrived.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.CrossTreeReceiverStatus)]
internal sealed record CrossTreeReceiverStatus
{
    /// <summary><c>true</c> once the first terminal froze the wait set.</summary>
    [Id(0)] public required bool Opened { get; init; }

    /// <summary><c>true</c> once the decision is durable.</summary>
    [Id(1)] public required bool Decided { get; init; }

    /// <summary>The source cluster that authored the operation.</summary>
    [Id(2)] public required string OriginClusterId { get; init; }

    /// <summary>The cross-tree operation id.</summary>
    [Id(3)] public required string OperationId { get; init; }

    /// <summary>The frozen wait set.</summary>
    [Id(4)] public required IReadOnlyList<string> WaitSet { get; init; }

    /// <summary>The trees whose arrival is recorded.</summary>
    [Id(5)] public required IReadOnlyList<string> ArrivedTrees { get; init; }
}

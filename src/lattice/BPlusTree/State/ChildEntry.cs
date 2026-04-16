using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A single child entry in an internal node: the separator key and the grain id of the child.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ChildEntry)]
[Immutable]
internal sealed record ChildEntry
{
    /// <summary>
    /// The separator key. All keys strictly less than this value are routed to the
    /// <em>previous</em> child. This is <c>null</c> for the leftmost (catch-all) child.
    /// </summary>
    [Id(0)] public string? SeparatorKey { get; init; }

    /// <summary>The grain identity of the child node.</summary>
    [Id(1)] public required GrainId ChildId { get; init; }
}

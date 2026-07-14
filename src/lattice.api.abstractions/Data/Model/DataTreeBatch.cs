namespace Orleans.Lattice.Api.Data;

/// <summary>
/// One participating tree's slice of a cross-tree atomic write: the logical
/// <see cref="TreeId"/> to write into, the key / value <see cref="Upserts"/>,
/// and the <see cref="DeleteKeys"/> to commit atomically on that tree. Passed as
/// a list to the data API's cross-tree atomic surface, which flips visibility on
/// every participating tree at a single coordinator decision moment. Every
/// <see cref="TreeId"/> in the list must be distinct.
/// </summary>
/// <remarks>
/// Deliberately <b>not</b> marked <c>[Immutable]</c> for the same safe-copy
/// reason as <see cref="DataAtomicBatch"/> and the core
/// <c>LatticeTreeBatch</c>: its mutable buffers are unioned into the coordinator
/// grain's batch, so the type must remain copy-eligible.
/// </remarks>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataTreeBatch)]
public sealed record DataTreeBatch
{
    /// <summary>The logical tree this slice writes into. Must be non-empty and distinct within a batch.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The key / value pairs to write atomically on this tree. May be empty when the slice is delete-only.</summary>
    [Id(1)] public List<DataEntry> Upserts { get; init; } = [];

    /// <summary>The keys to delete atomically on this tree. May be empty when the slice is upsert-only.</summary>
    [Id(2)] public List<string> DeleteKeys { get; init; } = [];
}

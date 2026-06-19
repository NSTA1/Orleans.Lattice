
namespace Orleans.Lattice.Views;

/// <summary>
/// One participating view's recorded ready slice, held in the
/// <see cref="ViewCrossTreeCoordinatorState"/> until the wait set completes and
/// the coordinator issues the joint cross-tree flip. Carries the view's
/// active-generation tree id (resolved by the maintainer at staging time) and
/// the coalesced upsert entries to flip into that tree.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewCrossTreeSlice)]
internal sealed record ViewCrossTreeSlice
{
    /// <summary>The participating view's logical name.</summary>
    [Id(0)] public required string ViewName { get; init; }

    /// <summary>The view's active-generation tree id to flip into.</summary>
    [Id(1)] public required string ViewTreeId { get; init; }

    /// <summary>The coalesced upsert entries comprising this view's slice.</summary>
    [Id(2)] public required List<KeyValuePair<string, byte[]>> Upserts { get; init; }

    /// <summary>
    /// The retraction (tombstone) deletes comprising this view's slice. Carried
    /// inside the joint flip alongside <see cref="Upserts"/> so a re-key
    /// projection's old-key delete and new-key upsert flip atomically. Empty
    /// when the slice has no retractions.
    /// </summary>
    [Id(3)] public IReadOnlyList<string> Deletes { get; init; } = [];
}

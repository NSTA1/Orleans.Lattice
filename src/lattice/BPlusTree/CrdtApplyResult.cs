using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result returned by <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.ApplyCrdtDeltaAsync"/>
/// carrying the <see cref="HybridLogicalClock"/> stamped on the
/// committed entry plus an optional <see cref="SplitResult"/> when the
/// post-merge state-row write caused the leaf to exceed its sizing pin
/// and split. CRDT delta applies do not surface a Success flag because
/// delta merges are unconditional and convergent, so the apply always
/// completes; the returned version is the per-key HLC observable
/// through the legacy read path.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrdtApplyResult)]
[Immutable]
internal sealed record CrdtApplyResult
{
    /// <summary>The <see cref="HybridLogicalClock"/> stamped on the post-merge entry.</summary>
    [Id(0)] public HybridLogicalClock Version { get; init; }

    /// <summary>A split result if the apply caused the leaf to split, otherwise <c>null</c>.</summary>
    [Id(1)] public SplitResult? Split { get; init; }
}

using System.ComponentModel;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result returned by <see cref="IBPlusLeafGrain.SetIfVersionAsync"/> containing
/// whether the compare-and-swap succeeded, the current version of the entry,
/// and an optional <see cref="SplitResult"/> when a write triggered a leaf split.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CasResult)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record CasResult
{
    /// <summary>
    /// <c>true</c> if the write was applied because the expected version matched;
    /// <c>false</c> if the entry's current version did not match and the write was rejected.
    /// </summary>
    [Id(0)] public bool Success { get; init; }

    /// <summary>
    /// The current <see cref="HybridLogicalClock"/> version of the entry after the
    /// operation. When <see cref="Success"/> is <c>false</c>, this is the version
    /// that the caller should use for a retry.
    /// </summary>
    [Id(1)] public HybridLogicalClock CurrentVersion { get; init; }

    /// <summary>
    /// A split result if the write caused the leaf to split, otherwise <c>null</c>.
    /// Always <c>null</c> when <see cref="Success"/> is <c>false</c> (no write occurred).
    /// </summary>
    [Id(2)] public SplitResult? Split { get; init; }
}
using System.ComponentModel;
using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result returned by <see cref=""IBPlusLeafGrain.GetOrSetAsync""/> containing
/// the existing value (if the key was already live) and an optional
/// <see cref=""SplitResult""/> when a write triggered a leaf split.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.GetOrSetResult)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record GetOrSetResult
{
    /// <summary>
    /// The existing value if the key was already live, or <c>null</c> if the
    /// value was newly written.
    /// </summary>
    [Id(0)] public byte[]? ExistingValue { get; init; }

    /// <summary>
    /// A split result if the write caused the leaf to split, otherwise <c>null</c>.
    /// Always <c>null</c> when <see cref=""ExistingValue""/> is not <c>null</c>
    /// (no write occurred).
    /// </summary>
    [Id(1)] public SplitResult? Split { get; init; }
}

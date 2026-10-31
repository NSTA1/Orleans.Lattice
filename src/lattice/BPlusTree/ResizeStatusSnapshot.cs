namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The whole status read of a resize coordinator in one value, returned by
/// <see cref="ITreeResizeGrain.GetStatusAsync"/> so a status projection costs
/// one grain call instead of three: <see cref="ITreeResizeGrain.IsIdleAsync"/>,
/// <see cref="ITreeResizeGrain.GetUndoProgressAsync"/> and
/// <see cref="ITreeResizeGrain.GetProgressAsync"/>, each read exactly as those
/// methods read it.
/// </summary>
/// <param name="Idle">What <see cref="ITreeResizeGrain.IsIdleAsync"/> reports.</param>
/// <param name="UndoPending">
/// <see cref="ResizeUndoProgress.Pending"/> from
/// <see cref="ITreeResizeGrain.GetUndoProgressAsync"/>.
/// </param>
/// <param name="Progress">What <see cref="ITreeResizeGrain.GetProgressAsync"/> reports.</param>
[GenerateSerializer]
[Alias(TypeAliases.ResizeStatusSnapshot)]
[Immutable]
internal readonly record struct ResizeStatusSnapshot(
    [property: Id(0)] bool Idle,
    [property: Id(1)] bool UndoPending,
    [property: Id(2)] ResizeProgress Progress);

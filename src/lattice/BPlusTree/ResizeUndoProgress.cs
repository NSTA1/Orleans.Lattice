namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A point-in-time view of a resize coordinator's undo, returned by
/// <see cref="ITreeResizeGrain.GetUndoProgressAsync"/>.
/// </summary>
/// <param name="Pending">
/// <see langword="true"/> while an accepted undo has not yet finished unwinding.
/// </param>
/// <param name="FailedOperationId">
/// The operation id of a resize whose accepted undo could not be applied and was
/// withdrawn, or <see langword="null"/>.
/// </param>
/// <param name="FailureMessage">The reason that undo was withdrawn, or <see langword="null"/>.</param>
[GenerateSerializer]
[Alias(TypeAliases.ResizeUndoProgress)]
[Immutable]
internal readonly record struct ResizeUndoProgress(
    [property: Id(0)] bool Pending,
    [property: Id(1)] string? FailedOperationId,
    [property: Id(2)] string? FailureMessage);

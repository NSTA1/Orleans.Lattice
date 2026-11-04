namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The surface of the arm and of the per-leaf author attribution (#5833), without their
/// behaviour: the red proof's stand-in, replaced by the implementation.
/// </summary>
internal sealed partial class WalShardGrain
{
    /// <inheritdoc />
    public Task<WalAppendFenceArmResult> ArmAppendEpochAsync(
        Guid leafId,
        long epoch,
        Guid activation,
        WalRoutingInitiator deferredInitiator,
        long deferredGeneration,
        CancellationToken cancellationToken) =>
        throw new NotSupportedException("#5833: the arm is not implemented in the red proof.");

    /// <summary>The leaf-author table's size; test seam.</summary>
    internal int LeafAuthorCountForTesting => 0;

    /// <summary>The foreign high of a leaf for an activation; test seam.</summary>
    internal long ForeignHighForTesting(Guid leafId, Guid activation) => -1;

    /// <summary>Folds the leaf-author table; test seam.</summary>
    internal void FoldLeafAuthorsForTesting()
    {
    }
}

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The result of <see cref="IWalShardGrain.AppendStampedAsync"/> (epic #5605).
/// </summary>
/// <param name="Offset">The offset the entry was allocated, or <see cref="WalStampedBatchResult.Rejected"/> when it was not.</param>
/// <param name="UnstampedWatermark">
/// The highest offset the partition has allocated to an unstamped append, read in
/// the same locked step that allocated <paramref name="Offset"/>; <c>-1</c> when
/// none.
/// </param>
/// <param name="Outcome">Whether the entry was admitted, and if not why.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalStampedAppendResult)]
internal readonly record struct WalStampedAppendResult(
    [property: Id(0)] long Offset,
    [property: Id(1)] long UnstampedWatermark,
    [property: Id(2)] WalAppendFenceOutcome Outcome);

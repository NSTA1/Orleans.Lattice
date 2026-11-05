namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// One WAL partition's consistent cut for a splitting leaf (#5837, D4): the readable head
/// and every bound the leaf must have read past, read together in one turn of the shard.
/// A leaf whose applied frontier is at or above every bound has applied every record
/// below the head it does not own, so the head, not its own frontier, may be the split's
/// read position at the partition.
/// </summary>
/// <param name="Head">The readable head (issue #4621): no write can still land below it.</param>
/// <param name="ForeignHigh">
/// The highest offset another author may have allocated for the leaf, and the stale high
/// of every donor close the reader carried, never below the unattributed floor.
/// </param>
/// <param name="UnstampedWatermark">The unstamped high for the reader's key range: the floor, and every tabled unstamped record it owns (#5851, K1-K6).</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalSplitCut)]
internal readonly record struct WalSplitCut(
    [property: Id(0)] long Head,
    [property: Id(1)] long ForeignHigh,
    [property: Id(2)] long UnstampedWatermark);

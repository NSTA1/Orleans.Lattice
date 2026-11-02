namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The fence stamp a stamped WAL append carries (epic #5605): the leaf that
/// authored the append, the append epoch its activation claimed, and optionally a
/// shard-level initiator's routing generation. The stamp travels as a call
/// argument and is never part of the stored <see cref="WalRecord"/>, so the WAL
/// format is unchanged.
/// </summary>
/// <param name="LeafId">The authoring leaf's grain key.</param>
/// <param name="Epoch">The append epoch the leaf's activation claimed at the partition.</param>
/// <param name="Initiator">The shard-level initiator whose generation the write was admitted under, or <see cref="WalRoutingInitiator.None"/>.</param>
/// <param name="Generation">That initiator's routing generation; ignored when <paramref name="Initiator"/> is not set.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalAppendStamp)]
internal readonly record struct WalAppendStamp(
    [property: Id(0)] Guid LeafId,
    [property: Id(1)] long Epoch,
    [property: Id(2)] WalRoutingInitiator Initiator,
    [property: Id(3)] long Generation)
{
    /// <summary>A stamp carrying only a leaf's epoch.</summary>
    public static WalAppendStamp ForLeaf(Guid leafId, long epoch) => new(leafId, epoch, WalRoutingInitiator.None, 0);
}

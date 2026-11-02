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

    /// <summary>
    /// The coordinator-terminal stamp class (epic #5605, member 2; design note section 2,
    /// "Coordinator terminals"): carried by a saga terminal a coordinator appends,
    /// not a leaf. The WAL shard admits it without an entry check, and excludes it
    /// from the unstamped watermark only for a <c>TxCommit</c> or <c>TxAbort</c>
    /// record (<see cref="WalAppendFenceCore.IsCoordinatorTerminalOp"/>); any other
    /// record sent with it is admitted unstamped and reported, so the class can never
    /// bypass a fence or the watermark.
    /// </summary>
    public static WalAppendStamp CoordinatorTerminal =>
        new(Guid.Empty, 0, new WalRoutingInitiator(WalRoutingInitiatorKind.CoordinatorTerminal, 0), 0);

    /// <summary><see langword="true"/> for the <see cref="CoordinatorTerminal"/> stamp class.</summary>
    public bool IsCoordinatorTerminal => Initiator.Kind == WalRoutingInitiatorKind.CoordinatorTerminal;
}

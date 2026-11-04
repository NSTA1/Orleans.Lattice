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

    /// <summary>
    /// The bulk-seed stamp class (#5812; design note section 2, the census row "Bulk
    /// seeding"): carried by the rows a bulk operation seeds into a leaf it has not yet
    /// published, under the shard root's open bulk switch. It names no leaf, so the
    /// seeding leaf claims no append epoch, and it carries the shard root's routing
    /// generation captured after the switch's marker opened. The WAL shard admits it
    /// only on a positive match against that generation as it has loaded it, never on
    /// an absent entry, and does not report it in the unstamped watermark: every leaf
    /// that can own a seeded row is held on the switch's close heads, and every close
    /// re-claims the generation at every partition, which refuses a later seed.
    /// </summary>
    /// <param name="initiator">The shard root's initiator.</param>
    /// <param name="generation">Its routing generation, captured after the switch opened.</param>
    public static WalAppendStamp BulkSeed(WalRoutingInitiator initiator, long generation)
    {
        if (initiator.Kind != WalRoutingInitiatorKind.ShardRoot)
        {
            throw new ArgumentException("A bulk seed is stamped with a shard root's routing generation.", nameof(initiator));
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generation);
        return new(Guid.Empty, 0, initiator, generation);
    }

    /// <summary><see langword="true"/> for the <see cref="BulkSeed"/> stamp class: no leaf, and a shard root's generation.</summary>
    public bool IsBulkSeed => LeafId == Guid.Empty && Initiator.Kind == WalRoutingInitiatorKind.ShardRoot;
}

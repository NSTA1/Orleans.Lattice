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
/// <param name="Activation">
/// The authoring leaf activation's token (#5833, revision 2, B1): a random value per
/// leaf activation, never persisted. The WAL shard attributes each leaf-authored append
/// to it, so an arm can tell its own appends from every other activation's. Empty from
/// a build that does not send it, which counts as foreign to every claimant.
/// </param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalAppendStamp)]
internal readonly record struct WalAppendStamp(
    [property: Id(0)] Guid LeafId,
    [property: Id(1)] long Epoch,
    [property: Id(2)] WalRoutingInitiator Initiator,
    [property: Id(3)] long Generation,
    [property: Id(4)] Guid Activation = default)
{
    /// <summary>A stamp carrying only a leaf's epoch.</summary>
    public static WalAppendStamp ForLeaf(Guid leafId, long epoch, Guid activation = default) =>
        new(leafId, epoch, WalRoutingInitiator.None, 0, activation);

    /// <summary>
    /// The pre-arm stamp class (#5833, revision 2): carried by a leaf activation that has
    /// not armed a live advance at the partition, so it claims no append epoch and writes
    /// no fence row. It names the leaf and the activation's token, and is sent only with a
    /// routing generation (<see cref="WalRoutingStamp.Apply"/>), which the WAL shard checks
    /// as for a stamped append; no entry is checked. The shard attributes the record to
    /// the activation, and does not raise the shared unstamped watermark for it. One sent
    /// without a routing generation is admitted unstamped and reported (C4).
    /// </summary>
    /// <param name="leafId">The authoring leaf.</param>
    /// <param name="activation">The activation's token; never empty.</param>
    public static WalAppendStamp PreArm(Guid leafId, Guid activation)
    {
        if (leafId == Guid.Empty || activation == Guid.Empty)
        {
            throw new ArgumentException("A pre-arm stamp names a leaf and its activation's token.");
        }

        return new(leafId, 0, WalRoutingInitiator.None, 0, activation);
    }

    /// <summary><see langword="true"/> for the <see cref="PreArm"/> stamp class: a leaf, no epoch, an activation token.</summary>
    public bool IsPreArm => Epoch == 0 && LeafId != Guid.Empty && Activation != Guid.Empty;

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

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>The kind of a <see cref="WalRoutingInitiator"/> (epic #5605).</summary>
[GenerateSerializer]
[Alias(TypeAliases.WalRoutingInitiatorKind)]
internal enum WalRoutingInitiatorKind : byte
{
    /// <summary>No initiator.</summary>
    None = 0,

    /// <summary>A shard root's routing generation.</summary>
    ShardRoot = 1,

    /// <summary>The tree's range-delete generation.</summary>
    TreeRangeDelete = 2,

    /// <summary>
    /// Not an initiator: on a <see cref="WalRoutingStamp"/> handed to a leaf's writer,
    /// it forces the append through the existing, unstamped WAL method, which the
    /// leaf reports to itself (member 2, S3: a write that reached the leaf through an
    /// existing method). Never sent to a WAL shard.
    /// </summary>
    ForceUnstamped = 4,

    /// <summary>
    /// Not a generation: the coordinator-terminal stamp class (epic #5605, member 2),
    /// carried by a saga coordinator's <c>TxCommit</c> or <c>TxAbort</c> record. The
    /// WAL shard admits it with no entry check and does not raise its unstamped
    /// watermark for it; a record of any other kind sent with it is admitted
    /// unstamped and reported. See <see cref="WalAppendStamp.CoordinatorTerminal"/>.
    /// </summary>
    CoordinatorTerminal = 3,
}

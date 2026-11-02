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
}

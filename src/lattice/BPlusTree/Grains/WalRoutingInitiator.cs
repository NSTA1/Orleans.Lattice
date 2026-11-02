namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The shard-level initiator a WAL routing generation belongs to (epic #5605): a
/// shard root, or a tree's range-delete fence. <see cref="WalRoutingInitiatorKind.None"/>
/// marks a stamp that carries no routing generation, which is every stamp member 1
/// issues: only the hand-off fences of a later member stamp a generation.
/// </summary>
/// <param name="Kind">Which initiator the generation belongs to.</param>
/// <param name="Index">The shard root's shard index; 0 for the tree's range-delete fence.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalRoutingInitiator)]
internal readonly record struct WalRoutingInitiator(
    [property: Id(0)] WalRoutingInitiatorKind Kind,
    [property: Id(1)] int Index)
{
    /// <summary>No initiator: the stamp carries no routing generation.</summary>
    public static WalRoutingInitiator None => default;

    /// <summary>The initiator for shard root <paramref name="shardIndex"/>.</summary>
    public static WalRoutingInitiator ShardRoot(int shardIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardIndex);
        return new(WalRoutingInitiatorKind.ShardRoot, shardIndex);
    }

    /// <summary>The initiator for the tree's range-delete fence.</summary>
    public static WalRoutingInitiator TreeRangeDelete => new(WalRoutingInitiatorKind.TreeRangeDelete, 0);

    /// <summary><see langword="true"/> when the stamp names an initiator.</summary>
    public bool IsSet => Kind != WalRoutingInitiatorKind.None;
}

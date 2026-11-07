namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Capability marker (#5833, revision 2): a silo whose build advertises this grain
/// interface serves the pre-arm stamp class (<see cref="WalAppendStamp.PreArm"/>) on
/// <see cref="IWalShardGrain"/>'s stamped append methods, and the arm. While every
/// active silo advertises it, a leaf defers its append-fence rows until it arms a live
/// advance at a partition; until then, and again if a silo without it joins, leaves
/// claim eagerly as before, so no shard ever receives a pre-arm stamp it would read as
/// an unloaded entry. Implemented by <see cref="WalShardGrain"/>; it declares no methods.
/// </summary>
[Alias(TypeAliases.IWalAppendFencePreArmCapable)]
internal interface IWalAppendFencePreArmCapable : IGrainWithStringKey
{
}

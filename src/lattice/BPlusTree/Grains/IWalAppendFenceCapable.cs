namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Capability marker (epic #5605): a silo whose build advertises this grain
/// interface in its grain manifest serves the WAL append fence - the epoch and
/// routing-generation claims and the stamped append methods of
/// <see cref="IWalShardGrain"/> - and its leaves claim epochs and stamp their
/// appends. <see cref="IWalAppendFenceGate"/> lets a silo call those methods only
/// while every active silo advertises it, so no silo running an older build ever
/// receives a method it does not know. Implemented by <see cref="WalShardGrain"/>
/// so it is always part of a capable build's manifest; it declares no methods.
/// </summary>
[Alias(TypeAliases.IWalAppendFenceCapable)]
internal interface IWalAppendFenceCapable : IGrainWithStringKey
{
}

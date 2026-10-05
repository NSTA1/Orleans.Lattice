namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Capability marker (issue #4586): a silo whose build advertises this grain
/// interface in its grain manifest enforces and handles the WAL clock floor - its
/// <see cref="IWalShardGrain"/> refuses a write stamped below a published floor
/// and its producers re-stamp a refused write. <see cref="IWalClockFloorGate"/>
/// lets a partition start advancing its floor only once every active silo
/// advertises it, so no silo running an older build ever meets a refusal it
/// cannot handle. Implemented by <see cref="WalShardGrain"/> so it is always part
/// of a capable build's manifest; it declares no methods.
/// </summary>
[Alias(TypeAliases.IWalClockFloorCapable)]
internal interface IWalClockFloorCapable : IGrainWithStringKey
{
}

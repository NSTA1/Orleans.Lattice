namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The grain types whose calls <see cref="SiloStopRoutingLatch"/> can stop
/// sending once the type is proven withdrawn from the cluster during a silo stop
/// (issue #5100). Values are distinct bits so the latch holds them in one word.
/// </summary>
[Flags]
internal enum StopRoutedGrainType
{
    /// <summary>No grain type.</summary>
    None = 0,

    /// <summary><see cref="IWalMaterialiserPinGrain"/>.</summary>
    MaterialiserPin = 1,

    /// <summary><see cref="ILeafReplayCoordinatorGrain"/>.</summary>
    ReplayCoordinator = 2,
}

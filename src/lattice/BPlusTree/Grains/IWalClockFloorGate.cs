namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Decides whether a WAL partition may advance its clock floor (issue #4586).
/// A floor that only some silos understand would refuse writes from a silo whose
/// build cannot re-stamp them, so it advances only while every active silo is
/// floor-capable. A floor already published stays enforced whatever the gate
/// says - it is durable, and the low watermark a receiver already holds relies
/// on it - so a closed gate only stops it moving.
/// </summary>
internal interface IWalClockFloorGate
{
    /// <summary>
    /// <see langword="true"/> while every active silo in the cluster advertises
    /// <see cref="IWalClockFloorCapable"/>.
    /// </summary>
    bool IsOpen { get; }
}

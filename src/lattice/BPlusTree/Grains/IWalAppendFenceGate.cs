namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Decides whether this silo may claim append epochs and stamp WAL appends
/// (epic #5605). Open only while every active silo advertises
/// <see cref="IWalAppendFenceCapable"/>; it is not ratcheted, so it closes again
/// by itself if a silo running an older build joins. While it is closed a leaf
/// appends through the existing, unstamped methods, which the WAL shard always
/// admits and reports.
/// </summary>
internal interface IWalAppendFenceGate
{
    /// <summary>
    /// <see langword="true"/> while every active silo in the cluster advertises
    /// <see cref="IWalAppendFenceCapable"/>.
    /// </summary>
    bool IsOpen { get; }

    /// <summary>
    /// <see langword="true"/> while <see cref="IsOpen"/> and every active silo also
    /// advertises <see cref="IWalAppendFencePreArmCapable"/> (#5833): a leaf may then
    /// append under the pre-arm class and claim only when it arms. Closes again if a
    /// silo without it joins, when leaves claim eagerly once more.
    /// </summary>
    bool IsPreArmOpen => false;
}

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The three non-structural bounds that govern a single shard range-scan page
/// fill, resolved together so the shard root can arm them - synchronously, with
/// no registry round trip - as the very first statement of the grain call.
/// <para>
/// Resolving them without an <c>await</c> is the point: the clock these bounds
/// start has to cover everything the call does, and an <c>await</c> to fetch
/// them would put that fetch outside the window it is meant to bound.
/// </para>
/// </summary>
/// <param name="MaxLeaves">
/// Effective <see cref="LatticeOptions.MaxLeavesPerScanPage"/>. Zero or less
/// disables the leaf-count bound.
/// </param>
/// <param name="MaxDuration">
/// Effective <see cref="LatticeOptions.MaxScanPageDuration"/>, the cooperative
/// wall-clock budget sampled between leaf reads.
/// <see cref="TimeSpan.Zero"/> disables it.
/// </param>
/// <param name="StallDuration">
/// Effective <see cref="LatticeOptions.MaxScanPageStallDuration"/>, the hard
/// end-to-end ceiling on the whole grain call.
/// <see cref="Timeout.InfiniteTimeSpan"/> disables it.
/// </param>
internal readonly record struct ScanPageBounds(
    int MaxLeaves,
    TimeSpan MaxDuration,
    TimeSpan StallDuration)
{
    /// <summary>
    /// <see langword="true"/> when the hard stall ceiling is armed, so a page
    /// fill that never returns is faulted instead of holding its shard.
    /// </summary>
    public bool IsStallGuarded => StallDuration != Timeout.InfiniteTimeSpan;
}

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Internal seam reporting whether any adaptive shard split is currently in
/// flight anywhere in the cluster. The scale-in safety gate consults this: a
/// split in flight suppresses scale-in, because relocating load off a silo while
/// a shard is mid-split risks stranding the split's in-flight work.
/// <para>
/// The default implementation (<see cref="NoOpSplitActivityProbe"/>) reports no
/// splits in flight; a host that wires a real split-activity source (for example
/// one backed by the core cluster split-concurrency gate) can replace the
/// registration to make the gate split-aware.
/// </para>
/// </summary>
internal interface ISplitActivityProbe
{
    /// <summary>
    /// Returns <see langword="true"/> when at least one adaptive shard split is
    /// currently in flight in the cluster; otherwise <see langword="false"/>.
    /// Must be cheap and synchronous - it is read once per sample tick.
    /// </summary>
    /// <returns>Whether any shard split is in flight cluster-wide.</returns>
    bool AnySplitInFlight();
}

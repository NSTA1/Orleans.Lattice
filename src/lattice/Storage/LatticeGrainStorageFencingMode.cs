namespace Orleans.Lattice;

/// <summary>
/// What a silo does when its start-up probe shows that the grain storage
/// provider registered by <c>AddLattice</c> does not enforce ETags on write.
/// See <see cref="LatticeGrainStorageFencingOptions"/>.
/// </summary>
public enum LatticeGrainStorageFencingMode
{
    /// <summary>
    /// Run the probe and log a warning when the provider accepts a write that
    /// carries a stale ETag. The silo still starts. This is the default.
    /// </summary>
    Warn = 0,

    /// <summary>
    /// Run the probe and fail silo start when the provider accepts a write
    /// that carries a stale ETag. A probe that cannot reach a verdict (for
    /// example a transient storage fault) still only warns.
    /// </summary>
    Reject = 1,

    /// <summary>
    /// Do not run the probe. The silo performs no probe writes and logs only
    /// that the check is disabled.
    /// </summary>
    Disabled = 2,
}

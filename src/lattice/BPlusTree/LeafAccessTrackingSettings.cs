namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The two non-structural settings that drive a shard root's leaf-access
/// tracking and post-restart leaf-cache pre-warm, resolved together so the
/// shard root can decide - synchronously, with no registry round trip - whether
/// the feature is active at all.
/// </summary>
/// <param name="PreWarmCount">
/// Effective <see cref="LatticeOptions.LeafCachePreWarmCount"/>, clamped to
/// <see cref="LatticeOptions.MaxLeafCachePreWarmCount"/>. Zero means the feature
/// is off and no access is tracked.
/// </param>
/// <param name="FlushIntervalMs">
/// Effective <see cref="LatticeOptions.LeafAccessModelFlushIntervalMs"/>. Zero
/// means the model is persisted only on clean deactivation.
/// </param>
internal readonly record struct LeafAccessTrackingSettings(int PreWarmCount, int FlushIntervalMs)
{
    /// <summary>The disabled settings - no tracking, no pre-warm, no flush timer.</summary>
    public static LeafAccessTrackingSettings Disabled { get; } = new(0, 0);

    /// <summary><see langword="true"/> when leaf-access tracking and pre-warm are active.</summary>
    public bool IsEnabled => PreWarmCount > 0;
}

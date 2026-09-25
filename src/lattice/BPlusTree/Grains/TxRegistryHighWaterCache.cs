using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo, per-tree cache of the durable saga decision registry shard
/// high-water mark (issue #3501). Scoped to an <see cref="IGrainFactory"/>
/// instance, so two silos (or two test clusters) in one process never share an
/// entry.
/// <para>
/// The cache is only a starting point for a tree-wide read's key set, never a
/// substitute for reading the mark: <see cref="TxRegistryFanOut"/> always reads
/// the durable mark alongside its fan-out and widens when the mark exceeds the
/// cached value. An entry only ever grows, and only to a value the durable mark
/// has reported, so a stale entry can make a read cover too few keys for one
/// round (which the read then repairs) but never too many for the tree.
/// </para>
/// </summary>
internal static class TxRegistryHighWaterCache
{
    private static readonly ConditionalWeakTable<IGrainFactory, ConcurrentDictionary<string, int>> Caches = new();

    /// <summary>
    /// Returns the cached high-water mark for <paramref name="treeId"/>, or zero
    /// (the legacy registry only) when nothing has been observed yet.
    /// </summary>
    /// <param name="grainFactory">The silo's grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <returns>The cached mark, at least zero.</returns>
    public static int Get(IGrainFactory grainFactory, string treeId) =>
        Caches.TryGetValue(grainFactory, out var map) && map.TryGetValue(treeId, out var mark) ? mark : 0;

    /// <summary>
    /// Records an observed durable mark for <paramref name="treeId"/>, keeping
    /// the larger of it and the cached value.
    /// </summary>
    /// <param name="grainFactory">The silo's grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardHighWater">The observed durable mark.</param>
    /// <returns>The cached mark after the call.</returns>
    public static int Observe(IGrainFactory grainFactory, string treeId, int shardHighWater)
    {
        var mark = Math.Clamp(shardHighWater, 0, LatticeOptions.MaxTxRegistryShardCount);
        var map = Caches.GetValue(grainFactory, static _ => new ConcurrentDictionary<string, int>(StringComparer.Ordinal));
        return map.AddOrUpdate(
            treeId,
            static (_, observed) => observed,
            static (_, current, observed) => Math.Max(current, observed),
            mark);
    }
}

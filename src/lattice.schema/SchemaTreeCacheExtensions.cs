using System.Collections.Concurrent;

namespace Orleans.Lattice.Schema;

/// <summary>Shared admission and invalidation rules for per-tree schema caches.</summary>
internal static class SchemaTreeCacheExtensions
{
    // Unknown trees cache null sentinels too, so caller-supplied tree ids must
    // not grow the cache indefinitely. Refusing admission only costs a reload.
    internal const int MaxCachedTrees = 4096;

    internal static void CacheIfSpaceAvailable<T>(
        this ConcurrentDictionary<string, T> cache, string treeId, T value)
    {
        // Preserve last-writer-wins concurrent loads and allow refreshes at the
        // cap. This is the existing best-effort bound, not an eviction policy.
        if (cache.Count < MaxCachedTrees || cache.ContainsKey(treeId))
        {
            cache[treeId] = value;
        }
    }

    internal static void InvalidateForMutation<T>(
        this ConcurrentDictionary<string, T> cache, LatticeMutation mutation, string sourceTree)
    {
        // Backing-tree entries are keyed by the governed tree id.
        if (string.Equals(mutation.TreeId, sourceTree, StringComparison.Ordinal)
            && !string.IsNullOrEmpty(mutation.Key))
        {
            cache.TryRemove(mutation.Key, out _);
        }
    }
}

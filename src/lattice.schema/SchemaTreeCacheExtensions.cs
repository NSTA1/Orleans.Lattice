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

    /// <summary>
    /// Installs a loaded value unless an invalidation of the tree overtook the load,
    /// so an overtaken load never re-caches the pre-change answer (#5247).
    /// </summary>
    internal static void CacheIfCurrent<T>(
        this ConcurrentDictionary<string, T> cache,
        SchemaTreeCacheGenerations generations,
        string treeId,
        long captured,
        T value)
    {
        if (!generations.IsCurrent(treeId, captured))
        {
            return;
        }

        cache.CacheIfSpaceAvailable(treeId, value);

        // An invalidation can advance the generation between the check and the
        // write, and its removal then finds nothing; withdraw what this load wrote.
        if (!generations.IsCurrent(treeId, captured))
        {
            cache.TryRemove(new KeyValuePair<string, T>(treeId, value));
        }
    }

    /// <summary>
    /// Evicts a tree's entry and overtakes any load of it still in flight. The
    /// generation advances before the removal, so a load either sees the advance
    /// before it writes or writes before the removal runs.
    /// </summary>
    internal static void Invalidate<T>(
        this ConcurrentDictionary<string, T> cache, SchemaTreeCacheGenerations generations, string treeId)
    {
        generations.Advance(treeId);
        cache.TryRemove(treeId, out _);
    }

    internal static void InvalidateForMutation<T>(
        this ConcurrentDictionary<string, T> cache,
        SchemaTreeCacheGenerations generations,
        LatticeMutation mutation,
        string sourceTree)
    {
        // Backing-tree entries are keyed by the governed tree id.
        if (string.Equals(mutation.TreeId, sourceTree, StringComparison.Ordinal)
            && !string.IsNullOrEmpty(mutation.Key))
        {
            cache.Invalidate(generations, mutation.Key);
        }
    }
}

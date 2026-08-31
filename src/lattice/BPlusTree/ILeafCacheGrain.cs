using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A read-through cache for leaf data. Keyed by the <see cref="GrainId"/> string
/// of the backing <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/>.
/// Multiple activations may exist across silos (stateless worker).
/// </summary>
[Alias(TypeAliases.ILeafCacheGrain)]
internal interface ILeafCacheGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the value for <paramref name="key"/> from the local cache,
    /// falling through to the primary leaf grain on a miss.
    /// </summary>
    Task<byte[]?> GetAsync(string key);

    /// <summary>
    /// Returns <c>true</c> if <paramref name="key"/> exists and is not tombstoned,
    /// without transferring the value bytes.
    /// </summary>
    Task<bool> ExistsAsync(string key);

    /// <summary>
    /// Returns the values for the given <paramref name="keys"/> from the local cache,
    /// falling through to the primary leaf grain on a miss.
    /// Keys that do not exist or are tombstoned are omitted from the result.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys);

    /// <summary>
    /// Activates this cache on the calling silo and populates it from the
    /// primary leaf, without returning any payload to the caller.
    /// <para>
    /// Used by <see cref="IShardRootGrain.WarmUpAsync"/> to pay a cold leaf's
    /// activation and first-refresh cost up front, off the critical path of the
    /// first real read. Because the shard root is the only caller of this
    /// stateless-worker cache, the activation it creates is local to the silo
    /// that will serve the subsequent reads.
    /// </para>
    /// </summary>
    Task PreWarmAsync();
}

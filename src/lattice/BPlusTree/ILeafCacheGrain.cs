using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A read-through cache for leaf data. Keyed by the <see cref="GrainId"/> string
/// of the backing <see cref="IBPlusLeafGrain"/>.
/// Multiple activations may exist across silos (stateless worker).
/// </summary>
public interface ILeafCacheGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the value for <paramref name="key"/> from the local cache,
    /// falling through to the primary leaf grain on a miss.
    /// </summary>
    Task<byte[]?> GetAsync(string key);
}

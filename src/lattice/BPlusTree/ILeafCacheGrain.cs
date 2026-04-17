using System.ComponentModel;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A read-through cache for leaf data. Keyed by the <see cref="GrainId"/> string
/// of the backing <see cref="IBPlusLeafGrain"/>.
/// Multiple activations may exist across silos (stateless worker).
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface ILeafCacheGrain : IGrainWithStringKey
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
}

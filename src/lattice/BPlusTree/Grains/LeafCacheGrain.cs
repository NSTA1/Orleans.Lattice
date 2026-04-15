using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A <see cref="StatelessWorkerAttribute"/>-based read-through cache that sits
/// in front of a <see cref="BPlusLeafGrain"/>. Each silo may have its own
/// activation, serving reads from a local LWW-map cache.
///
/// On a cache miss or when the cache is stale, the grain fetches a
/// <see cref="StateDelta"/> from the primary leaf and merges it into the
/// local cache using <see cref="LwwValue{T}.Merge"/>. Because the merge is
/// commutative and idempotent, stale entries are harmlessly overwritten
/// without an explicit invalidation protocol.
/// </summary>
[StatelessWorker]
internal sealed class LeafCacheGrain(IGrainContext context, IGrainFactory grainFactory) : ILeafCacheGrain
{
    private readonly Dictionary<string, LwwValue<byte[]>> _cache = new(StringComparer.Ordinal);
    private VersionVector _version = new();

    /// <summary>
    /// The <see cref="GrainId"/> string of the primary leaf grain this cache
    /// is associated with. Parsed from the grain's own string key.
    /// </summary>
    private GrainId PrimaryLeafId => GrainId.Parse(context.GrainId.Key.ToString()!);

    public async Task<byte[]?> GetAsync(string key)
    {
        // Always pull a delta from the primary. The VersionVector comparison
        // makes this cheap — if nothing changed, the primary returns an empty
        // delta without scanning entries.
        await RefreshAsync();

        if (_cache.TryGetValue(key, out var cached) && !cached.IsTombstone)
        {
            return cached.Value;
        }

        return null;
    }

    private async Task RefreshAsync()
    {
        var primaryLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(PrimaryLeafId);
        var delta = await primaryLeaf.GetDeltaSinceAsync(_version);

        if (delta.IsEmpty)
            return;

        // Merge each entry using LWW semantics.
        foreach (var (key, lww) in delta.Entries)
        {
            if (_cache.TryGetValue(key, out var existing))
            {
                _cache[key] = LwwValue<byte[]>.Merge(existing, lww);
            }
            else
            {
                _cache[key] = lww;
            }
        }

        // Advance our version vector to reflect what we've received.
        _version = VersionVector.Merge(_version, delta.Version);
    }
}

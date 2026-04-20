using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Microsoft.Extensions.Options;

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
///
/// When <see cref="LatticeOptions.CacheTtl"/> is non-zero, the cache skips
/// the delta refresh if less than the configured duration has elapsed since
/// the last successful refresh, reducing RPC overhead at the cost of
/// potentially serving slightly stale data.
/// </summary>
[StatelessWorker]
internal sealed class LeafCacheGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : ILeafCacheGrain
{
    private readonly Dictionary<string, LwwValue<byte[]>> _cache = new(StringComparer.Ordinal);
    private VersionVector _version = new();
    private long _lastRefreshTicks;
    private string? _treeId;

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

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (_cache.TryGetValue(key, out var cached) && !cached.IsTombstone
            && !cached.IsExpired(nowTicks))
        {
            return cached.Value;
        }

        return null;
    }

    public async Task<bool> ExistsAsync(string key)
    {
        await RefreshAsync();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        return _cache.TryGetValue(key, out var cached) && !cached.IsTombstone
            && !cached.IsExpired(nowTicks);
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        await RefreshAsync();

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var result = new Dictionary<string, byte[]>(keys.Count);
        foreach (var key in keys)
        {
            if (_cache.TryGetValue(key, out var cached) && !cached.IsTombstone
                && !cached.IsExpired(nowTicks))
            {
                result[key] = cached.Value!;
            }
        }
        return result;
    }

    private async Task RefreshAsync()
    {
        // Check TTL: skip refresh if the cache was populated recently enough.
        var ttl = await GetCacheTtlAsync();
        if (ttl > TimeSpan.Zero && _lastRefreshTicks > 0)
        {
            var elapsed = Environment.TickCount64 - _lastRefreshTicks;
            if (elapsed < (long)ttl.TotalMilliseconds)
                return;
        }

        var primaryLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(PrimaryLeafId);
        var delta = await primaryLeaf.GetDeltaSinceAsync(_version);

        // If the primary leaf has been split, prune any cached entries that
        // now belong to the new sibling (keys >= SplitKey). This is idempotent
        // and safe to apply on every refresh — pruning a key that doesn't
        // exist is a no-operation.
        if (delta.SplitKey is not null)
        {
            var splitKey = delta.SplitKey;
            var keysToRemove = new List<string>();
            foreach (var key in _cache.Keys)
            {
                if (string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                    keysToRemove.Add(key);
            }
            foreach (var key in keysToRemove)
            {
                _cache.Remove(key);
            }
        }

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
        _lastRefreshTicks = Environment.TickCount64;
    }

    private async Task<TimeSpan> GetCacheTtlAsync()
    {
        if (_treeId is null)
        {
            var primaryLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(PrimaryLeafId);
            _treeId = await primaryLeaf.GetTreeIdAsync() ?? string.Empty;
        }
        return optionsMonitor.Get(_treeId).CacheTtl;
    }
}

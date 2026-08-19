using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for an
/// <see cref="OrMap{TKey, TValue}"/> stored under a single key in an
/// <see cref="ILattice"/>. The accessor is a lightweight, allocation-
/// free wrapper - construct it once via
/// <see cref="CrdtLatticeExtensions.OrMap{TKey, TValue}(ILattice, string)"/>
/// and reuse it for any number of operations on the same key.
/// <para>
/// Mutating methods read-modify-write under optimistic concurrency,
/// retrying on CAS failure up to a configurable budget. Concurrent
/// writes from different replicas under the same map key converge by
/// recursing into <typeparamref name="TValue"/>'s
/// <see cref="ICrdt{TSelf}.MergeFrom(TSelf)"/>; concurrent
/// <c>Set</c>/<c>Remove</c> on the same map key follow add-wins
/// observed-remove semantics.
/// </para>
/// </summary>
/// <typeparam name="TKey">
/// The map key type. Must support reasonable dictionary equality
/// (e.g. <see cref="string"/>, <see cref="int"/>, <see cref="Guid"/>).
/// </typeparam>
/// <typeparam name="TValue">
/// The recursively-mergeable value CRDT, constrained by
/// <see cref="ICrdt{TSelf}"/> with a public parameterless constructor.
/// </typeparam>
public readonly record struct OrMapAccessor<TKey, TValue>
    where TKey : notnull
    where TValue : ICrdt<TValue>, new()
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal OrMapAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current map state. Returns an empty
    /// <see cref="OrMap{TKey, TValue}"/> when the key is absent or
    /// tombstoned.
    /// </summary>
    public async Task<OrMap<TKey, TValue>> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>
    /// Returns the lattice-merged value at <paramref name="mapKey"/>,
    /// or <c>null</c> when the map key is absent or every observed dot
    /// for it has been tombstoned.
    /// </summary>
    public async Task<TValue?> GetValueAsync(TKey mapKey, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapKey);
        EnsureInitialised();
        var map = await GetAsync(cancellationToken).ConfigureAwait(false);
        return map.Get(mapKey);
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="mapKey"/> has at least
    /// one live (un-tombstoned) dot in the stored map.
    /// </summary>
    public async Task<bool> ContainsKeyAsync(TKey mapKey, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mapKey);
        EnsureInitialised();
        var map = await GetAsync(cancellationToken).ConfigureAwait(false);
        return map.ContainsKey(mapKey);
    }

    /// <summary>
    /// Writes <paramref name="value"/> at <paramref name="mapKey"/>
    /// from <paramref name="replicaId"/>, minting a fresh causal dot.
    /// Concurrent writes from other replicas survive the next merge
    /// and are folded into a single per-key value via
    /// <see cref="ICrdt{TSelf}.MergeFrom(TSelf)"/>.
    /// </summary>
    /// <param name="mapKey">The key inside the map to write under. Must not be <c>null</c>.</param>
    /// <param name="replicaId">The replica authoring the write. Must be non-empty.</param>
    /// <param name="value">The CRDT value snapshot to attach. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Maximum number of CAS retries before giving up.</param>
    public Task SetAsync(TKey mapKey, string replicaId, TValue value, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(mapKey);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);
        EnsureInitialised();
        return MutateAsync(map =>
        {
            var counter = NextCounter(map, replicaId);
            map.Set(mapKey, replicaId, value);
            return new OrMapDelta<TKey, TValue>
            {
                Adds = new[]
                {
                    new OrMapDeltaEntry<TKey, TValue>
                    {
                        Key = mapKey,
                        ReplicaId = replicaId,
                        Counter = counter,
                        Value = value,
                    },
                },
                Tombstones = Array.Empty<OrMapDeltaTombstone<TKey>>(),
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Writes <paramref name="value"/> at <paramref name="mapKey"/> from
    /// <paramref name="replicaId"/> and stamps the whole entry with a per-entry
    /// time-to-live of <paramref name="ttl"/>. The expiry is resolved to an
    /// absolute UTC instant on the handling silo and folded under the
    /// max-absolute-ticks convergence rule, so re-writing with a later
    /// <paramref name="ttl"/> extends the entry's life and a durable (no-TTL)
    /// write leaves any existing expiry unchanged. Once the instant passes the
    /// whole map reads as absent and is reaped by tombstone compaction.
    /// </summary>
    /// <param name="mapKey">The key inside the map to write under. Must not be <c>null</c>.</param>
    /// <param name="replicaId">The replica authoring the write. Must be non-empty.</param>
    /// <param name="value">The CRDT value snapshot to attach. Must not be <c>null</c>.</param>
    /// <param name="ttl">The positive time-to-live for the entry.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Reserved for API parity; the delta apply does not retry.</param>
    public Task SetAsync(TKey mapKey, string replicaId, TValue value, TimeSpan ttl, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(mapKey);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);
        EnsureInitialised();
        return MutateAsync(map =>
        {
            var counter = NextCounter(map, replicaId);
            map.Set(mapKey, replicaId, value);
            return new OrMapDelta<TKey, TValue>
            {
                Adds = new[]
                {
                    new OrMapDeltaEntry<TKey, TValue>
                    {
                        Key = mapKey,
                        ReplicaId = replicaId,
                        Counter = counter,
                        Value = value,
                    },
                },
                Tombstones = Array.Empty<OrMapDeltaTombstone<TKey>>(),
            };
        }, cancellationToken, maxAttempts, ttl);
    }

    /// <summary>
    /// Removes <paramref name="mapKey"/> by tombstoning every dot
    /// currently observed for it. Concurrent writes on other replicas
    /// (with dots not yet observed locally) survive the next merge -
    /// add-wins.
    /// </summary>
    public Task RemoveAsync(TKey mapKey, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(mapKey);
        EnsureInitialised();
        return MutateAsync(map =>
        {
            OrMapDeltaTombstone<TKey>[] observed;
            if (map.Adds.TryGetValue(mapKey, out var entries) && entries.Count > 0)
            {
                observed = new OrMapDeltaTombstone<TKey>[entries.Count];
                for (var i = 0; i < entries.Count; i++)
                {
                    observed[i] = new OrMapDeltaTombstone<TKey>
                    {
                        Key = mapKey,
                        ReplicaId = entries[i].ReplicaId,
                        Counter = entries[i].Counter,
                    };
                }
            }
            else
            {
                observed = Array.Empty<OrMapDeltaTombstone<TKey>>();
            }
            map.Remove(mapKey);
            return new OrMapDelta<TKey, TValue>
            {
                Adds = Array.Empty<OrMapDeltaEntry<TKey, TValue>>(),
                Tombstones = observed,
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state under
    /// CAS. Useful for replication consumers that have computed a
    /// delta out-of-band and want to apply it without reading the
    /// full map twice.
    /// </summary>
    public Task MergeAsync(OrMap<TKey, TValue> other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(map =>
        {
            map.MergeFrom(other);
            return new OrMapDelta<TKey, TValue>
            {
                Adds = FlattenAdds(other.Adds),
                Tombstones = FlattenTombstones(other.Tombstones),
            };
        }, cancellationToken, maxAttempts);
    }

    private static OrMapDeltaEntry<TKey, TValue>[] FlattenAdds(Dictionary<TKey, List<OrMapEntry<TValue>>> map)
    {
        if (map.Count == 0) return Array.Empty<OrMapDeltaEntry<TKey, TValue>>();
        var total = 0;
        foreach (var entries in map.Values) total += entries.Count;
        if (total == 0) return Array.Empty<OrMapDeltaEntry<TKey, TValue>>();
        var result = new OrMapDeltaEntry<TKey, TValue>[total];
        var i = 0;
        foreach (var (k, entries) in map)
        {
            foreach (var e in entries)
            {
                result[i++] = new OrMapDeltaEntry<TKey, TValue>
                {
                    Key = k,
                    ReplicaId = e.ReplicaId,
                    Counter = e.Counter,
                    Value = e.Value,
                };
            }
        }
        return result;
    }

    private static OrMapDeltaTombstone<TKey>[] FlattenTombstones(Dictionary<TKey, List<OrSetDot>> map)
    {
        if (map.Count == 0) return Array.Empty<OrMapDeltaTombstone<TKey>>();
        var total = 0;
        foreach (var dots in map.Values) total += dots.Count;
        if (total == 0) return Array.Empty<OrMapDeltaTombstone<TKey>>();
        var result = new OrMapDeltaTombstone<TKey>[total];
        var i = 0;
        foreach (var (k, dots) in map)
        {
            foreach (var d in dots)
            {
                result[i++] = new OrMapDeltaTombstone<TKey>
                {
                    Key = k,
                    ReplicaId = d.ReplicaId,
                    Counter = d.Counter,
                };
            }
        }
        return result;
    }

    private static long NextCounter(OrMap<TKey, TValue> map, string replicaId)
    {
        long max = 0;
        foreach (var entries in map.Adds.Values)
        {
            foreach (var e in entries)
            {
                if (string.Equals(e.ReplicaId, replicaId, StringComparison.Ordinal) && e.Counter > max) max = e.Counter;
            }
        }
        foreach (var dots in map.Tombstones.Values)
        {
            foreach (var d in dots)
            {
                if (string.Equals(d.ReplicaId, replicaId, StringComparison.Ordinal) && d.Counter > max) max = d.Counter;
            }
        }
        return max + 1;
    }

    private async Task MutateAsync(
        Func<OrMap<TKey, TValue>, OrMapDelta<TKey, TValue>> mutate,
        CancellationToken cancellationToken,
        int maxAttempts,
        TimeSpan ttl = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // CAS-free producer-side delta apply: see
        // PnCounterAccessor.MutateAsync for the single-read +
        // ApplyCrdtDeltaAsync rationale. The per-replica dot counter
        // is still computed from the local snapshot, in line with
        // OrSetAccessor's NextCounter contract.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<OrMapDelta<TKey, TValue>>.Default.Serialize(delta);
        if (ttl <= TimeSpan.Zero)
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.OrMap, deltaBytes, cancellationToken).ConfigureAwait(false);
        else
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.OrMap, deltaBytes, ttl, cancellationToken).ConfigureAwait(false);
    }

    private static OrMap<TKey, TValue> Decode(byte[]? bytes) =>
        bytes is null ? new OrMap<TKey, TValue>() : JsonLatticeSerializer<OrMap<TKey, TValue>>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "OrMapAccessor is uninitialised; obtain it via ILattice.OrMap<TKey, TValue>(key) instead of `default`.");
        }
    }
}

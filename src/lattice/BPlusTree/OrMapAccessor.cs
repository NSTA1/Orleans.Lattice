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
            map.Set(mapKey, replicaId, value);
        }, cancellationToken, maxAttempts);
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
            map.Remove(mapKey);
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
        }, cancellationToken, maxAttempts);
    }

    private async Task MutateAsync(Action<OrMap<TKey, TValue>> mutate, CancellationToken cancellationToken, int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var versioned = await _lattice.GetWithVersionAsync(_key, cancellationToken).ConfigureAwait(false);
            var current = Decode(versioned.Value);
            mutate(current);
            var bytes = JsonLatticeSerializer<OrMap<TKey, TValue>>.Default.Serialize(current);
            var ok = await _lattice.SetIfVersionAsync(_key, bytes, versioned.Version, cancellationToken).ConfigureAwait(false);
            if (ok) return;
        }
        throw new InvalidOperationException(
            $"OrMap CAS budget exhausted after {maxAttempts} attempts for key '{_key}'. " +
            "Increase maxAttempts or reduce contention.");
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

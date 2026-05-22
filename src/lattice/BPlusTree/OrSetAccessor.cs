using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for an <see cref="Primitives.OrSet"/>
/// stored under a single key in an <see cref="ILattice"/>. The accessor
/// is a lightweight, allocation-free wrapper - construct it once via
/// <see cref="CrdtLatticeExtensions.OrSet(ILattice, string)"/> and reuse
/// it for any number of operations on the same key.
/// <para>
/// Mutating methods read-modify-write under optimistic concurrency
/// control, retrying on CAS failure up to a configurable budget. Two
/// callers operating on the same key from different replicas converge
/// because the underlying merge is the OR-Set state lattice; concurrent
/// adds and removes survive the merge with their causal dot context
/// preserved.
/// </para>
/// </summary>
public readonly record struct OrSetAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal OrSetAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current set state. Returns an empty <see cref="Primitives.OrSet"/>
    /// when the key is absent or tombstoned.
    /// </summary>
    public async Task<OrSet> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>
    /// Adds <paramref name="element"/> with a fresh causal dot stamped
    /// <c>(<paramref name="replicaId"/>, counter)</c> where <c>counter</c>
    /// is the highest counter currently observed for that replica plus one.
    /// </summary>
    /// <param name="element">The element bytes to add. Must not be <c>null</c>.</param>
    /// <param name="replicaId">The replica authoring the add. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Maximum number of CAS retries before giving up.</param>
    public Task AddAsync(byte[] element, string replicaId, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return MutateAsync(set =>
        {
            var counter = NextCounter(set, replicaId);
            set.Add(element, replicaId, counter);
            return new OrSetDelta
            {
                Adds = new[] { new OrSetDeltaDot { Element = element, ReplicaId = replicaId, Counter = counter } },
                Removes = Array.Empty<OrSetDeltaDot>(),
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Removes <paramref name="element"/> by tombstoning every dot
    /// currently observed for it. A no-op (and a successful CAS round
    /// trip) when the element is not present.
    /// </summary>
    public Task RemoveAsync(byte[] element, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(element);
        EnsureInitialised();
        return MutateAsync(set =>
        {
            var key = Convert.ToBase64String(element);
            OrSetDeltaDot[] observed;
            if (set.Adds.TryGetValue(key, out var dots) && dots.Count > 0)
            {
                observed = new OrSetDeltaDot[dots.Count];
                for (var i = 0; i < dots.Count; i++)
                {
                    observed[i] = new OrSetDeltaDot
                    {
                        Element = element,
                        ReplicaId = dots[i].ReplicaId,
                        Counter = dots[i].Counter,
                    };
                }
            }
            else
            {
                observed = Array.Empty<OrSetDeltaDot>();
            }
            set.Remove(element);
            return new OrSetDelta
            {
                Adds = Array.Empty<OrSetDeltaDot>(),
                Removes = observed,
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>Returns <c>true</c> when <paramref name="element"/> is currently a member of the set.</summary>
    public async Task<bool> ContainsAsync(byte[] element, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(element);
        EnsureInitialised();
        var set = await GetAsync(cancellationToken).ConfigureAwait(false);
        return set.Contains(element);
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state under CAS.
    /// Useful for replication consumers that have computed a delta
    /// out-of-band and want to apply it without reading the full set
    /// twice.
    /// </summary>
    public Task MergeAsync(OrSet other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(set =>
        {
            set.MergeFrom(other);
            return new OrSetDelta
            {
                Adds = FlattenDots(other.Adds),
                Removes = FlattenDots(other.Tombstones),
            };
        }, cancellationToken, maxAttempts);
    }

    private static OrSetDeltaDot[] FlattenDots(Dictionary<string, List<OrSetDot>> map)
    {
        if (map.Count == 0) return Array.Empty<OrSetDeltaDot>();
        var total = 0;
        foreach (var dots in map.Values) total += dots.Count;
        if (total == 0) return Array.Empty<OrSetDeltaDot>();
        var result = new OrSetDeltaDot[total];
        var i = 0;
        foreach (var (key, dots) in map)
        {
            var element = Convert.FromBase64String(key);
            foreach (var d in dots)
            {
                result[i++] = new OrSetDeltaDot { Element = element, ReplicaId = d.ReplicaId, Counter = d.Counter };
            }
        }
        return result;
    }

    private static long NextCounter(OrSet set, string replicaId)
    {
        long max = 0;
        foreach (var dots in set.Adds.Values)
        {
            foreach (var d in dots)
            {
                if (d.ReplicaId == replicaId && d.Counter > max) max = d.Counter;
            }
        }
        foreach (var dots in set.Tombstones.Values)
        {
            foreach (var d in dots)
            {
                if (d.ReplicaId == replicaId && d.Counter > max) max = d.Counter;
            }
        }
        return max + 1;
    }

    private async Task MutateAsync<TDelta>(
        Func<OrSet, TDelta> mutate,
        CancellationToken cancellationToken,
        int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var versioned = await _lattice.GetWithVersionAsync(_key, cancellationToken).ConfigureAwait(false);
            var current = Decode(versioned.Value);
            var delta = mutate(current);
            var bytes = JsonLatticeSerializer<OrSet>.Default.Serialize(current);
            var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
            using (LatticeDeltaContext.With(deltaBytes))
            {
                var ok = await _lattice.SetIfVersionAsync(_key, bytes, versioned.Version, cancellationToken).ConfigureAwait(false);
                if (ok) return;
            }
        }
        throw new InvalidOperationException(
            $"OrSet CAS budget exhausted after {maxAttempts} attempts for key '{_key}'. " +
            "Increase maxAttempts or reduce contention.");
    }

    private static OrSet Decode(byte[]? bytes) =>
        bytes is null ? new OrSet() : JsonLatticeSerializer<OrSet>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "OrSetAccessor is uninitialised; obtain it via ILattice.OrSet(key) instead of `default`.");
        }
    }
}

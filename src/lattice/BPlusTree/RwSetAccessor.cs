using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a <see cref="Orleans.Lattice.RwSet"/>
/// stored under a single key in an <see cref="ILattice"/> - the
/// set-granularity counterpart of <see cref="RwFlagAccessor"/>. The accessor
/// is a lightweight, allocation-free wrapper - construct it once via
/// <see cref="CrdtLatticeExtensions.RwSet(ILattice, string)"/> and reuse it
/// for any number of operations on the same key.
/// <para>
/// Mutating methods author a typed <see cref="RwSetDelta"/> and apply it
/// through the single-writer leaf seam. Two callers operating on the same
/// key from different replicas converge because the underlying merge is the
/// remove-wins observed-remove set state lattice; concurrent adds and removes
/// survive the merge with their causal dot context preserved, and a remove
/// that an add has not observed continues to keep the element out of the set.
/// </para>
/// </summary>
public readonly record struct RwSetAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal RwSetAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current set state. Returns an empty
    /// <see cref="Orleans.Lattice.RwSet"/> when the key is absent or
    /// tombstoned.
    /// </summary>
    public async Task<RwSet> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>
    /// Adds <paramref name="element"/> with a fresh causal dot stamped
    /// <c>(<paramref name="replicaId"/>, counter)</c> where <c>counter</c> is
    /// the highest counter currently observed for that replica plus one, and
    /// cancels every remove dot currently observed for the element. A
    /// concurrent remove the add has not observed survives and keeps the
    /// element out of the set (remove-wins).
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
        return MutateAsync(set => AddDelta(set, element, replicaId), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Removes <paramref name="element"/> with a fresh causal remove dot
    /// stamped <c>(<paramref name="replicaId"/>, counter)</c>. The remove
    /// dominates any concurrent add that has not observed it (remove-wins) and
    /// keeps the element out until an add observes and cancels this dot.
    /// </summary>
    /// <param name="element">The element bytes to remove. Must not be <c>null</c>.</param>
    /// <param name="replicaId">The replica authoring the remove. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Maximum number of CAS retries before giving up.</param>
    public Task RemoveAsync(byte[] element, string replicaId, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return MutateAsync(set => RemoveDelta(set, element, replicaId), cancellationToken, maxAttempts);
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
    /// Reads the current live members of the set. Returns an empty list when
    /// the key is absent or every element has been removed.
    /// </summary>
    public async Task<IReadOnlyList<byte[]>> ToListAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var set = await GetAsync(cancellationToken).ConfigureAwait(false);
        return set.Elements().ToArray();
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state. Useful for
    /// replication consumers that have computed a set state out-of-band and
    /// want to apply it without reading the full state twice.
    /// </summary>
    public Task MergeAsync(RwSet other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(_ => new RwSetDelta
        {
            Adds = FlattenDots(other.Adds),
            Removes = FlattenDots(other.Removes),
            Tombstones = FlattenDots(other.Tombstones),
        }, cancellationToken, maxAttempts);
    }

    /// <summary>Mints the remove-wins add delta for <paramref name="element"/> from <paramref name="replicaId"/> against <paramref name="set"/>.</summary>
    private static RwSetDelta AddDelta(RwSet set, byte[] element, string replicaId)
    {
        var counter = NextCounter(set, replicaId);
        return new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = element, ReplicaId = replicaId, Counter = counter } },
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = ObservedRemoves(set, element),
        };
    }

    /// <summary>Mints the remove-wins remove delta for <paramref name="element"/> from <paramref name="replicaId"/> against <paramref name="set"/>.</summary>
    private static RwSetDelta RemoveDelta(RwSet set, byte[] element, string replicaId)
    {
        var counter = NextCounter(set, replicaId);
        return new RwSetDelta
        {
            Adds = Array.Empty<OrSetDeltaDot>(),
            Removes = new[] { new OrSetDeltaDot { Element = element, ReplicaId = replicaId, Counter = counter } },
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };
    }

    private static OrSetDeltaDot[] ObservedRemoves(RwSet set, byte[] element)
    {
        var key = Convert.ToBase64String(element);
        if (!set.Removes.TryGetValue(key, out var dots) || dots.Count == 0)
        {
            return Array.Empty<OrSetDeltaDot>();
        }
        var observed = new OrSetDeltaDot[dots.Count];
        for (var i = 0; i < dots.Count; i++)
        {
            observed[i] = new OrSetDeltaDot { Element = element, ReplicaId = dots[i].ReplicaId, Counter = dots[i].Counter };
        }
        return observed;
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

    private static long NextCounter(RwSet set, string replicaId)
    {
        long max = 0;
        foreach (var dots in set.Adds.Values)
        {
            foreach (var d in dots)
            {
                if (d.ReplicaId == replicaId && d.Counter > max) max = d.Counter;
            }
        }
        foreach (var dots in set.Removes.Values)
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

    private async Task MutateAsync(
        Func<RwSet, RwSetDelta> mutate,
        CancellationToken cancellationToken,
        int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // Producer-side delta apply: a single read (to compute the next dot
        // counter / observed-dot set the delta needs) plus one
        // ApplyCrdtDeltaAsync call. The leaf grain is the single writer
        // authority per key, so no inner CAS retry loop is required for
        // convergence. The maxAttempts parameter is preserved on the public
        // surface for parity with the other accessors and is validated for an
        // early-failure signal on misconfiguration.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<RwSetDelta>.Default.Serialize(delta);
        await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.RwSet, deltaBytes, cancellationToken).ConfigureAwait(false);
    }

    private static RwSet Decode(byte[]? bytes) =>
        bytes is null ? new RwSet() : JsonLatticeSerializer<RwSet>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "RwSetAccessor is uninitialised; obtain it via ILattice.RwSet(key) instead of `default`.");
        }
    }
}

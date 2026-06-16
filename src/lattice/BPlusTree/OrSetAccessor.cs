using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for an <see cref="Orleans.Lattice.OrSet"/>
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
    /// Reads the current set state. Returns an empty <see cref="Orleans.Lattice.OrSet"/>
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
        return MutateAsync(set => AddDelta(set, element, replicaId), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Stages an add as a <see cref="LatticeStagedCrdtWrite"/> for a cross-tree
    /// atomic write instead of applying it now. The minted add dot is identical
    /// to <see cref="AddAsync(byte[], string, CancellationToken, int)"/>'s; add
    /// the returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on an
    /// OR-Set-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="element">The element bytes to add. Must not be <c>null</c>.</param>
    /// <param name="replicaId">The replica authoring the add. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageAddAsync(byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return StageAsync(set => AddDelta(set, element, replicaId), cancellationToken);
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
        return MutateAsync(set => RemoveDelta(set, element), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Stages a remove as a <see cref="LatticeStagedCrdtWrite"/> for a cross-tree
    /// atomic write instead of applying it now. The tombstoned dots are identical
    /// to <see cref="RemoveAsync(byte[], CancellationToken, int)"/>'s; add the
    /// returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on an
    /// OR-Set-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="element">The element bytes to remove. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageRemoveAsync(byte[] element, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(element);
        EnsureInitialised();
        return StageAsync(set => RemoveDelta(set, element), cancellationToken);
    }

    /// <summary>Mints the add delta for <paramref name="element"/> from <paramref name="replicaId"/> against <paramref name="set"/>.</summary>
    private static OrSetDelta AddDelta(OrSet set, byte[] element, string replicaId)
    {
        var counter = NextCounter(set, replicaId);
        return new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = element, ReplicaId = replicaId, Counter = counter } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
    }

    /// <summary>Mints the remove delta tombstoning every dot observed for <paramref name="element"/> in <paramref name="set"/>.</summary>
    private static OrSetDelta RemoveDelta(OrSet set, byte[] element)
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
        return new OrSetDelta
        {
            Adds = Array.Empty<OrSetDeltaDot>(),
            Removes = observed,
        };
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
            // Do not mutate the local snapshot - the leaf grain folds
            // the typed delta authoritatively.
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
        // Producer-side delta apply: replaced the read-merge-write CAS
        // loop with a single read (to compute the next dot counter /
        // observed-dot set the delta needs) plus one
        // ApplyCrdtDeltaAsync call. The leaf grain is the single
        // writer authority per key, so the CAS retry budget is no
        // longer required for convergence; two concurrent adds against
        // the same replica id are still the caller's responsibility to
        // avoid (the OR-Set per-replica monotonicity contract was
        // identical under the old loop). The maxAttempts parameter is
        // preserved on the public surface for binary compatibility and
        // validated for an early-failure signal on misconfiguration,
        // but no longer drives an inner retry loop.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
        await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.OrSet, deltaBytes, cancellationToken).ConfigureAwait(false);
    }

    private static OrSet Decode(byte[]? bytes) =>
        bytes is null ? new OrSet() : JsonLatticeSerializer<OrSet>.Default.Deserialize(bytes);

    private async Task<LatticeStagedCrdtWrite> StageAsync(
        Func<OrSet, OrSetDelta> mint,
        CancellationToken cancellationToken)
    {
        // Mint-once: a single read mints the typed delta, folds it into the
        // snapshot to produce the merged state, and serialises both. No
        // ApplyCrdtDeltaAsync is issued here - the cross-tree saga performs the
        // durable write and replays the persisted delta verbatim.
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mint(snapshot);
        snapshot.MergeDelta(delta);
        var value = JsonLatticeSerializer<OrSet>.Default.Serialize(snapshot);
        var deltaBytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);
        return new LatticeStagedCrdtWrite(_key, value, deltaBytes);
    }

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "OrSetAccessor is uninitialised; obtain it via ILattice.OrSet(key) instead of `default`.");
        }
    }
}

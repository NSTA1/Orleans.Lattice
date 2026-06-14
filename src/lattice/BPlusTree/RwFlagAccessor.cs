using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a <see cref="Orleans.Lattice.RwFlag"/>
/// stored under a single key in an <see cref="ILattice"/>. The accessor is
/// a lightweight, allocation-free wrapper - construct it once via
/// <see cref="CrdtLatticeExtensions.RwFlag(ILattice, string)"/> and reuse it
/// for any number of operations on the same key.
/// <para>
/// Mutating methods author a typed <see cref="RwFlagDelta"/> and apply it
/// through the single-writer leaf seam. Two callers operating on the same
/// key from different replicas converge because the underlying merge is the
/// RW-Flag (remove-wins) state lattice; concurrent enables and disables
/// survive the merge with their causal dot context preserved, and a disable
/// that an enable has not observed continues to suppress the flag.
/// </para>
/// </summary>
public readonly record struct RwFlagAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal RwFlagAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current flag state. Returns an empty (disabled)
    /// <see cref="Orleans.Lattice.RwFlag"/> when the key is absent or
    /// tombstoned.
    /// </summary>
    public async Task<RwFlag> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>Returns <c>true</c> when the flag is currently enabled.</summary>
    public async Task<bool> IsEnabledAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var flag = await GetAsync(cancellationToken).ConfigureAwait(false);
        return flag.IsEnabled;
    }

    /// <summary>
    /// Enables the flag with a fresh causal dot stamped
    /// <c>(<paramref name="replicaId"/>, counter)</c> where <c>counter</c>
    /// is the highest counter currently observed for that replica plus one,
    /// and cancels every disable dot currently observed. A concurrent disable
    /// the enable has not observed survives and keeps the flag off
    /// (remove-wins).
    /// </summary>
    /// <param name="replicaId">The replica authoring the enable. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Maximum number of CAS retries before giving up.</param>
    public Task EnableAsync(string replicaId, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return MutateAsync(flag =>
        {
            var counter = NextCounter(flag, replicaId);
            return new RwFlagDelta
            {
                Enables = new[] { new OrSetDot { ReplicaId = replicaId, Counter = counter } },
                Disables = Array.Empty<OrSetDot>(),
                Tombstones = ObservedDisables(flag),
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Disables (removes) the flag with a fresh causal dot stamped
    /// <c>(<paramref name="replicaId"/>, counter)</c>. The disable dominates
    /// any concurrent enable that has not observed it (remove-wins) and keeps
    /// the flag off until an enable observes and cancels this dot.
    /// </summary>
    /// <param name="replicaId">The replica authoring the disable. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Maximum number of CAS retries before giving up.</param>
    public Task DisableAsync(string replicaId, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return MutateAsync(flag =>
        {
            var counter = NextCounter(flag, replicaId);
            return new RwFlagDelta
            {
                Enables = Array.Empty<OrSetDot>(),
                Disables = new[] { new OrSetDot { ReplicaId = replicaId, Counter = counter } },
                Tombstones = Array.Empty<OrSetDot>(),
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state. Useful for
    /// replication consumers that have computed a flag state out-of-band
    /// and want to apply it without reading the full state twice.
    /// </summary>
    public Task MergeAsync(RwFlag other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(_ => new RwFlagDelta
        {
            Enables = other.Enables.ToArray(),
            Disables = other.Disables.ToArray(),
            Tombstones = other.Tombstones.ToArray(),
        }, cancellationToken, maxAttempts);
    }

    private static OrSetDot[] ObservedDisables(RwFlag flag)
    {
        if (flag.Disables.Count == 0) return Array.Empty<OrSetDot>();
        var observed = new OrSetDot[flag.Disables.Count];
        for (var i = 0; i < flag.Disables.Count; i++)
        {
            observed[i] = flag.Disables[i];
        }
        return observed;
    }

    private static long NextCounter(RwFlag flag, string replicaId)
    {
        long max = 0;
        foreach (var d in flag.Enables)
        {
            if (d.ReplicaId == replicaId && d.Counter > max) max = d.Counter;
        }
        foreach (var d in flag.Disables)
        {
            if (d.ReplicaId == replicaId && d.Counter > max) max = d.Counter;
        }
        foreach (var d in flag.Tombstones)
        {
            if (d.ReplicaId == replicaId && d.Counter > max) max = d.Counter;
        }
        return max + 1;
    }

    private async Task MutateAsync(
        Func<RwFlag, RwFlagDelta> mutate,
        CancellationToken cancellationToken,
        int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // Producer-side delta apply: a single read (to compute the next
        // dot counter / observed-dot set the delta needs) plus one
        // ApplyCrdtDeltaAsync call. The leaf grain is the single writer
        // authority per key, so no inner CAS retry loop is required for
        // convergence. The maxAttempts parameter is preserved on the
        // public surface for parity with the other accessors and is
        // validated for an early-failure signal on misconfiguration.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<RwFlagDelta>.Default.Serialize(delta);
        await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.RwFlag, deltaBytes, cancellationToken).ConfigureAwait(false);
    }

    private static RwFlag Decode(byte[]? bytes) =>
        bytes is null ? new RwFlag() : JsonLatticeSerializer<RwFlag>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "RwFlagAccessor is uninitialised; obtain it via ILattice.RwFlag(key) instead of `default`.");
        }
    }
}

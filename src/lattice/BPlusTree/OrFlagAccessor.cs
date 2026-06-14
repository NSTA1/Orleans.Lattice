using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for an <see cref="Orleans.Lattice.OrFlag"/>
/// stored under a single key in an <see cref="ILattice"/>. The accessor is
/// a lightweight, allocation-free wrapper - construct it once via
/// <see cref="CrdtLatticeExtensions.OrFlag(ILattice, string)"/> and reuse it
/// for any number of operations on the same key.
/// <para>
/// Mutating methods author a typed <see cref="OrFlagDelta"/> and apply it
/// through the single-writer leaf seam. Two callers operating on the same
/// key from different replicas converge because the underlying merge is the
/// OR-Flag (enable-wins) state lattice; concurrent enables and disables
/// survive the merge with their causal dot context preserved.
/// </para>
/// </summary>
public readonly record struct OrFlagAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal OrFlagAccessor(ILattice lattice, string key)
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
    /// <see cref="Orleans.Lattice.OrFlag"/> when the key is absent or
    /// tombstoned.
    /// </summary>
    public async Task<OrFlag> GetAsync(CancellationToken cancellationToken = default)
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
    /// is the highest counter currently observed for that replica plus one.
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
            return new OrFlagDelta
            {
                Enables = new[] { new OrSetDot { ReplicaId = replicaId, Counter = counter } },
                Disables = Array.Empty<OrSetDot>(),
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Disables the flag by tombstoning every enable dot currently
    /// observed for it. A no-op (and a successful round trip) when the
    /// flag is not enabled.
    /// </summary>
    public Task DisableAsync(CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        EnsureInitialised();
        return MutateAsync(flag =>
        {
            OrSetDot[] observed;
            if (flag.Enables.Count > 0)
            {
                observed = new OrSetDot[flag.Enables.Count];
                for (var i = 0; i < flag.Enables.Count; i++)
                {
                    observed[i] = flag.Enables[i];
                }
            }
            else
            {
                observed = Array.Empty<OrSetDot>();
            }
            return new OrFlagDelta
            {
                Enables = Array.Empty<OrSetDot>(),
                Disables = observed,
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Merges <paramref name="other"/> into the stored state. Useful for
    /// replication consumers that have computed a flag state out-of-band
    /// and want to apply it without reading the full state twice.
    /// </summary>
    public Task MergeAsync(OrFlag other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(_ => new OrFlagDelta
        {
            Enables = other.Enables.ToArray(),
            Disables = other.Tombstones.ToArray(),
        }, cancellationToken, maxAttempts);
    }

    private static long NextCounter(OrFlag flag, string replicaId)
    {
        long max = 0;
        foreach (var d in flag.Enables)
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
        Func<OrFlag, OrFlagDelta> mutate,
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
        var deltaBytes = JsonLatticeSerializer<OrFlagDelta>.Default.Serialize(delta);
        await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.OrFlag, deltaBytes, cancellationToken).ConfigureAwait(false);
    }

    private static OrFlag Decode(byte[]? bytes) =>
        bytes is null ? new OrFlag() : JsonLatticeSerializer<OrFlag>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "OrFlagAccessor is uninitialised; obtain it via ILattice.OrFlag(key) instead of `default`.");
        }
    }
}

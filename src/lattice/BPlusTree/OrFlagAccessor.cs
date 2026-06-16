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
        return MutateAsync(flag => EnableDelta(flag, replicaId), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Stages an enable as a <see cref="LatticeStagedCrdtWrite"/> for a
    /// cross-tree atomic write instead of applying it now. The minted enable
    /// dot is identical to <see cref="EnableAsync(string, CancellationToken, int)"/>'s;
    /// add the returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on an
    /// OR-Flag-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="replicaId">The replica authoring the enable. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageEnableAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        EnsureInitialised();
        return StageAsync(flag => EnableDelta(flag, replicaId), cancellationToken);
    }

    /// <summary>
    /// Disables the flag by tombstoning every enable dot currently
    /// observed for it. A no-op (and a successful round trip) when the
    /// flag is not enabled.
    /// </summary>
    public Task DisableAsync(CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        EnsureInitialised();
        return MutateAsync(DisableDelta, cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Stages a disable as a <see cref="LatticeStagedCrdtWrite"/> for a
    /// cross-tree atomic write instead of applying it now. The tombstoned dots
    /// are identical to <see cref="DisableAsync(CancellationToken, int)"/>'s;
    /// add the returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on an
    /// OR-Flag-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageDisableAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        return StageAsync(DisableDelta, cancellationToken);
    }

    /// <summary>Mints the enable delta for <paramref name="replicaId"/> against <paramref name="flag"/>.</summary>
    private static OrFlagDelta EnableDelta(OrFlag flag, string replicaId)
    {
        var counter = NextCounter(flag, replicaId);
        return new OrFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = replicaId, Counter = counter } },
            Disables = Array.Empty<OrSetDot>(),
        };
    }

    /// <summary>Mints the disable delta tombstoning every enable dot observed in <paramref name="flag"/>.</summary>
    private static OrFlagDelta DisableDelta(OrFlag flag)
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

    private async Task<LatticeStagedCrdtWrite> StageAsync(
        Func<OrFlag, OrFlagDelta> mint,
        CancellationToken cancellationToken)
    {
        // Mint-once: a single read of the current state mints the typed delta,
        // folds it into the snapshot to produce the merged state, and serialises
        // both. No ApplyCrdtDeltaAsync is issued here - the cross-tree saga
        // performs the durable write and replays the persisted delta verbatim.
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mint(snapshot);
        snapshot.MergeDelta(delta);
        var value = JsonLatticeSerializer<OrFlag>.Default.Serialize(snapshot);
        var deltaBytes = JsonLatticeSerializer<OrFlagDelta>.Default.Serialize(delta);
        return new LatticeStagedCrdtWrite(_key, value, deltaBytes);
    }

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "OrFlagAccessor is uninitialised; obtain it via ILattice.OrFlag(key) instead of `default`.");
        }
    }
}

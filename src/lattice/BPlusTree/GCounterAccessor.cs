using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a <see cref="Orleans.Lattice.GCounter"/>
/// stored under a single key in an <see cref="ILattice"/>. The counter is
/// grow-only: it exposes an increment but no decrement, and negative amounts
/// are rejected at this boundary so the grow-only invariant holds. Mutating
/// methods apply a typed delta through the single-writer leaf grain.
/// </summary>
public readonly record struct GCounterAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal GCounterAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current counter state. Returns an empty
    /// <see cref="Orleans.Lattice.GCounter"/> when the key is absent or
    /// tombstoned.
    /// </summary>
    public async Task<GCounter> GetAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var bytes = await _lattice.GetAsync(_key, cancellationToken).ConfigureAwait(false);
        return Decode(bytes);
    }

    /// <summary>Reads the current scalar value of the counter.</summary>
    public async Task<long> ValueAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialised();
        var counter = await GetAsync(cancellationToken).ConfigureAwait(false);
        return counter.Value;
    }

    /// <summary>
    /// Increments the grow-only component for <paramref name="replicaId"/>
    /// by <paramref name="amount"/>. <paramref name="amount"/> must be
    /// non-negative - a grow-only counter never decreases.
    /// </summary>
    /// <remarks>
    /// When the caller has entered an ambient
    /// <see cref="LatticeIdempotencyContext"/> scope the leaf grain adds a
    /// pre-apply dedup guard, so a retry of the same logical operation under
    /// the same key collapses to a no-op. Without the scope the counter
    /// advances on every call.
    /// </remarks>
    public Task IncrementAsync(string replicaId, long amount = 1, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return MutateAsync(c => IncrementDelta(c, replicaId, amount), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Increments the grow-only component for <paramref name="replicaId"/> by
    /// <paramref name="amount"/> and stamps the whole entry with a per-entry
    /// time-to-live of <paramref name="ttl"/>. The expiry is resolved to an
    /// absolute UTC instant on the handling silo and folded under the
    /// max-absolute-ticks convergence rule, so re-writing with a later
    /// <paramref name="ttl"/> extends the entry's life and a durable
    /// (no-TTL) write leaves any existing expiry unchanged. Once the instant
    /// passes the whole counter reads as absent and is reaped by tombstone
    /// compaction.
    /// </summary>
    /// <param name="replicaId">The replica authoring the increment. Must be non-empty.</param>
    /// <param name="amount">The non-negative amount to add to the component.</param>
    /// <param name="ttl">The positive time-to-live for the entry.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <param name="maxAttempts">Reserved for API parity; the delta apply does not retry.</param>
    public Task IncrementAsync(string replicaId, long amount, TimeSpan ttl, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return MutateAsync(c => IncrementDelta(c, replicaId, amount), cancellationToken, maxAttempts, ttl);
    }

    /// <summary>
    /// Stages an increment as a <see cref="LatticeStagedCrdtWrite"/> for a
    /// cross-tree atomic write instead of applying it now. The per-replica
    /// component advance is identical to
    /// <see cref="IncrementAsync(string, long, CancellationToken, int)"/>'s;
    /// add the returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on a
    /// GCounter-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="replicaId">The replica authoring the increment. Must be non-empty.</param>
    /// <param name="amount">The non-negative amount to add to the component.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageIncrementAsync(string replicaId, long amount = 1, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return StageAsync(c => IncrementDelta(c, replicaId, amount), cancellationToken);
    }

    /// <summary>Merges <paramref name="other"/> into the stored state.</summary>
    public Task MergeAsync(GCounter other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(c =>
        {
            c.MergeFrom(other);
            return new GCounterDelta
            {
                Increments = new Dictionary<string, long>(other.Increments, StringComparer.Ordinal),
            };
        }, cancellationToken, maxAttempts);
    }

    /// <summary>Mints the increment delta for <paramref name="replicaId"/> against <paramref name="c"/>.</summary>
    private static GCounterDelta IncrementDelta(GCounter c, string replicaId, long amount)
    {
        c.Increment(replicaId, amount);
        var inc = new Dictionary<string, long>(StringComparer.Ordinal);
        if (c.Increments.TryGetValue(replicaId, out var value)) inc[replicaId] = value;
        return new GCounterDelta { Increments = inc };
    }

    private async Task MutateAsync<TDelta>(
        Func<GCounter, TDelta> mutate,
        CancellationToken cancellationToken,
        int maxAttempts,
        TimeSpan ttl = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // Producer-side delta apply: a single read to compute the next
        // per-replica component the delta carries, plus one
        // ApplyCrdtDeltaAsync call. The leaf grain is the single writer
        // authority per key, so no CAS retry loop is needed; maxAttempts is
        // preserved on the public surface for compatibility but no longer
        // drives an inner loop.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
        if (ttl <= TimeSpan.Zero)
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.GCounter, deltaBytes, cancellationToken).ConfigureAwait(false);
        else
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.GCounter, deltaBytes, ttl, cancellationToken).ConfigureAwait(false);
    }

    private static GCounter Decode(byte[]? bytes) =>
        bytes is null ? new GCounter() : JsonLatticeSerializer<GCounter>.Default.Deserialize(bytes);

    private async Task<LatticeStagedCrdtWrite> StageAsync(
        Func<GCounter, GCounterDelta> mint,
        CancellationToken cancellationToken)
    {
        // Mint-once: a single read mints the typed delta, folds it into the
        // snapshot to produce the merged state, and serialises both. The
        // follow-up MergeDelta is an idempotent pointwise-max. No
        // ApplyCrdtDeltaAsync is issued here - the cross-tree saga performs the
        // durable write and replays the persisted delta verbatim.
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mint(snapshot);
        snapshot.MergeDelta(delta);
        var value = JsonLatticeSerializer<GCounter>.Default.Serialize(snapshot);
        var deltaBytes = JsonLatticeSerializer<GCounterDelta>.Default.Serialize(delta);
        return new LatticeStagedCrdtWrite(_key, value, deltaBytes);
    }

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "GCounterAccessor is uninitialised; obtain it via ILattice.GCounter(key) instead of `default`.");
        }
    }
}

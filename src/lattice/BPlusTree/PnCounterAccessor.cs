using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a <see cref="Orleans.Lattice.PnCounter"/>
/// stored under a single key in an <see cref="ILattice"/>. Mutating
/// methods read-modify-write under optimistic concurrency, retrying on
/// CAS failure up to a configurable budget.
/// </summary>
public readonly record struct PnCounterAccessor
{
    /// <summary>Default CAS retry budget for mutating operations.</summary>
    public const int DefaultMaxAttempts = 16;

    private readonly ILattice _lattice;
    private readonly string _key;

    internal PnCounterAccessor(ILattice lattice, string key)
    {
        _lattice = lattice;
        _key = key;
    }

    /// <summary>The tree the accessor is bound to.</summary>
    public ILattice Lattice => _lattice;

    /// <summary>The key the accessor reads and writes.</summary>
    public string Key => _key;

    /// <summary>
    /// Reads the current counter state. Returns an empty <see cref="Orleans.Lattice.PnCounter"/>
    /// when the key is absent or tombstoned.
    /// </summary>
    public async Task<PnCounter> GetAsync(CancellationToken cancellationToken = default)
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
    /// Increments the positive component for <paramref name="replicaId"/>
    /// by <paramref name="amount"/>. <paramref name="amount"/> must be non-negative.
    /// </summary>
    /// <remarks>
    /// When the caller has entered an ambient
    /// <see cref="LatticeIdempotencyContext"/> scope the accessor adds a
    /// pre-CAS dedup guard: if the stored entry's HLC version already
    /// equals the supplied <see cref="LatticeIdempotencyKey.Timestamp"/>
    /// (and the origin matches), a previous attempt under the same key
    /// already advanced the counter and the second call drops to a
    /// no-op. Without the scope the counter advances on every call,
    /// which is the negative-control behaviour for the dedup feature.
    /// </remarks>
    public Task IncrementAsync(string replicaId, long amount = 1, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return MutateAsync(c => IncrementDelta(c, replicaId, amount), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Increments the positive component for <paramref name="replicaId"/> by
    /// <paramref name="amount"/> and stamps the whole entry with a per-entry
    /// time-to-live of <paramref name="ttl"/>. The expiry is resolved to an
    /// absolute UTC instant on the handling silo and folded under the
    /// max-absolute-ticks convergence rule, so re-writing with a later
    /// <paramref name="ttl"/> extends the entry's life and a durable (no-TTL)
    /// write leaves any existing expiry unchanged. Once the instant passes the
    /// whole counter reads as absent and is reaped by tombstone compaction.
    /// </summary>
    /// <param name="replicaId">The replica authoring the increment. Must be non-empty.</param>
    /// <param name="amount">The non-negative amount to add to the positive component.</param>
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
    /// <see cref="IncrementAsync(string, long, CancellationToken, int)"/>'s; add
    /// the returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on a
    /// PN-counter-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="replicaId">The replica authoring the increment. Must be non-empty.</param>
    /// <param name="amount">The non-negative amount to add to the positive component.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageIncrementAsync(string replicaId, long amount = 1, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return StageAsync(c => IncrementDelta(c, replicaId, amount), cancellationToken);
    }

    /// <summary>
    /// Increments the negative component for <paramref name="replicaId"/>
    /// by <paramref name="amount"/>. <paramref name="amount"/> must be non-negative.
    /// </summary>
    /// <remarks>
    /// Honours the same ambient
    /// <see cref="LatticeIdempotencyContext"/> dedup guard as
    /// <see cref="IncrementAsync"/>.
    /// </remarks>
    public Task DecrementAsync(string replicaId, long amount = 1, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return MutateAsync(c => DecrementDelta(c, replicaId, amount), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Stages a decrement as a <see cref="LatticeStagedCrdtWrite"/> for a
    /// cross-tree atomic write instead of applying it now. The per-replica
    /// component advance is identical to
    /// <see cref="DecrementAsync(string, long, CancellationToken, int)"/>'s; add
    /// the returned token to a builder slice via
    /// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on a
    /// PN-counter-mode tree. See <see cref="LatticeStagedCrdtWrite"/> for the
    /// merge-mode-matching, single-cluster concurrent-writer, and compensation
    /// contract.
    /// </summary>
    /// <param name="replicaId">The replica authoring the decrement. Must be non-empty.</param>
    /// <param name="amount">The non-negative amount to add to the negative component.</param>
    /// <param name="cancellationToken">Cancels the snapshot read.</param>
    public Task<LatticeStagedCrdtWrite> StageDecrementAsync(string replicaId, long amount = 1, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return StageAsync(c => DecrementDelta(c, replicaId, amount), cancellationToken);
    }

    /// <summary>Mints the increment delta for <paramref name="replicaId"/> against <paramref name="c"/>.</summary>
    private static PnCounterDelta IncrementDelta(PnCounter c, string replicaId, long amount)
    {
        c.Increment(replicaId, amount);
        var inc = new Dictionary<string, long>(StringComparer.Ordinal);
        if (c.Increments.TryGetValue(replicaId, out var value)) inc[replicaId] = value;
        return new PnCounterDelta
        {
            Increments = inc,
            Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
        };
    }

    /// <summary>Mints the decrement delta for <paramref name="replicaId"/> against <paramref name="c"/>.</summary>
    private static PnCounterDelta DecrementDelta(PnCounter c, string replicaId, long amount)
    {
        c.Decrement(replicaId, amount);
        var dec = new Dictionary<string, long>(StringComparer.Ordinal);
        if (c.Decrements.TryGetValue(replicaId, out var value)) dec[replicaId] = value;
        return new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(0, StringComparer.Ordinal),
            Decrements = dec,
        };
    }

    /// <summary>Merges <paramref name="other"/> into the stored state under CAS.</summary>
    public Task MergeAsync(PnCounter other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(c =>
        {
            c.MergeFrom(other);
            return new PnCounterDelta
            {
                Increments = new Dictionary<string, long>(other.Increments, StringComparer.Ordinal),
                Decrements = new Dictionary<string, long>(other.Decrements, StringComparer.Ordinal),
            };
        }, cancellationToken, maxAttempts);
    }

    private async Task MutateAsync<TDelta>(
        Func<PnCounter, TDelta> mutate,
        CancellationToken cancellationToken,
        int maxAttempts,
        TimeSpan ttl = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        // Producer-side delta apply: replaced the read-modify-write
        // CAS loop with a single read (to compute the next per-replica
        // counter the delta carries) plus one ApplyCrdtDeltaAsync
        // call. The leaf grain is the single writer authority per key,
        // so CAS retries are no longer needed for convergence. The
        // LatticeIdempotencyContext dedup guard now lives in
        // LatticeGrain.ApplyCrdtDeltaAsync (it routes through
        // RunMutationAsync when an idempotency scope is active), so
        // the accessor no longer needs to read the ambient key. The
        // maxAttempts parameter is preserved on the public surface
        // for binary compatibility but no longer drives an inner
        // retry loop.
        _ = maxAttempts;
        cancellationToken.ThrowIfCancellationRequested();
        var current = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mutate(current);
        var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
        if (ttl <= TimeSpan.Zero)
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.PnCounter, deltaBytes, cancellationToken).ConfigureAwait(false);
        else
            await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.PnCounter, deltaBytes, ttl, cancellationToken).ConfigureAwait(false);
    }

    private static PnCounter Decode(byte[]? bytes) =>
        bytes is null ? new PnCounter() : JsonLatticeSerializer<PnCounter>.Default.Deserialize(bytes);

    private async Task<LatticeStagedCrdtWrite> StageAsync(
        Func<PnCounter, PnCounterDelta> mint,
        CancellationToken cancellationToken)
    {
        // Mint-once: a single read mints the typed delta, folds it into the
        // snapshot to produce the merged state, and serialises both. The mint
        // closure advances the snapshot's component and the delta carries the
        // resulting per-replica total, so the follow-up MergeDelta is an
        // idempotent pointwise-max. No ApplyCrdtDeltaAsync is issued here - the
        // cross-tree saga performs the durable write and replays the persisted
        // delta verbatim.
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = await GetAsync(cancellationToken).ConfigureAwait(false);
        var delta = mint(snapshot);
        snapshot.MergeDelta(delta);
        var value = JsonLatticeSerializer<PnCounter>.Default.Serialize(snapshot);
        var deltaBytes = JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(delta);
        return new LatticeStagedCrdtWrite(_key, value, deltaBytes);
    }

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "PnCounterAccessor is uninitialised; obtain it via ILattice.PnCounter(key) instead of `default`.");
        }
    }
}

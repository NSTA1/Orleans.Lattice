using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed value-surface accessor for a <see cref="Primitives.PnCounter"/>
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
    /// Reads the current counter state. Returns an empty <see cref="Primitives.PnCounter"/>
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
        return MutateAsync(c =>
        {
            c.Increment(replicaId, amount);
            var inc = new Dictionary<string, long>(StringComparer.Ordinal);
            if (c.Increments.TryGetValue(replicaId, out var value)) inc[replicaId] = value;
            return new PnCounterDelta
            {
                Increments = inc,
                Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
            };
        }, cancellationToken, maxAttempts);
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
        return MutateAsync(c =>
        {
            c.Decrement(replicaId, amount);
            var dec = new Dictionary<string, long>(StringComparer.Ordinal);
            if (c.Decrements.TryGetValue(replicaId, out var value)) dec[replicaId] = value;
            return new PnCounterDelta
            {
                Increments = new Dictionary<string, long>(0, StringComparer.Ordinal),
                Decrements = dec,
            };
        }, cancellationToken, maxAttempts);
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
        int maxAttempts)
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
        await _lattice.ApplyCrdtDeltaAsync(_key, LatticeMergeMode.PnCounter, deltaBytes, cancellationToken).ConfigureAwait(false);
    }

    private static PnCounter Decode(byte[]? bytes) =>
        bytes is null ? new PnCounter() : JsonLatticeSerializer<PnCounter>.Default.Deserialize(bytes);

    private void EnsureInitialised()
    {
        if (_lattice is null)
        {
            throw new InvalidOperationException(
                "PnCounterAccessor is uninitialised; obtain it via ILattice.PnCounter(key) instead of `default`.");
        }
    }
}

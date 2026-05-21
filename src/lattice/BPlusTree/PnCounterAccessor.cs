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

        // Hoist the ambient idempotency-key read out of the CAS loop:
        // the AsyncLocal-backed RequestContext.Get on every iteration is
        // a small but visible cost on contended paths, and the key is
        // invariant across the call.
        var idemKey = LatticeIdempotencyContext.Current;

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var versioned = await _lattice.GetWithVersionAsync(_key, cancellationToken).ConfigureAwait(false);

            // Idempotency dedup guard: when an ambient
            // LatticeIdempotencyContext scope is active, the foreground
            // write below stamps versioned.Version with the supplied
            // key's HLC. A retry of the same logical operation observes
            // that HLC on the first GetWithVersionAsync read - which
            // means the prior attempt's per-replica advance already
            // landed and the retry must drop rather than advance the
            // counter a second time. Without the scope the check is a
            // no-op (versioned.Version is whatever the previous
            // unrelated write stamped) and the counter advances on
            // every call - which is the negative-control behaviour.
            if (idemKey is { } key
                && versioned.Version == key.Timestamp
                && versioned.Value is not null)
            {
                return;
            }

            var current = Decode(versioned.Value);
            var delta = mutate(current);
            var bytes = JsonLatticeSerializer<PnCounter>.Default.Serialize(current);
            var deltaBytes = JsonLatticeSerializer<TDelta>.Default.Serialize(delta);
            using (LatticeDeltaContext.With(deltaBytes))
            {
                var ok = await _lattice.SetIfVersionAsync(_key, bytes, versioned.Version, cancellationToken).ConfigureAwait(false);
                if (ok) return;
            }
        }
        throw new InvalidOperationException(
            $"PnCounter CAS budget exhausted after {maxAttempts} attempts for key '{_key}'. " +
            "Increase maxAttempts or reduce contention.");
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

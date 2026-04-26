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
    public Task IncrementAsync(string replicaId, long amount = 1, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return MutateAsync(c => c.Increment(replicaId, amount), cancellationToken, maxAttempts);
    }

    /// <summary>
    /// Increments the negative component for <paramref name="replicaId"/>
    /// by <paramref name="amount"/>. <paramref name="amount"/> must be non-negative.
    /// </summary>
    public Task DecrementAsync(string replicaId, long amount = 1, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        EnsureInitialised();
        return MutateAsync(c => c.Decrement(replicaId, amount), cancellationToken, maxAttempts);
    }

    /// <summary>Merges <paramref name="other"/> into the stored state under CAS.</summary>
    public Task MergeAsync(PnCounter other, CancellationToken cancellationToken = default, int maxAttempts = DefaultMaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(other);
        EnsureInitialised();
        return MutateAsync(c => c.MergeFrom(other), cancellationToken, maxAttempts);
    }

    private async Task MutateAsync(Action<PnCounter> mutate, CancellationToken cancellationToken, int maxAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var versioned = await _lattice.GetWithVersionAsync(_key, cancellationToken).ConfigureAwait(false);
            var current = Decode(versioned.Value);
            mutate(current);
            var bytes = JsonLatticeSerializer<PnCounter>.Default.Serialize(current);
            var ok = await _lattice.SetIfVersionAsync(_key, bytes, versioned.Version, cancellationToken).ConfigureAwait(false);
            if (ok) return;
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
